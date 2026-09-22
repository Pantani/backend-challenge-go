package postgres

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// Queries implements app.Queries on the pool (no locks).
type Queries struct {
	pool *pgxpool.Pool
}

// NewQueries builds the read side.
func NewQueries(pool *pgxpool.Pool) *Queries { return &Queries{pool: pool} }

// GetWallet implements app.Queries.
func (q *Queries) GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return getWallet(ctx, q.pool, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id)
}

// ListLedger implements app.Queries.
func (q *Queries) ListLedger(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]app.LedgerRow, error) {
	rows, err := q.pool.Query(ctx, `SELECT `+ledgerColumns+` FROM ledger_entries
		WHERE wallet_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`, walletID, afterSeq, limit)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	var out []app.LedgerRow
	for rows.Next() {
		row, err := scanLedgerRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, mapError(rows.Err())
}

// GetTransaction implements app.Queries.
func (q *Queries) GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	t, err := scanTransaction(q.pool.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrTransactionNotFound
	}
	return t, err
}

// GetTransactionByExternal implements app.Queries.
func (q *Queries) GetTransactionByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return getTransactionByExternal(ctx, q.pool, providerID, externalID)
}

// Reconcile reads the stored balance and the ledger sums in a single
// statement, i.e. in one consistent snapshot.
func (q *Queries) Reconcile(ctx context.Context, walletID uuid.UUID) (app.ReconciliationSnapshot, error) {
	var (
		snap     app.ReconciliationSnapshot
		stored   int64
		currency string
	)
	err := q.pool.QueryRow(ctx, `SELECT w.balance_minor, w.currency,
			COALESCE(SUM(l.amount_minor) FILTER (WHERE l.direction = 'CREDIT'), 0)::BIGINT,
			COALESCE(SUM(l.amount_minor) FILTER (WHERE l.direction = 'DEBIT'), 0)::BIGINT,
			COUNT(l.id)
		FROM wallets w LEFT JOIN ledger_entries l ON l.wallet_id = w.id
		WHERE w.id = $1 GROUP BY w.id`, walletID).Scan(&stored, &currency, &snap.Credits, &snap.Debits, &snap.Entries)
	if errors.Is(err, pgx.ErrNoRows) {
		return snap, app.ErrWalletNotFound
	}
	if err != nil {
		return snap, mapError(err)
	}
	snap.Stored, err = money.FromMinor(stored, money.Currency(currency))
	return snap, err
}

// ListDuePending implements app.Queries.
func (q *Queries) ListDuePending(ctx context.Context, now time.Time, limit int) ([]app.DueTransaction, error) {
	rows, err := q.pool.Query(ctx, `SELECT id, wallet_id FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= $1 ORDER BY next_attempt_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, mapError(err)
	}
	due, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (app.DueTransaction, error) {
		var d app.DueTransaction
		return d, row.Scan(&d.ID, &d.WalletID)
	})
	return due, mapError(err)
}

// OutboxStore implements app.OutboxStore.
type OutboxStore struct {
	pool *pgxpool.Pool
}

// NewOutboxStore builds the relay store.
func NewOutboxStore(pool *pgxpool.Pool) *OutboxStore { return &OutboxStore{pool: pool} }

// Claim leases, per partition (wallet), only the oldest unpublished record,
// and only while it is due and not leased. A later event of a wallet is never
// claimed before the earlier one is published, even when that one is failing
// with backoff or leased by another relay, so SQS FIFO receives each wallet's
// events in database order. FOR UPDATE SKIP LOCKED plus the conditions
// repeated on the locked row keep concurrent relays from claiming the same
// record.
func (s *OutboxStore) Claim(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]app.OutboxMessage, error) {
	rows, err := s.pool.Query(ctx, `WITH heads AS (
			SELECT DISTINCT ON (partition_key) event_id
			FROM outbox_events WHERE published_at IS NULL
			ORDER BY partition_key, seq)
		UPDATE outbox_events
		SET locked_by = $1, locked_until = $2::timestamptz + ($3::bigint * INTERVAL '1 millisecond'), attempts = attempts + 1
		WHERE event_id IN (
			SELECT o.event_id FROM outbox_events o JOIN heads h ON h.event_id = o.event_id
			WHERE o.published_at IS NULL AND o.next_attempt_at <= $2 AND (o.locked_until IS NULL OR o.locked_until < $2)
			ORDER BY o.seq LIMIT $4
			FOR UPDATE OF o SKIP LOCKED)
		RETURNING seq, event_id, event_type, aggregate_type, aggregate_id, partition_key, payload::text, occurred_at, attempts`,
		owner, now, lease.Milliseconds(), limit)
	if err != nil {
		return nil, mapError(err)
	}
	msgs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (app.OutboxMessage, error) {
		var m app.OutboxMessage
		var payload string
		err := row.Scan(&m.Seq, &m.EventID, &m.EventType, &m.AggregateType, &m.AggregateID, &m.PartitionKey, &payload, &m.OccurredAt, &m.Attempts)
		m.Payload = []byte(payload)
		return m, err
	})
	slices.SortFunc(msgs, func(a, b app.OutboxMessage) int { return int(a.Seq - b.Seq) })
	return msgs, mapError(err)
}

// MarkPublished implements app.OutboxStore.
func (s *OutboxStore) MarkPublished(ctx context.Context, eventID uuid.UUID, owner string, now time.Time) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE outbox_events SET published_at = $3, locked_by = NULL, locked_until = NULL, last_error = NULL
		WHERE event_id = $1 AND locked_by = $2 AND published_at IS NULL`, eventID, owner, now)
	return tag.RowsAffected() == 1, mapError(err)
}

// MarkFailed implements app.OutboxStore.
func (s *OutboxStore) MarkFailed(ctx context.Context, eventID uuid.UUID, owner string, next time.Time, cause string) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET locked_by = NULL, locked_until = NULL, next_attempt_at = $3, last_error = $4
		WHERE event_id = $1 AND locked_by = $2 AND published_at IS NULL`, eventID, owner, next, cause)
	return mapError(err)
}

// OldestPending implements app.OutboxStore.
func (s *OutboxStore) OldestPending(ctx context.Context) (time.Time, bool, error) {
	var oldest *time.Time
	err := s.pool.QueryRow(ctx, `SELECT MIN(occurred_at) FROM outbox_events WHERE published_at IS NULL`).Scan(&oldest)
	if err != nil || oldest == nil {
		return time.Time{}, false, mapError(err)
	}
	return *oldest, true, nil
}
