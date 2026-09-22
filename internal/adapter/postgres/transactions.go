package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

const transactionColumns = `id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code, result_balance_minor,
	result_currency, attempts, next_attempt_at, correlation_id, created_at, updated_at`

type transactionRepo struct{ db dbtx }

func (r transactionRepo) Create(ctx context.Context, t *wager.Transaction) error {
	ext := t.External()
	_, err := r.db.Exec(ctx, `INSERT INTO wager_transactions (`+transactionColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)`,
		t.ID(), string(t.Origin()), string(t.Kind()), string(t.Status()), t.WalletID(), t.PlayerID(),
		t.Amount().Minor(), string(t.Amount().Currency()),
		nullString(ext.ProviderID), nullString(ext.ExternalID), nullString(ext.IdempotencyKey), nullString(ext.PayloadHash),
		nullString(ext.RoundID), nullString(ext.GameID), nullString(ext.ReferenceExternalID),
		nullUUID(t.ReferenceTxID()), nullString(string(t.FailureCode())), resultBalance(t), resultCurrency(t),
		t.Attempts(), nullTime(t.NextAttemptAt()), t.CorrelationID(), t.CreatedAt(), t.UpdatedAt())
	return mapError(err)
}

// Save persists a transition. The guard trigger refuses to touch terminal
// rows or immutable columns, so a bug cannot rewrite history.
func (r transactionRepo) Save(ctx context.Context, t *wager.Transaction) error {
	_, err := r.db.Exec(ctx, `UPDATE wager_transactions
		SET status = $2, reference_transaction_id = $3, failure_code = $4, result_balance_minor = $5,
		    result_currency = $6, attempts = $7, next_attempt_at = $8, updated_at = $9
		WHERE id = $1`,
		t.ID(), string(t.Status()), nullUUID(t.ReferenceTxID()), nullString(string(t.FailureCode())),
		resultBalance(t), resultCurrency(t), t.Attempts(), nullTime(t.NextAttemptAt()), t.UpdatedAt())
	return mapError(err)
}

func (r transactionRepo) FindExisting(ctx context.Context, providerID, key, externalID string) ([]*wager.Transaction, error) {
	return queryTransactions(ctx, r.db, `SELECT `+transactionColumns+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND (idempotency_key = $2 OR external_transaction_id = $3)`,
		providerID, key, externalID)
}

func (r transactionRepo) GetByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return getTransactionByExternal(ctx, r.db, providerID, externalID)
}

func (r transactionRepo) HasProcessedReversal(ctx context.Context, referenceTxID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions
		WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK'))`,
		referenceTxID).Scan(&exists)
	return exists, mapError(err)
}

func (r transactionRepo) LockDuePending(ctx context.Context, id uuid.UUID, now time.Time) (*wager.Transaction, error) {
	t, err := scanTransaction(r.db.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions
		WHERE id = $1 AND status = 'PENDING_REFERENCE' AND next_attempt_at <= $2
		FOR UPDATE SKIP LOCKED`, id, now))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrNotDue
	}
	return t, err
}

// WakeDependents skips rows another instance is resolving right now; they
// are rescheduled by that instance anyway.
func (r transactionRepo) WakeDependents(ctx context.Context, providerID, externalID string, now time.Time) error {
	_, err := r.db.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at = $3
		WHERE id IN (
			SELECT id FROM wager_transactions
			WHERE status = 'PENDING_REFERENCE' AND provider_id = $1
			  AND reference_external_transaction_id = $2 AND next_attempt_at > $3
			FOR UPDATE SKIP LOCKED)`, providerID, externalID, now)
	return mapError(err)
}

func getTransactionByExternal(ctx context.Context, db dbtx, providerID, externalID string) (*wager.Transaction, error) {
	t, err := scanTransaction(db.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrTransactionNotFound
	}
	return t, err
}

func queryTransactions(ctx context.Context, db dbtx, query string, args ...any) ([]*wager.Transaction, error) {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()
	var out []*wager.Transaction
	for rows.Next() {
		t, err := scanTransaction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, mapError(rows.Err())
}

// transactionRow mirrors the nullable columns of wager_transactions.
type transactionRow struct {
	s                                          wager.Snapshot
	origin, kind, status, currency             string
	amount                                     int64
	provider, external, key, hash, round, game *string
	refExternal, failure, resultCurrency       *string
	refID                                      *uuid.UUID
	result                                     *int64
	next                                       *time.Time
}

func scanTransaction(row pgx.Row) (*wager.Transaction, error) {
	var r transactionRow
	err := row.Scan(&r.s.ID, &r.origin, &r.kind, &r.status, &r.s.WalletID, &r.s.PlayerID, &r.amount, &r.currency,
		&r.provider, &r.external, &r.key, &r.hash, &r.round, &r.game, &r.refExternal, &r.refID, &r.failure, &r.result,
		&r.resultCurrency, &r.s.Attempts, &r.next, &r.s.CorrelationID, &r.s.CreatedAt, &r.s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if err != nil {
		return nil, mapError(err)
	}
	return r.toDomain()
}

func (r transactionRow) toDomain() (*wager.Transaction, error) {
	c := money.Currency(r.currency)
	amount, err := money.FromMinor(r.amount, c)
	if err != nil {
		return nil, err
	}
	s := r.s
	s.Origin, s.Kind, s.Status, s.Amount = wager.Origin(r.origin), wager.Kind(r.kind), wager.Status(r.status), amount
	s.External = wager.External{
		ProviderID: deref(r.provider), ExternalID: deref(r.external), IdempotencyKey: deref(r.key),
		PayloadHash: deref(r.hash), RoundID: deref(r.round), GameID: deref(r.game), ReferenceExternalID: deref(r.refExternal),
	}
	s.FailureCode = wager.FailureCode(deref(r.failure))
	s.ReferenceTxID = derefUUID(r.refID)
	s.NextAttemptAt = derefTime(r.next)
	if r.result != nil {
		s.ResultBalance, _ = money.FromMinor(*r.result, money.Currency(deref(r.resultCurrency)))
	}
	return wager.Rehydrate(s)
}

func resultBalance(t *wager.Transaction) *int64 {
	if t.ResultBalance().Validate() != nil {
		return nil
	}
	v := t.ResultBalance().Minor()
	return &v
}

func resultCurrency(t *wager.Transaction) *string {
	if t.ResultBalance().Validate() != nil {
		return nil
	}
	return nullString(string(t.ResultBalance().Currency()))
}

func nullUUID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

func derefUUID(id *uuid.UUID) uuid.UUID {
	if id == nil {
		return uuid.Nil
	}
	return *id
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
