package postgres

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/event"
)

type outboxRepo struct{ db dbtx }

// Append stores the event snapshots; they become visible to publishers only
// when the surrounding transaction commits.
func (r outboxRepo) Append(ctx context.Context, records ...event.Record) error {
	for _, rec := range records {
		_, err := r.db.Exec(ctx, `INSERT INTO outbox_events
			(event_id, aggregate_type, aggregate_id, partition_key, event_type, payload, occurred_at, next_attempt_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
			rec.EventID, rec.AggregateType, rec.AggregateID, rec.PartitionKey, rec.EventType, string(rec.Payload), rec.OccurredAt)
		if err != nil {
			return mapError(err)
		}
	}
	return nil
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
// record. The lease expires at now+lease, computed here so the clock of the
// relay, not the database, decides.
func (s *OutboxStore) Claim(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]app.OutboxMessage, error) {
	msgs, err := collect(ctx, s.pool, scanOutboxMessage, `WITH heads AS (
			SELECT DISTINCT ON (partition_key) event_id
			FROM outbox_events WHERE published_at IS NULL AND dead_lettered_at IS NULL
			ORDER BY partition_key, seq)
		UPDATE outbox_events
		SET locked_by = $1, locked_until = $3, attempts = attempts + 1
		WHERE event_id IN (
			SELECT o.event_id FROM outbox_events o JOIN heads h ON h.event_id = o.event_id
			WHERE o.published_at IS NULL AND o.dead_lettered_at IS NULL AND o.next_attempt_at <= $2 AND (o.locked_until IS NULL OR o.locked_until < $2)
			ORDER BY o.seq LIMIT $4
			FOR UPDATE OF o SKIP LOCKED)
		RETURNING seq, event_id, event_type, aggregate_type, aggregate_id, partition_key, payload::text, occurred_at, attempts`,
		owner, now, now.Add(lease), limit)
	if err != nil {
		return nil, err
	}
	// UPDATE ... RETURNING does not preserve the order of the subquery.
	slices.SortFunc(msgs, func(a, b app.OutboxMessage) int { return cmp.Compare(a.Seq, b.Seq) })
	return msgs, nil
}

func scanOutboxMessage(row pgx.CollectableRow) (app.OutboxMessage, error) {
	var m app.OutboxMessage
	var payload string
	err := row.Scan(&m.Seq, &m.EventID, &m.EventType, &m.AggregateType, &m.AggregateID, &m.PartitionKey, &payload, &m.OccurredAt, &m.Attempts)
	m.Payload = []byte(payload)
	return m, err
}

// MarkPublished implements app.OutboxStore. It reports false when the lease
// is no longer held by owner (another relay took over after it expired).
func (s *OutboxStore) MarkPublished(ctx context.Context, eventID uuid.UUID, owner string, now time.Time) (bool, error) {
	return s.release(ctx, `published_at = $3, last_error = NULL`, eventID, owner, now)
}

// MarkFailed implements app.OutboxStore.
func (s *OutboxStore) MarkFailed(ctx context.Context, eventID uuid.UUID, owner string, next time.Time, cause string) error {
	_, err := s.release(ctx, `next_attempt_at = $3, last_error = $4`, eventID, owner, next, cause)
	return err
}

// MarkDead implements app.OutboxStore.
func (s *OutboxStore) MarkDead(ctx context.Context, eventID uuid.UUID, owner string, now time.Time, cause string) error {
	_, err := s.release(ctx, `dead_lettered_at = $3, last_error = $4`, eventID, owner, now, cause)
	return err
}

// release drops the lease of owner on the unpublished event while applying
// set (a SET clause whose placeholders start at $3, bound to args). The
// ownership guard is what makes a stale relay, whose lease expired and was
// taken over, unable to touch the record: it reports false instead.
func (s *OutboxStore) release(ctx context.Context, set string, eventID uuid.UUID, owner string, args ...any) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE outbox_events SET locked_by = NULL, locked_until = NULL, `+set+`
		WHERE event_id = $1 AND locked_by = $2 AND published_at IS NULL`, append([]any{eventID, owner}, args...)...)
	if err != nil {
		return false, mapError(err)
	}
	return tag.RowsAffected() == 1, nil
}

// OldestPending implements app.OutboxStore.
func (s *OutboxStore) OldestPending(ctx context.Context) (time.Time, bool, error) {
	var oldest *time.Time
	err := s.pool.QueryRow(ctx, `SELECT MIN(occurred_at) FROM outbox_events WHERE published_at IS NULL AND dead_lettered_at IS NULL`).Scan(&oldest)
	if err != nil || oldest == nil {
		return time.Time{}, false, mapError(err)
	}
	return *oldest, true, nil
}
