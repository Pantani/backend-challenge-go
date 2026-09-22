package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

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

type inboxRepo struct{ db dbtx }

// Register inserts the message or, on redelivery, returns the stored entry.
// A concurrent delivery of the same message blocks on the unique key until
// the first one commits or rolls back.
func (r inboxRepo) Register(ctx context.Context, consumer, messageID, hash string, now time.Time) (app.InboxEntry, bool, error) {
	var inserted bool
	err := r.db.QueryRow(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING RETURNING true`, consumer, messageID, hash, now).Scan(&inserted)
	if err == nil {
		return app.InboxEntry{PayloadHash: hash}, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return app.InboxEntry{}, false, mapError(err)
	}
	var entry app.InboxEntry
	err = r.db.QueryRow(ctx, `SELECT payload_hash, processed_at IS NOT NULL FROM inbox_messages
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).Scan(&entry.PayloadHash, &entry.Completed)
	return entry, false, mapError(err)
}

func (r inboxRepo) Complete(ctx context.Context, consumer, messageID string, transactionID uuid.UUID, now time.Time) error {
	_, err := r.db.Exec(ctx, `UPDATE inbox_messages SET processed_at = $4, transaction_id = $3
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID, nullUUID(transactionID), now)
	return mapError(err)
}
