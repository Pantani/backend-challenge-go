-- An empty reference is never valid (the application stores NULL instead).
ALTER TABLE wager_transactions ADD CONSTRAINT wager_transactions_reference_not_empty CHECK (
    reference_external_transaction_id IS NULL OR reference_external_transaction_id <> ''
);

-- Poison outbox events: after OUTBOX_MAX_ATTEMPTS failed publications the
-- event is dead-lettered (kept for audit and manual replay) so it stops
-- blocking the later events of its wallet.
ALTER TABLE outbox_events ADD COLUMN dead_lettered_at TIMESTAMPTZ;

DROP INDEX outbox_events_unpublished;
DROP INDEX outbox_events_partition_unpublished;
CREATE INDEX outbox_events_unpublished ON outbox_events (next_attempt_at, seq)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;
CREATE INDEX outbox_events_partition_unpublished ON outbox_events (partition_key, seq)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;
