DROP INDEX IF EXISTS outbox_events_partition_unpublished;
DROP INDEX IF EXISTS outbox_events_unpublished;
CREATE INDEX outbox_events_unpublished ON outbox_events (next_attempt_at, seq) WHERE published_at IS NULL;
CREATE INDEX outbox_events_partition_unpublished ON outbox_events (partition_key, seq) WHERE published_at IS NULL;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS dead_lettered_at;
ALTER TABLE wager_transactions DROP CONSTRAINT IF EXISTS wager_transactions_reference_not_empty;
