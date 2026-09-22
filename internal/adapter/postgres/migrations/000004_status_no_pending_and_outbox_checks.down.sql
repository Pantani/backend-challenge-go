ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS outbox_events_lease_pair;
ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS outbox_events_single_outcome;
ALTER TABLE wager_transactions DROP CONSTRAINT IF EXISTS wager_transactions_status_check;
ALTER TABLE wager_transactions ADD CONSTRAINT wager_transactions_status_check CHECK (
    status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')
);
