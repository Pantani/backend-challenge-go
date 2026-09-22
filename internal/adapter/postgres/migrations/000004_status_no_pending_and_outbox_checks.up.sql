-- PENDING is an in-memory state: a transaction is persisted only once it
-- concluded (PROCESSED, REJECTED, FAILED) or was deferred (PENDING_REFERENCE),
-- so a stored PENDING row could only come from a bug.
ALTER TABLE wager_transactions DROP CONSTRAINT wager_transactions_status_check;
ALTER TABLE wager_transactions ADD CONSTRAINT wager_transactions_status_check CHECK (
    status IN ('PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')
);

-- An outbox event has one outcome: published or dead-lettered, never both.
ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_single_outcome CHECK (
    published_at IS NULL OR dead_lettered_at IS NULL
);
-- A lease is an (owner, expiry) pair; half a lease would be unclaimable or
-- unexpirable.
ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_lease_pair CHECK (
    (locked_by IS NULL) = (locked_until IS NULL)
);
