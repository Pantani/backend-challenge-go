-- Removing the fencing token makes every active claim unverifiable. Release
-- all claims before restoring the legacy owner/expiry pair.
UPDATE outbox_events
SET locked_by = NULL, locked_until = NULL, claim_id = NULL
WHERE claim_id IS NOT NULL;

ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS outbox_events_lease_triplet;
ALTER TABLE outbox_events DROP COLUMN claim_id;
ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_lease_pair CHECK (
    (locked_by IS NULL) = (locked_until IS NULL)
);
