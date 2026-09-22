-- Claims created before this migration have no acquisition identity. Release
-- them so every relay must acquire a verifiable token before mutating a row.
ALTER TABLE outbox_events ADD COLUMN claim_id UUID;
UPDATE outbox_events
SET locked_by = NULL, locked_until = NULL
WHERE locked_by IS NOT NULL;

ALTER TABLE outbox_events DROP CONSTRAINT outbox_events_lease_pair;
ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_lease_triplet CHECK (
    (locked_by IS NULL) = (locked_until IS NULL)
    AND (locked_by IS NULL) = (claim_id IS NULL)
);
