DROP INDEX IF EXISTS outbox_events_partition_unpublished;
ALTER TABLE wager_transactions DROP CONSTRAINT IF EXISTS wager_transactions_result_currency;
ALTER TABLE wager_transactions DROP COLUMN IF EXISTS result_currency;
DROP TRIGGER IF EXISTS ledger_entries_match_wallet ON ledger_entries;
DROP FUNCTION IF EXISTS ledger_entries_match_wallet();
