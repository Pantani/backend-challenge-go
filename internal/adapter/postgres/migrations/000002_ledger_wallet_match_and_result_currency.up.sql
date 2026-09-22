-- A ledger entry inserted without the matching wallet update must also fail
-- at COMMIT: the deferred check now runs after ledger inserts too, so the
-- stored balance always equals the last ledger balance.
CREATE FUNCTION ledger_entries_match_wallet() RETURNS trigger
    LANGUAGE plpgsql AS
$$
DECLARE
    stored BIGINT;
    last_after BIGINT;
BEGIN
    SELECT balance_minor INTO stored FROM wallets WHERE id = NEW.wallet_id;
    SELECT balance_after_minor INTO last_after FROM ledger_entries
    WHERE wallet_id = NEW.wallet_id ORDER BY seq DESC LIMIT 1;
    IF stored IS DISTINCT FROM last_after THEN
        RAISE EXCEPTION 'ledger of wallet % ends at % but the stored balance is %', NEW.wallet_id, last_after, stored
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER ledger_entries_match_wallet
    AFTER INSERT ON ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_entries_match_wallet();

-- The balance observed when an operation concluded keeps its own currency:
-- a CURRENCY_MISMATCH rejection observes the wallet (e.g. BRL) balance while
-- the operation amount is in another currency (e.g. USD).
ALTER TABLE wager_transactions ADD COLUMN result_currency CHAR(3);

ALTER TABLE wager_transactions DISABLE TRIGGER wager_transactions_guard;
UPDATE wager_transactions w SET result_currency = wl.currency
FROM wallets wl WHERE wl.id = w.wallet_id AND w.result_balance_minor IS NOT NULL;
ALTER TABLE wager_transactions ENABLE TRIGGER wager_transactions_guard;

ALTER TABLE wager_transactions ADD CONSTRAINT wager_transactions_result_currency CHECK (
    (result_balance_minor IS NULL) = (result_currency IS NULL)
    AND (result_currency IS NULL OR result_currency ~ '^[A-Z]{3}$')
);

-- Supports the per-partition head lookup of the outbox relay.
CREATE INDEX outbox_events_partition_unpublished ON outbox_events (partition_key, seq) WHERE published_at IS NULL;
