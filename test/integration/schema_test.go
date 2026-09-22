//go:build integration

package integration_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
)

// sqlState runs statements in one transaction and returns the SQLSTATE of
// the failure ("" when everything, commit included, succeeded).
func sqlState(t *testing.T, statements ...string) string {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	for _, s := range statements {
		if _, err := tx.Exec(ctx, s); err != nil {
			return code(err)
		}
	}
	return code(tx.Commit(ctx))
}

func code(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// seededWallet inserts a wallet with an opening credit through SQL only.
func seededWallet(t *testing.T) (walletID, txID uuid.UUID) {
	t.Helper()
	walletID, txID = uuid.New(), uuid.New()
	w, tx := walletID.String(), txID.String()
	require.Empty(t, sqlState(t,
		`INSERT INTO wallets VALUES ('`+w+`', gen_random_uuid(), 'BRL', 10000, 1, now(), now())`,
		`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			result_balance_minor, result_currency, created_at, updated_at)
		 VALUES ('`+tx+`', 'INTERNAL', 'OPENING', 'PROCESSED', '`+w+`', gen_random_uuid(), 10000, 'BRL', 10000, 'BRL', now(), now())`,
		`INSERT INTO ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
			balance_before_minor, balance_after_minor, created_at)
		 VALUES (gen_random_uuid(), '`+w+`', '`+tx+`', 'CREDIT', 10000, 'BRL', 0, 10000, now())`,
	))
	return walletID, txID
}

func TestWalletConstraints(t *testing.T) {
	t.Parallel()
	w, _ := seededWallet(t)
	id := w.String()
	assert.Equal(t, "23514", sqlState(t, `UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = '`+id+`'`), "non-negative")
	assert.Equal(t, "23514", sqlState(t, `UPDATE wallets SET balance_minor = 1, version = version + 1 WHERE id = '`+id+`'`),
		"balance must match the ledger at commit")
	assert.Equal(t, "23514", sqlState(t, `UPDATE wallets SET version = version + 5 WHERE id = '`+id+`'`), "version moves with balance")
	assert.Equal(t, "23000", sqlState(t, `UPDATE wallets SET currency = 'USD' WHERE id = '`+id+`'`), "immutable identity")
	assert.Equal(t, "23000", sqlState(t, `DELETE FROM wallets WHERE id = '`+id+`'`))

	player := uuid.NewString()
	assert.Equal(t, "23505", sqlState(t,
		`INSERT INTO wallets VALUES (gen_random_uuid(), '`+player+`', 'BRL', 0, 1, now(), now())`,
		`INSERT INTO wallets VALUES (gen_random_uuid(), '`+player+`', 'BRL', 0, 1, now(), now())`), "one wallet per player and currency")
}

func TestLedgerIsAppendOnly(t *testing.T) {
	t.Parallel()
	w, tx := seededWallet(t)
	id := w.String()
	assert.Equal(t, "23000", sqlState(t, `UPDATE ledger_entries SET amount_minor = 1 WHERE wallet_id = '`+id+`'`))
	assert.Equal(t, "23000", sqlState(t, `DELETE FROM ledger_entries WHERE wallet_id = '`+id+`'`))
	assert.Equal(t, "23000", sqlState(t, `TRUNCATE ledger_entries CASCADE`))
	assert.Equal(t, "23514", sqlState(t, `INSERT INTO ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
		balance_before_minor, balance_after_minor, created_at)
		VALUES (gen_random_uuid(), '`+id+`', '`+tx.String()+`', 'DEBIT', 100, 'BRL', 10000, 9800, now())`), "balance math")
	assert.Equal(t, "23514", sqlState(t, `INSERT INTO ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
		balance_before_minor, balance_after_minor, created_at)
		VALUES (gen_random_uuid(), '`+id+`', gen_random_uuid(), 'DEBIT', 100, 'BRL', 5000, 4900, now())`), "chain from the last entry")
	assert.Equal(t, "23505", sqlState(t, `INSERT INTO ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
		balance_before_minor, balance_after_minor, created_at)
		VALUES (gen_random_uuid(), '`+id+`', '`+tx.String()+`', 'DEBIT', 100, 'BRL', 10000, 9900, now())`), "(walletId, transactionId) unique")
}

func TestLedgerEntryWithoutWalletUpdateFailsAtCommit(t *testing.T) {
	t.Parallel()
	w, _ := seededWallet(t)
	id := w.String()
	other := uuid.NewString()
	assert.Equal(t, "23514", sqlState(t,
		`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, result_balance_minor,
			result_currency, created_at, updated_at)
		 VALUES ('`+other+`', 'EXTERNAL', 'WIN', 'PROCESSED', '`+id+`', gen_random_uuid(), 500, 'BRL', 'p', '`+other+`',
			'`+other+`', 'h', 'r', 'g', 10500, 'BRL', now(), now())`,
		`INSERT INTO ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
			balance_before_minor, balance_after_minor, created_at)
		 VALUES (gen_random_uuid(), '`+id+`', '`+other+`', 'CREDIT', 500, 'BRL', 10000, 10500, now())`),
		"a chained entry that the wallet does not reflect is refused at commit")
}

func TestTransactionConstraints(t *testing.T) {
	t.Parallel()
	w, tx := seededWallet(t)
	insert := func(kind, status string, amount int, extra string) string {
		return `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			reference_external_transaction_id, failure_code, result_balance_minor, result_currency, next_attempt_at, created_at, updated_at)
			VALUES (gen_random_uuid(), 'EXTERNAL', '` + kind + `', '` + status + `', '` + w.String() + `', gen_random_uuid(), ` +
			strconv.Itoa(amount) + `, 'BRL', 'p', gen_random_uuid()::text, gen_random_uuid()::text, 'h', 'r', 'g', ` +
			extra + `, now(), now())`
	}
	cases := map[string]struct {
		sql  string
		want string
	}{
		"external OPENING":       {insert("OPENING", "PROCESSED", 1, "NULL, NULL, 1, 'BRL', NULL"), "23514"},
		"LOSS must be zero":      {insert("LOSS", "PROCESSED", 1, "NULL, NULL, 1, 'BRL', NULL"), "23514"},
		"BET must be positive":   {insert("BET", "PROCESSED", 0, "NULL, NULL, 1, 'BRL', NULL"), "23514"},
		"REFUND needs reference": {insert("REFUND", "PROCESSED", 1, "NULL, NULL, 1, 'BRL', NULL"), "23514"},
		"REJECTED needs code":    {insert("BET", "REJECTED", 1, "NULL, NULL, 1, 'BRL', NULL"), "23514"},
		"PROCESSED needs result": {insert("BET", "PROCESSED", 1, "NULL, NULL, NULL, NULL, NULL"), "23514"},
		"result needs currency":  {insert("BET", "PROCESSED", 1, "NULL, NULL, 1, NULL, NULL"), "23514"},
		"pending needs schedule": {insert("REFUND", "PENDING_REFERENCE", 1, "'x', NULL, NULL, NULL, NULL"), "23514"},
		"valid LOSS":             {insert("LOSS", "PROCESSED", 0, "NULL, NULL, 1, 'BRL', NULL"), ""},
	}
	for name, tc := range cases {
		assert.Equal(t, tc.want, sqlState(t, tc.sql), name)
	}
	assert.Equal(t, "23505", sqlState(t, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id,
		amount_minor, currency, result_balance_minor, result_currency, created_at, updated_at)
		VALUES (gen_random_uuid(), 'INTERNAL', 'OPENING', 'PROCESSED', '`+w.String()+`', gen_random_uuid(), 1, 'BRL', 1, 'BRL', now(), now())`),
		"a single opening credit per wallet")
	assert.Equal(t, "23000", sqlState(t, `UPDATE wager_transactions SET failure_code = 'X' WHERE id = '`+tx.String()+`'`), "terminal rows are frozen")
	assert.Equal(t, "23000", sqlState(t, `DELETE FROM wager_transactions WHERE id = '`+tx.String()+`'`))
}

func TestPendingRowsCanTransitionButNotChangeIdentity(t *testing.T) {
	t.Parallel()
	w, _ := seededWallet(t)
	id := uuid.NewString()
	require.Empty(t, sqlState(t, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
		provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, reference_external_transaction_id,
		attempts, next_attempt_at, created_at, updated_at)
		VALUES ('`+id+`', 'EXTERNAL', 'REFUND', 'PENDING_REFERENCE', '`+w.String()+`', gen_random_uuid(), 100, 'BRL',
		'p', '`+id+`', '`+id+`', 'h', 'r', 'g', 'missing', 1, now(), now(), now())`))
	assert.Empty(t, sqlState(t, `UPDATE wager_transactions SET attempts = 2 WHERE id = '`+id+`'`))
	assert.Equal(t, "23000", sqlState(t, `UPDATE wager_transactions SET amount_minor = 1 WHERE id = '`+id+`'`))
	assert.Equal(t, "23000", sqlState(t, `UPDATE wager_transactions SET payload_hash = 'other' WHERE id = '`+id+`'`))
}

func TestOutboxSnapshotIsImmutable(t *testing.T) {
	t.Parallel()
	id := uuid.NewString()
	require.Empty(t, sqlState(t, `INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, partition_key, event_type,
		payload, occurred_at, next_attempt_at) VALUES ('`+id+`', 'Wallet', gen_random_uuid(), 'k', 'T', '{"a":1}', now(), now())`))
	assert.Equal(t, "23000", sqlState(t, `UPDATE outbox_events SET payload = '{"a":2}' WHERE event_id = '`+id+`'`))
	assert.Equal(t, "23000", sqlState(t, `DELETE FROM outbox_events WHERE event_id = '`+id+`'`))
	assert.Empty(t, sqlState(t, `UPDATE outbox_events SET published_at = now() WHERE event_id = '`+id+`'`))
	assert.Equal(t, "23000", sqlState(t, `UPDATE outbox_events SET published_at = now() + interval '1 day' WHERE event_id = '`+id+`'`),
		"a publication cannot be rewritten")
}

func TestMigrationsApplyAndRevert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `CREATE DATABASE migrations_check`)
	require.NoError(t, err)
	url := strings.Replace(env.DatabaseURL, "/wallet?", "/migrations_check?", 1)

	m, err := postgres.NewMigrator(url)
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()
	v, dirty, err := m.Version()
	require.NoError(t, err)
	assert.Equal(t, uint(0), v)
	assert.False(t, dirty)

	require.NoError(t, m.Up())
	require.NoError(t, m.Up(), "no change is not an error")
	v, _, err = m.Version()
	require.NoError(t, err)
	assert.Equal(t, uint(2), v)
	assert.True(t, tableExists(t, url, "ledger_entries"))

	require.NoError(t, m.Down(1))
	v, _, err = m.Version()
	require.NoError(t, err)
	assert.Equal(t, uint(1), v)
	require.NoError(t, m.Down(5), "reverting more steps than exist stops at zero")
	assert.False(t, tableExists(t, url, "ledger_entries"))
	require.NoError(t, m.Up())
	assert.True(t, tableExists(t, url, "outbox_events"))

	_, err = postgres.NewMigrator("postgres://%%%")
	require.Error(t, err)
}

func tableExists(t *testing.T, url, table string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()
	var exists bool
	require.NoError(t, conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists))
	return exists
}
