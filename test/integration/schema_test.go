//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
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

// txRow describes a wager_transactions row for insertTransaction. A Provider
// makes the row EXTERNAL (external id and idempotency key = ID, fixed hash,
// round and game); empty strings and nil pointers become NULL (a pointer to
// "" is an empty string); ResultCurrency defaults to Currency when a Result
// is set.
type txRow struct {
	ID, WalletID, Kind, Status, Currency, Provider string
	Amount, Attempts                               int
	Reference, ResultCurrency                      *string
	ReferenceTxID, Failure                         string
	Result                                         *int
	NextAttempt                                    bool
}

func ptr[T any](v T) *T { return &v }

// lit quotes a SQL literal, mapping "" to NULL.
func lit(s string) string {
	if s == "" {
		return "NULL"
	}
	return "'" + s + "'"
}

// litp quotes an optional SQL literal, mapping nil to NULL.
func litp(s *string) string {
	if s == nil {
		return "NULL"
	}
	return "'" + *s + "'"
}

func (r txRow) resultColumns() string {
	if r.Result == nil {
		return "NULL, NULL"
	}
	currency := r.Currency
	if r.ResultCurrency != nil {
		currency = *r.ResultCurrency
	}
	return strconv.Itoa(*r.Result) + ", " + lit(currency)
}

// insertTransaction builds the INSERT of one wager_transactions row.
func insertTransaction(r txRow) string {
	if r.ID == "" {
		r.ID = uuid.NewString()
	}
	if r.Currency == "" {
		r.Currency = "BRL"
	}
	origin, external, next := "INTERNAL", "NULL, NULL, NULL, NULL, NULL, NULL", "NULL"
	if r.Provider != "" {
		origin, external = "EXTERNAL", fmt.Sprintf("'%s', '%s', '%s', 'h', 'r', 'g'", r.Provider, r.ID, r.ID)
	}
	if r.NextAttempt {
		next = "now()"
	}
	return fmt.Sprintf(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
		provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
		reference_external_transaction_id, reference_transaction_id, failure_code, result_balance_minor, result_currency,
		attempts, next_attempt_at, created_at, updated_at)
		VALUES ('%s', '%s', '%s', '%s', '%s', gen_random_uuid(), %d, '%s', %s, %s, %s, %s, %s, %d, %s, now(), now())`,
		r.ID, origin, r.Kind, r.Status, r.WalletID, r.Amount, r.Currency, external,
		litp(r.Reference), lit(r.ReferenceTxID), lit(r.Failure), r.resultColumns(), r.Attempts, next)
}

// insertLedgerEntry builds the INSERT of one ledger_entries row.
func insertLedgerEntry(walletID, txID, direction string, amount, before, after int, currency string) string {
	return fmt.Sprintf(`INSERT INTO ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
		balance_before_minor, balance_after_minor, created_at)
		VALUES (gen_random_uuid(), '%s', '%s', '%s', %d, '%s', %d, %d, now())`, walletID, txID, direction, amount, currency, before, after)
}

// seededWallet inserts a wallet with an opening credit through SQL only.
func seededWallet(t *testing.T) (walletID, txID uuid.UUID) {
	t.Helper()
	walletID, txID = uuid.New(), uuid.New()
	w, tx := walletID.String(), txID.String()
	require.Empty(t, sqlState(t,
		`INSERT INTO wallets VALUES ('`+w+`', gen_random_uuid(), 'BRL', 10000, 1, now(), now())`,
		insertTransaction(txRow{ID: tx, WalletID: w, Kind: "OPENING", Status: "PROCESSED", Amount: 10000, Result: ptr(10000)}),
		insertLedgerEntry(w, tx, "CREDIT", 10000, 0, 10000, "BRL"),
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
	assert.Equal(t, "23514", sqlState(t, `INSERT INTO wallets VALUES (gen_random_uuid(), gen_random_uuid(), 'BRL', 500, 1, now(), now())`),
		"a wallet inserted with a balance but no ledger entry is refused at commit")
}

func TestLedgerIsAppendOnly(t *testing.T) {
	t.Parallel()
	w, tx := seededWallet(t)
	id := w.String()
	assert.Equal(t, "23000", sqlState(t, `UPDATE ledger_entries SET amount_minor = 1 WHERE wallet_id = '`+id+`'`))
	assert.Equal(t, "23000", sqlState(t, `DELETE FROM ledger_entries WHERE wallet_id = '`+id+`'`))
	assert.Equal(t, "23000", sqlState(t, `TRUNCATE ledger_entries CASCADE`))
	assert.Equal(t, "23514", sqlState(t, insertLedgerEntry(id, tx.String(), "DEBIT", 100, 10000, 9800, "BRL")), "balance math")
	assert.Equal(t, "23514", sqlState(t, insertLedgerEntry(id, uuid.NewString(), "DEBIT", 100, 5000, 4900, "BRL")), "chain from the last entry")
	assert.Equal(t, "23514", sqlState(t, insertLedgerEntry(id, tx.String(), "DEBIT", 100, 10000, 9900, "USD")), "currency must match the wallet")
	assert.Equal(t, "23505", sqlState(t, insertLedgerEntry(id, tx.String(), "DEBIT", 100, 10000, 9900, "BRL")), "(walletId, transactionId) unique")
}

func TestLedgerEntryWithoutWalletUpdateFailsAtCommit(t *testing.T) {
	t.Parallel()
	w, _ := seededWallet(t)
	id := w.String()
	other := uuid.NewString()
	assert.Equal(t, "23514", sqlState(t,
		insertTransaction(txRow{ID: other, WalletID: id, Kind: "WIN", Status: "PROCESSED", Amount: 500, Provider: "p", Result: ptr(10500)}),
		insertLedgerEntry(id, other, "CREDIT", 500, 10000, 10500, "BRL")),
		"a chained entry that the wallet does not reflect is refused at commit")
}

func TestTransactionConstraints(t *testing.T) {
	t.Parallel()
	w, tx := seededWallet(t)
	insert := func(r txRow) string {
		r.WalletID, r.Provider = w.String(), "p"
		return insertTransaction(r)
	}
	cases := map[string]struct {
		sql  string
		want string
	}{
		"external OPENING":       {insert(txRow{Kind: "OPENING", Status: "PROCESSED", Amount: 1, Result: ptr(1)}), "23514"},
		"LOSS must be zero":      {insert(txRow{Kind: "LOSS", Status: "PROCESSED", Amount: 1, Result: ptr(1)}), "23514"},
		"BET must be positive":   {insert(txRow{Kind: "BET", Status: "PROCESSED", Amount: 0, Result: ptr(1)}), "23514"},
		"REFUND needs reference": {insert(txRow{Kind: "REFUND", Status: "PROCESSED", Amount: 1, Result: ptr(1)}), "23514"},
		"REJECTED needs code":    {insert(txRow{Kind: "BET", Status: "REJECTED", Amount: 1, Result: ptr(1)}), "23514"},
		"PROCESSED needs result": {insert(txRow{Kind: "BET", Status: "PROCESSED", Amount: 1}), "23514"},
		"result needs currency":  {insert(txRow{Kind: "BET", Status: "PROCESSED", Amount: 1, Result: ptr(1), ResultCurrency: ptr("")}), "23514"},
		"pending needs schedule": {insert(txRow{Kind: "REFUND", Status: "PENDING_REFERENCE", Amount: 1, Reference: ptr("x")}), "23514"},
		"empty reference":        {insert(txRow{Kind: "REFUND", Status: "PROCESSED", Amount: 1, Reference: ptr(""), Result: ptr(1)}), "23514"},
		"PENDING is not stored":  {insert(txRow{Kind: "BET", Status: "PENDING", Amount: 1}), "23514"},
		"valid LOSS":             {insert(txRow{Kind: "LOSS", Status: "PROCESSED", Amount: 0, Result: ptr(1)}), ""},
	}
	for name, tc := range cases {
		assert.Equal(t, tc.want, sqlState(t, tc.sql), name)
	}
	assert.Equal(t, "23505", sqlState(t, insertTransaction(txRow{WalletID: w.String(), Kind: "OPENING", Status: "PROCESSED", Amount: 1, Result: ptr(1)})),
		"a single opening credit per wallet")
	assert.Equal(t, "23000", sqlState(t, `UPDATE wager_transactions SET failure_code = 'X' WHERE id = '`+tx.String()+`'`), "terminal rows are frozen")
	assert.Equal(t, "23000", sqlState(t, `DELETE FROM wager_transactions WHERE id = '`+tx.String()+`'`))
}

func TestPendingRowsCanTransitionButNotChangeIdentity(t *testing.T) {
	t.Parallel()
	w, _ := seededWallet(t)
	id := uuid.NewString()
	require.Empty(t, sqlState(t, insertTransaction(txRow{ID: id, WalletID: w.String(), Kind: "REFUND", Status: "PENDING_REFERENCE",
		Amount: 100, Provider: "p", Reference: ptr("missing"), Attempts: 1, NextAttempt: true})))
	assert.Empty(t, sqlState(t, `UPDATE wager_transactions SET attempts = 2 WHERE id = '`+id+`'`))
	assert.Equal(t, "23000", sqlState(t, `UPDATE wager_transactions SET amount_minor = 1 WHERE id = '`+id+`'`))
	assert.Equal(t, "23000", sqlState(t, `UPDATE wager_transactions SET payload_hash = 'other' WHERE id = '`+id+`'`))
}

func TestSingleProcessedReversalPerTransaction(t *testing.T) {
	t.Parallel()
	w, _ := seededWallet(t)
	bet := uuid.NewString()
	refund := func() string {
		return insertTransaction(txRow{WalletID: w.String(), Kind: "REFUND", Status: "PROCESSED", Amount: 100, Provider: "p",
			Reference: ptr(bet), ReferenceTxID: bet, Result: ptr(10000)})
	}
	require.Empty(t, sqlState(t, insertTransaction(txRow{ID: bet, WalletID: w.String(), Kind: "BET", Status: "PROCESSED", Amount: 100,
		Provider: "p", Result: ptr(9900)})))
	require.Empty(t, sqlState(t, refund()))
	assert.Equal(t, "23505", sqlState(t, refund()), "a transaction is successfully reversed at most once")
	assert.Empty(t, sqlState(t, insertTransaction(txRow{WalletID: w.String(), Kind: "ROLLBACK", Status: "REJECTED", Amount: 100, Provider: "p",
		Reference: ptr(bet), ReferenceTxID: bet, Failure: "ALREADY_REVERSED", Result: ptr(10000)})), "rejected reversals may reference it again")
}

func TestOutboxOutcomeAndLeaseChecks(t *testing.T) {
	t.Parallel()
	id := uuid.NewString()
	require.Empty(t, sqlState(t, `INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, partition_key, event_type,
		payload, occurred_at, next_attempt_at) VALUES ('`+id+`', 'Wallet', gen_random_uuid(), 'k', 'T', '{"a":1}', now(), now())`))
	where := ` WHERE event_id = '` + id + `'`
	assert.Equal(t, "23514", sqlState(t, `UPDATE outbox_events SET published_at = now(), dead_lettered_at = now()`+where), "one outcome")
	assert.Equal(t, "23514", sqlState(t, `UPDATE outbox_events SET locked_by = 'relay'`+where), "lease owner without expiry")
	assert.Equal(t, "23514", sqlState(t, `UPDATE outbox_events SET locked_until = now()`+where), "lease expiry without owner")
	assert.Empty(t, sqlState(t, `UPDATE outbox_events SET locked_by = 'relay', locked_until = now()`+where), "a whole lease")
	assert.Empty(t, sqlState(t, `UPDATE outbox_events SET dead_lettered_at = now()`+where))
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
	assert.Equal(t, uint(4), v)
	assert.True(t, tableExists(t, url, "ledger_entries"))

	require.Error(t, m.Down(0), "steps must be positive")
	require.NoError(t, m.Down(1))
	v, _, err = m.Version()
	require.NoError(t, err)
	assert.Equal(t, uint(3), v)
	require.NoError(t, m.Down(5), "reverting more steps than exist stops at zero")
	assert.False(t, tableExists(t, url, "ledger_entries"))
	tables, functions := publicObjects(t, url)
	assert.Equal(t, []string{"schema_migrations"}, tables, "a full revert leaves only the migration bookkeeping")
	assert.Empty(t, functions, "a full revert drops every trigger function")
	require.NoError(t, m.Up(), "the schema can be rebuilt after a full revert")
	v, _, err = m.Version()
	require.NoError(t, err)
	assert.Equal(t, uint(4), v)
	assert.True(t, tableExists(t, url, "outbox_events"))

	_, err = postgres.NewMigrator("postgres://%%%")
	require.Error(t, err)
}

func tableExists(t *testing.T, url, table string) bool {
	t.Helper()
	var exists bool
	withConn(t, url, func(ctx context.Context, conn *pgx.Conn) error {
		return conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists)
	})
	return exists
}

// publicObjects lists the tables and the functions of the public schema.
func publicObjects(t *testing.T, url string) (tables, functions []string) {
	t.Helper()
	withConn(t, url, func(ctx context.Context, conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, `SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY 1`)
		if err != nil {
			return err
		}
		if tables, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
			return err
		}
		rows, err = conn.Query(ctx, `SELECT proname FROM pg_proc WHERE pronamespace = 'public'::regnamespace ORDER BY 1`)
		if err != nil {
			return err
		}
		functions, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	return tables, functions
}

func withConn(t *testing.T, url string, fn func(ctx context.Context, conn *pgx.Conn) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()
	require.NoError(t, fn(ctx, conn))
}
