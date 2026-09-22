//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/test/testenv"
)

// execStatements preserves both statement and deferred commit errors.
func execStatements(t *testing.T, statements ...string) error {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	for _, s := range statements {
		if _, err := tx.Exec(ctx, s); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func sqlState(err error) (string, error) {
	if err == nil {
		return "", nil
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return "", err
	}
	return pgErr.Code, nil
}

func rejectedState(t *testing.T, statements ...string) string {
	t.Helper()
	err := execStatements(t, statements...)
	require.Error(t, err, "SQL must fail")
	state, unclassified := sqlState(err)
	require.NoError(t, unclassified, "original SQL error: %v", err)
	require.NotEmpty(t, state, "original SQL error: %v", err)
	return state
}

func TestSQLStatePreservesNonPostgresErrors(t *testing.T) {
	sentinel := errors.New("broken connection")
	state, err := sqlState(sentinel)
	require.Empty(t, state)
	require.ErrorIs(t, err, sentinel)
	state, err = sqlState(nil)
	require.NoError(t, err)
	require.Empty(t, state)
	state, err = sqlState(fmt.Errorf("statement: %w", &pgconn.PgError{Code: "23514"}))
	require.NoError(t, err)
	require.Equal(t, "23514", state)
}

func TestGlobalProviderUniquenessAcrossWallets(t *testing.T) {
	t.Parallel()
	for _, sameKey := range []bool{true, false} {
		t.Run(fmt.Sprintf("same_key_%t", sameKey), func(t *testing.T) {
			assertGlobalUniqueness(t, sameKey)
		})
	}
}

func assertGlobalUniqueness(t *testing.T, sameKey bool) {
	t.Helper()
	s := newServices(t, defaultPolicy)
	wallets := []*wallet.Wallet{s.openWallet(t, "100.00"), s.openWallet(t, "100.00")}
	gate := &lookupGate{ready: make(chan struct{})}
	s.wagers.UoW = gatedUnitOfWork{UnitOfWork: s.wagers.UoW, gate: gate}
	commands := make([]app.SubmitCommand, len(wallets))
	for i, w := range wallets {
		in := s.input(w, "provider-a", "shared", "BET", "10.00", "")
		if sameKey {
			in.ExternalTransactionID += fmt.Sprint(i)
		} else {
			in.IdempotencyKey = uuid.NewString()
		}
		var err error
		commands[i], err = app.NewSubmitCommand(in)
		require.NoError(t, err)
	}
	type outcome struct {
		result app.SubmitResult
		err    error
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, err := testenv.Parallel(2, func(i int) (outcome, error) {
		res, err := s.wagers.Submit(ctx, commands[i])
		return outcome{res, err}, nil
	})
	require.NoError(t, err)
	expected := app.ErrDuplicateExternalID
	if sameKey {
		expected = app.ErrIdempotencyConflict
	}
	var success int
	for _, result := range results {
		if result.err != nil {
			require.ErrorIs(t, result.err, expected)
			continue
		}
		success++
		require.False(t, result.result.Replay, "different wallet payloads cannot replay")
	}
	require.Equal(t, 1, success)
	require.Equal(t, 1, s.debits(t, wallets[0])+s.debits(t, wallets[1]))
	var transactions int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND wallet_id = ANY($1)`, []uuid.UUID{wallets[0].ID(), wallets[1].ID()}).Scan(&transactions))
	require.Equal(t, 1, transactions)
	s.requireConsistent(t, wallets[0])
	s.requireConsistent(t, wallets[1])
}

// Both real PostgreSQL lookups finish before either transaction inserts. This
// forces the unique index race instead of relying on scheduler coincidence.
type lookupGate struct {
	arrived atomic.Int32
	ready   chan struct{}
}

type gatedUnitOfWork struct {
	app.UnitOfWork
	gate *lookupGate
}

func (u gatedUnitOfWork) Do(ctx context.Context, fn func(context.Context, app.Repositories) error) error {
	return u.UnitOfWork.Do(ctx, func(ctx context.Context, repositories app.Repositories) error {
		return fn(ctx, gatedRepositories{Repositories: repositories, gate: u.gate})
	})
}

type gatedRepositories struct {
	app.Repositories
	gate *lookupGate
}

func (r gatedRepositories) Transactions() app.TransactionRepository {
	return gatedTransactions{TransactionRepository: r.Repositories.Transactions(), gate: r.gate}
}

type gatedTransactions struct {
	app.TransactionRepository
	gate *lookupGate
}

func (r gatedTransactions) FindExisting(ctx context.Context, provider, key, external string) ([]*wager.Transaction, error) {
	rows, err := r.TransactionRepository.FindExisting(ctx, provider, key, external)
	if r.gate.arrived.Add(1) == 2 {
		close(r.gate.ready)
	}
	select {
	case <-r.gate.ready:
		return rows, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
	require.NoError(t, execStatements(t,
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
	assert.Equal(t, "23514", rejectedState(t, `UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = '`+id+`'`), "non-negative")
	assert.Equal(t, "23514", rejectedState(t, `UPDATE wallets SET balance_minor = 1, version = version + 1 WHERE id = '`+id+`'`),
		"balance must match the ledger at commit")
	assert.Equal(t, "23514", rejectedState(t, `UPDATE wallets SET version = version + 5 WHERE id = '`+id+`'`), "version moves with balance")
	assert.Equal(t, "23000", rejectedState(t, `UPDATE wallets SET currency = 'USD' WHERE id = '`+id+`'`), "immutable identity")
	assert.Equal(t, "23000", rejectedState(t, `DELETE FROM wallets WHERE id = '`+id+`'`))

	player := uuid.NewString()
	assert.Equal(t, "23505", rejectedState(t,
		`INSERT INTO wallets VALUES (gen_random_uuid(), '`+player+`', 'BRL', 0, 1, now(), now())`,
		`INSERT INTO wallets VALUES (gen_random_uuid(), '`+player+`', 'BRL', 0, 1, now(), now())`), "one wallet per player and currency")
	assert.Equal(t, "23514", rejectedState(t, `INSERT INTO wallets VALUES (gen_random_uuid(), gen_random_uuid(), 'BRL', 500, 1, now(), now())`),
		"a wallet inserted with a balance but no ledger entry is refused at commit")
}

func TestLedgerIsAppendOnly(t *testing.T) {
	t.Parallel()
	w, tx := seededWallet(t)
	id := w.String()
	assert.Equal(t, "23000", rejectedState(t, `UPDATE ledger_entries SET amount_minor = 1 WHERE wallet_id = '`+id+`'`))
	assert.Equal(t, "23000", rejectedState(t, `DELETE FROM ledger_entries WHERE wallet_id = '`+id+`'`))
	assert.Equal(t, "23000", rejectedState(t, `TRUNCATE ledger_entries CASCADE`))
	assert.Equal(t, "23514", rejectedState(t, insertLedgerEntry(id, tx.String(), "DEBIT", 100, 10000, 9800, "BRL")), "balance math")
	assert.Equal(t, "23514", rejectedState(t, insertLedgerEntry(id, uuid.NewString(), "DEBIT", 100, 5000, 4900, "BRL")), "chain from the last entry")
	assert.Equal(t, "23514", rejectedState(t, insertLedgerEntry(id, tx.String(), "DEBIT", 100, 10000, 9900, "USD")), "currency must match the wallet")
	assert.Equal(t, "23505", rejectedState(t, insertLedgerEntry(id, tx.String(), "DEBIT", 100, 10000, 9900, "BRL")), "(walletId, transactionId) unique")
}

func TestLedgerEntryWithoutWalletUpdateFailsAtCommit(t *testing.T) {
	t.Parallel()
	w, _ := seededWallet(t)
	id := w.String()
	other := uuid.NewString()
	assert.Equal(t, "23514", rejectedState(t,
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
		if tc.want == "" {
			require.NoError(t, execStatements(t, tc.sql), name)
			continue
		}
		assert.Equal(t, tc.want, rejectedState(t, tc.sql), name)
	}
	assert.Equal(t, "23505", rejectedState(t, insertTransaction(txRow{WalletID: w.String(), Kind: "OPENING", Status: "PROCESSED", Amount: 1, Result: ptr(1)})),
		"a single opening credit per wallet")
	assert.Equal(t, "23000", rejectedState(t, `UPDATE wager_transactions SET failure_code = 'X' WHERE id = '`+tx.String()+`'`), "terminal rows are frozen")
	assert.Equal(t, "23000", rejectedState(t, `DELETE FROM wager_transactions WHERE id = '`+tx.String()+`'`))
}

func TestPendingRowsCanTransitionButNotChangeIdentity(t *testing.T) {
	t.Parallel()
	w, _ := seededWallet(t)
	id := uuid.NewString()
	require.NoError(t, execStatements(t, insertTransaction(txRow{ID: id, WalletID: w.String(), Kind: "REFUND", Status: "PENDING_REFERENCE",
		Amount: 100, Provider: "p", Reference: ptr("missing"), Attempts: 1, NextAttempt: true})))
	require.NoError(t, execStatements(t, `UPDATE wager_transactions SET attempts = 2 WHERE id = '`+id+`'`))
	assert.Equal(t, "23000", rejectedState(t, `UPDATE wager_transactions SET amount_minor = 1 WHERE id = '`+id+`'`))
	assert.Equal(t, "23000", rejectedState(t, `UPDATE wager_transactions SET payload_hash = 'other' WHERE id = '`+id+`'`))
}

func TestSingleProcessedReversalPerTransaction(t *testing.T) {
	t.Parallel()
	w, _ := seededWallet(t)
	bet := uuid.NewString()
	refund := func() string {
		return insertTransaction(txRow{WalletID: w.String(), Kind: "REFUND", Status: "PROCESSED", Amount: 100, Provider: "p",
			Reference: ptr(bet), ReferenceTxID: bet, Result: ptr(10000)})
	}
	require.NoError(t, execStatements(t, insertTransaction(txRow{ID: bet, WalletID: w.String(), Kind: "BET", Status: "PROCESSED", Amount: 100,
		Provider: "p", Result: ptr(9900)})))
	require.NoError(t, execStatements(t, refund()))
	assert.Equal(t, "23505", rejectedState(t, refund()), "a transaction is successfully reversed at most once")
	require.NoError(t, execStatements(t, insertTransaction(txRow{WalletID: w.String(), Kind: "ROLLBACK", Status: "REJECTED", Amount: 100, Provider: "p",
		Reference: ptr(bet), ReferenceTxID: bet, Failure: "ALREADY_REVERSED", Result: ptr(10000)})), "rejected reversals may reference it again")
}

// Not parallel: running application fixtures lease and publish the shared outbox.
func TestOutboxOutcomeAndLeaseChecks(t *testing.T) {
	id := uuid.NewString()
	require.NoError(t, execStatements(t, `INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, partition_key, event_type,
		payload, occurred_at, next_attempt_at) VALUES ('`+id+`', 'Wallet', gen_random_uuid(), 'k', 'T', '{"a":1}', now(), now())`))
	where := ` WHERE event_id = '` + id + `'`
	assert.Equal(t, "23514", rejectedState(t, `UPDATE outbox_events SET published_at = now(), dead_lettered_at = now()`+where), "one outcome")
	assert.Equal(t, "23514", rejectedState(t, `UPDATE outbox_events SET locked_by = 'relay'`+where), "lease owner without expiry")
	assert.Equal(t, "23514", rejectedState(t, `UPDATE outbox_events SET locked_until = now()`+where), "lease expiry without owner")
	assert.Equal(t, "23514", rejectedState(t, `UPDATE outbox_events SET locked_by = 'relay', locked_until = now()`+where), "lease without claim token")
	require.NoError(t, execStatements(t, `UPDATE outbox_events SET locked_by = 'relay', locked_until = now(), claim_id = gen_random_uuid()`+where),
		"a whole claim triplet")
	require.NoError(t, execStatements(t, `UPDATE outbox_events SET dead_lettered_at = now()`+where))
}

// Not parallel: running application fixtures lease and publish the shared outbox.
func TestOutboxSnapshotIsImmutable(t *testing.T) {
	id := uuid.NewString()
	require.NoError(t, execStatements(t, `INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, partition_key, event_type,
		payload, occurred_at, next_attempt_at) VALUES ('`+id+`', 'Wallet', gen_random_uuid(), 'k', 'T', '{"a":1}', now(), now())`))
	assert.Equal(t, "23000", rejectedState(t, `UPDATE outbox_events SET payload = '{"a":2}' WHERE event_id = '`+id+`'`))
	assert.Equal(t, "23000", rejectedState(t, `DELETE FROM outbox_events WHERE event_id = '`+id+`'`))
	require.NoError(t, execStatements(t, `UPDATE outbox_events SET published_at = now() WHERE event_id = '`+id+`'`))
	assert.Equal(t, "23000", rejectedState(t, `UPDATE outbox_events SET published_at = now() + interval '1 day' WHERE event_id = '`+id+`'`),
		"a publication cannot be rewritten")
}

func TestMigrationsApplyAndRevert(t *testing.T) {
	t.Parallel()
	url := databaseForTest(t, "migrations_check")
	latest := latestMigration(t)

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
	assert.Equal(t, uint(latest), v)
	assert.True(t, tableExists(t, url, "ledger_entries"))

	require.Error(t, m.Down(0), "steps must be positive")
	require.NoError(t, m.Down(1))
	v, _, err = m.Version()
	require.NoError(t, err)
	assert.Equal(t, uint(latest-1), v)
	require.NoError(t, m.Down(latest+1), "reverting more steps than exist stops at zero")
	assert.False(t, tableExists(t, url, "ledger_entries"))
	tables, functions := publicObjects(t, url)
	assert.Equal(t, []string{"schema_migrations"}, tables, "a full revert leaves only the migration bookkeeping")
	assert.Empty(t, functions, "a full revert drops every trigger function")
	require.NoError(t, m.Up(), "the schema can be rebuilt after a full revert")
	v, _, err = m.Version()
	require.NoError(t, err)
	assert.Equal(t, uint(latest), v)
	assert.True(t, tableExists(t, url, "outbox_events"))

	_, err = postgres.NewMigrator("postgres://%%%")
	require.Error(t, err)
}

// Every boundary uses the migration filenames included by postgres' embed glob.
// A new migration needs an explicit data oracle, rather than silently passing.
func TestMigrationUpgradesPreserveRepresentativeData(t *testing.T) {
	t.Parallel()
	checks := map[int]func(context.Context, *testing.T, *pgx.Conn){
		2: verifyMigrationTwo,
		3: verifyMigrationThree,
		4: verifyMigrationFour,
		5: verifyMigrationFive,
	}
	for target := 2; target <= latestMigration(t); target++ {
		t.Run(fmt.Sprintf("v%d_to_v%d", target-1, target), func(t *testing.T) {
			check, ok := checks[target]
			require.True(t, ok, "add representative data and an oracle for migration %d", target)
			upgradeBoundary(t, target, check)
		})
	}
}

func TestMigrationFiveDownInvalidatesActiveClaims(t *testing.T) {
	t.Parallel()
	url := databaseForTest(t, "migration_five_down")
	m, err := postgres.NewMigrator(url)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	require.NoError(t, m.Up())

	withConn(t, url, func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `INSERT INTO outbox_events
			(event_id, aggregate_type, aggregate_id, partition_key, event_type, payload, occurred_at, next_attempt_at,
			 locked_by, locked_until, claim_id)
			VALUES (gen_random_uuid(), 'Wallet', gen_random_uuid(), 'active-v5', 'Upgrade', '{}', now(), now(),
			 'relay', now() + interval '1 hour', gen_random_uuid())`)
		require.NoError(t, err)
		require.NoError(t, m.Down(1))

		var owner *string
		var until *time.Time
		require.NoError(t, conn.QueryRow(ctx, `SELECT locked_by, locked_until FROM outbox_events
			WHERE partition_key = 'active-v5'`).Scan(&owner, &until))
		require.Nil(t, owner, "downgrade invalidates the unverifiable claim owner")
		require.Nil(t, until, "downgrade invalidates the unverifiable claim expiry")
		assertSchemaObjects(ctx, t, conn, nil, nil, []string{"outbox_events_lease_pair"})
		return nil
	})
}

func TestMigrationExplicitCommitSeparatesTransactions(t *testing.T) {
	t.Parallel()
	url := databaseForTest(t, "migration_commit")
	withConn(t, url, func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `CREATE TABLE migration_commit_probe (id INT)`)
		return err
	})
	source, err := iofs.New(fstest.MapFS{"1_commit.up.sql": &fstest.MapFile{Data: []byte(`
		BEGIN;
		INSERT INTO migration_commit_probe VALUES (1);
		COMMIT;
		SELECT 1 / 0;
	`)}}, ".")
	require.NoError(t, err)
	m, err := migrate.NewWithSourceInstance("iofs", source, "pgx5"+strings.TrimPrefix(url, "postgres"))
	if err != nil {
		require.NoError(t, errors.Join(err, source.Close()))
	}
	t.Cleanup(func() { srcErr, dbErr := m.Close(); require.NoError(t, errors.Join(srcErr, dbErr)) })
	requireMigrationSQLState(t, m.Up(), "22012")
	withConn(t, url, func(ctx context.Context, conn *pgx.Conn) error {
		var count int
		err := conn.QueryRow(ctx, `SELECT count(*) FROM migration_commit_probe`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count, "the first transaction commits before the later statement fails")
		return nil
	})
}

func TestMigrationStatementIsBounded(t *testing.T) {
	url := databaseForTest(t, "migration_timeout")
	source, err := iofs.New(fstest.MapFS{"1_slow.up.sql": &fstest.MapFile{Data: []byte("SELECT pg_sleep(6);")}}, ".")
	require.NoError(t, err)
	m, err := migrate.NewWithSourceInstance("iofs", source, "pgx5"+strings.TrimPrefix(url, "postgres"))
	if err != nil {
		require.NoError(t, errors.Join(err, source.Close()))
	}
	t.Cleanup(func() { srcErr, dbErr := m.Close(); require.NoError(t, errors.Join(srcErr, dbErr)) })
	err = m.Up()
	require.Error(t, err, "the database must cancel a migration statement before its six-second sleep completes")
	requireMigrationSQLState(t, err, "57014")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	config, err := pgx.ParseConfig(url)
	require.NoError(t, err)
	require.Equal(t, 3*time.Second, config.ConnectTimeout)
	var running int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
		WHERE datname = $1 AND state = 'active' AND query LIKE '%pg_sleep%'`, config.Database).Scan(&running))
	require.Zero(t, running, "no timed-out migration is left running in PostgreSQL")
}

func TestMigrationLockWaitIsBounded(t *testing.T) {
	url := databaseForTest(t, "migration_lock_timeout")
	source, err := iofs.New(fstest.MapFS{"1_locked.up.sql": &fstest.MapFile{
		Data: []byte("ALTER TABLE migration_lock_probe ADD COLUMN checked BOOLEAN;")}}, ".")
	require.NoError(t, err)
	m, err := migrate.NewWithSourceInstance("iofs", source, "pgx5"+strings.TrimPrefix(url, "postgres"))
	if err != nil {
		require.NoError(t, errors.Join(err, source.Close()))
	}
	t.Cleanup(func() { srcErr, dbErr := m.Close(); require.NoError(t, errors.Join(srcErr, dbErr)) })
	withConn(t, url, func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `CREATE TABLE migration_lock_probe (id INT)`)
		require.NoError(t, err)
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		_, err = tx.Exec(ctx, `LOCK TABLE migration_lock_probe IN ACCESS EXCLUSIVE MODE`)
		require.NoError(t, err)
		err = m.Up()
		require.Error(t, err)
		requireMigrationSQLState(t, err, "55P03")
		return nil
	})
}

func requireMigrationSQLState(t *testing.T, err error, expected string) {
	t.Helper()
	// migrate/database.Error exposes OrigErr but has no Unwrap method.
	var migrationError database.Error
	require.ErrorAs(t, err, &migrationError)
	state, unknown := sqlState(migrationError.OrigErr)
	require.NoError(t, unknown)
	require.Equal(t, expected, state, "original migration error: %v", err)
}

func upgradeBoundary(t *testing.T, target int, check func(context.Context, *testing.T, *pgx.Conn)) {
	t.Helper()
	url := databaseForTest(t, "upgrade")
	source, err := iofs.New(os.DirFS(filepath.Join("..", "..", "internal", "adapter", "postgres")), "migrations")
	require.NoError(t, err)
	m, err := migrate.NewWithSourceInstance("iofs", source, "pgx5"+strings.TrimPrefix(url, "postgres"))
	if err != nil {
		require.NoError(t, errors.Join(err, source.Close()))
	}
	t.Cleanup(func() { srcErr, dbErr := m.Close(); require.NoError(t, errors.Join(srcErr, dbErr)) })
	require.NoError(t, m.Migrate(uint(target-1)))
	withConn(t, url, func(ctx context.Context, conn *pgx.Conn) error {
		seedUpgradeRows(ctx, t, conn, target-1)
		before := migrationSnapshot(ctx, t, conn, target == 5)
		require.NoError(t, m.Migrate(uint(target)))
		version, dirty, err := m.Version()
		require.NoError(t, err)
		require.Equal(t, uint(target), version)
		require.False(t, dirty)
		require.Equal(t, before, migrationSnapshot(ctx, t, conn, target == 5), "existing financial and delivery data survives")
		verifyResultCurrencies(ctx, t, conn)
		check(ctx, t, conn)
		return nil
	})
}

func seedUpgradeRows(ctx context.Context, t *testing.T, conn *pgx.Conn, version int) {
	t.Helper()
	walletID, opening := uuid.NewString(), uuid.NewString()
	rows := []txRow{
		{ID: opening, WalletID: walletID, Kind: "OPENING", Status: "PROCESSED", Amount: 10000, Result: ptr(10000)},
		{WalletID: walletID, Kind: "BET", Status: "REJECTED", Amount: 100, Currency: "USD", Provider: "upgrade", Failure: "CURRENCY_MISMATCH", Result: ptr(10000), ResultCurrency: ptr("BRL")},
		{WalletID: walletID, Kind: "REFUND", Status: "PENDING_REFERENCE", Amount: 100, Provider: "upgrade", Reference: ptr("missing"), Attempts: 2, NextAttempt: true},
		{WalletID: walletID, Kind: "LOSS", Status: "FAILED", Provider: "upgrade", Failure: "REFERENCE_EXPIRED"},
	}
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `INSERT INTO wallets VALUES ($1, gen_random_uuid(), 'BRL', 10000, 1, now(), now())`, walletID)
	require.NoError(t, err)
	for _, row := range rows {
		statement := insertTransaction(row)
		if version == 1 {
			statement = legacyTransaction(row)
		}
		_, err = tx.Exec(ctx, statement)
		require.NoError(t, err)
	}
	_, err = tx.Exec(ctx, insertLedgerEntry(walletID, opening, "CREDIT", 10000, 0, 10000, "BRL"))
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	seedUpgradeOutbox(ctx, t, conn, version)
}

func legacyTransaction(row txRow) string {
	if row.Currency == "" {
		row.Currency = "BRL"
	}
	statement := strings.Replace(insertTransaction(row), "result_balance_minor, result_currency,", "result_balance_minor,", 1)
	balance := "NULL"
	if row.Result != nil {
		balance = strconv.Itoa(*row.Result)
	}
	// Only the result pair precedes attempts and the schedule in this fixture.
	old := row.resultColumns() + fmt.Sprintf(", %d,", row.Attempts)
	return strings.Replace(statement, old, balance+fmt.Sprintf(", %d,", row.Attempts), 1)
}

func seedUpgradeOutbox(ctx context.Context, t *testing.T, conn *pgx.Conn, version int) {
	t.Helper()
	for _, state := range []string{"unpublished", "published", "leased"} {
		_, err := conn.Exec(ctx, `INSERT INTO outbox_events
			(event_id, aggregate_type, aggregate_id, partition_key, event_type, payload, occurred_at, next_attempt_at,
			attempts, published_at, locked_by, locked_until) VALUES
			(gen_random_uuid(), 'Wallet', gen_random_uuid(), $1, 'Upgrade', '{"amount":"100.00"}', now(), now(), 2,
			CASE WHEN $1 = 'published' THEN now() END, CASE WHEN $1 = 'leased' THEN 'relay' END,
			CASE WHEN $1 = 'leased' THEN now() + interval '1 hour' END)`, state)
		require.NoError(t, err)
	}
	if version >= 3 {
		_, err := conn.Exec(ctx, `UPDATE outbox_events SET dead_lettered_at = now() WHERE partition_key = 'unpublished'`)
		require.NoError(t, err)
	}
}

func migrationSnapshot(ctx context.Context, t *testing.T, conn *pgx.Conn, ignoreClaims bool) map[string]string {
	t.Helper()
	snapshot := make(map[string]string)
	excluded := "'result_currency' - 'claim_id'"
	if ignoreClaims {
		// Version 5 deliberately invalidates unverifiable legacy leases.
		excluded += " - 'locked_by' - 'locked_until'"
	}
	for _, table := range []string{"wallets", "wager_transactions", "ledger_entries", "outbox_events"} {
		var data string
		// Added columns are checked independently; all original values must survive.
		query := `SELECT jsonb_agg(row ORDER BY row::text)::text FROM
			(SELECT (to_jsonb(t) - ` + excluded + `) || jsonb_build_object('dead_lettered_at', to_jsonb(t)->'dead_lettered_at') AS row FROM ` + table + ` t) q`
		require.NoError(t, conn.QueryRow(ctx, query).Scan(&data))
		snapshot[table] = data
	}
	return snapshot
}

func verifyResultCurrencies(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	t.Helper()
	var resultCurrencies []string
	rows, err := conn.Query(ctx, `SELECT result_currency FROM wager_transactions WHERE result_balance_minor IS NOT NULL ORDER BY kind`)
	require.NoError(t, err)
	resultCurrencies, err = pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.Equal(t, []string{"BRL", "BRL"}, resultCurrencies, "rejected USD wager observes its BRL wallet")
}

func verifyMigrationTwo(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	t.Helper()
	assertSchemaObjects(ctx, t, conn, []string{"outbox_events_partition_unpublished"},
		[]string{"ledger_entries_match_wallet", "wager_transactions_guard"}, []string{"wager_transactions_result_currency"})
	var walletID string
	require.NoError(t, conn.QueryRow(ctx, `SELECT id FROM wallets`).Scan(&walletID))
	id := uuid.NewString()
	assertConnSQLState(ctx, t, conn, "23514", insertTransaction(txRow{ID: id, WalletID: walletID, Kind: "WIN", Status: "PROCESSED", Amount: 100, Provider: "edge", Result: ptr(10100)}),
		insertLedgerEntry(walletID, id, "CREDIT", 100, 10000, 10100, "BRL"))
}

func verifyMigrationThree(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	t.Helper()
	assertSchemaObjects(ctx, t, conn, []string{"outbox_events_unpublished", "outbox_events_partition_unpublished"},
		[]string{"wager_transactions_guard"}, []string{"wager_transactions_reference_not_empty"})
	var predicates []string
	rows, err := conn.Query(ctx, `SELECT pg_get_expr(indpred, indrelid) FROM pg_index
		WHERE indexrelid IN ('outbox_events_unpublished'::regclass, 'outbox_events_partition_unpublished'::regclass)`)
	require.NoError(t, err)
	predicates, err = pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.Len(t, predicates, 2)
	for _, predicate := range predicates {
		require.Contains(t, predicate, "dead_lettered_at IS NULL")
	}
	var nulls int
	require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE dead_lettered_at IS NULL`).Scan(&nulls))
	require.Equal(t, 3, nulls)
	var walletID string
	require.NoError(t, conn.QueryRow(ctx, `SELECT id FROM wallets`).Scan(&walletID))
	assertConnSQLState(ctx, t, conn, "23514", insertTransaction(txRow{WalletID: walletID, Kind: "REFUND", Status: "PENDING_REFERENCE", Amount: 100,
		Provider: "edge", Reference: ptr(""), NextAttempt: true}))
}

func verifyMigrationFour(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	t.Helper()
	assertSchemaObjects(ctx, t, conn, []string{"outbox_events_unpublished"}, []string{"wager_transactions_guard"},
		[]string{"wager_transactions_status_check", "outbox_events_single_outcome", "outbox_events_lease_pair"})
	var dead int
	require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE dead_lettered_at IS NOT NULL`).Scan(&dead))
	require.Equal(t, 1, dead, "preexisting dead letter survives")
	assertConnSQLState(ctx, t, conn, "23514", `UPDATE outbox_events SET dead_lettered_at = now() WHERE partition_key = 'published'`)
	assertConnSQLState(ctx, t, conn, "23514", `UPDATE outbox_events SET locked_until = NULL WHERE partition_key = 'leased'`)
	assertConnSQLState(ctx, t, conn, "23514", `UPDATE wager_transactions SET status = 'PENDING' WHERE status = 'PENDING_REFERENCE'`)
}

func verifyMigrationFive(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	t.Helper()
	assertSchemaObjects(ctx, t, conn, nil, nil, []string{"outbox_events_lease_triplet"})
	var owner *string
	var until *time.Time
	var claimID *uuid.UUID
	require.NoError(t, conn.QueryRow(ctx, `SELECT locked_by, locked_until, claim_id FROM outbox_events
		WHERE partition_key = 'leased'`).Scan(&owner, &until, &claimID))
	require.Nil(t, owner, "upgrade invalidates the legacy claim owner")
	require.Nil(t, until, "upgrade invalidates the legacy claim expiry")
	require.Nil(t, claimID, "upgrade does not invent an unverifiable claim token")

	assertConnSQLState(ctx, t, conn, "23514", `UPDATE outbox_events
		SET locked_by = 'relay', locked_until = now() WHERE partition_key = 'leased'`)
	_, err := conn.Exec(ctx, `UPDATE outbox_events SET locked_by = 'relay', locked_until = now(), claim_id = gen_random_uuid()
		WHERE partition_key = 'leased'`)
	require.NoError(t, err, "a complete claim triplet is valid")
}

func assertSchemaObjects(ctx context.Context, t *testing.T, conn *pgx.Conn, indexes, triggers, constraints []string) {
	t.Helper()
	queries := []struct {
		names []string
		query string
	}{
		{indexes, `SELECT count(*) FROM pg_index WHERE indexrelid::regclass::text = ANY($1) AND indisvalid`},
		{triggers, `SELECT count(*) FROM pg_trigger WHERE tgname = ANY($1) AND tgenabled = 'O'`},
		{constraints, `SELECT count(*) FROM pg_constraint WHERE conname = ANY($1) AND convalidated`},
	}
	for _, query := range queries {
		var count int
		require.NoError(t, conn.QueryRow(ctx, query.query, query.names).Scan(&count))
		require.Equal(t, len(query.names), count, "named schema objects %v", query.names)
	}
}

func assertConnSQLState(ctx context.Context, t *testing.T, conn *pgx.Conn, expected string, statements ...string) {
	t.Helper()
	require.NotEmpty(t, expected)
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	for _, statement := range statements {
		_, err = tx.Exec(ctx, statement)
		if err != nil {
			break
		}
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	require.Error(t, err)
	state, unknown := sqlState(err)
	require.NoError(t, unknown)
	require.Equal(t, expected, state, "original error: %v", err)
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
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer closeCancel()
		require.NoError(t, conn.Close(closeCtx))
	}()
	require.NoError(t, fn(ctx, conn))
}
