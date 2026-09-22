//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// inCancelledTx runs op inside a unit of work whose context is cancelled
// first, so the statement fails like it would on a dropped connection.
func inCancelledTx(t *testing.T, op func(ctx context.Context, r app.Repositories) error) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return postgres.NewUnitOfWork(pool).Do(ctx, func(ctx context.Context, r app.Repositories) error {
		cancel()
		return op(ctx, r)
	})
}

func TestRepositoriesFailOnBrokenConnections(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "10.00")
	tx, err := wager.NewOpening(wager.OpeningParams{ID: uuid.New(), WalletID: w.ID(), PlayerID: w.PlayerID(), Amount: brl(t, "1.00"), Now: time.Now()})
	require.NoError(t, err)
	ops := map[string]func(ctx context.Context, r app.Repositories) error{
		"wallet create": func(ctx context.Context, r app.Repositories) error { return r.Wallets().Create(ctx, w) },
		"wallet lock": func(ctx context.Context, r app.Repositories) error {
			_, err := r.Wallets().GetForUpdate(ctx, w.ID())
			return err
		},
		"wallet save": func(ctx context.Context, r app.Repositories) error { return r.Wallets().Save(ctx, w, 1) },
		"tx create":   func(ctx context.Context, r app.Repositories) error { return r.Transactions().Create(ctx, tx) },
		"tx save":     func(ctx context.Context, r app.Repositories) error { return r.Transactions().Save(ctx, tx) },
		"tx find": func(ctx context.Context, r app.Repositories) error {
			_, err := r.Transactions().FindExisting(ctx, "p", "k", "e")
			return err
		},
		"tx by external": func(ctx context.Context, r app.Repositories) error {
			_, err := r.Transactions().GetByExternal(ctx, "p", "e")
			return err
		},
		"tx reversed": func(ctx context.Context, r app.Repositories) error {
			_, err := r.Transactions().HasProcessedReversal(ctx, uuid.New())
			return err
		},
		"tx lock": func(ctx context.Context, r app.Repositories) error {
			_, err := r.Transactions().LockDuePending(ctx, uuid.New(), time.Now())
			return err
		},
		"tx wake": func(ctx context.Context, r app.Repositories) error {
			return r.Transactions().WakeDependents(ctx, "p", "e", time.Now())
		},
		"outbox": func(ctx context.Context, r app.Repositories) error {
			return r.Outbox().Append(ctx, event.Record{EventID: uuid.New()})
		},
		"inbox": func(ctx context.Context, r app.Repositories) error {
			_, _, err := r.Inbox().Register(ctx, "c", "m", "h", time.Now())
			return err
		},
		"complete": func(ctx context.Context, r app.Repositories) error {
			return r.Inbox().Complete(ctx, "c", "m", uuid.Nil, time.Now())
		},
	}
	for name, op := range ops {
		err := inCancelledTx(t, op)
		assert.Error(t, err, name)
		assert.True(t, app.IsTransient(err), "%s: %v", name, err)
	}
}

func TestQueriesFailOnBrokenConnections(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q, store := postgres.NewQueries(pool), postgres.NewOutboxStore(pool)
	calls := map[string]func() error{
		"wallet":    func() error { _, err := q.GetWallet(ctx, uuid.New()); return err },
		"ledger":    func() error { _, err := q.ListLedger(ctx, uuid.New(), 0, 1); return err },
		"tx":        func() error { _, err := q.GetTransaction(ctx, uuid.New()); return err },
		"tx ext":    func() error { _, err := q.GetTransactionByExternal(ctx, "p", "e"); return err },
		"reconcile": func() error { _, err := q.Reconcile(ctx, uuid.New()); return err },
		"due":       func() error { _, err := q.ListDuePending(ctx, time.Now(), 1); return err },
		"claim":     func() error { _, err := store.Claim(ctx, "o", time.Now(), time.Second, 1); return err },
		"published": func() error { _, err := store.MarkPublished(ctx, uuid.New(), "o", time.Now()); return err },
		"failed":    func() error { return store.MarkFailed(ctx, uuid.New(), "o", time.Now(), "x") },
		"oldest":    func() error { _, _, err := store.OldestPending(ctx); return err },
		"ping":      func() error { return postgres.Ping(ctx, pool) },
		"begin":     func() error { return postgres.NewUnitOfWork(pool).Do(ctx, nil) },
	}
	for name, call := range calls {
		assert.Error(t, call(), name)
	}
}

func TestClosedPoolIsUnavailable(t *testing.T) {
	t.Parallel()
	closed, err := postgres.NewPool(context.Background(), postgres.Config{URL: env.DatabaseURL, MaxConns: 1, LockTimeout: time.Second, StatementTimeout: time.Second})
	require.NoError(t, err)
	closed.Close()
	_, err = postgres.NewQueries(closed).GetWallet(context.Background(), uuid.New())
	require.ErrorIs(t, err, app.ErrUnavailable)
}

func TestNotFoundAndCommitFailures(t *testing.T) {
	t.Parallel()
	q := postgres.NewQueries(pool)
	ctx := context.Background()
	_, err := q.GetWallet(ctx, uuid.New())
	require.ErrorIs(t, err, app.ErrWalletNotFound)
	_, err = q.GetTransaction(ctx, uuid.New())
	require.ErrorIs(t, err, app.ErrTransactionNotFound)
	_, err = q.Reconcile(ctx, uuid.New())
	require.ErrorIs(t, err, app.ErrWalletNotFound)
	oldest, ok, err := postgres.NewOutboxStore(pool).OldestPending(ctx)
	require.NoError(t, err)
	assert.Equal(t, ok, !oldest.IsZero())

	// A balance change without its ledger entry is refused at COMMIT by the
	// deferred constraint trigger, whatever the application code does.
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "10.00")
	err = postgres.NewUnitOfWork(pool).Do(ctx, func(ctx context.Context, r app.Repositories) error {
		locked, err := r.Wallets().GetForUpdate(ctx, w.ID())
		require.NoError(t, err)
		_, err = locked.Apply(walletMovement(t))
		require.NoError(t, err)
		return r.Wallets().Save(ctx, locked, 1)
	})
	require.Error(t, err)
	assert.Equal(t, "10.00", s.balance(t, w))

	_, err = s.wallets.Open(ctx, app.OpenWalletCommand{PlayerID: w.PlayerID(), InitialBalance: brl(t, "1.00")})
	require.ErrorIs(t, err, app.ErrWalletExists)
}

func TestCorruptRowsAreReportedNotHidden(t *testing.T) {
	t.Parallel()
	w, tx := uuid.NewString(), uuid.NewString()
	// XYZ passes the schema's format check but is not a supported currency.
	require.Empty(t, sqlState(t,
		`INSERT INTO wallets VALUES ('`+w+`', gen_random_uuid(), 'XYZ', 100, 1, now(), now())`,
		`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
		provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, result_balance_minor,
		result_currency, created_at, updated_at)
		VALUES ('`+tx+`', 'EXTERNAL', 'WIN', 'PROCESSED', '`+w+`', gen_random_uuid(), 100, 'XYZ',
		'corrupt', '`+tx+`', '`+tx+`', 'h', 'r', 'g', 100, 'XYZ', now(), now())`,
		`INSERT INTO ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
		balance_before_minor, balance_after_minor, created_at) VALUES (gen_random_uuid(), '`+w+`', '`+tx+`', 'CREDIT', 100, 'XYZ', 0, 100, now())`))

	ctx := context.Background()
	q := postgres.NewQueries(pool)
	wid, tid := uuid.MustParse(w), uuid.MustParse(tx)
	_, err := q.GetWallet(ctx, wid)
	assert.Error(t, err)
	_, err = q.ListLedger(ctx, wid, 0, 10)
	assert.Error(t, err)
	_, err = q.GetTransaction(ctx, tid)
	assert.Error(t, err)
	_, err = q.Reconcile(ctx, wid)
	assert.Error(t, err)
	err = postgres.NewUnitOfWork(pool).Do(ctx, func(ctx context.Context, r app.Repositories) error {
		_, err := r.Transactions().FindExisting(ctx, "corrupt", tx, tx)
		return err
	})
	assert.Error(t, err)
}

func walletMovement(t *testing.T) wallet.Movement {
	t.Helper()
	return wallet.Movement{EntryID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Credit, Amount: brl(t, "1.00"), Now: time.Now()}
}
