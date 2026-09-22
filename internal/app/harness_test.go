package app_test

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/observability"
)

var t0 = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeIDs returns random UUIDs, or uuid.Nil on the configured call numbers.
type fakeIDs struct {
	calls atomic.Int64
	nilAt atomic.Int64
}

func (f *fakeIDs) New() uuid.UUID {
	if f.calls.Add(1) == f.nilAt.Load() {
		return uuid.Nil
	}
	return uuid.New()
}

// failNextID makes the n-th next call return uuid.Nil.
func (f *fakeIDs) failNextID(n int64) { f.nilAt.Store(f.calls.Load() + n) }

type harness struct {
	store   *memStore
	clock   *fakeClock
	ids     *fakeIDs
	metrics *observability.Metrics
	logs    *bytes.Buffer
	wagers  *app.WagerService
	wallets *app.WalletService
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{store: newMemStore(), clock: &fakeClock{now: t0}, ids: &fakeIDs{}, logs: &bytes.Buffer{}}
	h.metrics = observability.NewMetrics(prometheus.NewRegistry())
	logger := slog.New(slog.NewJSONHandler(h.logs, nil))
	h.wagers = app.NewWagerService(app.WagerDeps{
		UoW: h.store, Queries: h.store, Clock: h.clock, IDs: h.ids, Metrics: h.metrics, Logger: logger,
		Policy: app.PendingPolicy{BaseDelay: time.Second, MaxDelay: time.Minute, MaxAttempts: 3, BatchSize: 10}, ConflictRetries: 2,
	})
	h.wallets = app.NewWalletService(app.WalletDeps{
		UoW: h.store, Queries: h.store, Clock: h.clock, IDs: h.ids, Metrics: h.metrics, Logger: logger,
	})
	return h
}

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	require.NoError(t, err)
	return m
}

func (h *harness) openWallet(t *testing.T, amount string) *wallet.Wallet {
	t.Helper()
	w, err := h.wallets.Open(context.Background(), app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(t, amount), CorrelationID: "open"})
	require.NoError(t, err)
	return w
}

// op describes a provider operation in tests.
type op struct {
	provider string
	ext      string
	key      string
	kind     string
	amount   string
	currency string
	ref      string
	round    string
}

func (h *harness) cmd(t *testing.T, w *wallet.Wallet, o op) app.SubmitCommand {
	t.Helper()
	cmd, err := app.NewSubmitCommand(input(w, o))
	require.NoError(t, err)
	return cmd
}

func input(w *wallet.Wallet, o op) app.SubmitInput {
	in := app.SubmitInput{
		ProviderID: "provider-a", ExternalTransactionID: o.ext, IdempotencyKey: o.key, PlayerID: w.PlayerID().String(),
		WalletID: w.ID().String(), RoundID: "round-1", GameID: "game-1", Kind: o.kind, Amount: o.amount, Currency: "BRL",
		ReferenceExternalTransactionID: o.ref, CorrelationID: "corr",
	}
	if o.provider != "" {
		in.ProviderID = o.provider
	}
	if o.key == "" {
		in.IdempotencyKey = in.ProviderID + ":" + o.ext
	}
	if o.currency != "" {
		in.Currency = o.currency
	}
	if o.round != "" {
		in.RoundID = o.round
	}
	return in
}

func (h *harness) submit(t *testing.T, w *wallet.Wallet, o op) app.SubmitResult {
	t.Helper()
	res, err := h.wagers.Submit(context.Background(), h.cmd(t, w, o))
	require.NoError(t, err)
	return res
}

func (h *harness) balance(t *testing.T, w *wallet.Wallet) string {
	t.Helper()
	got, err := h.wallets.Get(context.Background(), w.ID())
	require.NoError(t, err)
	return got.Balance().Amount()
}

func (h *harness) resolve(t *testing.T) int {
	t.Helper()
	n, err := h.wagers.ResolveDue(context.Background())
	require.NoError(t, err)
	return n
}
