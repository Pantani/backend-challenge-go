//go:build integration

// Package integration_test runs the adapters against real PostgreSQL,
// LocalStack SQS and Keycloak containers.
package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/test/testenv"
)

var (
	env  *testenv.Env
	pool *pgxpool.Pool
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	env, err = testenv.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start containers:", err)
		os.Exit(1)
	}
	code := runWithDatabase(ctx, m)
	env.Stop(ctx)
	os.Exit(code)
}

func runWithDatabase(ctx context.Context, m *testing.M) int {
	migrator, err := postgres.NewMigrator(env.DatabaseURL)
	if err == nil {
		err = migrator.Up()
	}
	if err == nil {
		pool, err = postgres.NewPool(ctx, postgres.Config{URL: env.DatabaseURL, MaxConns: 60, LockTimeout: 5 * time.Second, StatementTimeout: 10 * time.Second})
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "database:", err)
		return 1
	}
	defer pool.Close()
	return m.Run()
}

// syncBuffer is a log sink safe for concurrent writers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// services wires the use cases on the real database.
type services struct {
	wagers  *app.WagerService
	wallets *app.WalletService
	metrics *observability.Metrics
	logs    *syncBuffer
	// prefix keeps external ids unique across tests sharing the database.
	prefix string
}

func newServices(t *testing.T, policy app.PendingPolicy) services {
	t.Helper()
	logs := &syncBuffer{}
	logger := observability.NewLogger(logs, "debug", "it")
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	uow, queries := postgres.NewUnitOfWork(pool), postgres.NewQueries(pool)
	return services{
		wagers: app.NewWagerService(app.WagerDeps{UoW: uow, Queries: queries, Clock: app.SystemClock{}, IDs: app.UUIDv7{},
			Metrics: metrics, Logger: logger, Policy: policy, ConflictRetries: 10}),
		wallets: app.NewWalletService(app.WalletDeps{UoW: uow, Queries: queries, Clock: app.SystemClock{}, IDs: app.UUIDv7{},
			Metrics: metrics, Logger: logger}),
		metrics: metrics, logs: logs, prefix: uuid.NewString()[:8] + "-",
	}
}

var defaultPolicy = app.PendingPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, MaxAttempts: 5, BatchSize: 50}

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	require.NoError(t, err)
	return m
}

func (s services) openWallet(t *testing.T, amount string) *wallet.Wallet {
	t.Helper()
	w, err := s.wallets.Open(context.Background(), app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(t, amount)})
	require.NoError(t, err)
	return w
}

// input prefixes the external ids so tests never collide.
func (s services) input(w *wallet.Wallet, provider, ext, kind, amount, ref string) app.SubmitInput {
	if ref != "" {
		ref = s.prefix + ref
	}
	return submitInput(w, provider, s.prefix+ext, kind, amount, ref)
}

func submitInput(w *wallet.Wallet, provider, ext, kind, amount, ref string) app.SubmitInput {
	return app.SubmitInput{
		ProviderID: provider, ExternalTransactionID: ext, IdempotencyKey: provider + ":" + ext,
		PlayerID: w.PlayerID().String(), WalletID: w.ID().String(), RoundID: "round-1", GameID: "game-1",
		Kind: kind, Amount: amount, Currency: "BRL", ReferenceExternalTransactionID: ref,
	}
}

func (s services) submit(t *testing.T, w *wallet.Wallet, ext, kind, amount, ref string) app.SubmitResult {
	t.Helper()
	cmd, err := app.NewSubmitCommand(s.input(w, "provider-a", ext, kind, amount, ref))
	require.NoError(t, err)
	res, err := s.wagers.Submit(context.Background(), cmd)
	require.NoError(t, err)
	return res
}

func (s services) balance(t *testing.T, w *wallet.Wallet) string {
	t.Helper()
	got, err := s.wallets.Get(context.Background(), w.ID())
	require.NoError(t, err)
	return got.Balance().Amount()
}

// requireConsistent checks stored balance == credits - debits of the ledger.
func (s services) requireConsistent(t *testing.T, w *wallet.Wallet) app.Reconciliation {
	t.Helper()
	rec, err := s.wallets.Reconcile(context.Background(), w.ID())
	require.NoError(t, err)
	require.True(t, rec.Consistent, "stored %s calculated %s", rec.Stored, rec.Calculated)
	return rec
}

func (s services) debits(t *testing.T, w *wallet.Wallet) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID()).Scan(&n))
	return n
}

// provisionQueues creates a private set of FIFO queues for one test.
func provisionQueues(t *testing.T, maxReceive int) (sqsadapter.API, sqsadapter.Queues, sqsadapter.QueueNames) {
	t.Helper()
	ctx := context.Background()
	client, err := sqsadapter.NewClient(ctx, sqsadapter.ClientConfig{Region: "us-east-1", Endpoint: env.SQSEndpoint})
	require.NoError(t, err)
	sum := sha256.Sum256([]byte(t.Name()))
	prefix := hex.EncodeToString(sum[:6])
	names := sqsadapter.QueueNames{Input: prefix + "-in.fifo", DLQ: prefix + "-dlq.fifo", Events: prefix + "-events.fifo"}
	q, err := sqsadapter.Provision(ctx, client, sqsadapter.ProvisionConfig{Names: names, MaxReceiveCount: maxReceive, VisibilityTimeout: 2})
	require.NoError(t, err)
	return client, q, names
}
