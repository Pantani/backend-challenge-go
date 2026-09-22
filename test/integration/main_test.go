//go:build integration

// Package integration_test runs the adapters against real PostgreSQL,
// LocalStack SQS and Keycloak containers.
package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
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
	err := migrateDatabase()
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

func migrateDatabase() (err error) {
	migrator, err := postgres.NewMigrator(env.DatabaseURL)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, migrator.Close()) }()
	return migrator.Up()
}

func databaseForTest(t *testing.T, prefix string) string {
	t.Helper()
	name := prefix + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	identifier := pgx.Identifier{name}.Sanitize()
	_, err := pool.Exec(context.Background(), "CREATE DATABASE "+identifier)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), "DROP DATABASE "+identifier)
		require.NoError(t, err)
		var count int
		require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_database WHERE datname = $1`, name).Scan(&count))
		require.Zero(t, count, "temporary database removed")
	})
	u, err := url.Parse(env.DatabaseURL)
	require.NoError(t, err)
	u.Path = "/" + name
	return u.String()
}

// services wires the use cases on the real database.
type services struct {
	wagers  *app.WagerService
	wallets *app.WalletService
	metrics *observability.Metrics
	logs    *testutil.SyncBuffer
	// prefix keeps external ids unique across tests sharing the database.
	prefix string
}

func newServices(t *testing.T, policy app.PendingPolicy) services {
	t.Helper()
	logs := &testutil.SyncBuffer{}
	logger := observability.NewLogger(logs, "debug", "it")
	metrics := testutil.NewMetrics()
	uow, queries := postgres.NewUnitOfWork(pool), postgres.NewQueries(pool)
	return services{
		wagers: app.NewWagerService(app.WagerDeps{Deps: app.Deps{UoW: uow, Queries: queries, Clock: app.SystemClock{}, IDs: app.UUIDv7{},
			Metrics: metrics, Logger: logger}, Policy: policy, ConflictRetries: 10}),
		wallets: app.NewWalletService(app.WalletDeps{Deps: app.Deps{UoW: uow, Queries: queries, Clock: app.SystemClock{}, IDs: app.UUIDv7{},
			Metrics: metrics, Logger: logger}}),
		metrics: metrics, logs: logs, prefix: uuid.NewString() + "-",
	}
}

var defaultPolicy = app.PendingPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second, MaxAttempts: 5, BatchSize: 50}

func (s services) openWallet(t *testing.T, amount string) *wallet.Wallet {
	t.Helper()
	w, err := s.wallets.Open(context.Background(), app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: testutil.BRL(t, amount)})
	require.NoError(t, err)
	return w
}

// ids is the API identity of a domain wallet.
func ids(w *wallet.Wallet) testenv.Wallet {
	return testenv.Wallet{ID: w.ID().String(), PlayerID: w.PlayerID().String()}
}

// input prefixes the external ids so tests never collide.
func (s services) input(w *wallet.Wallet, provider, ext, kind, amount, ref string) app.SubmitInput {
	if ref != "" {
		ref = s.prefix + ref
	}
	return testenv.SubmitInput(ids(w), provider, s.prefix+ext, kind, amount, ref)
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
	n, err := testenv.CountDebits(context.Background(), pool, w.ID().String())
	require.NoError(t, err)
	return n
}

// provisionQueues creates a private set of FIFO queues for one test.
func provisionQueues(t *testing.T, maxReceive int) (sqsadapter.API, sqsadapter.Queues, sqsadapter.QueueNames) {
	t.Helper()
	ctx := context.Background()
	client, err := sqsadapter.NewClient(ctx, sqsadapter.ClientConfig{Region: "us-east-1", Endpoint: env.SQSEndpoint})
	require.NoError(t, err)
	prefix := uuid.NewString()
	names := sqsadapter.QueueNames{Input: prefix + "-in.fifo", DLQ: prefix + "-dlq.fifo", Events: prefix + "-events.fifo"}
	api := cleanupQueues{Client: client, t: t}
	q, err := sqsadapter.Provision(ctx, api, sqsadapter.ProvisionConfig{Names: names, MaxReceiveCount: maxReceive, VisibilityTimeout: 2})
	require.NoError(t, err)
	return client, q, names
}

// cleanupQueues registers each successful acquisition, including partial provisioning.
type cleanupQueues struct {
	*awssqs.Client
	t *testing.T
}

func (c cleanupQueues) CreateQueue(ctx context.Context, in *awssqs.CreateQueueInput, opts ...func(*awssqs.Options)) (*awssqs.CreateQueueOutput, error) {
	out, err := c.Client.CreateQueue(ctx, in, opts...)
	if err == nil {
		c.t.Cleanup(func() { c.removeQueue(context.WithoutCancel(ctx), aws.ToString(in.QueueName), out.QueueUrl) })
	}
	return out, err
}

func (c cleanupQueues) removeQueue(ctx context.Context, name string, queueURL *string) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := c.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: queueURL})
	require.NoError(c.t, err)
	_, err = c.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(name)})
	var missing *types.QueueDoesNotExist
	require.ErrorAs(c.t, err, &missing, "deleted queue %s must not resolve", name)
}

// The CLI owns its client, so register by name before it can acquire resources.
func cleanupCLIQueues(t *testing.T, names sqsadapter.QueueNames) {
	t.Helper()
	client, err := sqsadapter.NewClient(context.Background(), sqsadapter.ClientConfig{Region: "us-east-1", Endpoint: env.SQSEndpoint})
	require.NoError(t, err)
	c := cleanupQueues{Client: client, t: t}
	for _, name := range []string{names.DLQ, names.Input, names.Events} {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(name)})
			var missing *types.QueueDoesNotExist
			if errors.As(err, &missing) {
				return
			}
			require.NoError(t, err)
			c.removeQueue(ctx, name, out.QueueUrl)
		})
	}
}
