// Package bootstrap composes the application with Uber Fx. It is the only
// package that knows about Fx; domain, use cases and adapters stay free of it.
//
// Lifecycle order: resources are constructed first, so their OnStop hooks run
// last. Workers start before the HTTP server, and the HTTP server stops first
// (no new inputs), then the consumers and workers (in-flight work completes or
// its visibility is released), and finally the database pool.
package bootstrap

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/Pantani/backend-challenge-go/internal/adapter/auth"
	"github.com/Pantani/backend-challenge-go/internal/adapter/httpapi"
	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/config"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

// connectTimeout bounds dependency lookups done while constructing the graph.
const connectTimeout = 10 * time.Second

// LogOutput is where JSON logs are written.
type LogOutput struct{ io.Writer }

// Options returns every module of the service for the given configuration.
func Options(cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg, LogOutput{os.Stdout}),
		fx.WithLogger(func(l *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: l} }),
		ObservabilityModule, PostgresModule, SQSModule, AppModule, AuthModule, WorkerModule, HTTPModule,
	)
}

// New builds the application. extra options (fx.Replace, fx.Decorate,
// fx.Populate) let tests adjust the graph.
func New(cfg config.Config, extra ...fx.Option) *fx.App {
	return fx.New(Options(cfg), fx.Options(extra...),
		fx.StartTimeout(connectTimeout+cfg.ReadyTimeout), fx.StopTimeout(cfg.ShutdownTimeout))
}

// ObservabilityModule provides logging and metrics.
var ObservabilityModule = fx.Module("observability",
	fx.Provide(
		func(cfg config.Config, out LogOutput) *slog.Logger {
			return observability.NewLogger(out, cfg.LogLevel, cfg.InstanceID)
		},
		newRegistry,
		fx.Annotate(func(reg *prometheus.Registry) *observability.Metrics { return observability.NewMetrics(reg) },
			fx.As(fx.Self()), fx.As(new(app.Metrics))),
	),
)

func newRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// PostgresModule provides the pool and the persistence ports.
var PostgresModule = fx.Module("postgres",
	fx.Provide(
		newPool,
		fx.Annotate(postgres.NewUnitOfWork, fx.As(new(app.UnitOfWork))),
		fx.Annotate(postgres.NewQueries, fx.As(new(app.Queries))),
		fx.Annotate(postgres.NewOutboxStore, fx.As(new(app.OutboxStore))),
	),
)

// newPool validates the database on start and closes the pool last.
func newPool(lc fx.Lifecycle, cfg config.Config) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(context.Background(), postgres.Config{
		URL: cfg.DatabaseURL, MaxConns: int32(cfg.DBMaxConns),
		LockTimeout: cfg.DBLockTimeout, StatementTimeout: cfg.DBStatementTimeout,
	})
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return postgres.Ping(ctx, pool) },
		OnStop: func(context.Context) error {
			pool.Close()
			return nil
		},
	})
	return pool, nil
}

// SQSModule provides the SQS client, queues, publisher and consumer.
var SQSModule = fx.Module("sqs",
	fx.Provide(
		newSQSClient,
		newQueues,
		fx.Annotate(newPublisher, fx.As(new(worker.Publisher))),
		newConsumer,
	),
)

func newSQSClient(cfg config.Config) (sqsadapter.API, error) {
	client, err := sqsadapter.NewClient(context.Background(), sqsadapter.ClientConfig{Region: cfg.AWSRegion, Endpoint: cfg.AWSEndpoint})
	if err != nil {
		return nil, err
	}
	return client, nil
}

// newQueues resolves the queue URLs; missing queues fail the start.
func newQueues(api sqsadapter.API, cfg config.Config) (sqsadapter.Queues, error) {
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	return sqsadapter.ResolveQueues(ctx, api, sqsadapter.QueueNames{Input: cfg.SQSInputQueue, DLQ: cfg.SQSDLQ, Events: cfg.SQSEventsQueue})
}

func newPublisher(api sqsadapter.API, q sqsadapter.Queues) *sqsadapter.Publisher {
	return sqsadapter.NewPublisher(api, q.Events)
}

func newConsumer(api sqsadapter.API, q sqsadapter.Queues, cfg config.Config, svc *app.WagerService,
	logger *slog.Logger, metrics *observability.Metrics) *sqsadapter.Consumer {
	return sqsadapter.NewConsumer(api, sqsadapter.ConsumerConfig{
		Name: cfg.SQSConsumerName, QueueURL: q.Input, DLQURL: q.DLQ, MaxMessages: int32(cfg.SQSMaxMessages),
		WaitTime: cfg.SQSWaitTime, VisibilityTimeout: cfg.SQSVisibility, ProcessTimeout: cfg.SQSProcessTimeout,
		RetryBase: cfg.SQSRetryBase, RetryMax: cfg.SQSRetryMax,
	}, svc, logger, metrics)
}

// AppModule provides the use cases.
var AppModule = fx.Module("app",
	fx.Provide(
		func() app.Clock { return app.SystemClock{} },
		func() app.IDGenerator { return app.UUIDv7{} },
		newWagerService,
		newWalletService,
	),
)

type serviceDeps struct {
	fx.In
	UoW     app.UnitOfWork
	Queries app.Queries
	Clock   app.Clock
	IDs     app.IDGenerator
	Metrics app.Metrics
	Logger  *slog.Logger
	Config  config.Config
}

func newWagerService(d serviceDeps) *app.WagerService {
	return app.NewWagerService(app.WagerDeps{
		UoW: d.UoW, Queries: d.Queries, Clock: d.Clock, IDs: d.IDs, Metrics: d.Metrics, Logger: d.Logger,
		ConflictRetries: d.Config.ConflictRetries,
		Policy: app.PendingPolicy{BaseDelay: d.Config.PendingBaseDelay, MaxDelay: d.Config.PendingMaxDelay,
			MaxAttempts: d.Config.PendingMaxAttempts, BatchSize: d.Config.PendingBatch},
	})
}

func newWalletService(d serviceDeps) *app.WalletService {
	return app.NewWalletService(app.WalletDeps{
		UoW: d.UoW, Queries: d.Queries, Clock: d.Clock, IDs: d.IDs, Metrics: d.Metrics, Logger: d.Logger,
	})
}

// AuthModule provides the OIDC token verifier.
var AuthModule = fx.Module("auth",
	fx.Provide(fx.Annotate(func(cfg config.Config) *auth.Verifier {
		return auth.NewVerifier(context.Background(), auth.Config{Issuer: cfg.OIDCIssuer, JWKSURL: cfg.OIDCJWKSURL, Audience: cfg.OIDCAudience})
	}, fx.As(new(httpapi.TokenVerifier)))),
)

// WorkerModule starts the outbox relay, the pending resolver and the SQS
// consumers under one supervised group.
var WorkerModule = fx.Module("worker",
	fx.Provide(newGroup, newRelay, newPendingResolver),
	fx.Invoke(startWorkers),
)

func newGroup(lc fx.Lifecycle, logger *slog.Logger) *worker.Group {
	g := worker.NewGroup(context.Background(), logger)
	lc.Append(fx.StopHook(g.Stop))
	return g
}

func newRelay(store app.OutboxStore, pub worker.Publisher, clock app.Clock, cfg config.Config,
	logger *slog.Logger, metrics *observability.Metrics) *worker.Relay {
	return worker.NewRelay(store, pub, clock, worker.RelayConfig{
		Owner: cfg.InstanceID, BatchSize: cfg.OutboxBatch, Lease: cfg.OutboxLease,
		RetryBase: cfg.OutboxRetryBase, RetryMax: cfg.OutboxRetryMax, PublishTime: cfg.SQSProcessTimeout,
	}, logger, metrics)
}

func newPendingResolver(svc *app.WagerService, logger *slog.Logger) *worker.PendingResolver {
	return worker.NewPendingResolver(svc, logger)
}

type workerDeps struct {
	fx.In
	Lifecycle fx.Lifecycle
	Group     *worker.Group
	Relay     *worker.Relay
	Pending   *worker.PendingResolver
	Consumer  *sqsadapter.Consumer
	Config    config.Config
}

func startWorkers(d workerDeps) {
	d.Lifecycle.Append(fx.StartHook(func() {
		d.Group.Go("outbox-relay", func(ctx context.Context) { worker.Loop(ctx, d.Config.OutboxInterval, d.Relay.Tick) })
		d.Group.Go("pending-resolver", func(ctx context.Context) { worker.Loop(ctx, d.Config.PendingInterval, d.Pending.Tick) })
		for i := range d.Config.SQSConsumers {
			d.Group.Go(fmt.Sprintf("sqs-consumer-%d", i), d.Consumer.Run)
		}
	}))
}

// HTTPModule serves the API.
var HTTPModule = fx.Module("http",
	fx.Provide(newHandler, newServer, func() *Addr { return &Addr{} }),
	fx.Invoke(startServer),
)

type handlerDeps struct {
	fx.In
	Wallets  *app.WalletService
	Wagers   *app.WagerService
	Verifier httpapi.TokenVerifier
	Metrics  *observability.Metrics
	Registry *prometheus.Registry
	Pool     *pgxpool.Pool
	SQS      sqsadapter.API
	Queues   sqsadapter.Queues
	Logger   *slog.Logger
	Config   config.Config
}

func newHandler(d handlerDeps) http.Handler {
	return httpapi.NewHandler(httpapi.Deps{
		Wallets: d.Wallets, Wagers: d.Wagers, Verifier: d.Verifier, Metrics: d.Metrics, Logger: d.Logger,
		MetricsHandler: promhttp.HandlerFor(d.Registry, promhttp.HandlerOpts{}),
		ReadyTimeout:   d.Config.ReadyTimeout,
		Checks: []httpapi.HealthCheck{
			{Name: "postgres", Check: func(ctx context.Context) error { return postgres.Ping(ctx, d.Pool) }},
			{Name: "sqs", Check: sqsadapter.QueueCheck(d.SQS, d.Queues.Input)},
		},
	})
}

func newServer(h http.Handler, cfg config.Config) *http.Server {
	return &http.Server{Addr: cfg.HTTPAddr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
}

// Addr exposes the bound address (useful with ":0" in tests).
type Addr struct{ value string }

// String returns the listening address.
func (a *Addr) String() string { return a.value }

func startServer(lc fx.Lifecycle, srv *http.Server, addr *Addr, logger *slog.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", srv.Addr)
			if err != nil {
				return err
			}
			addr.value = ln.Addr().String()
			logger.Info("http server listening", "addr", addr.value)
			go func() { _ = srv.Serve(ln) }()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("http server shutting down")
			return srv.Shutdown(ctx)
		},
	})
}

// ensure the concrete SQS client satisfies the port.
var _ sqsadapter.API = (*awssqs.Client)(nil)
