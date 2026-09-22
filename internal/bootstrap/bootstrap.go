// Package bootstrap composes the application with Uber Fx. It is the only
// package that knows about Fx; domain, use cases and adapters stay free of it.
//
// Lifecycle order follows hook registration: the pool registers its hooks
// while it is constructed, the worker group's hooks are registered by the
// startWorkers invoke (after every constructor) and the HTTP server's by the
// startServer invoke, last. Stop runs the hooks in reverse: the HTTP server
// stops first (no new inputs), then the consumers and workers (in-flight work
// completes or its visibility is released), and finally the database pool
// closes.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

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

// LogOutput is where JSON logs are written.
type LogOutput struct{ io.Writer }

// startupContext keeps the construction-only context distinct from request
// contexts in the Fx graph. Constructors may use it while building the graph,
// but runtime operations must receive their own contexts.
type startupContext struct{ context.Context }

// Options returns every module of the service for the given configuration.
// It is intended for graph validation; New supplies the real startup context.
func Options(cfg config.Config) fx.Option {
	owner, rollback := newStartupOwners(context.Background(), cfg.ShutdownTimeout)
	return options(startupContext{Context: context.Background()}, cfg, owner, rollback)
}

func options(startCtx startupContext, cfg config.Config, owner *poolOwner, rollback *startupRollback) fx.Option {
	return fx.Options(
		fx.Supply(cfg, startCtx, owner, rollback, poolFactory(openPool), LogOutput{os.Stdout}),
		fx.WithLogger(newFxLogger),
		ObservabilityModule, PostgresModule, SQSModule, AppModule, AuthModule, WorkerModule, HTTPModule,
	)
}

// newFxLogger routes Fx events to the service logger at debug level, so
// the graph construction is visible with LOG_LEVEL=debug only.
func newFxLogger(l *slog.Logger) fxevent.Logger {
	fl := &fxevent.SlogLogger{Logger: l}
	fl.UseLogLevel(slog.LevelDebug)
	return fl
}

type lifecycleApplication interface {
	Err() error
	Start(context.Context) error
	Stop(context.Context) error
	Wait() <-chan fx.ShutdownSignal
}

// Application owns the Fx application and resources acquired while its graph
// is constructed. Startup ownership transfers to runtime only after the
// underlying Fx Start call has returned successfully.
type Application struct {
	app             lifecycleApplication
	owner           *poolOwner
	rollback        *startupRollback
	shutdownTimeout time.Duration
	buildErr        error
}

// New builds the application. ctx is the single startup budget for graph
// construction and lifecycle start hooks; callers cancel it after Start
// returns. The stop budget is the configured shutdown timeout. extra options
// (fx.Replace, fx.Decorate, fx.Populate) let tests adjust the graph.
func New(ctx context.Context, cfg config.Config, extra ...fx.Option) *Application {
	owner, rollback := newStartupOwners(ctx, cfg.ShutdownTimeout)
	fxApp := fx.New(options(startupContext{Context: ctx}, cfg, owner, rollback), fx.Options(extra...),
		fx.StartTimeout(cfg.StartupTimeout), fx.StopTimeout(cfg.ShutdownTimeout))
	application := newApplication(fxApp, owner, rollback, cfg.ShutdownTimeout)
	if err := fxApp.Err(); err != nil {
		application.buildErr = errors.Join(err, owner.Close())
	}
	return application
}

func newApplication(app lifecycleApplication, owner *poolOwner, rollback *startupRollback,
	shutdownTimeout time.Duration,
) *Application {
	return &Application{app: app, owner: owner, rollback: rollback, shutdownTimeout: shutdownTimeout}
}

// Err reports an error encountered while constructing the Fx graph.
func (a *Application) Err() error {
	if a.buildErr != nil {
		return a.buildErr
	}
	return a.app.Err()
}

// Start starts the Fx lifecycle and transfers startup resources to runtime
// ownership only after every start hook has completed successfully.
func (a *Application) Start(ctx context.Context) error {
	if err := a.app.Start(ctx); err != nil {
		return a.cleanupFailedStart(ctx, err)
	}
	if err := a.owner.Transfer(ctx); err != nil {
		return a.cleanupFailedStart(ctx, err)
	}
	return nil
}

func (a *Application) cleanupFailedStart(ctx context.Context, startErr error) error {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.shutdownTimeout)
	defer cancel()
	rollbackErr := a.rollback.Cleanup(stopCtx)
	stopErr := a.app.Stop(stopCtx)
	closeErr := a.owner.Close()
	return errors.Join(startErr, rollbackErr, stopErr, closeErr)
}

// Stop stops the Fx lifecycle and closes startup resources even when a stop
// hook fails or times out.
func (a *Application) Stop(ctx context.Context) error {
	return errors.Join(a.app.Stop(ctx), a.owner.Close())
}

// Wait reports Fx shutdown signals and exit codes.
func (a *Application) Wait() <-chan fx.ShutdownSignal { return a.app.Wait() }

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

// newRegistry is a private registry with the Go runtime and process collectors.
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

type poolHandle struct {
	pool  *pgxpool.Pool
	ping  func(context.Context) error
	close func() error
}

type poolFactory func(context.Context, postgres.Config) (poolHandle, error)

func openPool(ctx context.Context, cfg postgres.Config) (poolHandle, error) {
	pool, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		return poolHandle{}, err
	}
	return poolHandle{
		pool: pool,
		ping: func(ctx context.Context) error { return postgres.Ping(ctx, pool) },
		close: func() error {
			pool.Close()
			return nil
		},
	}, nil
}

type poolOwnership uint8

const (
	poolUnclaimed poolOwnership = iota
	poolStartupOwned
	poolRuntimeOwned
	poolClosing
	poolClosed
)

var errStartupOwnershipLost = errors.New("startup resource ownership was lost before application start completed")

// poolOwner serializes the startup watchdog, runtime transfer, and close.
// Transfer reports a watchdog in flight without waiting, allowing Application
// to stop runtime work before Close waits for resource release.
type poolOwner struct {
	mu               sync.Mutex
	handle           poolHandle
	state            poolOwnership
	stopStartupWatch func() bool
	closeDone        chan struct{}
	closeErr         error
	beforeClose      func() error
}

func (o *poolOwner) Claim(ctx context.Context, handle poolHandle) {
	o.mu.Lock()
	o.handle = handle
	o.state = poolStartupOwned
	o.closeDone = make(chan struct{})
	o.mu.Unlock()

	stop := context.AfterFunc(ctx, func() { _ = o.Close() })
	o.mu.Lock()
	o.stopStartupWatch = stop
	o.mu.Unlock()
}

func (o *poolOwner) Ping(ctx context.Context) error { return o.handle.ping(ctx) }

func (o *poolOwner) Transfer(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if ownershipUnavailable(o.state) {
		return startupOwnershipError(ctx)
	}
	if err := o.cancelledTransfer(ctx); err != nil {
		return err
	}
	if !o.stopStartupWatcher() {
		return startupOwnershipError(ctx)
	}
	o.state = poolRuntimeOwned
	return nil
}

func ownershipUnavailable(state poolOwnership) bool {
	return state == poolClosing || state == poolClosed
}

func (o *poolOwner) cancelledTransfer(ctx context.Context) error {
	err := ctx.Err()
	if err != nil && o.stopStartupWatch != nil {
		o.stopStartupWatch()
	}
	return err
}

func (o *poolOwner) stopStartupWatcher() bool {
	return o.stopStartupWatch == nil || o.stopStartupWatch()
}

func startupOwnershipError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errStartupOwnershipLost
}

// startupRollback retains direct access to runtime resources acquired during
// startup. Fx lifecycle rollback is best-effort when its startup context has
// expired, so failed startup cleanup must not depend on OnStop hooks running.
type startupRollback struct {
	mu         sync.Mutex
	group      *worker.Group
	server     *http.Server
	listener   net.Listener
	done       chan struct{}
	cleanupCtx context.Context
	err        error
}

func newStartupOwners(ctx context.Context, shutdownTimeout time.Duration) (*poolOwner, *startupRollback) {
	rollback := &startupRollback{}
	owner := &poolOwner{beforeClose: func() error { return rollback.cleanupWithin(ctx, shutdownTimeout) }}
	return owner, rollback
}

func (r *startupRollback) SetGroup(group *worker.Group) {
	r.mu.Lock()
	if r.done != nil {
		ctx := r.cleanupCtx
		r.mu.Unlock()
		_ = stopGroup(ctx, group)
		return
	}
	r.group = group
	r.mu.Unlock()
}

func (r *startupRollback) SetServer(server *http.Server) {
	r.mu.Lock()
	if r.done != nil {
		ctx := r.cleanupCtx
		r.mu.Unlock()
		_ = shutdownServer(ctx, server)
		return
	}
	r.server = server
	r.mu.Unlock()
}

func (r *startupRollback) SetListener(listener net.Listener) {
	r.mu.Lock()
	if r.done != nil {
		r.mu.Unlock()
		_ = closeAdmission(listener)
		return
	}
	r.listener = listener
	r.mu.Unlock()
}

// Cleanup closes HTTP admission, joins background work, and drains the server.
// The pool owner runs separately and always closes after these runtime users.
func (r *startupRollback) Cleanup(ctx context.Context) error {
	group, server, listener, done, run := r.beginCleanup(ctx)
	if !run {
		return r.waitCleanup(ctx, done)
	}
	admissionErr := closeAdmission(listener)
	workerErr := stopGroup(ctx, group)
	serverErr := shutdownServer(ctx, server)
	err := errors.Join(admissionErr, workerErr, serverErr)
	r.finishCleanup(err)
	return err
}

func (r *startupRollback) cleanupWithin(parent context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()
	return r.Cleanup(ctx)
}

func (r *startupRollback) beginCleanup(ctx context.Context) (*worker.Group, *http.Server, net.Listener, <-chan struct{}, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done != nil {
		return nil, nil, nil, r.done, false
	}
	r.done = make(chan struct{})
	r.cleanupCtx = ctx
	return r.group, r.server, r.listener, r.done, true
}

func (r *startupRollback) finishCleanup(err error) {
	r.mu.Lock()
	r.err = err
	close(r.done)
	r.mu.Unlock()
}

func (r *startupRollback) waitCleanup(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func closeAdmission(listener net.Listener) error {
	if listener == nil {
		return nil
	}
	err := listener.Close()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func stopGroup(ctx context.Context, group *worker.Group) error {
	if group == nil {
		return nil
	}
	return group.Stop(ctx)
}

func shutdownServer(ctx context.Context, server *http.Server) error {
	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}

func (o *poolOwner) Close() error {
	closeFn, beforeClose, done, shouldClose := o.beginClose()
	if !shouldClose {
		return o.waitClose(done)
	}
	err := closeOwnedResource(beforeClose, closeFn)
	o.finishClose(done, err)
	return err
}

func (o *poolOwner) beginClose() (func() error, func() error, chan struct{}, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state == poolClosing || o.state == poolClosed {
		return nil, nil, o.closeDone, false
	}
	needsRollback := o.state == poolStartupOwned
	o.state = poolClosing
	beforeClose := o.beforeClose
	if !needsRollback {
		beforeClose = nil
	}
	return o.handle.close, beforeClose, o.closeDone, true
}

func (o *poolOwner) waitClose(done <-chan struct{}) error {
	if done != nil {
		<-done
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.closeErr
}

func closeOwnedResource(beforeClose, closeFn func() error) error {
	var err error
	if beforeClose != nil {
		err = beforeClose()
	}
	if closeFn != nil {
		err = errors.Join(err, closeFn())
	}
	return err
}

func (o *poolOwner) finishClose(done chan struct{}, err error) {
	o.mu.Lock()
	o.closeErr = err
	o.state = poolClosed
	if done != nil {
		close(done)
	}
	o.mu.Unlock()
}

// newPool validates the database on start and closes the pool last.
func newPool(lc fx.Lifecycle, startCtx startupContext, cfg config.Config, owner *poolOwner,
	factory poolFactory,
) (*pgxpool.Pool, error) {
	handle, err := factory(startCtx, postgres.Config{
		URL: cfg.DatabaseURL, MaxConns: int32(cfg.DBMaxConns),
		LockTimeout: cfg.DBLockTimeout, StatementTimeout: cfg.DBStatementTimeout,
	})
	if err != nil {
		return nil, err
	}
	owner.Claim(startCtx, handle)
	lc.Append(fx.Hook{
		OnStart: owner.Ping,
		OnStop:  func(context.Context) error { return owner.Close() },
	})
	return handle.pool, nil
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

// NewSQSClient builds the SQS client for the configured region and endpoint.
// The CLI uses it too, so provisioning and serving share one setup.
func NewSQSClient(ctx context.Context, cfg config.SQS) (sqsadapter.API, error) {
	client, err := sqsadapter.NewClient(ctx, sqsadapter.ClientConfig{Region: cfg.AWSRegion, Endpoint: cfg.AWSEndpoint})
	if err != nil {
		return nil, err
	}
	return client, nil
}

// QueueNames returns the configured queue names.
func QueueNames(cfg config.SQS) sqsadapter.QueueNames {
	return sqsadapter.QueueNames{Input: cfg.SQSInputQueue, DLQ: cfg.SQSDLQ, Events: cfg.SQSEventsQueue}
}

func newSQSClient(startCtx startupContext, cfg config.Config) (sqsadapter.API, error) {
	return NewSQSClient(startCtx, cfg.SQS)
}

// newQueues resolves the queue URLs; missing queues fail the start.
func newQueues(startCtx startupContext, api sqsadapter.API, cfg config.Config) (sqsadapter.Queues, error) {
	return sqsadapter.ResolveQueues(startCtx, api, QueueNames(cfg.SQS))
}

func newPublisher(api sqsadapter.API, q sqsadapter.Queues) *sqsadapter.Publisher {
	return sqsadapter.NewPublisher(api, q.Events)
}

func newConsumer(api sqsadapter.API, q sqsadapter.Queues, cfg config.Config, svc *app.WagerService,
	logger *slog.Logger, metrics *observability.Metrics) (*sqsadapter.Consumer, error) {
	senders, err := sqsadapter.ParseSenderPolicy(cfg.SQSSenderProviders)
	if err != nil {
		return nil, err
	}
	return sqsadapter.NewConsumer(api, sqsadapter.ConsumerConfig{
		Name: cfg.SQSConsumerName, QueueURL: q.Input, DLQURL: q.DLQ, MaxMessages: int32(cfg.SQSMaxMessages),
		WaitTime: cfg.SQSWaitTime, VisibilityTimeout: cfg.SQSVisibilityTimeout, ProcessTimeout: cfg.SQSProcessTimeout,
		AckTimeout: cfg.SQSAckTimeout,
		RetryBase:  cfg.SQSRetryBase, RetryMax: cfg.SQSRetryMax, Senders: senders,
	}, svc, logger, metrics), nil
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
		Deps:            d.deps(),
		ConflictRetries: d.Config.ConflictRetries,
		Policy: app.PendingPolicy{BaseDelay: d.Config.PendingBaseDelay, MaxDelay: d.Config.PendingMaxDelay,
			MaxAttempts: d.Config.PendingMaxAttempts, BatchSize: d.Config.PendingBatch},
	})
}

func newWalletService(d serviceDeps) *app.WalletService {
	return app.NewWalletService(app.WalletDeps{Deps: d.deps()})
}

func (d serviceDeps) deps() app.Deps {
	return app.Deps{UoW: d.UoW, Queries: d.Queries, Clock: d.Clock, IDs: d.IDs, Metrics: d.Metrics, Logger: d.Logger}
}

// AuthModule provides the OIDC token verifier.
var AuthModule = fx.Module("auth",
	fx.Provide(fx.Annotate(func(startCtx startupContext, cfg config.Config) *auth.Verifier {
		return auth.NewVerifier(startCtx, auth.Config{Issuer: cfg.OIDCIssuer, JWKSURL: cfg.OIDCJWKSURL, Audience: cfg.OIDCAudience})
	}, fx.As(new(httpapi.TokenVerifier)))),
)

// WorkerModule starts the outbox relay, the pending resolver and the SQS
// consumers under one supervised group.
var WorkerModule = fx.Module("worker",
	fx.Provide(newGroup, newRelay, newPendingResolver),
	fx.Invoke(startWorkers),
)

// newGroup registers no hook: startWorkers does, so the group stops before
// the resources constructed earlier (the pool) are closed.
func newGroup(logger *slog.Logger, rollback *startupRollback) *worker.Group {
	group := worker.NewGroup(context.Background(), logger)
	rollback.SetGroup(group)
	return group
}

func newRelay(store app.OutboxStore, pub worker.Publisher, clock app.Clock, cfg config.Config,
	logger *slog.Logger, metrics *observability.Metrics) *worker.Relay {
	return worker.NewRelay(store, pub, clock, worker.RelayConfig{
		Owner: cfg.InstanceID, BatchSize: cfg.OutboxBatch, Lease: cfg.OutboxLease,
		RetryBase: cfg.OutboxRetryBase, RetryMax: cfg.OutboxRetryMax, PublishTime: cfg.OutboxPublishTimeout,
		FinalizeTime: cfg.OutboxFinalizeTimeout,
		MaxAttempts:  cfg.OutboxMaxAttempts,
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

// startWorkers launches the loops on start and stops the group on stop. Both
// hooks are appended here, after every constructor's, so the workers stop
// before the pool closes.
func startWorkers(d workerDeps) {
	d.Lifecycle.Append(fx.StartHook(func() {
		d.Group.Go("outbox-relay", func(ctx context.Context) { worker.Loop(ctx, d.Config.OutboxInterval, d.Relay.Tick) })
		d.Group.Go("pending-resolver", func(ctx context.Context) { worker.Loop(ctx, d.Config.PendingInterval, d.Pending.Tick) })
		for i := range d.Config.SQSConsumers {
			d.Group.Go(fmt.Sprintf("sqs-consumer-%d", i), d.Consumer.Run)
		}
	}))
	d.Lifecycle.Append(fx.StopHook(d.Group.Stop))
}

// HTTPModule serves the API.
var HTTPModule = fx.Module("http",
	fx.Provide(newHealthChecks, newHandler, newServer, func() *Addr { return &Addr{} }),
	fx.Invoke(startServer),
)

// newHealthChecks are the readiness probes: the database and the input queue.
func newHealthChecks(pool *pgxpool.Pool, api sqsadapter.API, q sqsadapter.Queues) []httpapi.HealthCheck {
	return []httpapi.HealthCheck{
		{Name: "postgres", Check: func(ctx context.Context) error { return postgres.Ping(ctx, pool) }},
		{Name: "sqs", Check: sqsadapter.QueueCheck(api, q.Input)},
	}
}

type handlerDeps struct {
	fx.In
	Wallets  *app.WalletService
	Wagers   *app.WagerService
	Verifier httpapi.TokenVerifier
	Metrics  *observability.Metrics
	Registry *prometheus.Registry
	Checks   []httpapi.HealthCheck
	Logger   *slog.Logger
	Config   config.Config
}

func newHandler(d handlerDeps) http.Handler {
	return httpapi.NewHandler(httpapi.Deps{
		Wallets: d.Wallets, Wagers: d.Wagers, Verifier: d.Verifier, Metrics: d.Metrics, Logger: d.Logger,
		MetricsHandler: promhttp.HandlerFor(d.Registry, promhttp.HandlerOpts{}),
		ReadyTimeout:   d.Config.ReadyTimeout,
		Checks:         d.Checks,
	})
}

// HTTP server deadlines: slow clients cannot hold a handler while sending the
// body, and a blocked response write is bounded. IdleTimeout falls back to
// ReadTimeout.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 15 * time.Second
	writeTimeout      = 30 * time.Second
)

func newServer(h http.Handler, cfg config.Config, rollback *startupRollback) *http.Server {
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: h,
		ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, WriteTimeout: writeTimeout}
	rollback.SetServer(server)
	return server
}

// Addr exposes the bound address (useful with ":0" in tests).
type Addr struct{ value string }

// String returns the listening address.
func (a *Addr) String() string { return a.value }

// startServer listens on start (the bound address is published through
// Addr) and drains the connections on stop.
func startServer(lc fx.Lifecycle, srv *http.Server, addr *Addr, logger *slog.Logger,
	rollback *startupRollback,
) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", srv.Addr)
			if err != nil {
				return err
			}
			rollback.SetListener(ln)
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
