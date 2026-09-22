package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	postgresadapter "github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/config"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

// blockingAPI parks every receive until its context is done.
type blockingAPI struct{ sqsadapter.API }

func (blockingAPI) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type blockingQueueAPI struct {
	sqsadapter.API
	once sync.Once
	seen chan struct{}
}

func (a *blockingQueueAPI) GetQueueUrl( //nolint:revive // name imposed by the AWS SDK interface
	ctx context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options),
) (*sqs.GetQueueUrlOutput, error) {
	a.once.Do(func() { close(a.seen) })
	<-ctx.Done()
	return nil, ctx.Err()
}

// emptyStore has nothing to relay.
type emptyStore struct{ app.OutboxStore }

func (emptyStore) Claim(context.Context, string, uuid.UUID, time.Time, time.Duration) (app.OutboxMessage, bool, error) {
	return app.OutboxMessage{}, false, nil
}
func (emptyStore) OldestPending(context.Context) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

type nothingPending struct{}

func (nothingPending) ResolveDue(context.Context) (int, error) { return 0, nil }

type noPublisher struct{}

func (noPublisher) Publish(context.Context, app.OutboxMessage) error { return nil }

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load(config.MapLookup(map[string]string{"HTTP_ADDR": "127.0.0.1:0", "SQS_CONSUMERS": "3",
		"OUTBOX_INTERVAL": "10ms", "PENDING_INTERVAL": "10ms"}))
	require.NoError(t, err)
	return cfg
}

func TestStartWorkersRunsAndStopsTheGroup(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := testutil.NewMetrics()
	group := newGroup(logger, &startupRollback{})
	relay := newRelay(emptyStore{}, noPublisher{}, app.SystemClock{}, cfg, logger, metrics)
	consumer := sqsadapter.NewConsumer(blockingAPI{}, sqsadapter.ConsumerConfig{QueueURL: "q", WaitTime: time.Second}, nil, logger, metrics)
	lc := fxtest.NewLifecycle(t)
	startWorkers(workerDeps{Lifecycle: lc, Group: group, Relay: relay, Pending: worker.NewPendingResolver(nothingPending{}, logger),
		Consumer: consumer, Config: cfg})
	lc.RequireStart()
	assert.Equal(t, 2+cfg.SQSConsumers, group.Running(), "relay, pending resolver and one goroutine per consumer")
	lc.RequireStop()
	assert.Zero(t, group.Running())
}

func TestStartServerBindsAndClosesTheListener(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	rollback := &startupRollback{}
	srv := newServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }), cfg, rollback)
	assert.Equal(t, 5*time.Second, srv.ReadHeaderTimeout)
	assert.Equal(t, 15*time.Second, srv.ReadTimeout)
	assert.Equal(t, 30*time.Second, srv.WriteTimeout)
	addr := &Addr{}
	lc := fxtest.NewLifecycle(t)
	startServer(lc, srv, addr, slog.New(slog.NewTextHandler(io.Discard, nil)), rollback)
	assert.Empty(t, addr.String(), "not bound before start")
	lc.RequireStart()
	require.NotEmpty(t, addr.String())
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr.String()+"/", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	lc.RequireStop()
	_, err = (&net.Dialer{Timeout: time.Second}).Dial("tcp", addr.String())
	require.Error(t, err, "the listener is closed on stop")

	badRollback := &startupRollback{}
	bad := newServer(srv.Handler, config.Config{HTTPAddr: "256.0.0.1:1"}, badRollback)
	lc = fxtest.NewLifecycle(t)
	startServer(lc, bad, &Addr{}, slog.New(slog.NewTextHandler(io.Discard, nil)), badRollback)
	require.Error(t, lc.Start(context.Background()))
}

func TestRegistryHasRuntimeCollectors(t *testing.T) {
	t.Parallel()
	reg := newRegistry()
	families, err := reg.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	assert.Contains(t, names, "go_goroutines")
	assert.Contains(t, names, "process_start_time_seconds")
}

func TestConstructionUsesStartupContext(t *testing.T) {
	t.Run("queue resolution", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		api := &blockingQueueAPI{seen: make(chan struct{})}
		app := New(ctx, testConfig(t), fx.Replace(fx.Annotate(api, fx.As(new(sqsadapter.API)))))

		assert.ErrorIs(t, app.Err(), context.DeadlineExceeded)
		assertClosed(t, api.seen)
	})

	t.Run("database ping", func(t *testing.T) {
		listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, listener.Close()) })
		accepted := make(chan struct{})
		serveStalledPostgres(t, listener, accepted)

		cfg := testConfig(t)
		cfg.DatabaseURL = "postgres://u:p@" + listener.Addr().String() + "/db?sslmode=disable&connect_timeout=10"
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		defer cancel()
		app := New(ctx, cfg, fx.Replace(fx.Annotate(fakeQueueAPI{}, fx.As(new(sqsadapter.API)))))
		require.NoError(t, app.Err())

		err = app.Start(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assertClosed(t, accepted)
	})
}

type fakeQueueAPI struct{ sqsadapter.API }

func (fakeQueueAPI) GetQueueUrl( //nolint:revive // name imposed by the AWS SDK interface
	_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options),
) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("http://sqs.local/queue")}, nil
}

func assertClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	default:
		require.Fail(t, "operation did not reach the blocking dependency")
	}
}

func serveStalledPostgres(t *testing.T, listener net.Listener, accepted chan<- struct{}) {
	t.Helper()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		_, _ = io.Copy(io.Discard, conn)
	}()
}

type poolSpy struct {
	pingErr  error
	closeErr error
	ping     func(context.Context) error
	pings    atomic.Int32
	closes   atomic.Int32
}

func (s *poolSpy) open(context.Context, postgresadapter.Config) (poolHandle, error) {
	return poolHandle{
		pool: &pgxpool.Pool{},
		ping: func(ctx context.Context) error {
			s.pings.Add(1)
			if s.ping != nil {
				return s.ping(ctx)
			}
			return s.pingErr
		},
		close: func() error {
			s.closes.Add(1)
			return s.closeErr
		},
	}, nil
}

type failedPoolConsumer struct{}

type lifecycleAppStub struct {
	start func(context.Context) error
	stop  func(context.Context) error
	stops atomic.Int32
}

func (*lifecycleAppStub) Err() error { return nil }

func (a *lifecycleAppStub) Start(ctx context.Context) error { return a.start(ctx) }

func (a *lifecycleAppStub) Stop(ctx context.Context) error {
	a.stops.Add(1)
	if a.stop != nil {
		return a.stop(ctx)
	}
	return nil
}

func (*lifecycleAppStub) Wait() <-chan fx.ShutdownSignal { return make(chan fx.ShutdownSignal) }

func TestPoolOwnershipClosesExactlyOnce(t *testing.T) {
	t.Run("downstream construction failure", func(t *testing.T) {
		spy := &poolSpy{}
		boom := errors.New("downstream construction failed")
		ctx, cancel := context.WithCancel(context.Background())
		app := New(ctx, testConfig(t),
			fx.Replace(poolFactory(spy.open)),
			fx.Replace(fx.Annotate(fakeQueueAPI{}, fx.As(new(sqsadapter.API)))),
			fx.Provide(func(*pgxpool.Pool) (failedPoolConsumer, error) { return failedPoolConsumer{}, boom }),
			fx.Invoke(func(failedPoolConsumer) {}),
		)

		assert.ErrorIs(t, app.Err(), boom)
		cancel()
		assert.EqualValues(t, 1, spy.closes.Load())
	})

	t.Run("ping failure", func(t *testing.T) {
		spy := &poolSpy{pingErr: errors.New("ping failed")}
		app := newAppWithPoolSpy(context.Background(), t, spy)
		require.NoError(t, app.Err())

		assert.ErrorIs(t, app.Start(context.Background()), spy.pingErr)
		require.NoError(t, app.Stop(context.Background()))
		assert.EqualValues(t, 1, spy.pings.Load())
		assert.EqualValues(t, 1, spy.closes.Load())
	})

	t.Run("ping timeout", func(t *testing.T) {
		spy := &poolSpy{ping: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}}
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		app := newAppWithPoolSpy(ctx, t, spy)
		require.NoError(t, app.Err())

		assert.ErrorIs(t, app.Start(ctx), context.DeadlineExceeded)
		require.Eventually(t, func() bool { return spy.closes.Load() == 1 }, time.Second, time.Millisecond)
		require.NoError(t, app.Stop(context.Background()))
		assert.EqualValues(t, 1, spy.pings.Load())
		assert.EqualValues(t, 1, spy.closes.Load())
	})

	t.Run("later hook canceled", func(t *testing.T) {
		spy := &poolSpy{}
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		app := newPoolLifecycleApp(ctx, t, spy, fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.StartHook(func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}))
		}))

		assert.ErrorIs(t, app.Start(ctx), context.DeadlineExceeded)
		require.Eventually(t, func() bool { return spy.closes.Load() == 1 }, time.Second, time.Millisecond)
		require.NoError(t, app.Stop(context.Background()))
		assert.EqualValues(t, 1, spy.pings.Load())
		assert.EqualValues(t, 1, spy.closes.Load())
	})

	t.Run("successful full start transfers ownership to stop", func(t *testing.T) {
		spy := &poolSpy{}
		ctx, cancel := context.WithCancel(context.Background())
		app := newPoolLifecycleApp(ctx, t, spy)
		require.NoError(t, app.Start(ctx))

		cancel()
		assert.Zero(t, spy.closes.Load(), "startup cancellation no longer owns a successfully started pool")
		require.NoError(t, app.Stop(context.Background()))
		require.NoError(t, app.Stop(context.Background()))
		assert.EqualValues(t, 1, spy.pings.Load())
		assert.EqualValues(t, 1, spy.closes.Load())
	})
}

func TestApplicationStopsBeforeWatchdogClose(t *testing.T) {
	spy := &poolSpy{}
	owner := &poolOwner{}
	ctx, cancel := context.WithCancel(context.Background())
	handle, err := spy.open(ctx, postgresadapter.Config{})
	require.NoError(t, err)
	closeStarted, stopCalled := make(chan struct{}), make(chan struct{})
	emergencyRelease := make(chan struct{})
	handle.close = func() error {
		close(closeStarted)
		select {
		case <-stopCalled:
		case <-emergencyRelease:
		}
		spy.closes.Add(1)
		return nil
	}
	owner.Claim(ctx, handle)
	underlying := &lifecycleAppStub{start: func(context.Context) error {
		cancel()
		<-closeStarted
		return nil
	}, stop: func(context.Context) error {
		close(stopCalled)
		return nil
	}}
	app := newApplication(underlying, owner, &startupRollback{}, time.Second)
	result := make(chan error, 1)
	go func() { result <- app.Start(ctx) }()

	select {
	case err := <-result:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		close(emergencyRelease)
		require.Fail(t, "Start did not stop runtime before waiting for watchdog close")
	}
	assert.EqualValues(t, 1, spy.closes.Load())
	assert.EqualValues(t, 1, underlying.stops.Load())
}

func TestApplicationStartErrorJoinsCleanupFailures(t *testing.T) {
	startErr, stopErr, closeErr := errors.New("start failed"), errors.New("stop failed"), errors.New("close failed")
	spy := &poolSpy{closeErr: closeErr}
	owner := &poolOwner{}
	handle, err := spy.open(context.Background(), postgresadapter.Config{})
	require.NoError(t, err)
	stopCalled := make(chan struct{})
	emergencyRelease := make(chan struct{})
	originalClose := handle.close
	handle.close = func() error {
		select {
		case <-stopCalled:
		case <-emergencyRelease:
		}
		return originalClose()
	}
	owner.Claim(context.Background(), handle)
	underlying := &lifecycleAppStub{
		start: func(context.Context) error { return startErr },
		stop: func(context.Context) error {
			close(stopCalled)
			return stopErr
		},
	}
	app := newApplication(underlying, owner, &startupRollback{}, time.Second)
	result := make(chan error, 1)
	go func() { result <- app.Start(context.Background()) }()

	select {
	case err := <-result:
		assert.ErrorIs(t, err, startErr)
		assert.ErrorIs(t, err, stopErr)
		assert.ErrorIs(t, err, closeErr)
	case <-time.After(time.Second):
		close(emergencyRelease)
		require.Fail(t, "Start error did not stop runtime before waiting for close")
	}
	assert.EqualValues(t, 1, spy.closes.Load())
	assert.EqualValues(t, 1, underlying.stops.Load())
}

func TestApplicationDirectRollbackOwnsCanceledStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	spy := &poolSpy{}
	owner, rollback := newStartupOwners(ctx, time.Second)
	workerDone := make(chan struct{})
	addr := &Addr{}
	poolClosedAfterRuntime := atomic.Bool{}
	handle, err := spy.open(ctx, postgresadapter.Config{})
	require.NoError(t, err)
	originalClose := handle.close
	handle.close = func() error {
		poolClosedAfterRuntime.Store(channelClosed(workerDone) && listenerClosed(addr.String()))
		return originalClose()
	}
	owner.Claim(ctx, handle)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	group := newGroup(logger, rollback)
	cfg := testConfig(t)
	server := newServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), cfg, rollback)
	runtimeHookCalled := atomic.Bool{}

	fxApp := fx.New(fx.Invoke(func(lc fx.Lifecycle) {
		startServer(lc, server, addr, logger, rollback)
		lc.Append(fx.StartHook(func() {
			group.Go("rollback-test", func(ctx context.Context) {
				<-ctx.Done()
				close(workerDone)
			})
		}))
		lc.Append(fx.Hook{
			OnStart: func(context.Context) error { return nil },
			OnStop: func(context.Context) error {
				runtimeHookCalled.Store(true)
				return nil
			},
		})
		lc.Append(fx.StartHook(func(context.Context) error {
			cancel()
			return context.Canceled
		}))
	}))
	app := newApplication(fxApp, owner, rollback, time.Second)
	result := make(chan error, 1)
	go func() { result <- app.Start(ctx) }()

	select {
	case err := <-result:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		require.Fail(t, "failed-start direct rollback hung")
	}
	assert.True(t, channelClosed(workerDone), "direct rollback joins the worker group")
	assert.True(t, listenerClosed(addr.String()), "direct rollback closes HTTP admission")
	assert.False(t, runtimeHookCalled.Load(), "Fx skips rollback hooks after the startup context is canceled")
	assert.True(t, poolClosedAfterRuntime.Load(), "runtime admission and workers close before the pool")
	assert.EqualValues(t, 1, spy.closes.Load())
}

func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func listenerClosed(addr string) bool {
	conn, err := (&net.Dialer{Timeout: 50 * time.Millisecond}).Dial("tcp", addr)
	if err != nil {
		return true
	}
	_ = conn.Close()
	return false
}

func newAppWithPoolSpy(ctx context.Context, t *testing.T, spy *poolSpy) *Application {
	t.Helper()
	return New(ctx, testConfig(t),
		fx.Replace(poolFactory(spy.open)),
		fx.Replace(fx.Annotate(fakeQueueAPI{}, fx.As(new(sqsadapter.API)))),
	)
}

func newPoolLifecycleApp(ctx context.Context, t *testing.T, spy *poolSpy, extra ...fx.Option) *Application {
	t.Helper()
	owner := &poolOwner{}
	options := []fx.Option{
		fx.Supply(startupContext{Context: ctx}, testConfig(t), owner, poolFactory(spy.open)),
		fx.Provide(newPool),
		fx.Invoke(func(*pgxpool.Pool) {}),
	}
	options = append(options, extra...)
	return newApplication(fx.New(options...), owner, &startupRollback{}, testConfig(t).ShutdownTimeout)
}
