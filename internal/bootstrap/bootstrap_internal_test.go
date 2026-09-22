package bootstrap

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx/fxtest"

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
	group := newGroup(logger)
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
	srv := newServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }), cfg)
	assert.Equal(t, 5*time.Second, srv.ReadHeaderTimeout)
	assert.Equal(t, 15*time.Second, srv.ReadTimeout)
	assert.Equal(t, 30*time.Second, srv.WriteTimeout)
	addr := &Addr{}
	lc := fxtest.NewLifecycle(t)
	startServer(lc, srv, addr, slog.New(slog.NewTextHandler(io.Discard, nil)))
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

	bad := newServer(srv.Handler, config.Config{HTTPAddr: "256.0.0.1:1"})
	lc = fxtest.NewLifecycle(t)
	startServer(lc, bad, &Addr{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
