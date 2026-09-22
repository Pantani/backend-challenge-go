package worker_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var (
	errBoom = errors.New("boom")
	t0      = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
)

type clock struct{ now time.Time }

func (c clock) Now() time.Time { return c.now }

func TestGroupStopsWorkers(t *testing.T) {
	logs := &bytes.Buffer{}
	g := worker.NewGroup(context.Background(), observability.NewLogger(&syncWriter{w: logs}, "info", "t"))
	var ticks atomic.Int64
	g.Go("ticker", func(ctx context.Context) { worker.Loop(ctx, time.Millisecond, func(context.Context) { ticks.Add(1) }) })
	require.Eventually(t, func() bool { return ticks.Load() > 2 }, time.Second, time.Millisecond)
	assert.Equal(t, 1, g.Running())

	require.NoError(t, g.Stop(context.Background()))
	assert.Zero(t, g.Running())
}

func TestGroupStopDeadline(t *testing.T) {
	g := worker.NewGroup(context.Background(), observability.NewLogger(&syncWriter{w: &bytes.Buffer{}}, "info", "t"))
	release := make(chan struct{})
	g.Go("stuck", func(context.Context) { <-release })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := g.Stop(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "1 workers still running")
	close(release)
	require.NoError(t, g.Stop(context.Background()))
}

// syncWriter makes a buffer safe for concurrent log writes.
type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func (s *syncWriter) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.String()
}

type fakeStore struct {
	claimErr     error
	msgs         []app.OutboxMessage
	published    []uuid.UUID
	failed       map[uuid.UUID]time.Time
	markOK       bool
	markErr      error
	markFailErr  error
	oldest       time.Time
	hasOldest    bool
	oldestErr    error
	claimedOwner string
}

func (s *fakeStore) Claim(_ context.Context, owner string, _ time.Time, _ time.Duration, _ int) ([]app.OutboxMessage, error) {
	s.claimedOwner = owner
	return s.msgs, s.claimErr
}

func (s *fakeStore) MarkPublished(_ context.Context, id uuid.UUID, _ string, _ time.Time) (bool, error) {
	if s.markOK {
		s.published = append(s.published, id)
	}
	return s.markOK, s.markErr
}

func (s *fakeStore) MarkFailed(_ context.Context, id uuid.UUID, _ string, next time.Time, _ string) error {
	s.failed[id] = next
	return s.markFailErr
}

func (s *fakeStore) OldestPending(context.Context) (time.Time, bool, error) {
	return s.oldest, s.hasOldest, s.oldestErr
}

type fakePublisher struct{ fail map[uuid.UUID]bool }

func (p fakePublisher) Publish(_ context.Context, m app.OutboxMessage) error {
	if p.fail[m.EventID] {
		return errBoom
	}
	return nil
}

func newRelay(store *fakeStore, pub fakePublisher, logs *syncWriter) *worker.Relay {
	return worker.NewRelay(store, pub, clock{t0}, worker.RelayConfig{
		Owner: "instance-1", BatchSize: 10, Lease: time.Minute, RetryBase: time.Second, RetryMax: 30 * time.Second, PublishTime: time.Second,
	}, observability.NewLogger(logs, "debug", "t"), observability.NewMetrics(prometheus.NewRegistry()))
}

func TestRelayPublishesAndRetries(t *testing.T) {
	ok, bad := uuid.New(), uuid.New()
	store := &fakeStore{markOK: true, failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{
		{EventID: ok, Attempts: 1}, {EventID: bad, Attempts: 3},
	}, hasOldest: true, oldest: t0.Add(-time.Second)}
	logs := &syncWriter{w: &bytes.Buffer{}}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{bad: true}}, logs).Tick(context.Background())

	assert.Equal(t, "instance-1", store.claimedOwner)
	assert.Equal(t, []uuid.UUID{ok}, store.published)
	assert.Equal(t, t0.Add(4*time.Second), store.failed[bad], "exponential backoff")
	assert.Contains(t, logs.String(), "outbox publish failed")
}

func TestRelayBackoffIsCapped(t *testing.T) {
	id := uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: id, Attempts: 40}}}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{id: true}}, &syncWriter{w: &bytes.Buffer{}}).Tick(context.Background())
	assert.Equal(t, t0.Add(30*time.Second), store.failed[id])
}

func TestRelayFailurePaths(t *testing.T) {
	logs := &syncWriter{w: &bytes.Buffer{}}
	newRelay(&fakeStore{claimErr: errBoom}, fakePublisher{}, logs).Tick(context.Background())
	assert.Contains(t, logs.String(), "outbox claim failed")

	id := uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: id}}, markFailErr: errBoom, oldestErr: errBoom}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{id: true}}, logs).Tick(context.Background())
	assert.Contains(t, logs.String(), "lease expiry will release it")

	lost := &fakeStore{msgs: []app.OutboxMessage{{EventID: uuid.New()}}, markOK: false}
	newRelay(lost, fakePublisher{}, logs).Tick(context.Background())
	assert.Contains(t, logs.String(), "republished with the same eventId")
}

type fakePending struct {
	n   int
	err error
}

func (f fakePending) ResolveDue(context.Context) (int, error) { return f.n, f.err }

func TestPendingResolver(t *testing.T) {
	logs := &syncWriter{w: &bytes.Buffer{}}
	logger := observability.NewLogger(logs, "info", "t")
	worker.NewPendingResolver(fakePending{n: 2}, logger).Tick(context.Background())
	assert.Contains(t, logs.String(), "pending references resolved")

	worker.NewPendingResolver(fakePending{err: errBoom}, logger).Tick(context.Background())
	assert.Contains(t, logs.String(), "pending reference batch failed")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := logs.String()
	worker.NewPendingResolver(fakePending{err: context.Canceled}, logger).Tick(ctx)
	assert.Equal(t, before, logs.String(), "cancellation during shutdown is not an error")
}
