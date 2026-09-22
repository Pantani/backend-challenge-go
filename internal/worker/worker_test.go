package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var (
	errBoom = errors.New("boom")
	t0      = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
)

func TestGroupStopsWorkers(t *testing.T) {
	g := worker.NewGroup(context.Background(), observability.NewLogger(&testutil.SyncBuffer{}, "info", "t"))
	var ticks atomic.Int64
	g.Go("ticker", func(ctx context.Context) { worker.Loop(ctx, time.Millisecond, func(context.Context) { ticks.Add(1) }) })
	require.Eventually(t, func() bool { return ticks.Load() > 2 }, time.Second, time.Millisecond)
	assert.Equal(t, 1, g.Running())

	require.NoError(t, g.Stop(context.Background()))
	assert.Zero(t, g.Running())
}

func TestGroupGoAfterStopIsDropped(t *testing.T) {
	logs := &testutil.SyncBuffer{}
	g := worker.NewGroup(context.Background(), observability.NewLogger(logs, "info", "t"))
	require.NoError(t, g.Stop(context.Background()))
	g.Go("late", func(context.Context) { t.Error("a worker must not start after Stop") })
	assert.Zero(t, g.Running())
	assert.Contains(t, logs.String(), "group already stopped")
	require.NoError(t, g.Stop(context.Background()), "Stop is idempotent")
}

func TestGroupCountsDuplicateNames(t *testing.T) {
	g := worker.NewGroup(context.Background(), observability.NewLogger(&testutil.SyncBuffer{}, "info", "t"))
	block := func(ctx context.Context) { <-ctx.Done() }
	g.Go("same", block)
	g.Go("same", block)
	assert.Equal(t, 2, g.Running(), "names are labels, not identities")
	require.NoError(t, g.Stop(context.Background()))
	assert.Zero(t, g.Running())
}

func TestDetachAndSleep(t *testing.T) {
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "v"))
	cancel()
	ctx, stop := worker.Detach(parent, time.Minute)
	defer stop()
	require.NoError(t, ctx.Err(), "the parent's cancellation is not inherited")
	assert.Equal(t, "v", ctx.Value(ctxKey{}), "values are")
	_, ok := ctx.Deadline()
	assert.True(t, ok)

	start := time.Now()
	worker.Sleep(parent, time.Minute)
	assert.Less(t, time.Since(start), time.Second, "a done context cuts the sleep short")
	worker.Sleep(context.Background(), time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(start), time.Millisecond)
}

type ctxKey struct{}

func TestGroupStopDeadline(t *testing.T) {
	g := worker.NewGroup(context.Background(), observability.NewLogger(&testutil.SyncBuffer{}, "info", "t"))
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
	rounds       int
	dead         []uuid.UUID
}

// Claim hands out the queued messages once, like a drained outbox.
func (s *fakeStore) Claim(_ context.Context, owner string, _ time.Time, _ time.Duration, _ int) ([]app.OutboxMessage, error) {
	s.claimedOwner = owner
	msgs := s.msgs
	s.msgs = nil
	s.rounds++
	return msgs, s.claimErr
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

func (s *fakeStore) MarkDead(_ context.Context, id uuid.UUID, _ string, _ time.Time, _ string) error {
	s.dead = append(s.dead, id)
	return s.markFailErr
}

func (s *fakeStore) OldestPending(context.Context) (time.Time, bool, error) {
	return s.oldest, s.hasOldest, s.oldestErr
}

type fakePublisher struct {
	fail map[uuid.UUID]bool
	hook func(ctx context.Context, m app.OutboxMessage) // observes each call
}

func (p fakePublisher) Publish(ctx context.Context, m app.OutboxMessage) error {
	if p.hook != nil {
		p.hook(ctx, m)
	}
	if p.fail[m.EventID] {
		return errBoom
	}
	return nil
}

func newRelay(store *fakeStore, pub fakePublisher, logs *testutil.SyncBuffer) *worker.Relay {
	return worker.NewRelay(store, pub, testutil.NewFakeClock(t0), worker.RelayConfig{
		Owner: "instance-1", BatchSize: 10, Lease: time.Minute, RetryBase: time.Second, RetryMax: 30 * time.Second, PublishTime: time.Second,
		MaxAttempts: 50,
	}, observability.NewLogger(logs, "debug", "t"), testutil.NewMetrics())
}

func TestRelayPublishesAndRetries(t *testing.T) {
	ok, bad := uuid.New(), uuid.New()
	store := &fakeStore{markOK: true, failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{
		{EventID: ok, Attempts: 1}, {EventID: bad, Attempts: 3},
	}, hasOldest: true, oldest: t0.Add(-time.Second)}
	logs := &testutil.SyncBuffer{}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{bad: true}}, logs).Tick(context.Background())

	assert.Equal(t, "instance-1", store.claimedOwner)
	assert.Equal(t, []uuid.UUID{ok}, store.published)
	assert.Equal(t, t0.Add(4*time.Second), store.failed[bad], "exponential backoff")
	assert.Contains(t, logs.String(), "outbox publish failed")
}

func TestRelayBackoffIsCapped(t *testing.T) {
	id := uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: id, Attempts: 40}}}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{id: true}}, &testutil.SyncBuffer{}).Tick(context.Background())
	assert.Equal(t, t0.Add(30*time.Second), store.failed[id])
}

func TestRelayFailurePaths(t *testing.T) {
	logs := &testutil.SyncBuffer{}
	newRelay(&fakeStore{claimErr: errBoom}, fakePublisher{}, logs).Tick(context.Background())
	assert.Contains(t, logs.String(), "outbox claim failed")

	id := uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: id}}, markFailErr: errBoom, oldestErr: errBoom}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{id: true}}, logs).Tick(context.Background())
	assert.Contains(t, logs.String(), "failure not recorded; lease expiry will release it")

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
	logs := &testutil.SyncBuffer{}
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

func TestRelayRunsRoundsUntilDrainedOrCancelled(t *testing.T) {
	store := &fakeStore{markOK: true, msgs: []app.OutboxMessage{{EventID: uuid.New()}}}
	newRelay(store, fakePublisher{}, &testutil.SyncBuffer{}).Tick(context.Background())
	assert.Equal(t, 2, store.rounds, "a second round finds nothing left")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := &fakeStore{markOK: true, msgs: []app.OutboxMessage{{EventID: uuid.New()}}}
	newRelay(cancelled, fakePublisher{}, &testutil.SyncBuffer{}).Tick(ctx)
	assert.Equal(t, 1, cancelled.rounds, "shutdown stops further rounds")
}

func TestRelayDeadLettersPoisonEvents(t *testing.T) {
	poison, last := uuid.New(), uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{
		{EventID: poison, Attempts: 50}, {EventID: last, Attempts: 49},
	}}
	logs := &testutil.SyncBuffer{}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{poison: true, last: true}}, logs).Tick(context.Background())
	assert.Equal(t, []uuid.UUID{poison}, store.dead)
	assert.NotContains(t, store.failed, poison, "a dead-lettered event is not rescheduled")
	assert.Contains(t, store.failed, last, "one attempt short of the limit is still retried")
	assert.Contains(t, logs.String(), "dead-lettered after exhausting its attempts")
}

func TestRelayMarkDeadFailureIsLogged(t *testing.T) {
	poison := uuid.New()
	store := &fakeStore{failed: map[uuid.UUID]time.Time{}, msgs: []app.OutboxMessage{{EventID: poison, Attempts: 50}}, markFailErr: errBoom}
	logs := &testutil.SyncBuffer{}
	newRelay(store, fakePublisher{fail: map[uuid.UUID]bool{poison: true}}, logs).Tick(context.Background())
	assert.Equal(t, []uuid.UUID{poison}, store.dead, "MarkDead was attempted")
	assert.Contains(t, logs.String(), "failure not recorded; lease expiry will release it")
}

func TestRelayPublishesWithinPublishTimeDetachedFromShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	first, second := uuid.New(), uuid.New()
	store := &fakeStore{markOK: true, msgs: []app.OutboxMessage{{EventID: first}, {EventID: second}}}
	var seen []uuid.UUID
	pub := fakePublisher{hook: func(pctx context.Context, m app.OutboxMessage) {
		seen = append(seen, m.EventID)
		deadline, ok := pctx.Deadline()
		assert.True(t, ok, "PublishTime bounds the publication")
		assert.WithinDuration(t, time.Now().Add(time.Second), deadline, 500*time.Millisecond)
		cancel() // shutdown while publishing
		assert.NoError(t, pctx.Err(), "the publication in flight is not aborted")
	}}
	newRelay(store, pub, &testutil.SyncBuffer{}).Tick(ctx)
	assert.Equal(t, []uuid.UUID{first}, seen, "the round stops between publications; the rest expires with its lease")
	assert.Equal(t, []uuid.UUID{first}, store.published)
}
