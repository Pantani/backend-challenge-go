package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

type budgetStore struct {
	app.OutboxStore
	advance func(time.Duration)
}

func (s budgetStore) StartAttempt(context.Context, uuid.UUID, uuid.UUID, time.Time, time.Duration) (int, bool, error) {
	s.advance(4 * time.Second)
	return 1, true, nil
}

func (budgetStore) MarkPublished(context.Context, uuid.UUID, uuid.UUID, time.Time) (bool, error) {
	return true, nil
}

type budgetPublisher struct{}

func (budgetPublisher) Publish(context.Context, app.OutboxMessage) error { return nil }

type budgetMetrics struct{}

func (budgetMetrics) OutboxPublished()        {}
func (budgetMetrics) OutboxFailure()          {}
func (budgetMetrics) OutboxDeadLettered()     {}
func (budgetMetrics) OutboxLag(time.Duration) {}

func TestRelaySharesFinalizeTimeAcrossDurablePhases(t *testing.T) {
	current := time.Unix(0, 0)
	advance := func(d time.Duration) { current = current.Add(d) }
	var timeouts []time.Duration
	relay := NewRelay(
		budgetStore{advance: advance}, budgetPublisher{}, app.SystemClock{},
		RelayConfig{PublishTime: 5 * time.Second, FinalizeTime: 10 * time.Second, MaxAttempts: 1},
		slog.New(slog.NewTextHandler(io.Discard, nil)), budgetMetrics{},
	)
	relay.timeoutNow = func() time.Time { return current }
	relay.detachContext = func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		timeouts = append(timeouts, timeout)
		return context.WithCancel(context.WithoutCancel(parent))
	}

	relay.publish(context.Background(), app.OutboxMessage{EventID: uuid.New(), ClaimID: uuid.New()})

	require.Equal(t, []time.Duration{10 * time.Second, 5 * time.Second, 6 * time.Second}, timeouts)
	assert.LessOrEqual(t, 4*time.Second+timeouts[1]+timeouts[2], 15*time.Second,
		"actual StartAttempt time, PublishTime, and terminal budget fit FinalizeTime + PublishTime")
}
