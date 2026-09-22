package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

// Publisher delivers one outbox record to the broker.
type Publisher interface {
	// Publish sends m. It must be idempotent per EventID: a record whose
	// confirmation was lost is published again with the same EventID, and
	// the broker (or the downstream consumer) deduplicates it.
	Publish(ctx context.Context, m app.OutboxMessage) error
}

// RelayMetrics records relay activity.
type RelayMetrics interface {
	// OutboxPublished counts a record confirmed as published.
	OutboxPublished()
	// OutboxFailure counts a failed publication attempt.
	OutboxFailure()
	// OutboxDeadLettered counts a record abandoned after MaxAttempts.
	OutboxDeadLettered()
	// OutboxLag reports the age of the oldest pending record (0 when none).
	OutboxLag(d time.Duration)
}

// RelayConfig configures the relay.
type RelayConfig struct {
	// Owner identifies this instance in leases.
	Owner string
	// BatchSize bounds how many records one claim round takes.
	BatchSize int
	// Lease is how long a claim is exclusive; a crashed publisher's records
	// become claimable again after it expires.
	Lease time.Duration
	// RetryBase is the delay after the first failed publication; it doubles
	// on every failure (app.Backoff) up to RetryMax.
	RetryBase time.Duration
	// RetryMax caps the retry delay.
	RetryMax time.Duration
	// PublishTime bounds one publication, including the store confirmation.
	// It applies through Detach, so an in-flight publication completes even
	// after the relay was told to stop.
	PublishTime time.Duration
	// MaxAttempts dead-letters a record after that many failed publications.
	MaxAttempts int
}

// Relay publishes committed outbox records. Several relays (instances) can
// run at once: claims use FOR UPDATE SKIP LOCKED plus a lease, retries use
// exponential backoff, and republication keeps the eventId.
type Relay struct {
	store     app.OutboxStore
	publisher Publisher
	clock     app.Clock
	cfg       RelayConfig
	logger    *slog.Logger
	metrics   RelayMetrics
}

// NewRelay builds a relay.
func NewRelay(store app.OutboxStore, publisher Publisher, clock app.Clock, cfg RelayConfig, logger *slog.Logger, metrics RelayMetrics) *Relay {
	return &Relay{store: store, publisher: publisher, clock: clock, cfg: cfg, logger: logger, metrics: metrics}
}

// maxRounds bounds how many claim rounds one tick runs. Each round claims at
// most one record per wallet, so rounds drain wallets with several events.
const maxRounds = 20

// Tick publishes claim rounds until nothing is due, then refreshes the lag.
func (r *Relay) Tick(ctx context.Context) {
	for range maxRounds {
		if r.round(ctx) == 0 || ctx.Err() != nil {
			break
		}
	}
	r.refreshLag(ctx)
}

// round claims and publishes one batch, returning how many were claimed.
// It stops between publications once ctx is done: the publication in flight
// completes, the remaining claims simply expire with their lease.
func (r *Relay) round(ctx context.Context) int {
	msgs, err := r.store.Claim(ctx, r.cfg.Owner, r.clock.Now(), r.cfg.Lease, r.cfg.BatchSize)
	if err != nil {
		r.logger.WarnContext(ctx, "outbox claim failed", "error", err)
		return 0
	}
	for _, m := range msgs {
		if ctx.Err() != nil {
			break
		}
		r.publish(ctx, m)
	}
	return len(msgs)
}

func (r *Relay) publish(parent context.Context, m app.OutboxMessage) {
	ctx, cancel := Detach(parent, r.cfg.PublishTime)
	defer cancel()
	log := r.logger.With("eventId", m.EventID, "eventType", m.EventType, "aggregateId", m.AggregateID)
	if err := r.publisher.Publish(ctx, m); err != nil {
		r.failed(ctx, log, m, err)
		return
	}
	r.confirm(ctx, log, m)
}

// failed schedules a retry with backoff, or dead-letters a record that
// exhausted its attempts so it stops blocking its wallet's later events.
func (r *Relay) failed(ctx context.Context, log *slog.Logger, m app.OutboxMessage, cause error) {
	r.metrics.OutboxFailure()
	var err error
	if m.Attempts >= r.cfg.MaxAttempts {
		log.ErrorContext(ctx, "outbox event dead-lettered after exhausting its attempts", "attempts", m.Attempts, "error", cause)
		r.metrics.OutboxDeadLettered()
		err = r.store.MarkDead(ctx, m.EventID, r.cfg.Owner, r.clock.Now(), cause.Error())
	} else {
		log.WarnContext(ctx, "outbox publish failed", "attempts", m.Attempts, "error", cause)
		err = r.store.MarkFailed(ctx, m.EventID, r.cfg.Owner, r.clock.Now().Add(r.backoff(m.Attempts)), cause.Error())
	}
	if err != nil {
		log.WarnContext(ctx, "outbox failure not recorded; lease expiry will release it", "error", err)
	}
}

// confirm records the publication. If this fails (or the lease was lost) the
// record is published again later with the same eventId.
func (r *Relay) confirm(ctx context.Context, log *slog.Logger, m app.OutboxMessage) {
	ok, err := r.store.MarkPublished(ctx, m.EventID, r.cfg.Owner, r.clock.Now())
	if err != nil || !ok {
		log.WarnContext(ctx, "outbox publication not confirmed; it will be republished with the same eventId", "error", err)
		return
	}
	r.metrics.OutboxPublished()
}

// backoff is the delay before the next attempt after attempts failures.
func (r *Relay) backoff(attempts int) time.Duration {
	return app.Backoff(r.cfg.RetryBase, r.cfg.RetryMax, attempts-1)
}

func (r *Relay) refreshLag(ctx context.Context) {
	oldest, ok, err := r.store.OldestPending(ctx)
	if err != nil {
		return
	}
	lag := time.Duration(0)
	if ok {
		lag = r.clock.Now().Sub(oldest)
	}
	r.metrics.OutboxLag(lag)
}
