package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

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
	// BatchSize bounds how many singular claims one round processes.
	BatchSize int
	// Lease is how long a claim is exclusive; a crashed publisher's records
	// become claimable again after it expires.
	Lease time.Duration
	// RetryBase is the delay after the first failed publication; it doubles
	// on every failure (app.Backoff) up to RetryMax.
	RetryBase time.Duration
	// RetryMax caps the retry delay.
	RetryMax time.Duration
	// PublishTime bounds one broker publication.
	PublishTime time.Duration
	// FinalizeTime is the total budget shared by attempt accounting and the
	// durable confirmation, retry, or dead-letter mutation.
	FinalizeTime time.Duration
	// MaxAttempts dead-letters a record after that many failed publications.
	MaxAttempts int
}

// Relay publishes committed outbox records. Several relays (instances) can
// run at once: claims use FOR UPDATE SKIP LOCKED plus a lease, retries use
// exponential backoff, and republication keeps the eventId.
type Relay struct {
	store         app.OutboxStore
	publisher     Publisher
	clock         app.Clock
	cfg           RelayConfig
	logger        *slog.Logger
	metrics       RelayMetrics
	timeoutNow    func() time.Time
	detachContext func(context.Context, time.Duration) (context.Context, context.CancelFunc)
}

// NewRelay builds a relay.
func NewRelay(store app.OutboxStore, publisher Publisher, clock app.Clock, cfg RelayConfig, logger *slog.Logger, metrics RelayMetrics) *Relay {
	return &Relay{
		store: store, publisher: publisher, clock: clock, cfg: cfg, logger: logger, metrics: metrics,
		timeoutNow: time.Now, detachContext: Detach,
	}
}

type finalizationBudget struct {
	remaining time.Duration
	now       func() time.Time
	detach    func(context.Context, time.Duration) (context.Context, context.CancelFunc)
}

func (b *finalizationBudget) context(parent context.Context) (context.Context, context.CancelFunc) {
	started := b.now()
	ctx, cancel := b.detach(parent, b.remaining)
	return ctx, func() {
		cancel()
		b.remaining = subtractElapsed(b.remaining, b.now().Sub(started))
	}
}

func subtractElapsed(remaining, elapsed time.Duration) time.Duration {
	if elapsed <= 0 {
		return remaining
	}
	if elapsed >= remaining {
		return 0
	}
	return remaining - elapsed
}

// maxRounds bounds how many acquisition rounds one tick runs. Each round
// processes up to BatchSize singular claims, so repeated rounds drain wallets.
const maxRounds = 20

// Tick publishes singular-acquisition rounds until nothing is due, then
// refreshes the lag.
func (r *Relay) Tick(ctx context.Context) {
	for range maxRounds {
		if !r.round(ctx) || ctx.Err() != nil {
			break
		}
	}
	r.refreshLag(ctx)
}

// round claims and publishes up to BatchSize records, acquiring each claim
// immediately before its work. It reports whether another round may have due
// work and stops before acquiring a claim after cancellation.
func (r *Relay) round(ctx context.Context) bool {
	for range r.cfg.BatchSize {
		if ctx.Err() != nil {
			return false
		}
		m, ok, err := r.store.Claim(ctx, r.cfg.Owner, uuid.New(), r.clock.Now(), r.cfg.Lease)
		if err != nil {
			r.logger.WarnContext(ctx, "outbox claim failed", "error", err)
			return false
		}
		if !ok {
			return false
		}
		r.publish(ctx, m)
	}
	return true
}

func (r *Relay) publish(parent context.Context, m app.OutboxMessage) {
	log := r.logger.With("eventId", m.EventID, "eventType", m.EventType, "aggregateId", m.AggregateID)
	finalize := &finalizationBudget{remaining: r.cfg.FinalizeTime, now: r.timeoutNow, detach: r.detachContext}
	attempts, ok := r.startAttempt(parent, log, m, finalize)
	if !ok {
		return
	}
	m.Attempts = attempts
	publishCtx, cancelPublish := r.detachContext(parent, r.cfg.PublishTime)
	err := r.publisher.Publish(publishCtx, m)
	cancelPublish()
	finalizeCtx, cancelFinalize := finalize.context(parent)
	defer cancelFinalize()
	if err != nil {
		r.failed(finalizeCtx, log, m, err)
		return
	}
	r.confirm(finalizeCtx, log, m)
}

func (r *Relay) startAttempt(
	parent context.Context, log *slog.Logger, m app.OutboxMessage, finalize *finalizationBudget,
) (int, bool) {
	ctx, cancel := finalize.context(parent)
	defer cancel()
	attempts, ok, err := r.store.StartAttempt(ctx, m.EventID, m.ClaimID)
	if err != nil || !ok {
		log.WarnContext(ctx, "outbox publication attempt not started; claim was lost", "error", err)
		return 0, false
	}
	return attempts, true
}

// failed schedules a retry with backoff, or dead-letters a record that
// exhausted its attempts so it stops blocking its wallet's later events.
func (r *Relay) failed(ctx context.Context, log *slog.Logger, m app.OutboxMessage, cause error) {
	if m.Attempts >= r.cfg.MaxAttempts {
		ok, err := r.store.MarkDead(ctx, m.EventID, m.ClaimID, r.clock.Now(), cause.Error())
		if err != nil || !ok {
			log.WarnContext(ctx, "outbox failure not recorded; lease expiry will release it", "error", err)
			return
		}
		r.metrics.OutboxFailure()
		r.metrics.OutboxDeadLettered()
		log.ErrorContext(ctx, "outbox event dead-lettered after exhausting its attempts", "attempts", m.Attempts, "error", cause)
		return
	}
	ok, err := r.store.MarkFailed(ctx, m.EventID, m.ClaimID, r.clock.Now().Add(r.backoff(m.Attempts)), cause.Error())
	if err != nil || !ok {
		log.WarnContext(ctx, "outbox failure not recorded; lease expiry will release it", "error", err)
		return
	}
	r.metrics.OutboxFailure()
	log.WarnContext(ctx, "outbox publish failed", "attempts", m.Attempts, "error", cause)
}

// confirm records the publication. If this fails (or the lease was lost) the
// record is published again later with the same eventId.
func (r *Relay) confirm(ctx context.Context, log *slog.Logger, m app.OutboxMessage) {
	ok, err := r.store.MarkPublished(ctx, m.EventID, m.ClaimID, r.clock.Now())
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
