package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

// Publisher delivers one outbox record to the broker.
type Publisher interface {
	Publish(ctx context.Context, m app.OutboxMessage) error
}

// RelayMetrics records relay activity.
type RelayMetrics interface {
	OutboxPublished()
	OutboxFailure()
	OutboxDeadLettered()
	OutboxLag(d time.Duration)
}

// RelayConfig configures the relay.
type RelayConfig struct {
	// Owner identifies this instance in leases.
	Owner     string
	BatchSize int
	// Lease is how long a claim is exclusive; a crashed publisher's records
	// become claimable again after it expires.
	Lease       time.Duration
	RetryBase   time.Duration
	RetryMax    time.Duration
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
func (r *Relay) round(ctx context.Context) int {
	msgs, err := r.store.Claim(ctx, r.cfg.Owner, r.clock.Now(), r.cfg.Lease, r.cfg.BatchSize)
	if err != nil {
		r.logger.WarnContext(ctx, "outbox claim failed", "error", err)
		return 0
	}
	for _, m := range msgs {
		r.publish(ctx, m)
	}
	return len(msgs)
}

func (r *Relay) publish(parent context.Context, m app.OutboxMessage) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), r.cfg.PublishTime)
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

func (r *Relay) backoff(attempts int) time.Duration {
	delay := r.cfg.RetryBase << min(max(attempts-1, 0), 16)
	return min(delay, r.cfg.RetryMax)
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

// PendingService resolves due pending references.
type PendingService interface {
	ResolveDue(ctx context.Context) (int, error)
}

// PendingResolver periodically resolves PENDING_REFERENCE operations.
type PendingResolver struct {
	svc    PendingService
	logger *slog.Logger
}

// NewPendingResolver builds the resolver.
func NewPendingResolver(svc PendingService, logger *slog.Logger) *PendingResolver {
	return &PendingResolver{svc: svc, logger: logger}
}

// Tick resolves one batch.
func (p *PendingResolver) Tick(ctx context.Context) {
	n, err := p.svc.ResolveDue(ctx)
	if err != nil && ctx.Err() == nil {
		p.logger.WarnContext(ctx, "pending reference batch failed", "error", err)
	}
	if n > 0 {
		p.logger.InfoContext(ctx, "pending references resolved", "count", n)
	}
}
