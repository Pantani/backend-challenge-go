package app

import (
	"errors"
	"time"
)

// Metric and log sources.
const (
	SourceHTTP   = "http"
	SourceSQS    = "sqs"
	SourceWorker = "worker"
)

// PendingPolicy controls how PENDING_REFERENCE operations are retried.
type PendingPolicy struct {
	// BaseDelay is the delay after the first deferral; it doubles each time.
	BaseDelay time.Duration
	// MaxDelay caps the exponential backoff.
	MaxDelay time.Duration
	// MaxAttempts is how many deferrals happen before the operation is
	// rejected with REFERENCE_NOT_FOUND (or REFERENCE_NOT_PROCESSED).
	MaxAttempts int
	// BatchSize bounds how many due operations one worker tick resolves.
	BatchSize int
}

// maxShift keeps BaseDelay << attempts far from int64 overflow.
const maxShift = 20

// Next returns when the attempt-th retry should run (exponential backoff).
func (p PendingPolicy) Next(attempts int, now time.Time) time.Time {
	delay := p.BaseDelay << min(attempts, maxShift)
	if delay <= 0 || delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	return now.Add(delay)
}

// retryConflicts runs fn again while it fails with ErrConflict, at most
// retries extra times. Conflicts come from losing a race inside PostgreSQL
// (unique violation, stale version, deadlock, serialization failure); the
// retried transaction observes the committed winner and converges.
func retryConflicts(retries int, onConflict func(), fn func() error) error {
	err := fn()
	for i := 0; i < retries && errors.Is(err, ErrConflict); i++ {
		onConflict()
		err = fn()
	}
	return err
}

// sequence runs steps in order and stops at the first error. It keeps
// multi-step persistence flat and readable.
func sequence(steps ...func() error) error {
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}
