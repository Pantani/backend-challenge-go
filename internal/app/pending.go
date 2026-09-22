package app

import (
	"context"
	"errors"

	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

// ResolveDue retries one batch of due PENDING_REFERENCE operations and
// returns how many were concluded or rescheduled. Several instances can run
// it concurrently: each operation is claimed with SKIP LOCKED under its
// wallet lock, and state lives in PostgreSQL, so a restart loses nothing.
func (s *WagerService) ResolveDue(ctx context.Context) (int, error) {
	due, err := s.Queries.ListDuePending(ctx, s.Clock.Now(), s.Policy.BatchSize)
	if err != nil {
		return 0, err
	}
	resolved := 0
	for _, d := range due {
		if ctx.Err() != nil {
			break
		}
		if s.resolveOne(ctx, d) {
			resolved++
		}
	}
	return resolved, ctx.Err()
}

func (s *WagerService) resolveOne(ctx context.Context, d DueTransaction) bool {
	var status wager.Status
	err := retryConflicts(s.ConflictRetries, s.onConflict("pending"), func() error {
		return s.UoW.Do(ctx, func(ctx context.Context, r Repositories) error {
			var err error
			status, err = s.resolveInTx(ctx, r, d)
			return err
		})
	})
	switch {
	case err == nil:
		s.Metrics.PendingResolution(status)
		return true
	case errors.Is(err, ErrNotDue):
		return false
	case IsTransient(err):
		s.Logger.WarnContext(ctx, "pending reference retry postponed", "transactionId", d.ID, "walletId", d.WalletID, "error", err)
		return false
	}
	s.markFailed(ctx, d, err)
	return false
}

func (s *WagerService) resolveInTx(ctx context.Context, r Repositories, d DueTransaction) (wager.Status, error) {
	w, err := r.Wallets().GetForUpdate(ctx, d.WalletID)
	if err != nil {
		return "", err
	}
	tx, err := r.Transactions().LockDuePending(ctx, d.ID, s.Clock.Now())
	if err != nil {
		return "", err
	}
	out, err := s.conclude(ctx, r, w, tx, "")
	if err != nil {
		return "", err
	}
	return tx.Status(), s.persist(ctx, r, out, false)
}

// markFailed records a permanent, non-business failure for auditing so the
// operation stops being retried forever.
func (s *WagerService) markFailed(ctx context.Context, d DueTransaction, cause error) {
	s.Logger.ErrorContext(ctx, "pending reference failed permanently", "transactionId", d.ID, "walletId", d.WalletID, "error", cause)
	err := s.UoW.Do(ctx, func(ctx context.Context, r Repositories) error {
		tx, err := r.Transactions().LockDuePending(ctx, d.ID, s.Clock.Now())
		if err != nil {
			return err
		}
		if err := tx.Fail(wager.CodeInternalFailure, s.Clock.Now()); err != nil {
			return err
		}
		return r.Transactions().Save(ctx, tx)
	})
	if err != nil {
		s.Logger.ErrorContext(ctx, "could not mark pending reference as failed", "transactionId", d.ID, "error", err)
		return
	}
	s.Metrics.PendingResolution(wager.StatusFailed)
}
