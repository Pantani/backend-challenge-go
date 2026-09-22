package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

func pendingRefund(t *testing.T, h *harness, ref string) app.SubmitResult {
	t.Helper()
	w := h.openWallet(t, "100.00")
	h.submit(t, w, op{ext: "b", kind: "BET", amount: "30.00"})
	res := h.submit(t, w, op{ext: "r", kind: "REFUND", amount: "30.00", ref: ref})
	require.Equal(t, wager.StatusPendingReference, res.Transaction.Status())
	h.clock.Advance(time.Hour)
	return res
}

func (h *harness) status(t *testing.T, res app.SubmitResult) wager.Status {
	t.Helper()
	got, err := h.wagers.Get(context.Background(), app.Caller{Internal: true}, res.Transaction.ID())
	require.NoError(t, err)
	return got.Status()
}

func TestResolveDueListingFailureAndCancellation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.store.failOn("q.due", errBoom)
	_, err := h.wagers.ResolveDue(context.Background())
	require.ErrorIs(t, err, errBoom)

	pendingRefund(t, h, "missing")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err := h.wagers.ResolveDue(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, n)
}

func TestResolveDueSkipsClaimedAndTransientFailures(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	res := pendingRefund(t, h, "missing")

	h.store.failOn("txs.lock", app.ErrNotDue)
	assert.Zero(t, h.resolve(t))
	h.store.failOn("txs.lock", app.ErrUnavailable)
	assert.Zero(t, h.resolve(t))
	assert.Contains(t, h.logs.String(), "pending reference retry postponed")
	assert.Equal(t, wager.StatusPendingReference, h.status(t, res))
}

func TestResolveDuePermanentFailureMarksFailed(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	res := pendingRefund(t, h, "missing")
	h.store.failOn("txs.getByExternal", errBoom)

	assert.Zero(t, h.resolve(t))
	assert.Equal(t, wager.StatusFailed, h.status(t, res))
}

func TestMarkFailedCanFail(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	res := pendingRefund(t, h, "missing")
	h.store.failOn("wallets.get", errBoom)
	h.store.failOn("txs.lock", app.ErrNotDue)

	assert.Zero(t, h.resolve(t))
	assert.Contains(t, h.logs.String(), "could not mark pending reference as failed")
	assert.Equal(t, wager.StatusPendingReference, h.status(t, res))
}

func TestDefensiveTransitionsOnTerminalRows(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"await", "process"} {
		h := newHarness(t)
		res := pendingRefund(t, h, "missing")
		if name == "process" {
			// Make the reference processable after the refund was deferred.
			h.store.putTx(res.Transaction.ID(), func(s *wager.Snapshot) { s.External.ReferenceExternalID = "b" })
		}
		h.store.lockMutate = func(s *wager.Snapshot) {
			s.Status, s.ResultBalance = wager.StatusProcessed, brl(t, "1.00")
		}
		assert.Zero(t, h.resolve(t), name)
		assert.Contains(t, h.logs.String(), "could not mark pending reference as failed", name)
	}
}

func TestDefensiveRejectOnTerminalRow(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	res := pendingRefund(t, h, "missing")
	h.store.putTx(res.Transaction.ID(), func(s *wager.Snapshot) { s.Attempts = 99 })
	h.store.lockMutate = func(s *wager.Snapshot) {
		s.Status, s.FailureCode = wager.StatusRejected, wager.CodeReferenceNotFound
	}
	assert.Zero(t, h.resolve(t))
	assert.Contains(t, h.logs.String(), "pending reference failed permanently")
}

func TestPendingPolicyBackoff(t *testing.T) {
	t.Parallel()
	p := app.PendingPolicy{BaseDelay: time.Second, MaxDelay: time.Minute}
	assert.Equal(t, t0.Add(time.Second), p.Next(0, t0))
	assert.Equal(t, t0.Add(8*time.Second), p.Next(3, t0))
	assert.Equal(t, t0.Add(time.Minute), p.Next(10, t0))
	assert.Equal(t, t0.Add(time.Minute), p.Next(1000, t0))
	assert.Equal(t, t0.Add(time.Minute), app.PendingPolicy{MaxDelay: time.Minute}.Next(1, t0))
}

func TestIsTransient(t *testing.T) {
	t.Parallel()
	for _, err := range []error{app.ErrConflict, app.ErrUnavailable, context.Canceled, context.DeadlineExceeded} {
		assert.True(t, app.IsTransient(err), err)
	}
	assert.False(t, app.IsTransient(app.ErrValidation))
	assert.False(t, app.IsTransient(errBoom))
}
