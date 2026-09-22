//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/test/testenv"
)

func openWallet(t *testing.T, amount string) testenv.Wallet {
	t.Helper()
	return instances[0].client().OpenWallet(t, amount)
}

func submit(t *testing.T, inst *instance, w testenv.Wallet, ext, kind, amount, ref string) testenv.Response {
	t.Helper()
	res, err := submitE(inst, w, ext, kind, amount, ref)
	require.NoError(t, err)
	return res
}

// submitE is the goroutine-safe variant of submit.
func submitE(inst *instance, w testenv.Wallet, ext, kind, amount, ref string) (testenv.Response, error) {
	return inst.client().Submit(context.Background(), w, "provider-a", ext, kind, amount, ref)
}

func balance(t *testing.T, w testenv.Wallet) string {
	t.Helper()
	res := call(t, instances[1], http.MethodGet, "/wallets/"+w.ID, "wallet-service", "", nil)
	require.Equal(t, http.StatusOK, res.Status)
	return res.Body["balance"].(map[string]any)["amount"].(string)
}

func debits(t *testing.T, w testenv.Wallet) int {
	t.Helper()
	n, err := testenv.CountDebits(context.Background(), pool, w.ID)
	require.NoError(t, err)
	return n
}

type outcome struct {
	res testenv.Response
	err error
}

// concurrently submits n requests at once, round-robin across the
// instances, and fails the test on the test goroutine if any request failed
// (FailNow must never run on a worker goroutine).
func concurrently(t *testing.T, n int, fn func(i int, inst *instance) (testenv.Response, error)) []testenv.Response {
	t.Helper()
	outcomes := testenv.Parallel(n, func(i int) outcome {
		res, err := fn(i, instances[i%len(instances)])
		return outcome{res: res, err: err}
	})
	results, errs := make([]testenv.Response, n), make([]error, n)
	for i, o := range outcomes {
		results[i], errs[i] = o.res, o.err
	}
	require.NoError(t, errors.Join(errs...))
	return results
}

func statusCount(results []testenv.Response) map[int]int {
	count := map[int]int{}
	for _, r := range results {
		count[r.Status]++
	}
	return count
}

func TestFiftyIdenticalBetsAcrossThreeInstances(t *testing.T) {
	w := openWallet(t, "1000.00")
	ext := uuid.NewString()
	results := concurrently(t, 50, func(_ int, inst *instance) (testenv.Response, error) {
		return submitE(inst, w, ext, "BET", "25.00", "")
	})
	assert.Equal(t, map[int]int{http.StatusCreated: 1, http.StatusOK: 49}, statusCount(results), "one processing, 49 idempotent replays")
	assert.Equal(t, 1, debits(t, w))
	assert.Equal(t, "975.00", balance(t, w))
}

func TestTwoBetsRaceAcrossInstances(t *testing.T) {
	w := openWallet(t, "100.00")
	exts := []string{uuid.NewString(), uuid.NewString()}
	results := concurrently(t, 2, func(i int, inst *instance) (testenv.Response, error) {
		return submitE(inst, w, exts[i], "BET", "80.00", "")
	})

	statuses := map[string]string{}
	for _, r := range results {
		statuses[r.Body["status"].(string)] = fmt.Sprint(r.Body["failureCode"])
	}
	assert.Equal(t, map[string]string{"PROCESSED": "<nil>", "REJECTED": "INSUFFICIENT_FUNDS"}, statuses)
	assert.Equal(t, "20.00", balance(t, w))
	assert.Equal(t, 1, debits(t, w))

	concurrently(t, 4, func(i int, inst *instance) (testenv.Response, error) {
		return submitE(inst, w, exts[i%2], "BET", "80.00", "")
	})
	assert.Equal(t, "20.00", balance(t, w), "resending does not change the outcome")
}

func TestDistinctWalletsInParallelAcrossInstances(t *testing.T) {
	wallets := []testenv.Wallet{openWallet(t, "100.00"), openWallet(t, "100.00"), openWallet(t, "100.00"), openWallet(t, "100.00")}
	results := concurrently(t, 40, func(i int, inst *instance) (testenv.Response, error) {
		return submitE(inst, wallets[i%4], uuid.NewString(), "BET", "10.00", "")
	})
	assert.Equal(t, map[int]int{http.StatusCreated: 40}, statusCount(results))
	for _, w := range wallets {
		assert.Equal(t, "0.00", balance(t, w))
	}
}

func sendMessage(t *testing.T, messageID string, w testenv.Wallet, ext, kind, amount string) {
	t.Helper()
	env := testenv.Envelope(messageID, testenv.SubmitInput(w, "provider-a", ext, kind, amount, ""))
	require.NoError(t, testenv.SendMessage(context.Background(), sqsClient, queues.Input, env))
}

func TestSameOperationThroughHTTPAndSQSAcrossInstances(t *testing.T) {
	w := openWallet(t, "100.00")
	ext := uuid.NewString()
	sendMessage(t, "msg-"+ext, w, ext, "BET", "10.00")
	sendMessage(t, "msg-dup-"+ext, w, ext, "BET", "10.00") // same operation, other message
	res := submit(t, instances[2], w, ext, "BET", "10.00", "")
	require.Contains(t, []int{http.StatusCreated, http.StatusOK}, res.Status)

	require.Eventually(t, func() bool {
		var done int
		_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM inbox_messages
			WHERE message_id IN ($1, $2) AND processed_at IS NOT NULL`, "msg-"+ext, "msg-dup-"+ext).Scan(&done)
		return done == 2
	}, 30*time.Second, 200*time.Millisecond)
	assert.Equal(t, "90.00", balance(t, w))
	assert.Equal(t, 1, debits(t, w))
}

func TestRefundBeforeBetAcrossInstances(t *testing.T) {
	w := openWallet(t, "100.00")
	bet, refund := uuid.NewString(), uuid.NewString()
	pending := submit(t, instances[0], w, refund, "REFUND", "40.00", bet)
	require.Equal(t, http.StatusAccepted, pending.Status)
	assert.Equal(t, "PENDING_REFERENCE", pending.Body["status"])

	require.Equal(t, http.StatusCreated, submit(t, instances[1], w, bet, "BET", "40.00", "").Status)
	require.Eventually(t, func() bool {
		res := call(t, instances[2], http.MethodGet, "/providers/provider-a/wagering/transactions/"+refund, "provider-a", "", nil)
		return res.Body["status"] == "PROCESSED"
	}, 20*time.Second, 200*time.Millisecond, "resolved by the worker of whichever instance got it")
	assert.Equal(t, "100.00", balance(t, w))

	other := call(t, instances[2], http.MethodGet, "/wagering/transactions/"+pending.Body["transactionId"].(string), "provider-b", "", nil)
	assert.Equal(t, http.StatusNotFound, other.Status, "provider isolation holds on every instance")
}

// TestZZCrashAndRestart kills every instance (no graceful shutdown) and
// verifies that idempotency, pending references and balances survive.
func TestZZCrashAndRestart(t *testing.T) {
	w := openWallet(t, "100.00")
	bet, refund, lateBet := uuid.NewString(), uuid.NewString(), uuid.NewString()
	first := submit(t, instances[0], w, bet, "BET", "30.00", "")
	require.Equal(t, http.StatusCreated, first.Status)
	require.Equal(t, http.StatusAccepted, submit(t, instances[1], w, refund, "REFUND", "20.00", lateBet).Status)

	for _, inst := range instances {
		inst.kill()
	}
	for _, inst := range instances {
		require.NoError(t, inst.start())
	}

	replay := submit(t, instances[2], w, bet, "BET", "30.00", "")
	assert.Equal(t, http.StatusOK, replay.Status)
	assert.Equal(t, first.Body["transactionId"], replay.Body["transactionId"])
	require.Equal(t, http.StatusCreated, submit(t, instances[0], w, lateBet, "BET", "20.00", "").Status)
	require.Eventually(t, func() bool {
		res := call(t, instances[1], http.MethodGet, "/providers/provider-a/wagering/transactions/"+refund, "provider-a", "", nil)
		return res.Body["status"] == "PROCESSED"
	}, 30*time.Second, 200*time.Millisecond, "the pending reference survived the crash")
	assert.Equal(t, "70.00", balance(t, w))
}

// TestZZZFinalConsistency reconciles every wallet and checks the outbox.
func TestZZZFinalConsistency(t *testing.T) {
	rows, err := pool.Query(context.Background(), `SELECT id FROM wallets`)
	require.NoError(t, err)
	var ids []string
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id.String())
	}
	require.NoError(t, rows.Err())
	for i, id := range ids {
		rec := call(t, instances[i%3], http.MethodPost, "/wallets/"+id+"/reconciliation", "wallet-service", "", nil)
		require.Equal(t, http.StatusOK, rec.Status)
		assert.Equal(t, true, rec.Body["consistent"], id)
	}

	var negative int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM wallets WHERE balance_minor < 0`).Scan(&negative))
	assert.Zero(t, negative)
	require.Eventually(t, func() bool {
		var pending int
		_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&pending)
		return pending == 0
	}, 30*time.Second, 200*time.Millisecond, "every committed event was published by some instance")
}
