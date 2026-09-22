//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testWallet struct {
	id, player string
}

func openWallet(t *testing.T, amount string) testWallet {
	t.Helper()
	player := uuid.NewString()
	res := call(t, instances[0], http.MethodPost, "/wallets", "wallet-service",
		`{"playerId":"`+player+`","initialBalance":{"amount":"`+amount+`","currency":"BRL"}}`, nil)
	require.Equal(t, http.StatusCreated, res.status, res.body)
	return testWallet{id: res.body["id"].(string), player: player}
}

func opBody(w testWallet, ext, kind, amount, ref string) string {
	body := map[string]any{
		"providerId": "provider-a", "externalTransactionId": ext, "playerId": w.player, "walletId": w.id,
		"roundId": "round-1", "gameId": "game-1", "kind": kind, "money": map[string]string{"amount": amount, "currency": "BRL"},
	}
	if ref != "" {
		body["referenceExternalTransactionId"] = ref
	}
	raw, _ := json.Marshal(body)
	return string(raw)
}

func submit(t *testing.T, inst *instance, w testWallet, ext, kind, amount, ref string) response {
	t.Helper()
	return call(t, inst, http.MethodPost, "/wagering/transactions", "provider-a", opBody(w, ext, kind, amount, ref),
		map[string]string{"Idempotency-Key": "provider-a:" + ext})
}

func balance(t *testing.T, w testWallet) string {
	t.Helper()
	res := call(t, instances[1], http.MethodGet, "/wallets/"+w.id, "wallet-service", "", nil)
	require.Equal(t, http.StatusOK, res.status)
	return res.body["balance"].(map[string]any)["amount"].(string)
}

func debits(t *testing.T, w testWallet) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.id).Scan(&n))
	return n
}

// concurrently runs fn n times at once, round-robin across the instances.
func concurrently(n int, fn func(i int, inst *instance)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			fn(i, instances[i%len(instances)])
		}()
	}
	close(start)
	wg.Wait()
}

func TestFiftyIdenticalBetsAcrossThreeInstances(t *testing.T) {
	w := openWallet(t, "1000.00")
	ext := uuid.NewString()
	statuses := make([]int, 50)
	concurrently(50, func(i int, inst *instance) { statuses[i] = submit(t, inst, w, ext, "BET", "25.00", "").status })

	count := map[int]int{}
	for _, s := range statuses {
		count[s]++
	}
	assert.Equal(t, map[int]int{http.StatusCreated: 1, http.StatusOK: 49}, count, "one processing, 49 idempotent replays")
	assert.Equal(t, 1, debits(t, w))
	assert.Equal(t, "975.00", balance(t, w))
}

func TestTwoBetsRaceAcrossInstances(t *testing.T) {
	w := openWallet(t, "100.00")
	exts := []string{uuid.NewString(), uuid.NewString()}
	results := make([]response, 2)
	concurrently(2, func(i int, inst *instance) { results[i] = submit(t, inst, w, exts[i], "BET", "80.00", "") })

	statuses := map[string]string{}
	for _, r := range results {
		statuses[r.body["status"].(string)] = fmt.Sprint(r.body["failureCode"])
	}
	assert.Equal(t, map[string]string{"PROCESSED": "<nil>", "REJECTED": "INSUFFICIENT_FUNDS"}, statuses)
	assert.Equal(t, "20.00", balance(t, w))
	assert.Equal(t, 1, debits(t, w))

	concurrently(4, func(i int, inst *instance) { submit(t, inst, w, exts[i%2], "BET", "80.00", "") })
	assert.Equal(t, "20.00", balance(t, w), "resending does not change the outcome")
}

func TestDistinctWalletsInParallelAcrossInstances(t *testing.T) {
	wallets := []testWallet{openWallet(t, "100.00"), openWallet(t, "100.00"), openWallet(t, "100.00"), openWallet(t, "100.00")}
	concurrently(40, func(i int, inst *instance) {
		res := submit(t, inst, wallets[i%4], uuid.NewString(), "BET", "10.00", "")
		assert.Equal(t, http.StatusCreated, res.status)
	})
	for _, w := range wallets {
		assert.Equal(t, "0.00", balance(t, w))
	}
}

func sendMessage(t *testing.T, messageID string, w testWallet, ext, kind, amount string) {
	t.Helper()
	var data map[string]any
	require.NoError(t, json.Unmarshal([]byte(opBody(w, ext, kind, amount, "")), &data))
	data["idempotencyKey"] = "provider-a:" + ext
	body, err := json.Marshal(map[string]any{"messageId": messageID, "type": "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339), "data": data})
	require.NoError(t, err)
	_, err = sqsClient.SendMessage(context.Background(), &awssqs.SendMessageInput{QueueUrl: aws.String(queues.Input),
		MessageBody: aws.String(string(body)), MessageGroupId: aws.String(w.id), MessageDeduplicationId: aws.String(messageID)})
	require.NoError(t, err)
}

func TestSameOperationThroughHTTPAndSQSAcrossInstances(t *testing.T) {
	w := openWallet(t, "100.00")
	ext := uuid.NewString()
	sendMessage(t, "msg-"+ext, w, ext, "BET", "10.00")
	sendMessage(t, "msg-dup-"+ext, w, ext, "BET", "10.00") // same operation, other message
	res := submit(t, instances[2], w, ext, "BET", "10.00", "")
	require.Contains(t, []int{http.StatusCreated, http.StatusOK}, res.status)

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
	require.Equal(t, http.StatusAccepted, pending.status)
	assert.Equal(t, "PENDING_REFERENCE", pending.body["status"])

	require.Equal(t, http.StatusCreated, submit(t, instances[1], w, bet, "BET", "40.00", "").status)
	require.Eventually(t, func() bool {
		res := call(t, instances[2], http.MethodGet, "/providers/provider-a/wagering/transactions/"+refund, "provider-a", "", nil)
		return res.body["status"] == "PROCESSED"
	}, 20*time.Second, 200*time.Millisecond, "resolved by the worker of whichever instance got it")
	assert.Equal(t, "100.00", balance(t, w))

	other := call(t, instances[2], http.MethodGet, "/wagering/transactions/"+pending.body["transactionId"].(string), "provider-b", "", nil)
	assert.Equal(t, http.StatusNotFound, other.status, "provider isolation holds on every instance")
}

// TestZZCrashAndRestart kills every instance (no graceful shutdown) and
// verifies that idempotency, pending references and balances survive.
func TestZZCrashAndRestart(t *testing.T) {
	w := openWallet(t, "100.00")
	bet, refund, lateBet := uuid.NewString(), uuid.NewString(), uuid.NewString()
	first := submit(t, instances[0], w, bet, "BET", "30.00", "")
	require.Equal(t, http.StatusCreated, first.status)
	require.Equal(t, http.StatusAccepted, submit(t, instances[1], w, refund, "REFUND", "20.00", lateBet).status)

	for _, inst := range instances {
		inst.kill()
	}
	for _, inst := range instances {
		require.NoError(t, inst.start())
	}

	replay := submit(t, instances[2], w, bet, "BET", "30.00", "")
	assert.Equal(t, http.StatusOK, replay.status)
	assert.Equal(t, first.body["transactionId"], replay.body["transactionId"])
	require.Equal(t, http.StatusCreated, submit(t, instances[0], w, lateBet, "BET", "20.00", "").status)
	require.Eventually(t, func() bool {
		res := call(t, instances[1], http.MethodGet, "/providers/provider-a/wagering/transactions/"+refund, "provider-a", "", nil)
		return res.body["status"] == "PROCESSED"
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
		require.Equal(t, http.StatusOK, rec.status)
		assert.Equal(t, true, rec.body["consistent"], id)
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
