//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

func newMetrics() *observability.Metrics { return observability.NewMetrics(prometheus.NewRegistry()) }

func newRelay(t *testing.T, owner string, store app.OutboxStore, pub worker.Publisher) *worker.Relay {
	t.Helper()
	return newRelayWithAttempts(t, owner, store, pub, 1000)
}

func newRelayWithAttempts(t *testing.T, owner string, store app.OutboxStore, pub worker.Publisher, maxAttempts int) *worker.Relay {
	t.Helper()
	return worker.NewRelay(store, pub, app.SystemClock{}, worker.RelayConfig{
		Owner: owner, BatchSize: 500, Lease: time.Second, RetryBase: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond,
		PublishTime: 5 * time.Second, MaxAttempts: maxAttempts,
	}, observability.NewLogger(io.Discard, "error", owner), newMetrics())
}

// unpublished counts outbox rows of a wallet not yet published.
func unpublished(t *testing.T, walletID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE partition_key = $1 AND published_at IS NULL`, walletID.String()).Scan(&n))
	return n
}

// drain reads every message currently in a queue.
func drain(t *testing.T, api sqsadapter.API, url string) []string {
	t.Helper()
	var bodies []string
	for range 20 {
		out, err := api.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{QueueUrl: aws.String(url), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		require.NoError(t, err)
		if len(out.Messages) == 0 && len(bodies) > 0 {
			return bodies
		}
		for _, m := range out.Messages {
			bodies = append(bodies, aws.ToString(m.Body))
			_, _ = api.DeleteMessage(context.Background(), &awssqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle})
		}
	}
	return bodies
}

func eventIDs(t *testing.T, bodies []string) map[string]int {
	t.Helper()
	ids := map[string]int{}
	for _, b := range bodies {
		var env struct {
			EventID string `json:"eventId"`
		}
		require.NoError(t, json.Unmarshal([]byte(b), &env))
		ids[env.EventID]++
	}
	return ids
}

// Not parallel: relays claim the whole (shared) outbox.
func TestTwoPublishersShareTheOutbox(t *testing.T) {
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 3)
	w := s.openWallet(t, "100.00")
	for i := range 5 {
		s.submit(t, w, fmt.Sprintf("pub-%d", i), "BET", "1.00", "")
	}
	require.Equal(t, 12, unpublished(t, w.ID()), "committed events wait for a publisher (commit happened, publication did not)")

	pub := sqsadapter.NewPublisher(api, q.Events)
	store := postgres.NewOutboxStore(pool)
	relays := []*worker.Relay{newRelay(t, "relay-a", store, pub), newRelay(t, "relay-b", store, pub)}
	parallel(2, func(i int) bool { relays[i].Tick(context.Background()); return true })
	require.Eventually(t, func() bool { relays[0].Tick(context.Background()); return unpublished(t, w.ID()) == 0 }, 10*time.Second, 100*time.Millisecond)

	ids := eventIDs(t, drain(t, api, q.Events))
	var mine int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE partition_key = $1`, w.ID().String()).Scan(&mine))
	assert.GreaterOrEqual(t, len(ids), mine, "every event of the wallet reached the queue")
	for id, n := range ids {
		assert.Equal(t, 1, n, "event %s delivered once (FIFO deduplication by eventId)", id)
	}
}

// forgetfulStore publishes but crashes before confirming.
type forgetfulStore struct{ app.OutboxStore }

func (forgetfulStore) MarkPublished(context.Context, uuid.UUID, string, time.Time) (bool, error) {
	return false, fmt.Errorf("crash before confirming")
}

// recordingPublisher remembers every published event id.
type recordingPublisher struct {
	mu  sync.Mutex
	ids []uuid.UUID
}

func (p *recordingPublisher) Publish(_ context.Context, m app.OutboxMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ids = append(p.ids, m.EventID)
	return nil
}

func (p *recordingPublisher) count(id uuid.UUID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, x := range p.ids {
		n += boolToInt(x == id)
	}
	return n
}

// Not parallel: relays claim the whole (shared) outbox.
func TestCrashBetweenPublishAndConfirmIsRecovered(t *testing.T) {
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "10.00")
	var eventID uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT event_id FROM outbox_events WHERE partition_key = $1 ORDER BY seq LIMIT 1`, w.ID().String()).Scan(&eventID))

	pub := &recordingPublisher{}
	store := postgres.NewOutboxStore(pool)
	newRelay(t, "crashing", forgetfulStore{store}, pub).Tick(context.Background())
	require.GreaterOrEqual(t, pub.count(eventID), 1)
	require.Equal(t, 2, unpublished(t, w.ID()), "not confirmed")

	// Another instance takes over once the lease expires, keeping the eventId.
	survivor := newRelay(t, "survivor", store, pub)
	require.Eventually(t, func() bool { survivor.Tick(context.Background()); return unpublished(t, w.ID()) == 0 }, 10*time.Second, 200*time.Millisecond)
	assert.GreaterOrEqual(t, pub.count(eventID), 2, "republished with the same eventId")
}

// failingPublisher fails the first attempts to exercise the backoff.
type failingPublisher struct {
	mu    sync.Mutex
	fails int
}

func (p *failingPublisher) Publish(context.Context, app.OutboxMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fails > 0 {
		p.fails--
		return fmt.Errorf("broker down")
	}
	return nil
}

// Not parallel: relays claim the whole (shared) outbox.
func TestPublicationRetriesWithBackoff(t *testing.T) {
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "10.00")
	relay := newRelay(t, "retry", postgres.NewOutboxStore(pool), &failingPublisher{fails: 1000})
	relay.Tick(context.Background())
	var attempts int
	var lastError string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT max(attempts), max(last_error) FROM outbox_events
		WHERE partition_key = $1`, w.ID().String()).Scan(&attempts, &lastError))
	assert.GreaterOrEqual(t, attempts, 1)
	assert.Equal(t, "broker down", lastError)
	assert.Equal(t, 2, unpublished(t, w.ID()))

	ok := newRelay(t, "retry", postgres.NewOutboxStore(pool), &failingPublisher{})
	require.Eventually(t, func() bool { ok.Tick(context.Background()); return unpublished(t, w.ID()) == 0 }, 10*time.Second, 100*time.Millisecond)
}

// localSenders trusts the LocalStack account id, the SenderId of every
// message sent to LocalStack.
var localSenders = sqsadapter.SenderPolicy{"000000000000": {"*"}}

// consumerFor builds a consumer on the test queues.
func consumerFor(api sqsadapter.API, q sqsadapter.Queues, svc sqsadapter.Processor) *sqsadapter.Consumer {
	return sqsadapter.NewConsumer(api, sqsadapter.ConsumerConfig{
		Name: "it-consumer", QueueURL: q.Input, DLQURL: q.DLQ, MaxMessages: 10, WaitTime: time.Second,
		VisibilityTimeout: 2 * time.Second, ProcessTimeout: time.Second, RetryBase: time.Second, RetryMax: time.Second,
		Senders: localSenders,
	}, svc, observability.NewLogger(io.Discard, "error", "c"), newMetrics())
}

func sendMessage(t *testing.T, api sqsadapter.API, q sqsadapter.Queues, messageID string, in app.SubmitInput) {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"messageId": messageID, "type": sqsadapter.MessageType, "occurredAt": "2026-09-08T12:00:00.000Z",
		"data": map[string]any{
			"providerId": in.ProviderID, "externalTransactionId": in.ExternalTransactionID, "idempotencyKey": in.IdempotencyKey,
			"playerId": in.PlayerID, "walletId": in.WalletID, "roundId": in.RoundID, "gameId": in.GameID, "kind": in.Kind,
			"money": map[string]string{"amount": in.Amount, "currency": in.Currency},
		},
	})
	require.NoError(t, err)
	sendRaw(t, api, q, messageID, string(data), in.WalletID)
}

func sendRaw(t *testing.T, api sqsadapter.API, q sqsadapter.Queues, dedup, body, group string) {
	t.Helper()
	_, err := api.SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl: aws.String(q.Input), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup),
	})
	require.NoError(t, err)
}

func (s services) tx(t *testing.T, provider, ext string) *wager.Transaction {
	t.Helper()
	got, err := s.wagers.GetByExternal(context.Background(), app.Caller{Internal: true}, provider, ext)
	require.NoError(t, err)
	return got
}

// noDeleteAPI simulates a consumer crash after the commit and before the
// message is removed from the queue.
type noDeleteAPI struct{ sqsadapter.API }

func (noDeleteAPI) DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	return nil, fmt.Errorf("process killed before DeleteMessage")
}

func TestConsumerCrashAfterCommitIsRedeliveredAndDeduplicated(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 5)
	w := s.openWallet(t, "100.00")
	in := s.input(w, "provider-a", "sqs-bet", "BET", "10.00", "")
	sendMessage(t, api, q, s.prefix+"msg-1", in)

	consumerFor(noDeleteAPI{api}, q, s.wagers).PollOnce(context.Background())
	assert.Equal(t, "90.00", s.balance(t, w), "committed before the crash")

	// After the visibility timeout the message comes back to another instance.
	other := newServices(t, defaultPolicy)
	c := consumerFor(api, q, other.wagers)
	require.Eventually(t, func() bool {
		c.PollOnce(context.Background())
		return queueDepth(t, api, q.Input) == 0
	}, 15*time.Second, 100*time.Millisecond)
	assert.Equal(t, "90.00", s.balance(t, w), "the redelivery did not debit again")
	assert.Equal(t, 1, s.debits(t, w))

	var processed bool
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT processed_at IS NOT NULL FROM inbox_messages
		WHERE consumer_name = 'it-consumer' AND message_id = $1`, s.prefix+"msg-1").Scan(&processed))
	assert.True(t, processed)
}

func queueDepth(t *testing.T, api sqsadapter.API, url string) int {
	t.Helper()
	out, err := api.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(url),
		AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"}})
	require.NoError(t, err)
	var visible, hidden int
	_, _ = fmt.Sscan(out.Attributes["ApproximateNumberOfMessages"], &visible)
	_, _ = fmt.Sscan(out.Attributes["ApproximateNumberOfMessagesNotVisible"], &hidden)
	return visible + hidden
}

func TestInvalidMessagesGoToTheDLQ(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 5)
	sendRaw(t, api, q, "bad-1", `{"messageId":"bad-1","type":"WagerTransactionRequested","data":{"money":{"amount":25.0}}}`, "g")
	consumerFor(api, q, s.wagers).PollOnce(context.Background())
	dead := drain(t, api, q.DLQ)
	require.Len(t, dead, 1)
	assert.Contains(t, dead[0], "bad-1")
	assert.Zero(t, queueDepth(t, api, q.Input))
}

// flakyProcessor always fails transiently (e.g. PostgreSQL unavailable).
type flakyProcessor struct{}

func (flakyProcessor) ConsumeMessage(context.Context, app.InboundMessage) (app.ConsumeResult, error) {
	return app.ConsumeResult{}, app.ErrUnavailable
}

func TestTransientFailuresAreRetriedThenRedrivenToTheDLQ(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 2)
	w := s.openWallet(t, "100.00")
	sendMessage(t, api, q, s.prefix+"flaky", s.input(w, "provider-a", "flaky", "BET", "1.00", ""))

	c := consumerFor(api, q, flakyProcessor{})
	require.Eventually(t, func() bool {
		c.PollOnce(context.Background())
		return queueDepth(t, api, q.DLQ) == 1
	}, 30*time.Second, 200*time.Millisecond, "after maxReceiveCount the redrive policy moves it")
	assert.Equal(t, "100.00", s.balance(t, w))

	// Once PostgreSQL is back, replaying the DLQ message processes it once.
	dead := drain(t, api, q.DLQ)
	require.Len(t, dead, 1)
	sendRaw(t, api, q, "replayed", dead[0], w.ID().String())
	consumerFor(api, q, s.wagers).PollOnce(context.Background())
	assert.Equal(t, "99.00", s.balance(t, w))
}

func TestSameOperationThroughHTTPAndSQS(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 5)
	w := s.openWallet(t, "100.00")
	in := s.input(w, "provider-a", "cross", "BET", "10.00", "")

	cmd, err := app.NewSubmitCommand(in)
	require.NoError(t, err)
	results := parallel(2, func(i int) error {
		if i == 0 {
			_, err := s.wagers.Submit(context.Background(), cmd)
			return err
		}
		sendMessage(t, api, q, s.prefix+"cross-msg", in)
		consumerFor(api, q, s.wagers).PollOnce(context.Background())
		return nil
	})
	require.NoError(t, results[0])
	require.Eventually(t, func() bool { return queueDepth(t, api, q.Input) == 0 }, 10*time.Second, 100*time.Millisecond)
	assert.Equal(t, "90.00", s.balance(t, w))
	assert.Equal(t, 1, s.debits(t, w))
	assert.Equal(t, wager.StatusProcessed, s.tx(t, "provider-a", s.prefix+"cross").Status())
}

func TestSQSSenderMustBeBoundToTheProvider(t *testing.T) {
	t.Parallel()
	s := newServices(t, defaultPolicy)
	api, q, _ := provisionQueues(t, 5)
	w := s.openWallet(t, "100.00")
	sendMessage(t, api, q, s.prefix+"spoofed", s.input(w, "provider-a", "spoofed", "BET", "10.00", ""))

	onlyB := sqsadapter.NewConsumer(api, sqsadapter.ConsumerConfig{
		Name: "it-consumer", QueueURL: q.Input, DLQURL: q.DLQ, MaxMessages: 10, WaitTime: time.Second,
		VisibilityTimeout: 2 * time.Second, ProcessTimeout: time.Second, RetryBase: time.Second, RetryMax: time.Second,
		Senders: sqsadapter.SenderPolicy{"000000000000": {"provider-b"}},
	}, s.wagers, observability.NewLogger(io.Discard, "error", "c"), newMetrics())
	onlyB.PollOnce(context.Background())

	dead := drain(t, api, q.DLQ)
	require.Len(t, dead, 1, "a sender acting for another provider goes to the DLQ")
	assert.Equal(t, "100.00", s.balance(t, w), "no financial effect")
}

// orderedPublisher fails the first attempt of one event and records order.
type orderedPublisher struct {
	recordingPublisher
	failOnce uuid.UUID
	failed   bool
}

func (p *orderedPublisher) Publish(ctx context.Context, m app.OutboxMessage) error {
	p.mu.Lock()
	fail := m.EventID == p.failOnce && !p.failed
	p.failed = p.failed || fail
	p.mu.Unlock()
	if fail {
		return fmt.Errorf("broker hiccup")
	}
	return p.recordingPublisher.Publish(ctx, m)
}

// Not parallel: relays claim the whole (shared) outbox.
func TestOutboxKeepsPerWalletOrderWhenAnEventFails(t *testing.T) {
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "100.00")
	s.submit(t, w, "ordered-1", "BET", "1.00", "")
	s.submit(t, w, "ordered-2", "BET", "1.00", "")
	var order []uuid.UUID
	rows, err := pool.Query(context.Background(), `SELECT event_id FROM outbox_events WHERE partition_key = $1 ORDER BY seq`, w.ID().String())
	require.NoError(t, err)
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		order = append(order, id)
	}
	require.NoError(t, rows.Err())
	require.Len(t, order, 6)

	pub := &orderedPublisher{failOnce: order[0]}
	store := postgres.NewOutboxStore(pool)
	relays := []*worker.Relay{newRelay(t, "ordered-a", store, pub), newRelay(t, "ordered-b", store, pub)}
	require.Eventually(t, func() bool {
		parallel(2, func(i int) bool { relays[i].Tick(context.Background()); return true })
		return unpublished(t, w.ID()) == 0
	}, 20*time.Second, 100*time.Millisecond)

	var published []uuid.UUID
	pub.mu.Lock()
	for _, id := range pub.ids {
		if slices.Contains(order, id) {
			published = append(published, id)
		}
	}
	pub.mu.Unlock()
	assert.Equal(t, order, published, "the failing head held back the rest of the wallet's events")
}

// poisonPublisher always fails one event (e.g. a payload the broker refuses).
type poisonPublisher struct {
	recordingPublisher
	poison uuid.UUID
}

func (p *poisonPublisher) Publish(ctx context.Context, m app.OutboxMessage) error {
	if m.EventID == p.poison {
		return fmt.Errorf("broker refuses this event")
	}
	return p.recordingPublisher.Publish(ctx, m)
}

// Not parallel: relays claim the whole (shared) outbox.
func TestPoisonOutboxEventIsDeadLetteredAndUnblocksItsWallet(t *testing.T) {
	s := newServices(t, defaultPolicy)
	w := s.openWallet(t, "10.00")
	var poison uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT event_id FROM outbox_events WHERE partition_key = $1 ORDER BY seq LIMIT 1`, w.ID().String()).Scan(&poison))

	pub := &poisonPublisher{poison: poison}
	relay := newRelayWithAttempts(t, "poison", postgres.NewOutboxStore(pool), pub, 3)
	require.Eventually(t, func() bool { relay.Tick(context.Background()); return unpublished(t, w.ID()) == 1 }, 20*time.Second, 50*time.Millisecond,
		"the event behind the poison one is published once the poison is dead-lettered")

	var dead bool
	var lastError string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT dead_lettered_at IS NOT NULL, last_error FROM outbox_events
		WHERE event_id = $1`, poison).Scan(&dead, &lastError))
	assert.True(t, dead)
	assert.Equal(t, "broker refuses this event", lastError)
	assert.Zero(t, pub.count(poison))
}

func TestProvisioningReconcilesChangedAttributes(t *testing.T) {
	t.Parallel()
	api, q, names := provisionQueues(t, 3)
	_, err := sqsadapter.Provision(context.Background(), api, sqsadapter.ProvisionConfig{Names: names, MaxReceiveCount: 7, VisibilityTimeout: 45})
	require.NoError(t, err, "rerunning with different attributes does not fail with QueueNameExists")

	out, err := api.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(q.Input),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameVisibilityTimeout, types.QueueAttributeNameRedrivePolicy}})
	require.NoError(t, err)
	assert.Equal(t, "45", out.Attributes["VisibilityTimeout"])
	assert.Contains(t, out.Attributes["RedrivePolicy"], "7")
}
