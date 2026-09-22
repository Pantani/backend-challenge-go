package sqs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/observability"
)

var errBoom = errors.New("boom")

func body(messageID, amount string) string {
	return fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z",
"data":{"providerId":"provider-a","externalTransactionId":"t-1","idempotencyKey":"provider-a:t-1",
"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37",
"roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":%q,"currency":"BRL"}}}`, messageID, amount)
}

func TestDecodeMessage(t *testing.T) {
	t.Parallel()
	msg, err := sqsadapter.DecodeMessage("consumer", body("msg-1", "25.00"))
	require.NoError(t, err)
	assert.Equal(t, "msg-1", msg.MessageID)
	assert.Equal(t, "consumer", msg.Consumer)
	assert.Equal(t, "provider-a:t-1", msg.Command.IdempotencyKey)
	assert.Equal(t, "msg-1", msg.Command.CorrelationID)

	same, err := sqsadapter.DecodeMessage("consumer", body("msg-2", "25.00"))
	require.NoError(t, err)
	assert.Equal(t, msg.Hash, same.Hash, "hash covers the content, not the message id")
	other, err := sqsadapter.DecodeMessage("consumer", body("msg-1", "26.00"))
	require.NoError(t, err)
	assert.NotEqual(t, msg.Hash, other.Hash)

	httpEquivalent, err := app.NewSubmitCommand(app.SubmitInput{
		ProviderID: "provider-a", ExternalTransactionID: "t-1", IdempotencyKey: "whatever", PlayerID: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		WalletID: "0192f291-27dd-7d3f-8071-5f8685deef37", RoundID: "round-987", GameID: "fortune-chimp", Kind: "BET", Amount: "25.00", Currency: "BRL",
	})
	require.NoError(t, err)
	assert.Equal(t, httpEquivalent.PayloadHash, msg.Command.PayloadHash, "HTTP and SQS share the payload hash")
}

func TestDecodeMessageInvalid(t *testing.T) {
	t.Parallel()
	cases := []string{
		"not json",
		`{"messageId":"m","type":"Other","data":{}}`,
		`{"type":"WagerTransactionRequested","data":{}}`,
		`{"messageId":"m","type":"WagerTransactionRequested","extra":1}`,
		strings.Replace(body("m", "25.00"), `"25.00"`, `25.00`, 1),
		body("m", "1e2"),
	}
	for _, c := range cases {
		_, err := sqsadapter.DecodeMessage("consumer", c)
		assert.ErrorIs(t, err, sqsadapter.ErrInvalidMessage, c)
	}
}

type fakeProcessor struct {
	results []error
	dup     bool
	calls   int
}

func (p *fakeProcessor) ConsumeMessage(context.Context, app.InboundMessage) (app.ConsumeResult, error) {
	p.calls++
	err := p.results[0]
	p.results = p.results[1:]
	res := app.ConsumeResult{Duplicate: p.dup}
	if !p.dup {
		res.Result.Transaction = sampleTx()
	}
	return res, err
}

func sampleTx() *wager.Transaction {
	cmd, _ := sqsadapter.DecodeMessage("c", body("m", "1.00"))
	t, _ := wager.NewExternal(wager.ExternalParams{
		ID: uuid.New(), WalletID: cmd.Command.WalletID, PlayerID: cmd.Command.PlayerID, Kind: wager.KindBet,
		Amount: cmd.Command.Amount, Now: time.Now(),
		External: wager.External{ProviderID: "p", ExternalID: "e", IdempotencyKey: "k", PayloadHash: "h", RoundID: "r", GameID: "g"},
	})
	return t
}

func message(id, bodyText, receiveCount string) types.Message {
	return types.Message{
		MessageId: aws.String(id), ReceiptHandle: aws.String("rh-" + id), Body: aws.String(bodyText),
		Attributes: map[string]string{string(types.MessageSystemAttributeNameApproximateReceiveCount): receiveCount},
	}
}

type consumerFixture struct {
	api  *fakeAPI
	proc *fakeProcessor
	logs *bytes.Buffer
	c    *sqsadapter.Consumer
}

func newConsumer(t *testing.T, proc *fakeProcessor, msgs ...types.Message) *consumerFixture {
	t.Helper()
	f := &consumerFixture{api: newFakeAPI(), proc: proc, logs: &bytes.Buffer{}}
	f.api.receive = func(context.Context) (*awssqs.ReceiveMessageOutput, error) {
		return &awssqs.ReceiveMessageOutput{Messages: msgs}, nil
	}
	f.c = sqsadapter.NewConsumer(f.api, sqsadapter.ConsumerConfig{
		Name: "consumer", QueueURL: "http://sqs/in", DLQURL: "http://sqs/dlq", MaxMessages: 10, WaitTime: time.Second,
		VisibilityTimeout: 30 * time.Second, ProcessTimeout: time.Second, RetryBase: 2 * time.Second, RetryMax: 10 * time.Second,
	}, proc, observability.NewLogger(f.logs, "debug", "t"), observability.NewMetrics(prometheus.NewRegistry()))
	return f
}

func TestConsumerAcksProcessedAndDuplicateMessages(t *testing.T) {
	t.Parallel()
	f := newConsumer(t, &fakeProcessor{results: []error{nil}}, message("a", body("m-a", "1.00"), "1"))
	f.c.PollOnce(context.Background())
	assert.Equal(t, []string{"rh-a"}, f.api.deleted)

	d := newConsumer(t, &fakeProcessor{results: []error{nil}, dup: true}, message("b", body("m-b", "1.00"), "2"))
	d.c.PollOnce(context.Background())
	assert.Equal(t, []string{"rh-b"}, d.api.deleted)
	assert.Contains(t, d.logs.String(), `"outcome":"duplicate"`)
}

func TestConsumerRetriesTransientFailuresWithBackoff(t *testing.T) {
	t.Parallel()
	f := newConsumer(t, &fakeProcessor{results: []error{app.ErrUnavailable, app.ErrConflict}},
		message("a", body("m-a", "1.00"), "1"), message("b", body("m-b", "1.00"), "9"))
	f.c.PollOnce(context.Background())
	assert.Empty(t, f.api.deleted, "never deleted before a durable commit")
	assert.Equal(t, map[string]int32{"rh-a": 2, "rh-b": 10}, f.api.visibility)
}

func TestConsumerDeadLettersPermanentFailures(t *testing.T) {
	t.Parallel()
	f := newConsumer(t, &fakeProcessor{results: []error{app.ErrIdempotencyConflict}},
		message("a", "garbage", "1"), message("b", body("m-b", "1.00"), "1"))
	f.c.PollOnce(context.Background())
	require.Len(t, f.api.sent, 2)
	assert.Equal(t, "http://sqs/dlq", aws.ToString(f.api.sent[0].QueueUrl))
	assert.Equal(t, "a", aws.ToString(f.api.sent[0].MessageDeduplicationId))
	assert.Contains(t, aws.ToString(f.api.sent[1].MessageAttributes["failureReason"].StringValue), "idempotency")
	assert.Equal(t, []string{"rh-a", "rh-b"}, f.api.deleted)
}

func TestConsumerKeepsMessageWhenDLQSendFails(t *testing.T) {
	t.Parallel()
	f := newConsumer(t, &fakeProcessor{}, message("a", "garbage", "1"))
	f.api.errs["send"] = errBoom
	f.c.PollOnce(context.Background())
	assert.Empty(t, f.api.deleted)
	assert.Contains(t, f.logs.String(), "DLQ send failed")
}

func TestConsumerToleratesDeleteAndVisibilityFailures(t *testing.T) {
	t.Parallel()
	f := newConsumer(t, &fakeProcessor{results: []error{nil, app.ErrUnavailable}},
		message("a", body("m-a", "1.00"), "1"), message("b", body("m-b", "1.00"), "1"))
	f.api.errs["delete"] = errBoom
	f.api.errs["visibility"] = errBoom
	f.c.PollOnce(context.Background())
	assert.Contains(t, f.logs.String(), "redelivery will be deduplicated")
	assert.Contains(t, f.logs.String(), "change visibility failed")
}

func TestConsumerReleasesUnstartedMessagesOnShutdown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	proc := &fakeProcessor{results: []error{nil}}
	f := newConsumer(t, proc, message("a", body("m-a", "1.00"), "1"), message("b", body("m-b", "1.00"), "1"))
	f.api.receive = func(context.Context) (*awssqs.ReceiveMessageOutput, error) {
		cancel() // SIGTERM arrives while the batch is being handled
		return &awssqs.ReceiveMessageOutput{Messages: []types.Message{message("a", body("m-a", "1.00"), "1")}}, nil
	}
	f.c.Run(ctx)
	assert.Zero(t, proc.calls)
	assert.Equal(t, map[string]int32{"rh-a": 0}, f.api.visibility)
}

func TestConsumerReceiveFailures(t *testing.T) {
	t.Parallel()
	f := newConsumer(t, &fakeProcessor{})
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	f.api.receive = func(context.Context) (*awssqs.ReceiveMessageOutput, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return nil, errBoom
	}
	f.c.Run(ctx)
	assert.Equal(t, 2, calls)
	assert.Contains(t, f.logs.String(), "sqs receive failed")
}

func TestConsumerSleepCompletes(t *testing.T) {
	t.Parallel()
	f := newConsumer(t, &fakeProcessor{})
	f.api.receive = func(context.Context) (*awssqs.ReceiveMessageOutput, error) { return nil, errBoom }
	c := sqsadapter.NewConsumer(f.api, sqsadapter.ConsumerConfig{RetryBase: time.Millisecond}, f.proc,
		observability.NewLogger(f.logs, "info", "t"), observability.NewMetrics(prometheus.NewRegistry()))
	start := time.Now()
	c.PollOnce(context.Background())
	assert.GreaterOrEqual(t, time.Since(start), time.Millisecond)
}

func TestLongFailureReasonIsTruncated(t *testing.T) {
	t.Parallel()
	f := newConsumer(t, &fakeProcessor{results: []error{errors.New(strings.Repeat("x", 500))}}, message("a", body("m", "1.00"), "1"))
	f.c.PollOnce(context.Background())
	require.Len(t, f.api.sent, 1)
	assert.Len(t, aws.ToString(f.api.sent[0].MessageAttributes["failureReason"].StringValue), 256)
}

func TestPublisher(t *testing.T) {
	t.Parallel()
	api := newFakeAPI()
	p := sqsadapter.NewPublisher(api, "http://sqs/events")
	id := uuid.New()
	require.NoError(t, p.Publish(context.Background(), app.OutboxMessage{EventID: id, EventType: "WalletBalanceChanged",
		AggregateType: "Wallet", PartitionKey: "wallet-1", Payload: []byte(`{"eventId":"x"}`)}))
	require.Len(t, api.sent, 1)
	in := api.sent[0]
	assert.Equal(t, "wallet-1", aws.ToString(in.MessageGroupId))
	assert.Equal(t, id.String(), aws.ToString(in.MessageDeduplicationId))
	assert.Equal(t, `{"eventId":"x"}`, aws.ToString(in.MessageBody))
	assert.Equal(t, "WalletBalanceChanged", aws.ToString(in.MessageAttributes["eventType"].StringValue))
}

func TestProvisionAndResolveQueues(t *testing.T) {
	t.Parallel()
	api := newFakeAPI()
	names := sqsadapter.QueueNames{Input: "in.fifo", DLQ: "dlq.fifo", Events: "events.fifo"}
	q, err := sqsadapter.Provision(context.Background(), api, sqsadapter.ProvisionConfig{Names: names, MaxReceiveCount: 5, VisibilityTimeout: 30})
	require.NoError(t, err)
	assert.Equal(t, sqsadapter.Queues{Input: "http://sqs/in.fifo", DLQ: "http://sqs/dlq.fifo", Events: "http://sqs/events.fifo"}, q)
	require.Len(t, api.created, 3)
	assert.Contains(t, api.created[1].Attributes["RedrivePolicy"], `"maxReceiveCount":"5"`)
	assert.Equal(t, "true", api.created[2].Attributes["FifoQueue"])

	resolved, err := sqsadapter.ResolveQueues(context.Background(), api, names)
	require.NoError(t, err)
	assert.Equal(t, q, resolved)
	require.NoError(t, sqsadapter.QueueCheck(api, q.Input)(context.Background()))
}

func TestProvisionFailures(t *testing.T) {
	t.Parallel()
	names := sqsadapter.QueueNames{Input: "in.fifo", DLQ: "dlq.fifo", Events: "events.fifo"}
	for _, failing := range []string{"create:dlq.fifo", "attrs", "create:in.fifo", "create:events.fifo"} {
		api := newFakeAPI()
		api.errs[failing] = errBoom
		_, err := sqsadapter.Provision(context.Background(), api, sqsadapter.ProvisionConfig{Names: names})
		assert.ErrorIs(t, err, errBoom, failing)
	}
	api := newFakeAPI()
	api.errs["url:dlq.fifo"] = errBoom
	_, err := sqsadapter.ResolveQueues(context.Background(), api, names)
	require.ErrorIs(t, err, errBoom)
}

func TestNewClient(t *testing.T) {
	t.Parallel()
	c, err := sqsadapter.NewClient(context.Background(), sqsadapter.ClientConfig{Region: "us-east-1", Endpoint: "http://localhost:4566"})
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:4566", aws.ToString(c.Options().BaseEndpoint))
	c, err = sqsadapter.NewClient(context.Background(), sqsadapter.ClientConfig{Region: "us-east-1"})
	require.NoError(t, err)
	assert.Nil(t, c.Options().BaseEndpoint)
}

func TestNewClientConfigError(t *testing.T) {
	t.Setenv("AWS_PROFILE", "profile-that-does-not-exist")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/missing")
	_, err := sqsadapter.NewClient(context.Background(), sqsadapter.ClientConfig{Region: "us-east-1"})
	require.Error(t, err)
}
