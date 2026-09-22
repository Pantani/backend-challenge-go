package sqs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/contract"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var errBoom = errors.New("boom")

const (
	testWalletID = "0192f291-27dd-7d3f-8071-5f8685deef37"
	testWalletA  = "0192f291-27dd-7d3f-8071-5f8685deef38"
	testWalletB  = "0192f291-27dd-7d3f-8071-5f8685deef39"
)

func body(messageID, amount string) string {
	return bodyAt(messageID, "2026-09-08T12:00:00.000Z", amount, testWalletID)
}

func bodyAt(messageID, occurredAt, amount, walletID string) string {
	return fmt.Sprintf(`{"messageId":%s,"type":"WagerTransactionRequested","occurredAt":%s,
"data":{"providerId":"provider-a","externalTransactionId":"t-1","idempotencyKey":"provider-a:t-1",
"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":%s,
"roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":%s,"currency":"BRL"}}}`,
		jsonString(messageID), jsonString(occurredAt), jsonString(walletID), jsonString(amount))
}

func jsonString(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
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

func TestEncodeMessageRoundTrip(t *testing.T) {
	t.Parallel()
	env := sqsadapter.Envelope{
		MessageID: "msg-rt", Type: sqsadapter.MessageType, OccurredAt: "2026-09-08T12:00:00Z",
		Data: sqsadapter.MessageData{IdempotencyKey: "provider-a:t-1", Operation: contract.Operation{
			ProviderID: "provider-a", ExternalTransactionID: "t-1", PlayerID: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
			WalletID: "0192f291-27dd-7d3f-8071-5f8685deef37", RoundID: "round-987", GameID: "fortune-chimp", Kind: "REFUND",
			Money: contract.Money{Amount: "25.00", Currency: "BRL"}, ReferenceExternalTransactionID: "bet-1",
		}},
	}
	encoded, err := sqsadapter.EncodeMessage(env)
	require.NoError(t, err)
	msg, err := sqsadapter.DecodeMessage("consumer", encoded)
	require.NoError(t, err)
	assert.Equal(t, "msg-rt", msg.MessageID)
	assert.Equal(t, "provider-a:t-1", msg.Command.IdempotencyKey)
	assert.Equal(t, wager.KindRefund, msg.Command.Kind)
	assert.Equal(t, "bet-1", msg.Command.ReferenceExternalID)
	assert.Equal(t, "25.00 BRL", msg.Command.Amount.String())

	fromLiteral, err := sqsadapter.DecodeMessage("consumer", body("msg-rt", "25.00"))
	require.NoError(t, err)
	assert.NotEqual(t, fromLiteral.Hash, msg.Hash, "a different operation hashes differently")
	assert.Equal(t, "msg-rt", fromLiteral.MessageID)
}

func TestConsumerAckTimeoutBoundsTheDelete(t *testing.T) {
	t.Parallel()
	cfg := consumerConfig()
	cfg.AckTimeout = 250 * time.Millisecond
	f := newConsumerWith(t, cfg, &fakeProcessor{results: []error{nil}}, message("a", body("m-a", "1.00"), "1"))
	f.c.PollOnce(context.Background())
	require.Equal(t, []string{"rh-a"}, f.api.deleted)
	assert.Greater(t, f.api.budgets["rh-a"], time.Duration(0))
	assert.LessOrEqual(t, f.api.budgets["rh-a"], cfg.AckTimeout, "the configured budget replaces DefaultAckTimeout")

	d := newConsumer(t, &fakeProcessor{results: []error{nil}}, message("b", body("m-b", "1.00"), "1"))
	d.c.PollOnce(context.Background())
	assert.Greater(t, d.api.budgets["rh-b"], cfg.AckTimeout, "the default budget is used when none is configured")
	assert.LessOrEqual(t, d.api.budgets["rh-b"], sqsadapter.DefaultAckTimeout)
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
		body("m", "1.00") + `{"messageId":"other"}`,
		body("m", "1.00") + " garbage",
	}
	for _, c := range cases {
		_, err := sqsadapter.DecodeMessage("consumer", c)
		assert.ErrorIs(t, err, sqsadapter.ErrInvalidMessage, c)
	}
}

func TestDecodeMessageRejectsInvalidEnvelopeMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		messageID  string
		occurredAt string
		want       string
	}{
		{name: "blank id", messageID: " ", occurredAt: "2026-09-22T12:00:00Z", want: "invalid messageId"},
		{name: "oversized id", messageID: strings.Repeat("x", 129), occurredAt: "2026-09-22T12:00:00Z", want: "invalid messageId"},
		{name: "non-printable id", messageID: "message-\x7f", occurredAt: "2026-09-22T12:00:00Z", want: "invalid messageId"},
		{name: "missing time", messageID: "message-1", want: "invalid occurredAt"},
		{name: "invalid time", messageID: "message-1", occurredAt: "yesterday", want: "invalid occurredAt"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := sqsadapter.DecodeMessage("consumer", bodyAt(tt.messageID, tt.occurredAt, "1.00", testWalletID))
			require.ErrorIs(t, err, sqsadapter.ErrInvalidMessage)
			assert.ErrorContains(t, err, tt.want)
		})
	}
}

func TestDecodeMessagePreservesMessageID(t *testing.T) {
	t.Parallel()
	const messageID = " message-1 "
	msg, err := sqsadapter.DecodeMessage("consumer", bodyAt(messageID, "2026-09-22T12:00:00Z", "1.00", testWalletID))
	require.NoError(t, err)
	assert.Equal(t, messageID, msg.MessageID)
}

// fakeProcessor returns its results in order (nil once they run out). hook,
// when set, runs before each call with the processing context and may
// override the result.
type fakeProcessor struct {
	results []error
	dup     bool
	calls   int
	hook    func(ctx context.Context) error
}

func (p *fakeProcessor) ConsumeMessage(ctx context.Context, _ app.InboundMessage) (app.ConsumeResult, error) {
	p.calls++
	var err error
	if len(p.results) > 0 {
		err = p.results[0]
		p.results = p.results[1:]
	}
	if p.hook != nil {
		err = p.hook(ctx)
	}
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
	return groupMessage(id, bodyText, receiveCount, testWalletID)
}

func groupMessage(id, bodyText, receiveCount, group string) types.Message {
	return fifoMessage(id, bodyText, receiveCount, group, envelopeMessageID(bodyText))
}

func fifoMessage(id, bodyText, receiveCount, group, dedup string) types.Message {
	return types.Message{
		MessageId: aws.String(id), ReceiptHandle: aws.String("rh-" + id), Body: aws.String(bodyText),
		Attributes: map[string]string{
			string(types.MessageSystemAttributeNameApproximateReceiveCount): receiveCount,
			string(types.MessageSystemAttributeNameSenderId):                "AIDA-PROVIDER-A",
			string(types.MessageSystemAttributeNameMessageGroupId):          group,
			string(types.MessageSystemAttributeNameMessageDeduplicationId):  dedup,
		},
	}
}

func envelopeMessageID(bodyText string) string {
	var env struct {
		MessageID string `json:"messageId"`
	}
	_ = json.Unmarshal([]byte(bodyText), &env)
	return env.MessageID
}

type consumerFixture struct {
	api  *fakeAPI
	proc *fakeProcessor
	logs *bytes.Buffer
	c    *sqsadapter.Consumer
}

// consumerConfig is the standard test configuration.
func consumerConfig() sqsadapter.ConsumerConfig {
	return sqsadapter.ConsumerConfig{
		Name: "consumer", QueueURL: "http://sqs/in", DLQURL: "http://sqs/dlq", MaxMessages: 10, WaitTime: time.Second,
		VisibilityTimeout: 30 * time.Second, ProcessTimeout: time.Second, RetryBase: 2 * time.Second, RetryMax: 10 * time.Second,
		Senders: sqsadapter.SenderPolicy{"AIDA-PROVIDER-A": {"provider-a"}},
	}
}

func newConsumer(t *testing.T, proc *fakeProcessor, msgs ...types.Message) *consumerFixture {
	t.Helper()
	return newConsumerWith(t, consumerConfig(), proc, msgs...)
}

func newConsumerWith(t *testing.T, cfg sqsadapter.ConsumerConfig, proc *fakeProcessor, msgs ...types.Message) *consumerFixture {
	t.Helper()
	f := &consumerFixture{api: newFakeAPI(), proc: proc, logs: &bytes.Buffer{}}
	f.api.setReceive(func(context.Context) (*awssqs.ReceiveMessageOutput, error) {
		return &awssqs.ReceiveMessageOutput{Messages: msgs}, nil
	})
	f.c = sqsadapter.NewConsumer(f.api, cfg, proc, observability.NewLogger(f.logs, "debug", "t"), testutil.NewMetrics())
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
		groupMessage("a", bodyAt("m-a", "2026-09-08T12:00:00Z", "1.00", testWalletA), "1", testWalletA),
		groupMessage("b", bodyAt("m-b", "2026-09-08T12:00:00Z", "1.00", testWalletB), "9", testWalletB))
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
	f.api.setReceive(func(context.Context) (*awssqs.ReceiveMessageOutput, error) {
		cancel() // SIGTERM arrives while the batch is being handled
		return &awssqs.ReceiveMessageOutput{Messages: []types.Message{message("a", body("m-a", "1.00"), "1")}}, nil
	})
	f.c.Run(ctx)
	assert.Zero(t, proc.calls)
	assert.Equal(t, map[string]int32{"rh-a": 0}, f.api.visibility)
}

func TestConsumerReceiveFailures(t *testing.T) {
	t.Parallel()
	f := newConsumer(t, &fakeProcessor{})
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	f.api.setReceive(func(context.Context) (*awssqs.ReceiveMessageOutput, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return nil, errBoom
	})
	f.c.Run(ctx)
	assert.Equal(t, 2, calls)
	assert.Contains(t, f.logs.String(), "sqs receive failed")
}

func TestConsumerSleepCompletes(t *testing.T) {
	t.Parallel()
	f := newConsumerWith(t, sqsadapter.ConsumerConfig{RetryBase: time.Millisecond}, &fakeProcessor{})
	f.api.setReceive(func(context.Context) (*awssqs.ReceiveMessageOutput, error) { return nil, errBoom })
	start := time.Now()
	f.c.PollOnce(context.Background())
	assert.GreaterOrEqual(t, time.Since(start), time.Millisecond)
}

func TestConsumerPausesOnEmptyReceiveWithoutLongPolling(t *testing.T) {
	t.Parallel()
	cfg := consumerConfig()
	cfg.WaitTime, cfg.RetryBase = 0, 20*time.Millisecond
	f := newConsumerWith(t, cfg, &fakeProcessor{})
	start := time.Now()
	f.c.PollOnce(context.Background())
	assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond, "an idle consumer does not spin")

	polling := newConsumer(t, &fakeProcessor{})
	start = time.Now()
	polling.c.PollOnce(context.Background())
	assert.Less(t, time.Since(start), 20*time.Millisecond, "long polling already paces the loop")
}

func TestLongFailureReasonIsTruncated(t *testing.T) {
	t.Parallel()
	f := newConsumer(t, &fakeProcessor{results: []error{errors.New(strings.Repeat("x", 500))}}, message("a", body("m", "1.00"), "1"))
	f.c.PollOnce(context.Background())
	require.Len(t, f.api.sent, 1)
	assert.Len(t, aws.ToString(f.api.sent[0].MessageAttributes["failureReason"].StringValue), 256)

	// A multi-byte rune straddling the limit is dropped whole.
	reason := strings.Repeat("x", 255) + "é" + strings.Repeat("y", 300)
	u := newConsumer(t, &fakeProcessor{results: []error{errors.New(reason)}}, message("a", body("m", "1.00"), "1"))
	u.c.PollOnce(context.Background())
	require.Len(t, u.api.sent, 1, "the DLQ copy is valid UTF-8")
	assert.Equal(t, strings.Repeat("x", 255), aws.ToString(u.api.sent[0].MessageAttributes["failureReason"].StringValue))
}

func TestConsumerAppliesBackoffAfterProcessTimeout(t *testing.T) {
	t.Parallel()
	cfg := consumerConfig()
	cfg.ProcessTimeout = 10 * time.Millisecond
	proc := &fakeProcessor{hook: func(ctx context.Context) error {
		<-ctx.Done() // the use case blocks until its budget expires
		return fmt.Errorf("%w: %w", app.ErrUnavailable, ctx.Err())
	}}
	f := newConsumerWith(t, cfg, proc, message("a", body("m-a", "1.00"), "3"))
	f.c.PollOnce(context.Background())
	assert.Equal(t, map[string]int32{"rh-a": 8}, f.api.visibility, "the retry has its own budget")
	assert.Empty(t, f.api.deleted)
}

func TestConsumerAckBudgetSurvivesShutdown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proc := &fakeProcessor{hook: func(procCtx context.Context) error {
		cancel() // SIGTERM while message a is in flight
		assert.NoError(t, procCtx.Err(), "the processing context is detached from the shutdown signal")
		return nil
	}}
	f := newConsumer(t, proc, message("a", body("m-a", "1.00"), "1"), message("b", body("m-b", "1.00"), "1"))
	f.c.PollOnce(ctx)
	assert.Equal(t, 1, proc.calls, "the in-flight message completes, the next one is not started")
	assert.Equal(t, []string{"rh-a"}, f.api.deleted)
	assert.Equal(t, map[string]int32{"rh-b": 0}, f.api.visibility)
}

func TestConsumerBackoffCapAndReceiveCountParsing(t *testing.T) {
	t.Parallel()
	cfg := consumerConfig()
	cfg.RetryMax = 24 * time.Hour
	f := newConsumerWith(t, cfg, &fakeProcessor{results: []error{app.ErrUnavailable, app.ErrUnavailable, app.ErrUnavailable}},
		groupMessage("a", bodyAt("m-a", "2026-09-08T12:00:00Z", "1.00", testWalletA), "40", testWalletA),
		groupMessage("b", bodyAt("m-b", "2026-09-08T12:00:00Z", "1.00", testWalletB), "garbage", testWalletB),
		message("c", body("m-c", "1.00"), ""))
	f.c.PollOnce(context.Background())
	assert.Equal(t, map[string]int32{"rh-a": 24 * 3600, "rh-b": 2, "rh-c": 2}, f.api.visibility,
		"the shift is capped and an unreadable count is the first receive")
}

func TestConsumerReleasesGroupTailWhenDLQSendFails(t *testing.T) {
	t.Parallel()
	proc := &fakeProcessor{results: []error{app.ErrIdempotencyConflict, nil}}
	f := newConsumer(t, proc,
		groupMessage("a1", bodyAt("m-a1", "2026-09-08T12:00:00Z", "1.00", testWalletA), "1", testWalletA),
		groupMessage("a2", bodyAt("m-a2", "2026-09-08T12:00:00Z", "1.00", testWalletA), "1", testWalletA))
	f.api.errs["send"] = errBoom
	f.c.PollOnce(context.Background())
	assert.Equal(t, 1, proc.calls, "a2 waits for a1 to leave the queue")
	assert.Empty(t, f.api.deleted)
	assert.Equal(t, map[string]int32{"rh-a2": 0}, f.api.visibility)
}

func TestDeadLetterKeepsMessageGroup(t *testing.T) {
	t.Parallel()
	f := newConsumer(t, &fakeProcessor{}, groupMessage("a", "garbage", "1", "wallet-a"), groupMessage("b", "garbage", "1", ""))
	f.c.PollOnce(context.Background())
	require.Len(t, f.api.sent, 2)
	assert.Equal(t, "wallet-a", aws.ToString(f.api.sent[0].MessageGroupId))
	assert.Equal(t, "dead-letters", aws.ToString(f.api.sent[1].MessageGroupId))
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
	for _, c := range api.created {
		assert.Equal(t, map[string]string{"FifoQueue": "true"}, c.Attributes, "only the immutable attribute on create")
	}
	require.Len(t, api.configured, 1, "mutable attributes are reconciled on existing queues")
	assert.Equal(t, "http://sqs/in.fifo", aws.ToString(api.configured[0].QueueUrl))
	assert.Contains(t, api.configured[0].Attributes["RedrivePolicy"], `"maxReceiveCount":"5"`)
	assert.Equal(t, "30", api.configured[0].Attributes["VisibilityTimeout"])

	resolved, err := sqsadapter.ResolveQueues(context.Background(), api, names)
	require.NoError(t, err)
	assert.Equal(t, q, resolved)
	require.NoError(t, sqsadapter.QueueCheck(api, q.Input)(context.Background()))
}

func TestProvisionFailures(t *testing.T) {
	t.Parallel()
	names := sqsadapter.QueueNames{Input: "in.fifo", DLQ: "dlq.fifo", Events: "events.fifo"}
	for _, failing := range []string{"create:dlq.fifo", "attrs", "create:in.fifo", "set:http://sqs/in.fifo", "create:events.fifo"} {
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

func TestConsumerRejectsSendersActingForOtherProviders(t *testing.T) {
	t.Parallel()
	proc := &fakeProcessor{}
	cfg := consumerConfig()
	cfg.Senders = sqsadapter.SenderPolicy{"AIDA-PROVIDER-A": {"provider-b"}}
	f := newConsumerWith(t, cfg, proc, message("a", body("m-a", "1.00"), "1"))
	f.c.PollOnce(context.Background())
	assert.Zero(t, proc.calls, "the operation never reaches the use case")
	require.Len(t, f.api.sent, 1)
	assert.Contains(t, aws.ToString(f.api.sent[0].MessageAttributes["failureReason"].StringValue), "sender is not allowed")
	assert.Equal(t, []string{"rh-a"}, f.api.deleted)
}

func TestSenderPolicy(t *testing.T) {
	t.Parallel()
	p, err := sqsadapter.ParseSenderPolicy("AIDA1=provider-a|provider-b; ROLE2=*")
	require.NoError(t, err)
	require.NoError(t, p.Authorize("AIDA1", "provider-b"))
	require.NoError(t, p.Authorize("ROLE2", "anything"))
	require.ErrorIs(t, p.Authorize("AIDA1", "provider-c"), sqsadapter.ErrUnauthorizedSender)
	require.ErrorIs(t, p.Authorize("", "provider-a"), sqsadapter.ErrUnauthorizedSender, "a missing SenderId is never trusted")
	for _, bad := range []string{"", "AIDA1", "=provider-a", "AIDA1=", "AIDA1=p;", "AIDA1=a | b", "AIDA1=a||b"} {
		_, err := sqsadapter.ParseSenderPolicy(bad)
		assert.ErrorIs(t, err, sqsadapter.ErrInvalidSenderPolicy, bad)
	}

	merged, err := sqsadapter.ParseSenderPolicy("AIDA1=provider-a;AIDA1=provider-b;ROLE2=*|provider-c")
	require.NoError(t, err)
	require.NoError(t, merged.Authorize("AIDA1", "provider-a"), "a repeated sender merges its providers")
	require.NoError(t, merged.Authorize("AIDA1", "provider-b"))
	require.NoError(t, merged.Authorize("ROLE2", "provider-z"), "* alongside an explicit list still allows everything")
}

func TestConsumerKeepsGroupOrderAfterARetry(t *testing.T) {
	t.Parallel()
	proc := &fakeProcessor{results: []error{app.ErrUnavailable, nil}}
	f := newConsumer(t, proc,
		groupMessage("a1", bodyAt("m-a1", "2026-09-08T12:00:00Z", "1.00", testWalletA), "1", testWalletA),
		groupMessage("a2", bodyAt("m-a2", "2026-09-08T12:00:00Z", "1.00", testWalletA), "1", testWalletA),
		groupMessage("b1", bodyAt("m-b1", "2026-09-08T12:00:00Z", "1.00", testWalletB), "1", testWalletB))
	f.c.PollOnce(context.Background())

	assert.Equal(t, 2, proc.calls, "a2 is never processed before its retried head")
	assert.Equal(t, []string{"rh-b1"}, f.api.deleted, "other groups keep flowing")
	assert.Equal(t, map[string]int32{"rh-a1": 2, "rh-a2": 0}, f.api.visibility, "the tail is released right away")
}

func TestConsumerRejectsInvalidFIFOIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		msg  types.Message
	}{
		{
			name: "message group differs from wallet",
			msg: fifoMessage("group", bodyAt("message-1", "2026-09-22T12:00:00Z", "1.00", testWalletA), "1",
				testWalletB, "message-1"),
		},
		{
			name: "deduplication id differs from message id",
			msg: fifoMessage("dedup", bodyAt("message-1", "2026-09-22T12:00:00Z", "1.00", testWalletA), "1",
				testWalletA, "other-message"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			proc := &fakeProcessor{}
			f := newConsumer(t, proc, tt.msg)
			f.c.PollOnce(context.Background())
			assert.Zero(t, proc.calls)
			require.Len(t, f.api.sent, 1)
			assert.Equal(t, []string{aws.ToString(tt.msg.ReceiptHandle)}, f.api.deleted)
		})
	}
}
