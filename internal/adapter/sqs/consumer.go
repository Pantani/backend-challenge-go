package sqs

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/observability"
)

// Consumer outcomes, used as metric labels.
const (
	OutcomeProcessed = "processed"
	OutcomeDuplicate = "duplicate"
	OutcomeRetry     = "retry"
	OutcomeDLQ       = "dlq"
	OutcomeReleased  = "released"
)

// Processor handles a decoded message (app.WagerService).
type Processor interface {
	ConsumeMessage(ctx context.Context, msg app.InboundMessage) (app.ConsumeResult, error)
}

// ConsumerMetrics counts consumed messages.
type ConsumerMetrics interface {
	SQSMessage(outcome string)
}

// ConsumerConfig configures the consumer.
type ConsumerConfig struct {
	Name              string
	QueueURL          string
	DLQURL            string
	MaxMessages       int32
	WaitTime          time.Duration
	VisibilityTimeout time.Duration
	// ProcessTimeout bounds one message; it must stay below the visibility
	// timeout so a message is never processed twice concurrently.
	ProcessTimeout time.Duration
	RetryBase      time.Duration
	RetryMax       time.Duration
	// Senders binds broker identities to the providers they may act for.
	Senders SenderPolicy
}

// Consumer polls the FIFO input queue. A message is deleted only after its
// handling committed; transient failures are retried with exponential
// backoff through the visibility timeout, and after maxReceiveCount the
// queue redrive policy moves the message to the DLQ. Invalid messages and
// permanent errors go to the DLQ immediately.
type Consumer struct {
	api     API
	cfg     ConsumerConfig
	proc    Processor
	logger  *slog.Logger
	metrics ConsumerMetrics
}

// NewConsumer builds a consumer.
func NewConsumer(api API, cfg ConsumerConfig, proc Processor, logger *slog.Logger, metrics ConsumerMetrics) *Consumer {
	return &Consumer{api: api, cfg: cfg, proc: proc, logger: logger, metrics: metrics}
}

// Run polls until ctx is cancelled (SIGTERM): it then stops fetching,
// finishes the message in progress within ProcessTimeout and releases the
// visibility of the messages it had not started.
func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		c.PollOnce(ctx)
	}
}

// PollOnce receives and handles one batch.
func (c *Consumer) PollOnce(ctx context.Context) {
	out, err := c.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.cfg.QueueURL),
		MaxNumberOfMessages: c.cfg.MaxMessages,
		WaitTimeSeconds:     int32(c.cfg.WaitTime.Seconds()),
		VisibilityTimeout:   int32(c.cfg.VisibilityTimeout.Seconds()),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount, types.MessageSystemAttributeNameSenderId,
			types.MessageSystemAttributeNameMessageGroupId,
		},
	})
	if err != nil {
		c.receiveFailed(ctx, err)
		return
	}
	c.process(ctx, out.Messages)
}

// process handles a batch in order. A batch can carry several messages of
// the same MessageGroupId (the in-order tail of a wallet): once a message is
// retried, the rest of its group is released instead of processed, so the
// group is redelivered in order after the retried head.
func (c *Consumer) process(ctx context.Context, msgs []types.Message) {
	blocked := map[string]bool{}
	for i, m := range msgs {
		if ctx.Err() != nil {
			c.release(ctx, msgs[i:])
			return
		}
		c.step(ctx, m, blocked)
	}
}

func (c *Consumer) step(ctx context.Context, m types.Message, blocked map[string]bool) {
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if blocked[group] {
		c.release(ctx, []types.Message{m})
		return
	}
	if !c.handle(ctx, m) && group != "" {
		blocked[group] = true
	}
}

func (c *Consumer) receiveFailed(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}
	c.logger.WarnContext(ctx, "sqs receive failed", "error", err)
	sleep(ctx, c.cfg.RetryBase)
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// handle runs detached from the shutdown signal so the message in progress
// completes (or times out) instead of being aborted mid-way. It reports false
// when the message was left for a retry.
func (c *Consumer) handle(parent context.Context, m types.Message) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), c.cfg.ProcessTimeout)
	defer cancel()
	ctx = observability.WithAttrs(ctx, slog.String("sqsMessageId", aws.ToString(m.MessageId)))
	msg, err := c.decode(m)
	if err != nil {
		c.deadLetter(ctx, m, err)
		return true
	}
	ctx = observability.WithAttrs(ctx, slog.String("messageId", msg.MessageID), slog.String("correlationId", msg.MessageID),
		slog.String("providerId", msg.Command.ProviderID), slog.String("walletId", msg.Command.WalletID.String()))
	res, err := c.proc.ConsumeMessage(ctx, msg)
	switch {
	case err == nil:
		c.ack(ctx, m, res)
	case app.IsTransient(err):
		c.retry(ctx, m, err)
		return false
	default:
		c.deadLetter(ctx, m, err)
	}
	return true
}

// decode validates the message and binds its providerId to the sender.
func (c *Consumer) decode(m types.Message) (app.InboundMessage, error) {
	msg, err := DecodeMessage(c.cfg.Name, aws.ToString(m.Body))
	if err != nil {
		return app.InboundMessage{}, err
	}
	sender := m.Attributes[string(types.MessageSystemAttributeNameSenderId)]
	return msg, c.cfg.Senders.Authorize(sender, msg.Command.ProviderID)
}

func (c *Consumer) ack(ctx context.Context, m types.Message, res app.ConsumeResult) {
	outcome := OutcomeProcessed
	if res.Duplicate {
		outcome = OutcomeDuplicate
	} else {
		ctx = observability.WithAttrs(ctx, slog.String("transactionId", res.Result.Transaction.ID().String()))
	}
	c.logger.InfoContext(ctx, "sqs message handled", "outcome", outcome)
	c.metrics.SQSMessage(outcome)
	c.delete(ctx, m)
}

func (c *Consumer) delete(ctx context.Context, m types.Message) {
	_, err := c.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: m.ReceiptHandle})
	if err != nil {
		// The handling is committed; a redelivery is absorbed by the inbox.
		c.logger.WarnContext(ctx, "sqs delete failed; redelivery will be deduplicated", "error", err)
	}
}

// retry hides the message for an exponential backoff based on its receive
// count; SQS redrives it to the DLQ after maxReceiveCount receives.
func (c *Consumer) retry(ctx context.Context, m types.Message, cause error) {
	delay := c.backoff(receiveCount(m))
	c.logger.WarnContext(ctx, "sqs message will be retried", "error", cause, "delaySeconds", int(delay.Seconds()))
	c.metrics.SQSMessage(OutcomeRetry)
	c.changeVisibility(ctx, m, delay)
}

func (c *Consumer) backoff(count int) time.Duration {
	delay := c.cfg.RetryBase << min(max(count-1, 0), 16)
	return min(delay, c.cfg.RetryMax)
}

func receiveCount(m types.Message) int {
	n, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	return n
}

func (c *Consumer) changeVisibility(ctx context.Context, m types.Message, d time.Duration) {
	_, err := c.api.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: int32(d.Seconds()),
	})
	if err != nil {
		c.logger.WarnContext(ctx, "sqs change visibility failed", "error", err)
	}
}

// deadLetter copies the message to the DLQ with the failure reason, then
// removes it from the input queue. If the copy fails the message stays and
// the redrive policy eventually moves it.
func (c *Consumer) deadLetter(ctx context.Context, m types.Message, cause error) {
	c.logger.ErrorContext(ctx, "sqs message sent to DLQ", "error", cause)
	_, err := c.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.cfg.DLQURL),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String("dead-letters"),
		MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"failureReason": {DataType: aws.String("String"), StringValue: aws.String(truncate(cause.Error(), 256))},
		},
	})
	if err != nil {
		c.logger.ErrorContext(ctx, "sqs DLQ send failed", "error", err)
		return
	}
	c.metrics.SQSMessage(OutcomeDLQ)
	c.delete(ctx, m)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// release makes unstarted messages visible again for a safe redelivery.
func (c *Consumer) release(parent context.Context, msgs []types.Message) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), c.cfg.ProcessTimeout)
	defer cancel()
	for _, m := range msgs {
		c.metrics.SQSMessage(OutcomeReleased)
		c.changeVisibility(ctx, m, 0)
	}
}

// Publisher sends outbox events to the FIFO events queue. MessageGroupId is
// the wallet id (per-wallet ordering) and MessageDeduplicationId the stable
// eventId, so a republication inside the FIFO window is dropped by SQS and
// later ones carry the same eventId for consumer-side deduplication.
type Publisher struct {
	api      API
	queueURL string
}

// NewPublisher builds a publisher.
func NewPublisher(api API, queueURL string) *Publisher {
	return &Publisher{api: api, queueURL: queueURL}
}

// Publish implements the relay publisher.
func (p *Publisher) Publish(ctx context.Context, m app.OutboxMessage) error {
	_, err := p.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(m.Payload)),
		MessageGroupId:         aws.String(m.PartitionKey),
		MessageDeduplicationId: aws.String(m.EventID.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType":     {DataType: aws.String("String"), StringValue: aws.String(m.EventType)},
			"eventId":       {DataType: aws.String("String"), StringValue: aws.String(m.EventID.String())},
			"aggregateType": {DataType: aws.String("String"), StringValue: aws.String(m.AggregateType)},
		},
	})
	return err
}
