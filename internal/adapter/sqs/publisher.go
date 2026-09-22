package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

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

// Publish implements worker.Publisher. It is idempotent per EventID (the
// FIFO deduplication id). Payloads are not size-checked: event envelopes
// are a few hundred bytes, far below the 256 KiB SQS limit, and an oversize
// payload would surface as a publish error and be retried until dead.
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
