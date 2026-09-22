// Package sqs connects the service to AWS SQS (LocalStack locally): queue
// provisioning, the wager-transactions consumer and the outbox publisher.
package sqs

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// API is the subset of the SQS client used by the service.
type API interface {
	CreateQueue(ctx context.Context, in *sqs.CreateQueueInput, opts ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error)
	GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, opts ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// ClientConfig configures the SQS client. Credentials come from the default
// AWS chain (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY locally).
type ClientConfig struct {
	Region   string
	Endpoint string
}

// NewClient builds the SQS client.
func NewClient(ctx context.Context, cfg ClientConfig) (*sqs.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

// QueueNames are the logical queue names.
type QueueNames struct {
	Input  string
	DLQ    string
	Events string
}

// Queues are the resolved queue URLs.
type Queues struct {
	Input  string
	DLQ    string
	Events string
}

// ResolveQueues looks up the URLs of existing queues.
func ResolveQueues(ctx context.Context, api API, names QueueNames) (Queues, error) {
	var q Queues
	for _, pair := range []struct {
		name string
		dst  *string
	}{{names.Input, &q.Input}, {names.DLQ, &q.DLQ}, {names.Events, &q.Events}} {
		out, err := api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(pair.name)})
		if err != nil {
			return Queues{}, fmt.Errorf("resolve queue %s: %w", pair.name, err)
		}
		*pair.dst = aws.ToString(out.QueueUrl)
	}
	return q, nil
}

// ProvisionConfig configures queue creation.
type ProvisionConfig struct {
	Names             QueueNames
	MaxReceiveCount   int
	VisibilityTimeout int
}

// Provision creates (idempotently) the FIFO input queue, its FIFO DLQ with
// the redrive policy, and the FIFO events queue that receives the outbox.
func Provision(ctx context.Context, api API, cfg ProvisionConfig) (Queues, error) {
	dlq, err := createFIFO(ctx, api, cfg.Names.DLQ, nil)
	if err != nil {
		return Queues{}, err
	}
	dlqArn, err := queueArn(ctx, api, dlq)
	if err != nil {
		return Queues{}, err
	}
	redrive := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"%d"}`, dlqArn, cfg.MaxReceiveCount)
	input, err := createFIFO(ctx, api, cfg.Names.Input, map[string]string{
		string(types.QueueAttributeNameRedrivePolicy):     redrive,
		string(types.QueueAttributeNameVisibilityTimeout): strconv.Itoa(cfg.VisibilityTimeout),
	})
	if err != nil {
		return Queues{}, err
	}
	events, err := createFIFO(ctx, api, cfg.Names.Events, nil)
	return Queues{Input: input, DLQ: dlq, Events: events}, err
}

func createFIFO(ctx context.Context, api API, name string, attrs map[string]string) (string, error) {
	all := map[string]string{string(types.QueueAttributeNameFifoQueue): "true"}
	for k, v := range attrs {
		all[k] = v
	}
	out, err := api.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: all})
	if err != nil {
		return "", fmt.Errorf("create queue %s: %w", name, err)
	}
	return aws.ToString(out.QueueUrl), nil
}

func queueArn(ctx context.Context, api API, url string) (string, error) {
	out, err := api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", fmt.Errorf("queue arn: %w", err)
	}
	return out.Attributes[string(types.QueueAttributeNameQueueArn)], nil
}

// QueueCheck returns a readiness probe for a queue.
func QueueCheck(api API, url string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		_, err := api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(url), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		})
		return err
	}
}
