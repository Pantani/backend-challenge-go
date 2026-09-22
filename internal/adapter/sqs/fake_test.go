package sqs_test

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// fakeAPI records calls and serves canned responses.
type fakeAPI struct {
	mu          sync.Mutex
	receive     func(ctx context.Context) (*sqs.ReceiveMessageOutput, error)
	errs        map[string]error
	deleted     []string
	visibility  map[string]int32
	sent        []*sqs.SendMessageInput
	created     []*sqs.CreateQueueInput
	configured  []*sqs.SetQueueAttributesInput
	attrQueries int
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{errs: map[string]error{}, visibility: map[string]int32{}}
}

func (f *fakeAPI) err(op string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errs[op]
}

func (f *fakeAPI) CreateQueue(_ context.Context, in *sqs.CreateQueueInput, _ ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	if err := f.err("create:" + aws.ToString(in.QueueName)); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, in)
	return &sqs.CreateQueueOutput{QueueUrl: aws.String("http://sqs/" + aws.ToString(in.QueueName))}, nil
}

func (f *fakeAPI) GetQueueUrl( //nolint:revive // name imposed by the AWS SDK interface
	_ context.Context, in *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	if err := f.err("url:" + aws.ToString(in.QueueName)); err != nil {
		return nil, err
	}
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("http://sqs/" + aws.ToString(in.QueueName))}, nil
}

func (f *fakeAPI) GetQueueAttributes(_ context.Context, in *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	f.mu.Lock()
	f.attrQueries++
	f.mu.Unlock()
	if err := f.err("attrs"); err != nil {
		return nil, err
	}
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{
		string(types.QueueAttributeNameQueueArn): "arn:aws:sqs:us-east-1:000000000000:" + aws.ToString(in.QueueUrl),
	}}, nil
}

func (f *fakeAPI) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return f.receive(ctx)
}

func (f *fakeAPI) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if err := f.err("delete"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeAPI) ChangeMessageVisibility(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	if err := f.err("visibility"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visibility[aws.ToString(in.ReceiptHandle)] = in.VisibilityTimeout
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeAPI) SetQueueAttributes(_ context.Context, in *sqs.SetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error) {
	if err := f.err("set:" + aws.ToString(in.QueueUrl)); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.configured = append(f.configured, in)
	return &sqs.SetQueueAttributesOutput{}, nil
}

func (f *fakeAPI) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if err := f.err("send"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, in)
	return &sqs.SendMessageOutput{}, nil
}
