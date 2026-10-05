package mocks

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/aws"
	"dhickie/benthos-sqs-fifo/src/input/internal/models"

	"github.com/stretchr/testify/mock"
)

type MockSqsClient struct {
	mock.Mock
}

func (m *MockSqsClient) SetMessageVisibility(ctx context.Context, newTimeoutSeconds int32, msgs ...*models.SqsMessage) (*aws.BatchOpResult, error) {
	args := m.Called(ctx, newTimeoutSeconds, msgs)
	return args.Get(0).(*aws.BatchOpResult), args.Error(1)
}

func (m *MockSqsClient) ReceiveMessages(ctx context.Context, maxMsgs int) ([]*models.SqsMessage, error) {
	args := m.Called(ctx, maxMsgs)
	return args.Get(0).([]*models.SqsMessage), args.Error(1)
}

func (m *MockSqsClient) DeleteMessages(ctx context.Context, msgs []*models.SqsMessage) (*aws.BatchOpResult, error) {
	args := m.Called(ctx, msgs)
	return args.Get(0).(*aws.BatchOpResult), args.Error(1)
}

func (m *MockSqsClient) GetQueueVisibilityTimeout(ctx context.Context) (int, error) {
	args := m.Called(ctx)
	return args.Int(0), args.Error(1)
}
