package aws

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/models"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"strconv"
	"uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	maxBatchSize = 10
)

// SqsClient provides access to functions of the SQS API
type SqsClient struct {
	client *sqs.Client
	conf   *models.InputConfig
}

// ISqsClient is the interface for any type providing access to the SQS API
type ISqsClient interface {
	SetMessageVisibility(
		ctx context.Context,
		newTimeoutSeconds int32,
		msgs ...*models.SqsMessage) (*BatchOpResult, error)

	ReceiveMessages(ctx context.Context, maxMsgs int) ([]*models.SqsMessage, error)

	DeleteMessages(ctx context.Context, msgs []*models.SqsMessage) (*BatchOpResult, error)

	GetQueueVisibilityTimeout(ctx context.Context) (int, error)
}

// NewSqsClient returns a new client using the provided configuration
func NewSqsClient(conf *models.InputConfig, aconf *aws.Config) *SqsClient {
	return &SqsClient{
		client: sqs.NewFromConfig(*aconf),
		conf:   conf,
	}
}

// SetMessageVisibility updates the visibility of between 1 and 10 messages to a time in the future.
// Returns a slice containing any message IDs which didn't have their visibility timeouts updated successfully.
func (c *SqsClient) SetMessageVisibility(
	ctx context.Context,
	newTimeoutSeconds int32,
	msgs ...*models.SqsMessage) (*BatchOpResult, error) {
	if len(msgs) == 0 {
		return newEmptyBatchOpResult(), nil
	}

	if len(msgs) > maxBatchSize {
		return nil, newBatchSizeError(len(msgs), "SetMessageVisibility")
	}

	req := sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: &c.conf.QueueUrl,
		Entries:  []types.ChangeMessageVisibilityBatchRequestEntry{},
	}
	for _, msg := range msgs {
		entry := types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                msg.Msg.MessageId,
			ReceiptHandle:     msg.Msg.ReceiptHandle,
			VisibilityTimeout: newTimeoutSeconds,
		}
		req.Entries = append(req.Entries, entry)
	}

	res, err := c.client.ChangeMessageVisibilityBatch(ctx, &req)
	if err != nil {
		return nil, err
	}

	if len(res.Failed) == 0 { // All successful
		return newEmptyBatchOpResult(), nil
	}

	bId := uuid.New().String()
	return toBatchOpResult(res.Successful, res.Failed, bId, func(entry types.ChangeMessageVisibilityBatchResultEntry) *string {
		return entry.Id
	}), nil
}

// ReceiveMessages pulls a batch of messages from the queue
func (c *SqsClient) ReceiveMessages(ctx context.Context, maxMsgs int) ([]*models.SqsMessage, error) {
	maxM := int32(min(float64(maxMsgs), float64(maxBatchSize)))
	req := sqs.ReceiveMessageInput{
		QueueUrl:            &c.conf.QueueUrl,
		MaxNumberOfMessages: maxM,
		MessageAttributeNames: []string{
			"All",
		},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			"All",
		},
	}

	res, err := c.client.ReceiveMessage(ctx, &req)
	if err != nil {
		return nil, err
	}

	msgs := make([]*models.SqsMessage, len(res.Messages))
	for i, rawMsg := range res.Messages {
		msgs[i] = models.NewSqsMessage(rawMsg, c.conf.VisibilityTimeoutSeconds)
	}

	return msgs, nil
}

// DeleteMessages deletes a batch of messages from the queue
func (c *SqsClient) DeleteMessages(ctx context.Context, msgs []*models.SqsMessage) (*BatchOpResult, error) {
	if len(msgs) == 0 {
		return newEmptyBatchOpResult(), nil
	}

	if len(msgs) > maxBatchSize {
		return nil, newBatchSizeError(len(msgs), "DeleteMessages")
	}

	req := sqs.DeleteMessageBatchInput{
		QueueUrl: &c.conf.QueueUrl,
		Entries:  make([]types.DeleteMessageBatchRequestEntry, len(msgs)),
	}
	for i, msg := range msgs {
		entry := types.DeleteMessageBatchRequestEntry{
			Id:            msg.Msg.MessageId,
			ReceiptHandle: msg.Msg.ReceiptHandle,
		}
		req.Entries[i] = entry
	}

	res, err := c.client.DeleteMessageBatch(ctx, &req)
	if err != nil {
		return nil, err
	}

	bId := uuid.New().String()
	return toBatchOpResult(res.Successful, res.Failed, bId, func(entry types.DeleteMessageBatchResultEntry) *string {
		return entry.Id
	}), nil
}

// GetQueueVisibilityTimeout gets the infrastructure configured visibility timeout for the configured queue
func (c *SqsClient) GetQueueVisibilityTimeout(ctx context.Context) (int, error) {
	req := sqs.GetQueueAttributesInput{
		QueueUrl: &c.conf.QueueUrl,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameVisibilityTimeout,
		},
	}

	res, err := c.client.GetQueueAttributes(ctx, &req)
	if err != nil {
		return 0, err
	}

	attr := res.Attributes[string(types.QueueAttributeNameVisibilityTimeout)]
	i, err := strconv.ParseInt(attr, 10, 32)
	if err != nil {
		return 0, err
	}

	return int(i), nil
}

type BatchSuccessResult interface {
	types.DeleteMessageBatchResultEntry | types.ChangeMessageVisibilityBatchResultEntry
}

func toBatchOpResult[T BatchSuccessResult](
	successes []T, failures []types.BatchResultErrorEntry, batchId string, idExtractor func(T) *string) *BatchOpResult {
	s := util.Select(successes, func(v T) *BatchItemSuccess {
		return newBatchItemSuccess(*idExtractor(v))
	})
	f := util.Select(failures, func(v types.BatchResultErrorEntry) *BatchItemFailure {
		return newBatchItemFailure("DeleteMessages", batchId, v.Id, v.Code, v.SenderFault)
	})

	return newBatchOpResult(s, f)
}
