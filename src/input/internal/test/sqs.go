package test

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/util"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SqsHelper struct {
	client *sqs.Client
}

func NewSqsHelper(client *sqs.Client) *SqsHelper {
	return &SqsHelper{client: client}
}

func (h *SqsHelper) SendMessage(ctx context.Context, msg *models.SqsMessage, queueUrl string) error {
	gId := msg.GetGroupId()
	req := &sqs.SendMessageInput{
		MessageBody:            msg.Msg.Body,
		QueueUrl:               &queueUrl,
		DelaySeconds:           0,
		MessageAttributes:      msg.Msg.MessageAttributes,
		MessageDeduplicationId: msg.Msg.MessageId,
		MessageGroupId:         &gId,
	}

	if res, err := h.client.SendMessage(ctx, req); err != nil {
		return err
	} else {
		msg.Msg.MessageId = res.MessageId
	}

	return nil
}

func (h *SqsHelper) ReceiveMessages(ctx context.Context, queueUrl string) ([]*models.SqsMessage, error) {
	req := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: 10,
		MessageAttributeNames: []string{
			"All",
		},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			"All",
		},
	}

	if res, err := h.client.ReceiveMessage(ctx, req); err != nil {
		return nil, err
	} else {
		return util.Select(res.Messages, func(msg types.Message) *models.SqsMessage {
			return models.NewSqsMessage(msg, 30)
		}), nil
	}
}

func (h *SqsHelper) DeleteMessages(ctx context.Context, queueUrl string, msgs []*models.SqsMessage) error {
	req := &sqs.DeleteMessageBatchInput{
		Entries: util.Select(msgs, func(m *models.SqsMessage) types.DeleteMessageBatchRequestEntry {
			return types.DeleteMessageBatchRequestEntry{
				Id:            m.Msg.MessageId,
				ReceiptHandle: m.Msg.ReceiptHandle,
			}
		}),
		QueueUrl: &queueUrl,
	}

	if _, err := h.client.DeleteMessageBatch(ctx, req); err != nil {
		return err
	}

	return nil
}

func (h *SqsHelper) PurgeQueue(ctx context.Context, queueUrl string) error {
	req := &sqs.PurgeQueueInput{
		QueueUrl: &queueUrl,
	}
	if _, err := h.client.PurgeQueue(ctx, req); err != nil {
		return err
	}
	return nil
}
