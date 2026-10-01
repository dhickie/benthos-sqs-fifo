package test

import (
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test/wait"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/util"
	"slices"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func CreateMessages(nGroups, nMsgs int) []*models.SqsMessage {
	var msgs []*models.SqsMessage
	gIds := make([]string, 0, nGroups)
	for range nGroups {
		gIds = append(gIds, uuid.New().String())
	}

	for range nMsgs {
		msgId := uuid.NewV4().String()

		for j := range nGroups {
			gId := gIds[j]
			receiptHandle := uuid.New().String()
			rawMsg := types.Message{
				Attributes: map[string]string{
					"MessageGroupId": gId,
				},
				Body:          &msgId,
				MessageId:     &msgId,
				ReceiptHandle: &receiptHandle,
			}
			msg := models.NewSqsMessage(rawMsg, 2)
			msgs = append(msgs, msg)
		}
	}

	return msgs
}

func SeedTestMessages(t *testing.T, rngSeed int64, sqsHelper *SqsHelper, queueUrl string, nGroups, nMsgs int) map[string][]*models.SqsMessage {
	ctx := t.Context()
	msgs := make(map[string][]*models.SqsMessage)
	mu := &sync.Mutex{}
	rng := NewRng(rngSeed)
	errs := make([]error, nGroups)

	genFunc := func(i int) func() {
		return func() {
			group := CreateMessages(1, nMsgs)
			gId := group[0].GetGroupId()
			mu.Lock()
			msgs[gId] = make([]*models.SqsMessage, 0, nMsgs)
			msgs[gId] = append(msgs[gId], group...)
			mu.Unlock()

			for len(group) > 0 {
				if err := sqsHelper.SendMessage(ctx, group[0], queueUrl); err != nil {
					t.Logf("An error occurred sending the test message: %v", err)
					errs[i] = err
					return
				}

				group = group[1:]
				w := rng.RandRange(1, 1000)
				d := time.Duration(w) * time.Microsecond
				if err := wait.For(ctx, d); err != nil {
					t.Logf("An error occurred sending the test message: %v", err)
					errs[i] = err
					return
				}
			}
		}
	}

	wg := sync.WaitGroup{}
	for i := range nGroups {
		wg.Go(genFunc(i))
	}
	wg.Wait()

	errored := slices.ContainsFunc(errs, func(e error) bool {
		return e != nil
	})
	if errored {
		t.FailNow()
	}

	return msgs
}

func CreateMessagesWithDeadline(nGroups, nMsgs, deadlineSeconds int) []*models.SqsMessage {
	msgs := CreateMessages(nGroups, nMsgs)

	util.Select(msgs, func(m *models.SqsMessage) *models.SqsMessage {
		m.Deadline = time.Now().Add(time.Duration(deadlineSeconds) * time.Second)
		return m
	})

	return msgs
}

func BatchSuccessResult(msgs []*models.SqsMessage) *aws.BatchOpResult {
	ids := util.Select(msgs, func(m *models.SqsMessage) *aws.BatchItemSuccess {
		return &aws.BatchItemSuccess{
			MsgId: *m.Msg.MessageId,
		}
	})
	return &aws.BatchOpResult{
		Successes: ids,
		Failures:  []*aws.BatchItemFailure{},
	}
}
