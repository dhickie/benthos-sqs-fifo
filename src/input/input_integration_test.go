//go:build integration

package sqs_fifo

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test/wait"
	"slices"
	"sync"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
)

const (
	BaseEndpoint    = "http://localhost:4566"
	Region          = "eu-west-1"
	QueueName       = "test.fifo"
	QueueUrl        = "http://localhost:4566/000000000000/test.fifo"
	NumGroups       = 10
	NumMsgsPerGroup = 10
	NumReadThreads  = 5
	RngSeed         = 0 // 0 = current timestamp
)

func TestInput(t *testing.T) {
	// Arrange
	ctx := t.Context()
	aConf, err := createAwsConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := createClient(aConf)

	t.Log("Checking input queue...")
	createQueueIfRequired(t, ctx, client)

	t.Log("Generating test data...")
	iMsgs := generateTestData(t, client) // Publish test messages to the queue
	input := createIntegrationTestInput(t, aConf)

	t.Log("Connecting input...")
	if err := input.Connect(ctx); err != nil { // Start the input
		t.Fatal(err)
	}
	defer input.Close(ctx)

	totalMsgs := NumGroups * NumMsgsPerGroup
	msgs := make([]*service.Message, 0, totalMsgs)
	mu := &sync.Mutex{}
	errs := make([]error, NumReadThreads)
	readFunc := func(i int) func() { // We'll read across multiple threads to test concurrency
		return func() {
			for range totalMsgs / NumReadThreads {
				if msg, ackFunc, err := input.Read(ctx); err != nil {
					t.Errorf("An error occurred while reading input: %v", err)
					errs[i] = err
					return
				} else {
					mu.Lock()
					msgs = append(msgs, msg)
					mu.Unlock()
					if err := ackFunc(ctx, nil); err != nil {
						t.Errorf("An error occurred while acking the message: %v", err)
						errs[i] = err
						return
					}
				}
			}
		}
	}

	// Act - read out all the messages across multiple threads
	t.Log("Starting read threads...")
	wg := &sync.WaitGroup{}
	for i := range NumReadThreads {
		wg.Go(readFunc(i))
	}
	wg.Wait()

	errored := slices.ContainsFunc(errs, func(err error) bool {
		if err != nil {
			return true
		}
		return false
	})
	if errored {
		t.FailNow()
	}

	// Assert
	t.Log("Asserting message ordering...")
	oMsgs := make(map[string][]*service.Message) // Group the messages by id
	for _, msg := range msgs {
		if gId, ok := msg.MetaGet(metaKeyGroupId); !ok {
			t.Fatal("Unable to find group ID on message")
		} else if _, ok := oMsgs[gId]; !ok {
			oMsgs[gId] = make([]*service.Message, 0, NumMsgsPerGroup)
			oMsgs[gId] = append(oMsgs[gId], msg)
		} else {
			oMsgs[gId] = append(oMsgs[gId], msg)
		}
	}

	// Check for each group that the messages were output in the same order they were added to the queue
	for k, v := range oMsgs {
		iGroup := iMsgs[k]
		for i, msg := range v {
			if oMsgId, ok := msg.MetaGet(metaKeyMsgId); !ok {
				t.Fatal("Unable to find message ID on output message")
			} else {
				assert.Equal(
					t,
					*iGroup[i].Msg.MessageId,
					oMsgId,
					"Messages should have been output in the same order as they were input for each group")
			}
		}
	}
}

// Generates random test messages distributed across a number of different groups, and sends them to the queue in a
// randomised interleaved order
func generateTestData(t *testing.T, client *sqs.Client) map[string][]*models.SqsMessage {
	ctx := t.Context()
	msgs := make(map[string][]*models.SqsMessage)
	mu := &sync.Mutex{}
	rng := test.NewRng(RngSeed)
	errs := make([]error, NumGroups)

	genFunc := func(i int) func() {
		return func() {
			group := test.CreateMessages(1, NumMsgsPerGroup)
			gId := group[0].GetGroupId()
			mu.Lock()
			msgs[gId] = make([]*models.SqsMessage, 0, NumMsgsPerGroup)
			msgs[gId] = append(msgs[gId], group...)
			mu.Unlock()

			for len(group) > 0 {
				if err := sendMessage(ctx, client, group[0]); err != nil {
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
	for i := range NumGroups {
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

// Sends a message to the queue
func sendMessage(ctx context.Context, client *sqs.Client, message *models.SqsMessage) error {
	gId := message.GetGroupId()
	req := &sqs.SendMessageInput{
		MessageBody:            message.Msg.Body,
		QueueUrl:               awssdk.String(QueueUrl),
		DelaySeconds:           0,
		MessageAttributes:      message.Msg.MessageAttributes,
		MessageDeduplicationId: message.Msg.MessageId,
		MessageGroupId:         &gId,
	}

	if res, err := client.SendMessage(ctx, req); err != nil {
		return err
	} else {
		message.Msg.MessageId = res.MessageId
	}

	return nil
}

// Creates the test queue if it doesn't already exist
func createQueueIfRequired(t *testing.T, ctx context.Context, client *sqs.Client) {
	if exists, err := testQueueExists(ctx, client); err != nil {
		t.Fatal(err)
	} else if exists {
		// Purge the queue of any messages from previous runs
		t.Log("Queue already exists, purging...")
		req := &sqs.PurgeQueueInput{
			QueueUrl: awssdk.String(QueueUrl),
		}
		if _, err := client.PurgeQueue(ctx, req); err != nil {
			t.Fatal(err)
		}
		return
	}

	t.Log("New queue needed, creating input queue...")
	req := &sqs.CreateQueueInput{
		QueueName: awssdk.String(QueueName),
		Attributes: map[string]string{
			"FifoQueue": "true",
		},
	}
	if _, err := client.CreateQueue(ctx, req); err != nil {
		t.Fatal(err)
	}
}

// Checks whether the test queue already exists
func testQueueExists(ctx context.Context, client *sqs.Client) (bool, error) {
	var next *string
	for {
		req := &sqs.ListQueuesInput{}
		if next != nil {
			req.NextToken = next
		}

		if res, err := client.ListQueues(ctx, req); err != nil {
			return false, err
		} else if slices.Contains(res.QueueUrls, QueueUrl) {
			return true, nil
		} else if res.NextToken == nil {
			return false, nil
		} else {
			next = res.NextToken
		}
	}
}

func createAwsConfig(ctx context.Context) (*awssdk.Config, error) {
	aConf, err := config.LoadDefaultConfig(ctx, func(o *config.LoadOptions) error {
		o.BaseEndpoint = BaseEndpoint
		o.Region = Region

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &aConf, nil
}

// Creates the SQS client to talk to Floci
func createClient(conf *awssdk.Config) *sqs.Client {
	return sqs.NewFromConfig(*conf)
}

func createIntegrationTestInput(t *testing.T, aconf *awssdk.Config) *SqsFifoInput {
	iconf := &models.InputConfig{
		QueueUrl:                 QueueUrl,
		BaseEndpoint:             BaseEndpoint,
		VisibilityTimeoutSeconds: 30,
		MinReceiveBatchSize:      5,
		MaxReceiveBatchSize:      10,
		MaxInFlightMessages:      30,
		MaxProcessingAttempts:    3,
		MaxPendingAcks:           10,
	}

	sqsClient := aws.NewSqsClient(iconf, aconf)
	logger := test.NewTestLogger(t)
	return NewSqsFifoInput(sqsClient, iconf, logger)
}
