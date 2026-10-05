//go:build integration

package sqs_fifo

import (
	"dhickie/benthos-sqs-fifo/src/input/internal/test"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
)

func TestInput(t *testing.T) {
	// Arrange
	if err := os.Setenv("AWS_ACCESS_KEY_ID", "test"); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("AWS_SECRET_ACCESS_KEY", "test"); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	aConf, err := createAwsConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := createClient(aConf)
	sqsHelper := test.NewSqsHelper(client)

	t.Log("Purging input queue...")
	if err := sqsHelper.PurgeQueue(ctx, QueueName); err != nil {
		t.Fatal(err)
	}

	t.Log("Generating test data...")
	iMsgs := test.SeedTestMessages(t, RngSeed, sqsHelper, QueueUrl, NumGroups, NumMsgsPerGroup) // Publish test messages to the queue
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
