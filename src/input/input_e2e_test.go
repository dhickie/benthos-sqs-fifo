//go:build e2e

package sqs_fifo

import (
	"dhickie/benthos-sqs-fifo/src/input/internal/models"
	"dhickie/benthos-sqs-fifo/src/input/internal/test"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	E2EInputQueueUrl  = "http://localhost:4566/000000000000/e2etest.fifo"
	E2EOutputQueueUrl = "http://localhost:4566/000000000000/e2etest-output.fifo"
)

func TestE2EPipeline(t *testing.T) {
	// Test setup
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

	t.Log("Purging input and output queues...")
	if err := sqsHelper.PurgeQueue(ctx, E2EInputQueueUrl); err != nil {
		t.Fatal(err)
	}
	if err := sqsHelper.PurgeQueue(ctx, E2EOutputQueueUrl); err != nil {
		t.Fatal(err)
	}

	// Publish test data to the input queue
	t.Log("Generating test data...")
	iMsgs := test.SeedTestMessages(t, RngSeed, sqsHelper, E2EInputQueueUrl, NumGroups, NumMsgsPerGroup)

	// Receive messages from the output queue
	oMsgs := make(map[string][]*models.SqsMessage)
	totalMsgs := NumGroups * NumMsgsPerGroup
	nMsgs := 0
	for nMsgs < totalMsgs {
		if batch, err := sqsHelper.ReceiveMessages(ctx, E2EOutputQueueUrl); err != nil {
			t.Fatal(err)
		} else {
			for _, msg := range batch {
				gId := msg.GetGroupId()
				if _, ok := oMsgs[gId]; !ok {
					oMsgs[gId] = make([]*models.SqsMessage, 0, NumMsgsPerGroup)
				}
				oMsgs[gId] = append(oMsgs[gId], msg)
				nMsgs++
			}

			if err := sqsHelper.DeleteMessages(ctx, E2EOutputQueueUrl, batch); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Assert that messages in each group were published in the correct order
	for gId, msgs := range iMsgs {
		oBatch := oMsgs[gId]
		for i, msg := range msgs {
			assert.Equal(t, *msg.Msg.Body, *oBatch[i].Msg.Body, "Messages should have been output in the same order as they were input for each group")
		}
	}
}
