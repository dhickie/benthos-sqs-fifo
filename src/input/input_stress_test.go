//go:build stress

package sqs_fifo

import (
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test/mocks"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"testing"

	"github.com/stretchr/testify/mock"
)

const (
	NumMessages      = 100_000
	RandSeed         = 0 // Value of 0 means use fresh random data
	MinPerGroup      = 1
	MaxPerGroup      = 10
	ConcurrentGroups = 10
	MinSqsLatencyMs  = 10
	MaxSqsLatencyMs  = 200
)

var testData []*models.SqsMessage
var nextMessageIndex int = 0
var rng = rand.New(&testSource{})

func TestInput_UnderHighLoad(t *testing.T) {
	generateTestData()
	input := createStressTestInput()
	cErr := input.Connect(t.Context())
	if cErr != nil {
		t.Error(cErr)
	}
	defer input.Close(t.Context())

	t.Log("Starting")
	start := time.Now()

	logInterval := NumMessages / 10
	for i := range NumMessages {
		_, ackFunc, rErr := input.Read(t.Context())
		if rErr != nil {
			t.Fatal(rErr)
		}

		aErr := ackFunc(t.Context(), nil)
		if aErr != nil {
			t.Fatal(aErr)
		}

		if i%logInterval == 0 {
			t.Log("Processed msg " + strconv.Itoa(i) + " of " + strconv.Itoa(NumMessages) + " messages")
		}
	}

	elapsed := time.Since(start)

	fmt.Println("Total time: " + elapsed.String())
	fmt.Println("Avg time per msg: " + (elapsed / NumMessages).String())
	fmt.Printf("Processing rate per second: %v\n", float64(NumMessages)/elapsed.Seconds())
}

func generateTestData() {
	testData = make([]*models.SqsMessage, 0, NumMessages)
	groups := make([][]*models.SqsMessage, ConcurrentGroups)
	for i := range groups {
		nMsgs := randRange(MinPerGroup, MaxPerGroup)
		groups[i] = test.CreateMessages(1, nMsgs)
	}

	nextGroup := 0
	for {
		testData = append(testData, groups[nextGroup][0])

		if len(testData) == NumMessages {
			break
		}

		if len(groups[nextGroup]) == 1 {
			nMsgs := randRange(MinPerGroup, MaxPerGroup)
			groups[nextGroup] = test.CreateMessages(1, nMsgs)
		} else {
			groups[nextGroup] = groups[nextGroup][1:]
		}

		if nextGroup == ConcurrentGroups-1 {
			nextGroup = 0
		} else {
			nextGroup++
		}
	}
}

func randRange(min, max int) int {
	return min + rng.IntN(max-min)
}

func createStressTestInput() *SqsFifoInput {
	config := &models.InputConfig{
		QueueUrl:                 "",
		BaseEndpoint:             "",
		VisibilityTimeoutSeconds: 30,
		MinReceiveBatchSize:      5,
		MaxReceiveBatchSize:      10,
		MaxInFlightMessages:      30,
		MaxProcessingAttempts:    3,
		MaxPendingAcks:           10,
	}

	var rCall *mock.Call
	rCallback := func(args mock.Arguments) {
		n := args.Int(1)
		remaining := len(testData) - nextMessageIndex
		if n > remaining {
			n = remaining
		}
		nextMsgs := testData[nextMessageIndex : nextMessageIndex+n]
		nextMessageIndex += n
		rCall.ReturnArguments = mock.Arguments{nextMsgs, nil}

		latency := randRange(MinSqsLatencyMs, MaxSqsLatencyMs)
		rCall.After(time.Duration(latency) * time.Millisecond)
	}
	var dCall *mock.Call
	dCallback := func(args mock.Arguments) {
		msgs := args.Get(1).([]*models.SqsMessage)
		res := test.BatchSuccessResult(msgs)
		dCall.ReturnArguments = mock.Arguments{res, nil}

		latency := randRange(MinSqsLatencyMs, MaxSqsLatencyMs)
		rCall.After(time.Duration(latency) * time.Millisecond)
	}
	var vCall *mock.Call
	vCallback := func(args mock.Arguments) {
		msgs := args.Get(2).([]*models.SqsMessage)
		res := test.BatchSuccessResult(msgs)
		vCall.ReturnArguments = mock.Arguments{res, nil}

		latency := randRange(MinSqsLatencyMs, MaxSqsLatencyMs)
		rCall.After(time.Duration(latency) * time.Millisecond)
	}
	client := new(mocks.MockSqsClient)
	rCall = client.On("ReceiveMessages", mock.Anything, mock.Anything).Run(rCallback).Return(nil, nil)
	dCall = client.On("DeleteMessages", mock.Anything, mock.Anything).Run(dCallback).Return(nil, nil)
	vCall = client.On("SetMessageVisibility", mock.Anything, mock.Anything, mock.Anything).Run(vCallback).Return(nil, nil)
	client.On("GetQueueVisibilityTimeout", mock.Anything).Return(30, nil)

	return NewSqsFifoInput(client, config, nil)
}

// For ensuring repeatable input
type testSource struct {
}

func (t *testSource) Uint64() uint64 {
	if RandSeed == 0 {
		return uint64(time.Now().UnixNano())
	}

	return uint64(RandSeed)
}
