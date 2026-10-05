//go:build unit

package tracking

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/models"
	"dhickie/benthos-sqs-fifo/src/input/internal/test"
	mocks2 "dhickie/benthos-sqs-fifo/src/input/internal/test/mocks"
	"dhickie/benthos-sqs-fifo/src/input/internal/test/wait"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestAdd_ReturnsCorrectNumberOfInflightMessages_WhenAddingInitialMessages(t *testing.T) {
	// Arrange
	tracker := createTracker(t, nil, nil)
	msgs := test.CreateMessages(3, 3)

	// Act
	n := tracker.Add(msgs)

	// Assert
	assert.Equal(t, len(msgs), n, "The number of inflight messages should be the same as the number of messages added")
}

func TestAdd_ReturnsCorrectNumberOfInflightMessages_WhenAddingAdditionalMessages(t *testing.T) {
	// Arrange
	tracker := createTracker(t, nil, nil)
	msgs := test.CreateMessages(3, 3)
	tracker.Add(msgs)
	msgs = test.CreateMessages(3, 3)

	// Act
	n := tracker.Add(msgs)

	// Assert
	assert.Equal(t, len(msgs)*2, n, "The number of inflight messages should be the same as the number of messages added")
}

func TestPeek_ReturnsCorrectMessage(t *testing.T) {
	// Arrange
	tracker := createTracker(t, nil, nil)
	msgs := test.CreateMessages(3, 3)
	tracker.Add(msgs)
	msg := msgs[5]

	// Act
	pMsg, err := tracker.Peek(msg.Msg.MessageId)

	// Assert
	assert.NoError(t, err)
	assert.EqualValues(t, msg, pMsg, "The peeked message should be the same as the added message")
}

func TestFlush_ReturnsFirstMessageFromEachGroupOnly(t *testing.T) {
	// Arrange
	tracker := createTracker(t, nil, nil)
	msgs := test.CreateMessages(2, 2)
	tracker.Add(msgs)

	// Act
	flushed := make([]*models.SqsMessage, 0, 2)
	deadline := time.Now().Add(10 * time.Millisecond)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	for {
		msg, err := tracker.Flush(ctx)
		if err != nil {
			break
		}
		flushed = append(flushed, msg)
	}

	// Assert
	assert.Equal(t, 2, len(flushed), "Only the first message from each group should be flushed")
	assert.EqualValues(t, msgs[0], flushed[0], "The first message should be from the first group")
	assert.EqualValues(t, msgs[1], flushed[1], "The second message should be from the second group")
}

func TestFlush_ReturnsNewMessageIfMessageIsAddedWhileWaiting(t *testing.T) {
	// Arrange
	tracker := createTracker(t, nil, nil)
	msgs := test.CreateMessages(1, 1)
	deadline := time.Now().Add(20 * time.Millisecond)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()

	// Act
	go func() {
		t := time.NewTimer(10 * time.Millisecond)
		<-t.C
		tracker.Add(msgs)
	}()
	msg, err := tracker.Flush(ctx)

	// Assert
	assert.NoError(t, err, "The message should have been flushed before the context was cancelled")
	assert.EqualValues(t, msgs[0], msg, "The added message should have been flushed")
}

func TestAck_RemovesMessageFromTracker(t *testing.T) {
	// Arrange
	tracker := createTracker(t, nil, nil)
	msgs := test.CreateMessages(1, 1)
	tracker.Add(msgs)

	// Act
	nBefore := tracker.Length()
	tracker.Ack(*msgs[0].Msg.MessageId)
	nAfter := tracker.Length()
	_, err := tracker.Peek(msgs[0].Msg.MessageId)

	// Assert
	assert.Error(t, err, "Error should have been returned when peeking acked message")
	assert.Equal(t, 1, nBefore, "Length should be 1 after adding the message")
	assert.Equal(t, 0, nAfter, "Length should be 0 after removing the message")
}

func TestAck_MakesNextMessageInGroupAvailable(t *testing.T) {
	// Arrange
	tracker := createTracker(t, nil, nil)
	msgs := test.CreateMessages(1, 2)
	tracker.Add(msgs)

	// Act
	_, err1 := tracker.Flush(t.Context())
	tracker.Ack(*msgs[0].Msg.MessageId)

	next, err2 := tracker.Flush(t.Context())

	// Assert
	assert.NoError(t, err1, "No error should be returned when flushing the first message")
	assert.NoError(t, err2, "No error should be returned when flushing the second message")
	assert.EqualValues(t, msgs[1], next, "The second message from the group should have been available")
}

func TestNack_RemovesEntireGroupFromTracker_WhenMaxAttemptsReached(t *testing.T) {
	// Arrange
	msgs := test.CreateMessages(1, 2)
	setup := func(c *mocks2.MockSqsClient) {
		c.
			On("SetMessageVisibility", mock.Anything, mock.Anything, msgs).
			Return(test.BatchSuccessResult(msgs), nil)
	}
	tracker := createTracker(t, nil, setup)
	tracker.Add(msgs)

	// Act
	nInit := tracker.Length()
	_, fErr := tracker.Flush(t.Context())
	nErr := tracker.Nack(t.Context(), msgs[0].Msg.MessageId)
	nFinal := tracker.Length()

	// Assert
	assert.Equal(t, 2, nInit, "The initial length should be 2")
	assert.NoError(t, fErr, "No error should be returned when flushing the initial message")
	assert.NoError(t, nErr, "No error should be returned when nacking the initial message")
	assert.Equal(t, 0, nFinal, "Both messages should have been abandoned")
}

func TestNack_RetriesFailures_WhenMaxAttemptsNotReached(t *testing.T) {
	msgs := test.CreateMessages(1, 2)
	setupConf := func(c *models.InputConfig) {
		c.MaxProcessingAttempts = 2
	}
	setupClient := func(c *mocks2.MockSqsClient) {
		c.
			On("SetMessageVisibility", mock.Anything, mock.Anything, msgs).
			Return(test.BatchSuccessResult(msgs), nil)
	}
	tracker := createTracker(t, setupConf, setupClient)
	tracker.Add(msgs)
	flushFunc := func(ctx context.Context) (bool, error) {
		_, err := tracker.Flush(ctx)
		if err != nil {
			return false, err
		}

		return true, nil
	}

	// Act
	nInit := tracker.Length()
	_, fErr := tracker.Flush(t.Context())
	nErr := tracker.Nack(t.Context(), msgs[0].Msg.MessageId)
	nFinal := tracker.Length()
	mRetry, fErr2 := tracker.Flush(t.Context())
	fErr3 := wait.Until(t.Context(), flushFunc, 10*time.Millisecond)

	// Assert
	assert.Equal(t, 2, nInit, "The initial length should be 2")
	assert.NoError(t, fErr, "No error should be returned when flushing the initial message")
	assert.NoError(t, nErr, "No error should be returned when nacking the initial message")
	assert.Equal(t, 2, nFinal, "The message should be retried after nacking")
	assert.EqualValues(t, msgs[0], mRetry, "The initial message should be front of the queue after nacking")
	assert.NoError(t, fErr2, "No error should be returned when flushing the initial message again")
	assert.Error(t, fErr3, "The second message shouldn't be returned while the initial message is still in flight")
}

func TestLength_IncludesFlushedButNotYetAckedMessages(t *testing.T) {
	// Arrange
	msgs := test.CreateMessages(1, 2)
	tracker := createTracker(t, nil, nil)
	tracker.Add(msgs)

	// Act
	nInit := tracker.Length()
	_, fErr := tracker.Flush(t.Context())
	nFinal := tracker.Length()

	// Assert
	assert.Equal(t, 2, nInit, "The initial length should be 2")
	assert.NoError(t, fErr, "No error should be returned when flushing the initial message")
	assert.Equal(t, 2, nFinal, "The final length should be 2")
}

func TestStart_StartsRefreshLoop(t *testing.T) {
	// Arrange
	msgs := test.CreateMessages(1, 1)
	calls := 0
	setupConf := func(c *models.InputConfig) {
		c.VisibilityTimeoutSeconds = 2
	}
	setupClient := func(c *mocks2.MockSqsClient) {
		c.
			On("SetMessageVisibility", mock.Anything, mock.Anything, msgs).
			Run(func(args mock.Arguments) {
				calls++
			}).
			Return(test.BatchSuccessResult(msgs), nil)
	}
	tracker := createTracker(t, setupConf, setupClient)
	tracker.Add(msgs)

	// Act
	tracker.RegisterLoops()
	tracker.lt.Start()
	<-time.After(2 * time.Second)
	tracker.lt.Kill()
	<-tracker.lt.Stopped()

	// Assert
	assert.Less(t, time.Now(), msgs[0].Deadline, "The deadline should have been extended")
	assert.Greater(t, calls, 0, "The SQS API should have been called to set the visibility deadline")
}

func createTracker(t *testing.T, confFunc func(*models.InputConfig), clientFunc func(*mocks2.MockSqsClient)) *MessageTracker {
	conf := mocks2.NewMockConfig()
	if confFunc != nil {
		confFunc(conf)
	}

	sqs := new(mocks2.MockSqsClient)
	if clientFunc != nil {
		clientFunc(sqs)
	}

	lt := util.NewLifetime(nil)
	readCond := util.NewAsyncCond()
	logger := test.NewTestLogger(t)
	return NewMessageTracker(conf, readCond, sqs, lt, logger)
}
