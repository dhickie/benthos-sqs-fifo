//go:build unit

package reading

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/models"
	"dhickie/benthos-sqs-fifo/src/input/internal/test"
	"dhickie/benthos-sqs-fifo/src/input/internal/test/mocks"
	"dhickie/benthos-sqs-fifo/src/input/internal/test/wait"
	"dhickie/benthos-sqs-fifo/src/input/internal/tracking"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestAck_SignalsAckLoopToRun_WhenHittingMaxPendingAcks(t *testing.T) {
	// Assemble
	setupConf := func(config *models.InputConfig) {
		config.MaxPendingAcks = 2
	}
	reader := createReader(t, setupConf, nil)
	waitFunc := func(ctx context.Context) (bool, error) {
		err := reader.ackCond.Wait(ctx)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// Act
	reader.Ack(randomId())
	err1 := wait.Until(t.Context(), waitFunc, 10*time.Millisecond)
	reader.Ack(randomId())
	err2 := wait.Until(t.Context(), waitFunc, 10*time.Millisecond)

	// Assert
	assert.Error(t, err1, "The ack loop should not have been signalled")
	assert.NoError(t, err2, "The ack loop should have been signalled")
}

func TestNack_SignalsNackLoopToRun_WhenHittingMaxPendingNacks(t *testing.T) {
	// Assemble
	setupConf := func(config *models.InputConfig) {
		config.MaxPendingAcks = 2
	}
	reader := createReader(t, setupConf, nil)
	waitFunc := func(ctx context.Context) (bool, error) {
		err := reader.nackCond.Wait(ctx)
		if err != nil {
			return false, err
		}

		return true, nil
	}

	// Act
	reader.Nack(randomId())
	err1 := wait.Until(t.Context(), waitFunc, 10*time.Millisecond)
	reader.Nack(randomId())
	err2 := wait.Until(t.Context(), waitFunc, 10*time.Millisecond)

	// Assert
	assert.Error(t, err1, "The nack loop should not have been signalled")
	assert.NoError(t, err2, "The nack loop should have been signalled")
}

func TestReadLoop_PerformsRead_WhenTriggeredByReadCondition(t *testing.T) {
	// Assemble
	msgs := test.CreateMessages(1, 1)
	setupConf := func(c *models.InputConfig) {
		c.MaxInFlightMessages = 1
	}
	setupClient := func(c *mocks.MockSqsClient) {
		c.On("ReceiveMessages", mock.Anything, mock.Anything).Return(msgs, nil)
		c.On("SetMessageVisibility", mock.Anything, mock.Anything, mock.Anything).Return(test.BatchSuccessResult(msgs), nil)
	}
	reader := createReader(t, setupConf, setupClient)

	// Act
	reader.RegisterLoops()
	reader.lt.Start()
	reader.readCond.Signal()
	msg, err := reader.Next(t.Context())
	reader.lt.Kill()
	<-reader.lt.Stopped()

	// Act
	assert.NoError(t, err, "No error should have been returned from Next()")
	assert.EqualValues(t, msgs[0], msg, "The message should have been read")
}

// This also tests the ack loop
func TestReadLoop_PerformsRead_WhenTriggeredBySpareCapacity(t *testing.T) {
	// Assemble
	initMsgs := test.CreateMessagesWithDeadline(1, 1, 30)
	nextMsgs := test.CreateMessagesWithDeadline(1, 1, 30)
	setupConf := func(c *models.InputConfig) {
		c.MaxInFlightMessages = 1
		c.MinReceiveBatchSize = 1
		c.MaxPendingAcks = 1
		c.VisibilityTimeoutSeconds = 30
	}
	setupClient := func(c *mocks.MockSqsClient) {
		c.On("ReceiveMessages", mock.Anything, mock.Anything).Return(initMsgs, nil).Once()
		c.On("ReceiveMessages", mock.Anything, mock.Anything).Return(nextMsgs, nil).Once()
		c.On("DeleteMessages", mock.Anything, mock.Anything).Return(test.BatchSuccessResult(initMsgs), nil)
	}
	reader := createReader(t, setupConf, setupClient)
	lengthWaitFunc := func(ctx context.Context) (bool, error) {
		l := reader.tracker.Length()
		return l == 1, nil
	}
	var nextMsg *models.SqsMessage
	var msgErr error
	msgWaitFunc := func(ctx context.Context) (bool, error) {
		nextMsg, msgErr = reader.Next(ctx)
		if msgErr != nil {
			return false, msgErr
		}

		return nextMsg != nil, nil
	}

	// Act & Assert
	reader.RegisterLoops()
	reader.lt.Start()

	wErr := wait.Until(t.Context(), lengthWaitFunc, 50*time.Millisecond)
	assert.NoError(t, wErr, "No error should have been returned when waiting for the initial message to be available")

	initMsg, iErr := reader.Next(t.Context()) // Read the initial message
	assert.NoError(t, iErr, "No error should have been returned when reading the initial message")
	reader.Ack(initMsg.Msg.MessageId) // Ack the initial message

	wErr = wait.Until(t.Context(), msgWaitFunc, 50*time.Millisecond) // Wait until a new message is available
	assert.NoError(t, wErr, "A second message should have been available")
	assert.EqualValues(t, nextMsgs[0], nextMsg, "The second message should have been read after the initial message was acked")

	reader.lt.Kill()
	<-reader.lt.Stopped()
}

func TestNackLoop_PerformsNack_WhenHittingMaxPendingNacks(t *testing.T) {
	// Assemble
	msgs := test.CreateMessagesWithDeadline(1, 1, 30)
	setupConf := func(c *models.InputConfig) {
		c.MaxInFlightMessages = 1
		c.MinReceiveBatchSize = 1
		c.MaxPendingAcks = 1
		c.MaxProcessingAttempts = 1
		c.VisibilityTimeoutSeconds = 30
	}
	setupClient := func(c *mocks.MockSqsClient) {
		c.On("ReceiveMessages", mock.Anything, mock.Anything).Return(msgs, nil).Once()
		c.On("ReceiveMessages", mock.Anything, mock.Anything).Return([]*models.SqsMessage{}, nil)
		c.On("SetMessageVisibility", mock.Anything, mock.Anything, mock.Anything).Return(test.BatchSuccessResult(msgs), nil)
	}
	reader := createReader(t, setupConf, setupClient)
	lengthWaitFunc := func(expectedLength int) func(context.Context) (bool, error) {
		return func(ctx context.Context) (bool, error) {
			l := reader.tracker.Length()
			return l == expectedLength, nil
		}
	}

	// Act & Assert
	reader.RegisterLoops()
	reader.lt.Start()

	wErr := wait.Until(t.Context(), lengthWaitFunc(1), 50*time.Millisecond)
	assert.NoError(t, wErr, "No error should have been returned when waiting for the initial message to be available")

	initMsg, iErr := reader.Next(t.Context()) // Read the initial message
	assert.NoError(t, iErr, "No error should have been returned when reading the initial message")
	reader.Nack(initMsg.Msg.MessageId) // Nack the initial message

	wErr = wait.Until(t.Context(), lengthWaitFunc(0), 50*time.Millisecond) // Wait for the number of in-flight messages to hit 0
	assert.NoError(t, wErr, "No second message should have been available")

	reader.lt.Kill()
	<-reader.lt.Stopped()
}

func createReader(
	t *testing.T,
	setupConf func(config *models.InputConfig),
	setupClient func(c *mocks.MockSqsClient)) *SqsFifoReader {

	client := new(mocks.MockSqsClient)
	if setupClient != nil {
		setupClient(client)
	}

	config := mocks.NewMockConfig()
	if setupConf != nil {
		setupConf(config)
	}

	lt := util.NewLifetime(nil)
	readCond := util.NewAsyncCond()
	logger := test.NewTestLogger(t)
	tracker := tracking.NewMessageTracker(config, readCond, client, lt, logger)
	return NewSqsFifoReader(tracker, readCond, client, config, lt, logger)
}

func randomId() *string {
	id := uuid.NewV4().String()
	return &id
}
