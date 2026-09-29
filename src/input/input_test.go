//go:build unit

package sqs_fifo

import (
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/test/mocks"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestConnectionTest_ReturnsSuccess_WhenQueueVisibilityTimeoutIsRetrievedSuccessfully(t *testing.T) {
	// Arrange
	input := createInput(t, nil, nil)

	// Act
	res := input.ConnectionTest(t.Context())

	// Assert
	assert.NoError(t, res[0].Err, "The connection test should have been successful")
}

func TestConnectionTest_SetsVisibilityTimeout(t *testing.T) {
	// Arrange
	setupConf := func(config *models.InputConfig) {
		config.VisibilityTimeoutSeconds = 0
	}
	input := createInput(t, setupConf, nil)

	// Act
	input.ConnectionTest(t.Context())

	// Assert
	assert.NotEqual(t, 0, input.conf.VisibilityTimeoutSeconds, "Visibility timeout should have been set")
}

func TestConnectionTest_ReturnsFailure_IfSqsCallFails(t *testing.T) {
	// Arrange
	setupSqs := func(client *mocks.MockSqsClient) {
		client.On("GetQueueVisibilityTimeout", mock.Anything).Unset()
		client.On("GetQueueVisibilityTimeout", mock.Anything).Return(0, errors.New("fail"))
	}
	input := createInput(t, nil, setupSqs)

	// Act
	res := input.ConnectionTest(t.Context())

	// Assert
	assert.Error(t, res[0].Err, "Connection test should have failed")
}

// Also tests Read() and returned ack function
func TestConnect_StartsCallbackLoop_AndProcessesAcks(t *testing.T) {
	// Arrange
	setupConf := func(config *models.InputConfig) {
		config.MaxPendingAcks = 1
	}
	msgs := test.CreateMessages(1, 2)
	setupSqs := func(client *mocks.MockSqsClient) {
		// Return two messages from the same group, so we can check that the first message has been acked
		client.On("ReceiveMessages", mock.Anything, mock.Anything).Unset()
		client.On("ReceiveMessages", mock.Anything, mock.Anything).Return(msgs, nil).Once()
		client.On("ReceiveMessages", mock.Anything, mock.Anything).Return([]*models.SqsMessage{}, nil)
	}
	input := createInput(t, setupConf, setupSqs)
	t.Cleanup(func() {
		_ = input.Close(t.Context())
	})

	// Act
	cErr := input.Connect(t.Context()) // Connect the input
	if cErr != nil {
		t.Fatal(cErr)
	}
	msg1, ackFunc, rErr1 := input.Read(t.Context()) // Read the first message
	if rErr1 != nil {
		t.Fatal(rErr1)
	}
	aErr := ackFunc(t.Context(), nil) // Ack the message
	if aErr != nil {
		t.Fatal(aErr)
	}
	msg2, ackFunc, rErr2 := input.Read(t.Context()) // Read the second message
	if rErr2 != nil {
		t.Fatal(rErr2)
	}

	// Assert
	msg1Bytes, err1 := msg1.AsBytes()
	msg2Bytes, err2 := msg2.AsBytes()
	assert.NoError(t, err1, "The first message should have been converted to bytes")
	assert.Equal(t, []byte(*msgs[0].Msg.Body), msg1Bytes, "The first message should have been received")
	assert.NoError(t, err2, "The second message should have been converted to bytes")
	assert.EqualValues(t, []byte(*msgs[1].Msg.Body), msg2Bytes, "The second message should have been received")
}

func TestConnect_StartsCallbackLoop_AndProcessesNacks(t *testing.T) {
	// Arrange
	setupConf := func(config *models.InputConfig) {
		config.MaxPendingAcks = 1
		config.MaxProcessingAttempts = 1
	}
	msgs := test.CreateMessages(2, 2)
	setupSqs := func(client *mocks.MockSqsClient) {
		// Return two messages from the same group, so we can check that the first message has been acked
		client.On("ReceiveMessages", mock.Anything, mock.Anything).Unset()
		client.On("ReceiveMessages", mock.Anything, mock.Anything).Return(msgs, nil).Once()
		client.On("ReceiveMessages", mock.Anything, mock.Anything).Return([]*models.SqsMessage{}, nil)
	}
	input := createInput(t, setupConf, setupSqs)
	t.Cleanup(func() {
		_ = input.Close(t.Context())
	})

	// Act
	cErr := input.Connect(t.Context()) // Connect the input
	if cErr != nil {
		t.Fatal(cErr)
	}
	msg1, ackFunc, rErr1 := input.Read(t.Context()) // Read the first message
	if rErr1 != nil {
		t.Fatal(rErr1)
	}
	aErr := ackFunc(t.Context(), errors.New("fail")) // Nack the message
	if aErr != nil {
		t.Fatal(aErr)
	}
	msg2, ackFunc, rErr2 := input.Read(t.Context()) // Read the second message
	if rErr2 != nil {
		t.Fatal(rErr2)
	}

	// Assert
	msg1GroupId, ok := msg1.MetaGet(metaKeyGroupId)
	assert.True(t, ok, "The first message should have had group ID metadata")
	msg2GroupId, ok := msg2.MetaGet(metaKeyGroupId)
	assert.True(t, ok, "The second message should have had group ID metadata")
	assert.NotEqual(t, msg1GroupId, msg2GroupId, "The first and second message should have been from different group IDs")
}

func TestNext_GetsMessagesWithExpectedMetadata(t *testing.T) {
	// Arrange
	msgs := test.CreateMessages(1, 1)
	msgs[0].Msg.Attributes[attKeyReceiveCount] = "1"
	testAttributeDataType := "string"
	testAttributeName := "testAttribute"
	testAttributeValue := "value"
	attributes := make(map[string]types.MessageAttributeValue)
	attributes[testAttributeName] = types.MessageAttributeValue{
		DataType:    &testAttributeDataType,
		StringValue: &testAttributeValue,
	}
	msgs[0].Msg.MessageAttributes = attributes

	setupSqs := func(client *mocks.MockSqsClient) {
		// Return two messages from the same group, so we can check that the first message has been acked
		client.On("ReceiveMessages", mock.Anything, mock.Anything).Unset()
		client.On("ReceiveMessages", mock.Anything, mock.Anything).Return(msgs, nil).Once()
		client.On("ReceiveMessages", mock.Anything, mock.Anything).Return([]*models.SqsMessage{}, nil)
	}
	input := createInput(t, nil, setupSqs)
	t.Cleanup(func() {
		_ = input.Close(t.Context())
	})

	// Act
	cErr := input.Connect(t.Context()) // Connect the input
	if cErr != nil {
		t.Fatal(cErr)
	}
	msg, _, rErr := input.Read(t.Context()) // Read the first message
	if rErr != nil {
		t.Fatal(rErr)
	}

	// Assert
	expectedAttributes := map[string]string{
		metaKeyMsgId:         *msgs[0].Msg.MessageId,
		metaKeyReceiptHandle: *msgs[0].Msg.ReceiptHandle,
		metaKeyReceiveCount:  msgs[0].Msg.Attributes[attKeyReceiveCount],
		metaKeyGroupId:       msgs[0].Msg.Attributes[attKeyGroupId],
	}
	for k, v := range expectedAttributes {
		actual, ok := msg.MetaGet(k)
		assert.True(t, ok)
		assert.Equal(t, v, actual)
	}

	for k, v := range msgs[0].Msg.MessageAttributes {
		actual, ok := msg.MetaGet(k)
		assert.True(t, ok)
		assert.Equal(t, *v.StringValue, actual)
	}
}

func createInput(t *testing.T, confFunc func(*models.InputConfig), sqsFunc func(*mocks.MockSqsClient)) *SqsFifoInput {
	config := mocks.NewMockConfig()
	if confFunc != nil {
		confFunc(config)
	}

	var rCall *mock.Call
	rCallback := func(args mock.Arguments) {
		n := args.Int(1)
		nextMsgs := test.CreateMessages(1, n)
		rCall.ReturnArguments = mock.Arguments{nextMsgs, nil}
	}
	var dCall *mock.Call
	dCallback := func(args mock.Arguments) {
		msgs := args.Get(1).([]*models.SqsMessage)
		res := test.BatchSuccessResult(msgs)
		dCall.ReturnArguments = mock.Arguments{res, nil}
	}
	var vCall *mock.Call
	vCallback := func(args mock.Arguments) {
		msgs := args.Get(2).([]*models.SqsMessage)
		res := test.BatchSuccessResult(msgs)
		vCall.ReturnArguments = mock.Arguments{res, nil}
	}
	client := new(mocks.MockSqsClient)
	rCall = client.On("ReceiveMessages", mock.Anything, mock.Anything).Run(rCallback).Return(nil, nil)
	dCall = client.On("DeleteMessages", mock.Anything, mock.Anything).Run(dCallback).Return(nil, nil)
	vCall = client.On("SetMessageVisibility", mock.Anything, mock.Anything, mock.Anything).Run(vCallback).Return(nil, nil)
	client.On("GetQueueVisibilityTimeout", mock.Anything).Return(30, nil)

	if sqsFunc != nil {
		sqsFunc(client)
	}

	logger := test.NewTestLogger(t)
	return NewSqsFifoInput(client, config, logger)
}
