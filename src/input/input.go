package sqs_fifo

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/reading"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/tracking"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/util"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	metaKeyMsgId         = "sqs_message_id"
	metaKeyReceiptHandle = "sqs_receipt_handle"
	metaKeyReceiveCount  = "sqs_approximate_receive_count"
	metaKeyGroupId       = "sqs_message_group_id"

	attKeyReceiveCount = "ApproximateReceiveCount"
	attKeyGroupId      = "MessageGroupId"
)

// SqsFifoInput is the top level object that interacts with the Connect SDK.
type SqsFifoInput struct {
	conf     *models.InputConfig    // The configuration for the input
	reader   *reading.SqsFifoReader // Reads from the queue, and makes messages available for processing.
	ackChan  chan *string           // Channel used to indicate a message has been processed successfully.
	nackChan chan *string           // Channel used to indicate a message has not been processed successfully.
	lt       *util.Lifetime         // Used to manage the lt of child goroutines
	logger   *service.Logger        // Used to write custom logs
}

// NewSqsFifoInput returns a new input object, ready for connecting to SQS
func NewSqsFifoInput(sqsClient aws.ISqsClient, conf *models.InputConfig, logger *service.Logger) *SqsFifoInput {
	lt := util.NewLifetime(logger)
	readCond := util.NewAsyncCond()
	tracker := tracking.NewMessageTracker(conf, readCond, sqsClient, lt, logger)
	reader := reading.NewSqsFifoReader(tracker, readCond, sqsClient, conf, lt, logger)

	return &SqsFifoInput{
		conf:     conf,
		reader:   reader,
		ackChan:  make(chan *string),
		nackChan: make(chan *string),
		lt:       lt,
		logger:   logger,
	}
}

// ConnectionTest tests that the input can reach the target queue successfully by running a health check
func (i *SqsFifoInput) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	tOut, err := i.reader.GetQueueVisibilityTimeout(ctx)
	if err != nil {
		return service.ConnectionTestFailed(err).AsList()
	}

	// Set the visibility timeout for use later
	i.conf.VisibilityTimeoutSeconds = tOut

	return service.ConnectionTestSucceeded().AsList()
}

// Connect starts the input by starting the reading and ack/nack callback loop
func (i *SqsFifoInput) Connect(ctx context.Context) error {
	// Get the visibility timeout for the queue if we haven't got it already
	if i.conf.VisibilityTimeoutSeconds == 0 {
		tOut, err := i.reader.GetQueueVisibilityTimeout(ctx)
		if err != nil {
			return err
		}

		i.conf.VisibilityTimeoutSeconds = tOut
	}

	i.reader.RegisterLoops()

	i.lt.Register(i.callbackLoop)
	i.logger.Debug("Ack callback loop registered")

	i.lt.Start()
	i.logger.Debug("Loops started")

	return nil
}

// Read gets the next message to be processed by the pipeline, and a callback function to call once it has been
// fully processed
func (i *SqsFifoInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	sqsMsg, err := i.reader.Next(ctx)
	if err != nil {
		return nil, nil, err
	}

	i.logger.Debugf("Retrieved message ID %v from input", *sqsMsg.Msg.MessageId)

	sMsg := service.NewMessage([]byte(*sqsMsg.Msg.Body))
	addSQSMetadata(sMsg, sqsMsg)

	ackFunc := func(pCtx context.Context, err error) error {
		if err != nil {
			select {
			case <-pCtx.Done():
				return pCtx.Err()
			case i.nackChan <- sqsMsg.Msg.MessageId:
			}
		} else {
			select {
			case <-pCtx.Done():
				return pCtx.Err()
			case i.ackChan <- sqsMsg.Msg.MessageId:
			}
		}

		return nil
	}

	return sMsg, ackFunc, nil
}

// Close closes the connection to the SQS queue, shutting down gracefully if possible
func (i *SqsFifoInput) Close(ctx context.Context) error {
	i.lt.Terminate()
	i.logger.Debug("Received close call - beginning graceful termination")

	if d, ok := ctx.Deadline(); ok {
		// Give as long as we can to terminate gracefully
		i.logger.Debugf("Termination deadline set as %v", d)

		ttl := time.Until(d)
		select {
		case <-ctx.Done():
			i.logger.Debug("Graceful termination cancelled before completion")
			return ctx.Err()
		case <-time.After(ttl - time.Second):
			i.logger.Debug("Termination deadline expired - issuing kill order")
			i.lt.Kill()
		case <-i.lt.Stopped():
			i.logger.Debug("Graceful termination completed successfully")
			return nil
		}

		select {
		case <-ctx.Done():
			i.logger.Debug("Hard termination cancelled before completion")
			return ctx.Err()
		case <-i.lt.Stopped():
			i.logger.Debug("Hard termination completed successfully")
			return nil
		}
	}

	// Just kill immediately
	i.logger.Debug("No termination deadline set - issuing kill order")
	i.lt.Kill()
	select {
	case <-ctx.Done():
		i.logger.Debug("Hard termination cancelled before completion")
		return ctx.Err()
	case <-i.lt.Stopped():
		i.logger.Debug("Hard termination completed successfully")
		return nil
	}
}

// Processes Acks and Nacks from the pipeline and hands them over to their respective processing loops
func (i *SqsFifoInput) callbackLoop(ltHandle util.ILifetimeHandle) {
loop:
	for {
		select {
		case mId := <-i.ackChan:
			i.logger.Debugf("Input received Ack for message ID %v", *mId)
			i.reader.Ack(mId)
		case mId := <-i.nackChan:
			i.logger.Debugf("Input received Nack for message ID %v", *mId)
			i.reader.Nack(mId)
		case <-ltHandle.Terminated():
			break loop
		case <-ltHandle.Killed():
			break loop
		}
	}
}

// Adds SQS metadata to the outgoing Connect message for later parts of the pipeline
func addSQSMetadata(sMsg *service.Message, sqsMsg *models.SqsMessage) {
	sMsg.MetaSetMut(metaKeyMsgId, *sqsMsg.Msg.MessageId)
	sMsg.MetaSetMut(metaKeyReceiptHandle, *sqsMsg.Msg.ReceiptHandle)

	addAttributeMetadataIfNotNil(sMsg, metaKeyReceiveCount, attKeyReceiveCount, sqsMsg.Msg.Attributes)
	addAttributeMetadataIfNotNil(sMsg, metaKeyGroupId, attKeyGroupId, sqsMsg.Msg.Attributes)

	for k, v := range sqsMsg.Msg.MessageAttributes {
		if v.StringValue != nil {
			sMsg.MetaSetMut(k, *v.StringValue)
		}
	}
}

// Adds an SQS attribute (not to be confused with message attributes) to the connect message if it exists
func addAttributeMetadataIfNotNil(msg *service.Message, mKey string, aKey string, attributes map[string]string) {
	if v, ok := attributes[aKey]; ok {
		msg.MetaSetMut(mKey, v)
	}
}
