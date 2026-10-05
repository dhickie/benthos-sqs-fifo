package reading

import (
	"container/list"
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/aws"
	"dhickie/benthos-sqs-fifo/src/input/internal/models"
	"dhickie/benthos-sqs-fifo/src/input/internal/tracking"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type SqsFifoReader struct {
	client  aws.ISqsClient           // A client for the SQS API
	tracker tracking.IMessageTracker // Tracks all in-flight messages

	pendingAck  *list.List      // Messages that have been acknowledged by the runtime but not yet deleted
	ackLock     *sync.Mutex     // A lock for protecting the collection of pending acks
	ackCond     *util.AsyncCond // For signalling the ackloop to process acks
	pendingNack *list.List      // Messages that have had a negative acknowledgement by the runtime but not yet processed
	nackLock    *sync.Mutex     // A lock for protecting the collection of pending nacks
	nackCond    *util.AsyncCond // For signalling the nackloop to process nacks
	readCond    *util.AsyncCond // For being signalled that there is capacity to read more messages from the queue

	conf   *models.InputConfig // The configuration for the input
	lt     *util.Lifetime      // Manages application lifetime and shutdown events
	logger *service.Logger     // For writing custom logs
}

func NewSqsFifoReader(
	mTracker tracking.IMessageTracker,
	readCond *util.AsyncCond,
	sqsClient aws.ISqsClient,
	conf *models.InputConfig,
	lt *util.Lifetime,
	logger *service.Logger) *SqsFifoReader {

	readCond.Signal() // Start in a signalled state to start reading immediately

	return &SqsFifoReader{
		client:      sqsClient,
		tracker:     mTracker,
		pendingAck:  list.New(),
		ackLock:     &sync.Mutex{},
		ackCond:     util.NewAsyncCond(),
		pendingNack: list.New(),
		nackLock:    &sync.Mutex{},
		nackCond:    util.NewAsyncCond(),
		readCond:    readCond,
		conf:        conf,
		lt:          lt,
		logger:      logger,
	}
}

// GetQueueVisibilityTimeout gets the visibility timeout of the queue in seconds
func (r *SqsFifoReader) GetQueueVisibilityTimeout(ctx context.Context) (int, error) {
	return r.client.GetQueueVisibilityTimeout(ctx)
}

// RegisterLoops registers the refresh loop with the application lifetime, so that it can be started with the other loops
func (r *SqsFifoReader) RegisterLoops() {
	r.tracker.RegisterLoops()

	r.lt.Register(r.readLoop)
	r.lt.Register(r.ackLoop)
	r.lt.Register(r.nackLoop)
	r.logger.Debug("Read, ack & nack loops registered")
}

// Next returns the next message available for processing
func (r *SqsFifoReader) Next(ctx context.Context) (*models.SqsMessage, error) {
	return r.tracker.Flush(ctx)
}

// Ack acknowledges a message and adds it to the list of pending acknowledgements
func (r *SqsFifoReader) Ack(id *string) {
	r.ackLock.Lock()
	defer r.ackLock.Unlock()

	r.pendingAck.PushBack(id)
	if r.pendingAck.Len() >= r.conf.MaxPendingAcks {
		r.ackCond.Signal()
	}
}

// Nack acknowledges a message has failed processing and adds it to the list of pending failed acknowledgements
func (r *SqsFifoReader) Nack(id *string) {
	r.nackLock.Lock()
	defer r.nackLock.Unlock()

	r.pendingNack.PushBack(id)
	if r.pendingNack.Len() >= r.conf.MaxPendingAcks {
		r.nackCond.Signal()
	}
}

// Reads messages from the queue if there's room in the buffer
func (r *SqsFifoReader) readLoop(ltHandle util.ILifetimeHandle) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

readLoop:
	for {
		select {
		case <-ltHandle.Terminated():
			r.logger.Debug("Read loop received graceful termination order - breaking loop")
			break readLoop
		case <-ltHandle.Killed():
			r.logger.Debug("Read loop received kill order - breaking loop")
			break readLoop
		case <-t.C:
			r.read(ltHandle.KillContext())
		case <-r.readCond.WaitChan():
			r.read(ltHandle.KillContext())
		}
	}
}

func (r *SqsFifoReader) read(ctx context.Context) {
	// Only pull more messages if there's capacity in the tracker
	capacity := r.conf.MaxInFlightMessages - r.tracker.Length()
	if capacity >= r.conf.MinReceiveBatchSize {
		msgs, err := r.client.ReceiveMessages(ctx, capacity)
		if err != nil {
			r.logger.Errorf("Failed to receive messages from queue: %v", err.Error())
			return
		}

		r.logger.Debugf("Received %d messages from queue", len(msgs))
		newLen := r.tracker.Add(msgs)
		if r.conf.MaxInFlightMessages-newLen >= r.conf.MinReceiveBatchSize {
			// Still room for more
			r.readCond.Signal()
		}
	}
}

// Acknowledges pending messages by deleting them from the queue and removing them from the tracker
func (r *SqsFifoReader) ackLoop(ltHandle util.ILifetimeHandle) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

ackLoop:
	for {
		select {
		case <-ltHandle.Terminated():
			// Process pending acks first, then break out of the loop
			r.logger.Debug("AckLoop received graceful termination order - processing pending then breaking loop")
			if err := r.ack(ltHandle.KillContext()); err != nil {
				r.logger.Debug("Ack loop received kill order before processing of pending acks could complete")
			}
			break ackLoop
		case <-ltHandle.Killed():
			break ackLoop
		case <-t.C:
			if err := r.ack(ltHandle.KillContext()); err != nil {
				r.logger.Debug("Ack loop received kill order - breaking loop")
				break ackLoop
			}
		case <-r.ackCond.WaitChan():
			if err := r.ack(ltHandle.KillContext()); err != nil {
				r.logger.Debug("Ack loop received kill order - breaking loop")
				break ackLoop
			}
		}
	}
}

func (r *SqsFifoReader) ack(ctx context.Context) error {
	r.ackLock.Lock()
	defer r.ackLock.Unlock()

	r.logger.Debugf("Ack loop found %v acks to process", r.pendingAck.Len())

	batch := make([]*models.SqsMessage, 0, 10)
	failedIds := make([]*string, 0)
	for e := r.pendingAck.Front(); e != nil; e = e.Next() {
		msg, err := r.tracker.Peek(e.Value.(*string))
		if err != nil {
			r.logger.Errorf("Failed to get message details during ack: %v", err.Error())
			return nil
		}

		batch = append(batch, msg)
		if len(batch) == 10 || e.Next() == nil {
			res, err := r.client.DeleteMessages(ctx, batch)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}

				r.logger.Errorf("Failed to delete messages during ack: %v", err.Error())
				return nil
			}

			for _, failure := range res.Failures {
				r.logger.Error(failure.Sprint()) // Log failed batch members
				// If the failure was a client error, don't add it back to the pending ack queue
				// It was probably trying to delete a message with an invalid receipt ID
				if !failure.ClientError {
					failedIds = append(failedIds, failure.MsgId)
				}
			}

			for _, success := range res.Successes {
				r.tracker.Ack(success.MsgId)
			}

			// Reset the batch slice
			batch = batch[:0]
		}
	}

	// Clear the pending list
	r.pendingAck = r.pendingAck.Init()
	// Add any failed acks back to the pending list
	for _, id := range failedIds {
		r.pendingAck.PushBack(id)
	}
	return nil
}

func (r *SqsFifoReader) nackLoop(ltHandle util.ILifetimeHandle) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

nackLoop:
	for {
		select {
		case <-ltHandle.Terminated():
			// Process pending nacks first, then break out of the loop
			r.logger.Debug("NackLoop received graceful termination order - processing pending then breaking loop")
			if err := r.nack(ltHandle.KillContext()); err != nil {
				r.logger.Debug("Nack loop received kill order before processing of pending nacks could complete")
			}
			break nackLoop
		case <-ltHandle.Killed():
			r.logger.Debug("Nack loop received kill order - breaking loop")
			break nackLoop
		case <-t.C:
			if err := r.nack(ltHandle.KillContext()); err != nil {
				r.logger.Debug("Nack loop received kill order - breaking loop")
				break nackLoop
			}
		case <-r.nackCond.WaitChan():
			if err := r.nack(ltHandle.KillContext()); err != nil {
				r.logger.Debug("Nack loop received kill order - breaking loop")
				break nackLoop
			}
		}
	}
}

func (r *SqsFifoReader) nack(ctx context.Context) error {
	r.nackLock.Lock()
	defer r.nackLock.Unlock()

	r.logger.Debugf("Nack loop found %v nacks to process", r.pendingNack.Len())

	ids := make([]*string, 0, r.pendingNack.Len())
	for e := r.pendingNack.Front(); e != nil; e = e.Next() {
		id := e.Value.(*string)
		if _, err := r.tracker.Peek(id); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return err
			}

			r.logger.Errorf("Failed to get message details during nack: %v", err.Error())
			return nil
		}

		// We don't batch here for Nacks because the tracker will need to batch again anyway when resetting
		// message visibility for additional messages in the group
		ids = append(ids, id)
		err := r.tracker.Nack(ctx, ids...)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return err
			}

			// This can't happen - Nack only returns an error if the context is cancelled, so just panic if this happens
			panic("Nack failed with an unknown error: " + err.Error())
		}
	}

	// Clear the pending list
	r.pendingNack = r.pendingNack.Init()
	return nil
}
