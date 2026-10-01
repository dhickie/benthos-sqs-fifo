package tracking

import (
	"container/list"
	"context"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/util"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// MessageTracker tracks all messages that have been received from SQS but haven't yet been deleted from the queue
type MessageTracker struct {
	groups        map[string]*groupTracker      // Messages split by message group ID
	idMap         map[string]*models.SqsMessage // Map of message IDs to messages
	pendingFlush  []*models.SqsMessage          // Messages pending a flush downstream
	refreshQueue  *list.List                    // A time ordered list of all in-flight messages by their visibility expiry
	refreshMap    map[string]*list.Element      // Map of message IDs to elements in the refresh queue
	conf          *models.InputConfig           // The configuration for the input
	sqs           aws.ISqsClient                // The SQS client
	m             *sync.RWMutex                 // The mutex used to provide thread safety
	msgsAvailable *util.ContextCond             // Used to signal that messages are available to be flushed
	lt            *util.Lifetime                // Manages application lifetime and shutdown events
	readCond      *util.AsyncCond               // For signalling the read loop that there is capacity for more messages
	logger        *service.Logger               // For writing custom logs
}

// IMessageTracker is the interface for any type that provides tracking of in flight messages
type IMessageTracker interface {
	RegisterLoops()
	Add(msgs []*models.SqsMessage) int
	Peek(id *string) (*models.SqsMessage, error)
	Flush(ctx context.Context) (*models.SqsMessage, error)
	Ack(id string)
	Nack(ctx context.Context, ids ...*string) error
	Length() int
}

// NewMessageTracker returns a new message tracker using the provided configuration and SQS client
func NewMessageTracker(
	conf *models.InputConfig,
	readCond *util.AsyncCond,
	sqs aws.ISqsClient,
	lt *util.Lifetime,
	logger *service.Logger) *MessageTracker {

	m := &sync.RWMutex{}
	return &MessageTracker{
		groups:        make(map[string]*groupTracker),
		idMap:         make(map[string]*models.SqsMessage),
		pendingFlush:  make([]*models.SqsMessage, 0, conf.MaxInFlightMessages),
		refreshQueue:  list.New(),
		refreshMap:    make(map[string]*list.Element),
		conf:          conf,
		sqs:           sqs,
		m:             m,
		msgsAvailable: util.NewContextCond(m),
		lt:            lt,
		readCond:      readCond,
		logger:        logger,
	}
}

// RegisterLoops registers the refresh loop with the application lifetime, so that it can be started with the other loops
func (t *MessageTracker) RegisterLoops() {
	t.lt.Register(t.refreshLoop)
	t.logger.Debug("Visibility deadline refresh loop registered")
}

// Add adds a collection of messages to the tracker.
// Returns the current number of in-flight messages after adding the new batch
func (t *MessageTracker) Add(msgs []*models.SqsMessage) int {
	t.m.Lock()
	defer t.m.Unlock()

	// Group the messages by message group ID
	tempMap := make(map[string][]*models.SqsMessage)
	for _, msg := range msgs {
		mgid := msg.GetGroupId()

		_, ok := tempMap[mgid]
		if !ok {
			tempMap[mgid] = make([]*models.SqsMessage, 0, 10)
		}

		tempMap[mgid] = append(tempMap[mgid], msg)

		// Add to the relevant maps/lists to support later processes
		t.idMap[*msg.Msg.MessageId] = msg
		e := t.refreshQueue.PushBack(msg)
		t.refreshMap[*msg.Msg.MessageId] = e
	}

	// Add them to their respective groups, and add to the pending flush list if there's a new message ready to pass
	// downstream
	for k, v := range tempMap {
		_, ok := t.groups[k]
		if !ok {
			t.groups[k] = newGroupTracker()
		}
		next := t.groups[k].add(v)
		if next != nil {
			if len(t.pendingFlush) == 0 {
				t.msgsAvailable.Signal() // Signal Flush() that a new message is available
			}
			t.pendingFlush = append(t.pendingFlush, next)
		}
	}

	return t.length()
}

// Peek gets a message by its ID without flushing or acknowledging it
func (t *MessageTracker) Peek(id *string) (*models.SqsMessage, error) {
	t.m.RLock()
	defer t.m.RUnlock()

	msg, ok := t.idMap[*id]
	if !ok {
		return nil, errors.New("Peek: Cannot find message with id " + *id)
	}

	return msg, nil
}

// Flush returns the next message to be sent downstream and removes it from the pending messages list
func (t *MessageTracker) Flush(ctx context.Context) (*models.SqsMessage, error) {
	t.m.Lock()
	defer t.m.Unlock()

	if len(t.pendingFlush) == 0 {
		// Wait for a message to be available if there currently aren't any
		for len(t.pendingFlush) == 0 {
			if err := t.msgsAvailable.Wait(ctx); err != nil {
				return nil, err
			}
		}
	}

	msg := t.pendingFlush[0]
	t.pendingFlush = t.pendingFlush[1:]
	if len(t.pendingFlush) > 0 {
		t.msgsAvailable.Signal() // Pass the baton to any other waiting threads
	}
	return msg, nil
}

// Ack acknowledges that a message has been processed and deleted from the SQS queue
func (t *MessageTracker) Ack(id string) {
	t.m.Lock()
	defer t.m.Unlock()

	msg, ok := t.idMap[id]
	if !ok {
		t.logger.Errorf("Ack: Unable to find message ID %v in ID map", id)
		return
	}

	// Delete the message from the front of its group
	gId := msg.GetGroupId()
	next, err := t.groups[gId].delete()
	if err != nil {
		panic(err)
	}

	if next != nil {
		// Add the next message to the queue if there is one
		t.pendingFlush = append(t.pendingFlush, next)

		// If the queue was empty before this message was added, a reader thread might be waiting for
		// a signal that a new message is available
		if len(t.pendingFlush) == 1 {
			t.msgsAvailable.Signal()
		}
	} else {
		// Delete the group from the map since it is now empty
		delete(t.groups, gId)
	}

	// Clean up the message ID map and refresh queue
	t.cleanupMessages(msg)

	// Signal for more messages if we have room
	if t.conf.MaxInFlightMessages-t.length() >= t.conf.MinReceiveBatchSize {
		t.readCond.Signal()
	}
}

// Nack acknowledges that messages have failed to be processed correctly,
// and either need to be retried or returned to the queue.
// Returns a collection of messages which are being abandoned.
func (t *MessageTracker) Nack(ctx context.Context, ids ...*string) error {
	t.m.Lock()
	defer t.m.Unlock()

	noRetry := make([]*models.SqsMessage, 0, len(ids))
	for _, id := range ids {
		msg, ok := t.idMap[*id]
		if !ok {
			t.logger.Errorf("Nack: Unable to find message ID %v in ID map", *id)
			return nil
		}

		// Increment the attempt counter
		msg.AttemptNo++

		// Retry - add it back to the end of pendingFlush
		if t.conf.MaxProcessingAttempts == 0 || msg.AttemptNo <= t.conf.MaxProcessingAttempts {
			t.pendingFlush = append(t.pendingFlush, msg)
			continue
		}

		// Abandon
		noRetry = append(noRetry, msg)
	}

	// For abandoned messages, abandon all messages in the group - we can't process them without breaking ordering
	abandon := make([]*models.SqsMessage, 0, len(noRetry))
	for _, msg := range noRetry {
		groupId := msg.GetGroupId()
		group, ok := t.groups[groupId]
		if !ok {
			t.logger.Errorf("Nack: Unable to find group ID %v in ID map", groupId)
			continue
		}

		abandon = append(abandon, group.queue...)

		// Clean up id maps/lists and delete the group - this is OK to do here as we are inside the lock
		t.cleanupMessages(group.queue...)
		delete(t.groups, groupId)
	}

	// Batch abandoned messages
	batches := slices.Chunk(abandon, 10)
	for batch := range batches {
		// Reset message visibility to make it available again
		res, err := t.sqs.SetMessageVisibility(ctx, 0, batch...)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}

			t.logger.Errorf("Nack: Unable to reset message visibility for batch: %v", err)
			return err
		}

		for _, failure := range res.Failures {
			t.logger.Warn(failure.Sprint())
		}
	}

	// Signal for more messages if we have room
	if t.conf.MaxInFlightMessages-t.length() >= t.conf.MinReceiveBatchSize {
		t.readCond.Signal()
	}

	return nil
}

// Length returns how many messages are currently in the message tracker awaiting flushing or acknowledgment
func (t *MessageTracker) Length() int {
	t.m.RLock()
	defer t.m.RUnlock()

	return t.length()
}

// Internal version of Length that doesn't need to lock - assumes the lock is held elsewhere
func (t *MessageTracker) length() int {
	i := 0
	for _, v := range t.groups {
		i += v.len()
	}

	return i
}

// Cleans up the tracking list/map when we no longer care about a message
func (t *MessageTracker) cleanupMessages(msgs ...*models.SqsMessage) {
	for _, msg := range msgs {
		id := msg.Msg.MessageId
		delete(t.idMap, *id)
		e := t.refreshMap[*id]
		delete(t.refreshMap, *id)
		t.refreshQueue.Remove(e)
	}
}

// refreshLoop checks for any messages requiring a refresh of their visibility timeout
func (t *MessageTracker) refreshLoop(ltHandle util.ILifetimeHandle) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

refreshLoop:
	for {
		select {
		case <-ltHandle.Terminated():
			t.logger.Debug("Refresh loop received graceful termination order - resetting visibility")
			t.resetVisibility(ltHandle.KillContext())
			break refreshLoop
		case <-ltHandle.Killed():
			t.logger.Debug("Refresh loop received kill order - breaking loop")
			break refreshLoop
		case <-ticker.C:
			t.refreshVisibility(ltHandle.KillContext())
		}
	}
}

func (t *MessageTracker) refreshVisibility(ctx context.Context) {
	t.m.Lock()
	defer t.m.Unlock()

	// Build the list of messages which currently need refreshing
	refMsgs := make([]*models.SqsMessage, 0, 10)
	for e := t.refreshQueue.Front(); e != nil; e = e.Next() {
		msg := e.Value.(*models.SqsMessage)
		if msg.RemainingDeadline() < t.conf.VisibilityTimeoutSeconds/2 {
			refMsgs = append(refMsgs, msg)
		} else {
			break
		}
	}

	t.logger.Debugf("Found %v messages requiring visibility deadline refresh", len(refMsgs))

	// Chunk into groups of 10 max
	for batch := range slices.Chunk(refMsgs, 10) {
		res, err := t.sqs.SetMessageVisibility(ctx, int32(t.conf.VisibilityTimeoutSeconds), batch...)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return
			}

			dref := util.SelectDeref(batch)
			t.logger.Warnf("Error refreshing message visibility: %v\nAffected message IDs: %v",
				err, dref)
			continue
		}

		// Logs any failures in the batch
		for _, failure := range res.Failures {
			t.logger.Warn(failure.Sprint())
		}

		// Refresh the deadline of any messages that succeeded, and move them to the back of the refresh queue
		for _, r := range res.Successes {
			msg := t.idMap[r.MsgId]
			e := t.refreshMap[r.MsgId]

			msg.RefreshDeadline(t.conf.VisibilityTimeoutSeconds)
			t.refreshQueue.MoveToBack(e)
		}
	}
}

// Resets the visibility of all messages to zero to allow other applications to pick them up as quickly as possible
func (t *MessageTracker) resetVisibility(ctx context.Context) {
	t.m.Lock()
	defer t.m.Unlock()

	msgs := make([]*models.SqsMessage, 0, t.conf.MaxInFlightMessages)
	for _, g := range t.groups {
		for _, msg := range g.queue {
			msgs = append(msgs, msg)
		}
	}

	// Batch to 10 max
	batches := slices.Chunk(msgs, 10)
	for batch := range batches {
		res, err := t.sqs.SetMessageVisibility(ctx, 0, batch...)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return
			}

			t.logger.Errorf("Error resetting message visibility for batch: %v", err)
			return
		}

		for _, failure := range res.Failures {
			t.logger.Error(failure.Sprint())
		}
	}
}
