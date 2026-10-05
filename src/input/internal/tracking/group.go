package tracking

import (
	"dhickie/benthos-sqs-fifo/src/input/internal/models"
)

// Tracks the current processing status of a message group that must be processed sequentially
// Each group can only have a single message in flight at once in order to guarantee ordering
type groupTracker struct {
	queue    []*models.SqsMessage // The queue of pending messages for this group
	inFlight bool                 // Whether the message at the front of the queue is currently in flight
}

// Create a new empty group tracker for a group of messages that share a message group ID
func newGroupTracker() *groupTracker {
	return &groupTracker{
		queue:    make([]*models.SqsMessage, 0, 10), // Max capacity is a bit of a guess - could be a better way to reason about this
		inFlight: false,
	}
}

// Add a collection of messages to the group tracker.
// If the group doesn't currently have an inflight message, it returns the next message and marks the group as in flight
func (t *groupTracker) add(msgs []*models.SqsMessage) *models.SqsMessage {
	t.queue = append(t.queue, msgs...)

	if !t.inFlight {
		t.inFlight = true
		return t.queue[0]
	}

	return nil
}

// Deletes the message at the front of the queue, and returns the next message if there is one ready
func (t *groupTracker) delete() (*models.SqsMessage, error) {
	if !t.inFlight {
		return nil, newTrackingError("group", "Cannot delete message for group that doesn't have an in-flight message")
	}

	t.queue = t.queue[1:]

	if len(t.queue) > 0 {
		return t.queue[0], nil
	}

	t.inFlight = false
	return nil, nil
}

// Returns how many messages are currently in flight for this message group
func (t *groupTracker) len() int {
	return len(t.queue)
}
