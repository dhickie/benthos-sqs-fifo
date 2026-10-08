package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Channel represents a medium for asynchronously sending messages of any type to one or more receivers.
// They are fully threadsafe, and can be used by any number of goroutines simultaneously for both sending and receiving.
// Messages sent to the channel are batched according to the batch policy provided when creating the channel. They are
// only propagated to any receivers when either the number of pending messages reaches the maximum batch size, or the
// duration since the previous pushed batch is greater than the maximum batch period.
//
// Channels are strictly FIFO and ordering is guaranteed.
type Channel[T any] struct {
	ctx    context.Context    // The context for the lifetime of the channel - cancelled when the channel has closed
	cancel context.CancelFunc // The function to cancel the lifetime context

	bp   *BatchPolicy  // The batch policy that governs when batches are propagated to receivers
	size *atomic.Int32 // The number of messages waiting to be received in the channel

	input  chan []*T // The input channel for incoming messages
	output chan []*T // The output channel for outgoing batches of messages

	ticker *time.Ticker // The ticker that governs the pushing of batches according to the batch policy

	messages []*T   // Collection of pending un-batched messages
	batches  [][]*T // Pending batches that haven't yet been received

	closed  bool // Set to true when the channel is closed
	closing bool // Set to true when the channel is closing and the channel is draining down

	batchLock *sync.Mutex       // The lock for providing thread safe access to the batch collection
	batchCond *util.ContextCond // Used for signaling a receiver thread that a new batch is available
	closeLock *sync.RWMutex     // Protects access to the closed/closing bools
	closeCond *util.ContextCond // Notifies the goroutine closing the channel that the channel is now empty
	wg        *sync.WaitGroup   // Used to wait for the batching and serving loops to exit
}

// NewChannel returns a pointer to a new channel that can send and receive messages of the specified type, and applies
// the provided batching policy to any sent messages before propagating messages to receivers.
func NewChannel[T any](bp *BatchPolicy) *Channel[T] {
	cLock := &sync.RWMutex{}
	bLock := &sync.Mutex{}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Channel[T]{
		ctx:       ctx,
		cancel:    cancel,
		bp:        bp,
		input:     make(chan []*T),
		output:    make(chan []*T),
		messages:  make([]*T, 0),
		ticker:    time.NewTicker(bp.Period),
		batches:   make([][]*T, 0),
		batchLock: bLock,
		batchCond: util.NewContextCond(bLock),
		closed:    false,
		closing:   false,
		closeLock: cLock,
		closeCond: util.NewContextCond(cLock),
		wg:        &sync.WaitGroup{},
		size:      new(atomic.Int32),
	}
	c.wg.Go(c.batchLoop)
	c.wg.Go(c.serveLoop)
	return c
}

// Send sends the provided message(s) to the channel. Returns an error if the channel is currently closing or has already
// closed.
func (c *Channel[T]) Send(m ...*T) error {
	c.closeLock.RLock()
	defer c.closeLock.RUnlock()

	// Check the channel isn't closing or already closed
	if c.closing {
		return errors.New("cannot send to a closing channel")
	}
	if c.closed {
		return errors.New("cannot send to a closed channel")
	}

	c.input <- m
	c.size.Add(int32(len(m)))
	return nil
}

// Receive receives the next batch of messages from the channel, blocking until one is available.
// Returns an error if the channel is already closed, or is forcefully closed while waiting for messages to be available.
// If the provided context is cancelled, any wait for a new batch is abandoned and an error is returned.
func (c *Channel[T]) Receive(ctx context.Context) ([]*T, error) {
	c.closeLock.RLock()
	defer c.closeLock.RUnlock()

	// Check the channel isn't already closed
	if c.closed {
		return nil, errors.New("cannot receive from a closed channel")
	}
	// If the channel is closing and is now empty, then signal the close function that closing can complete
	defer func() {
		if c.closing && c.size.Load() == 0 {
			c.closeCond.Signal()
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case v := <-c.output:
		c.size.Add(int32(len(v) * -1))
		return v, nil
	}
}

// Close initiates a graceful closure of the channel, and blocks until any remaining messages in the channel have been
// received. Any attempts to send more messages while the channel is closing will return an error. If the context
// provided to Close is cancelled while waiting, the channel is forcefully closed and any pending messages are lost.
func (c *Channel[T]) Close(ctx context.Context) {
	c.closeLock.Lock()
	defer c.closeLock.Unlock()

	// Wait until the channel is empty before closing if possible
	c.closing = true
	for c.size.Load() > 0 {
		if err := c.closeCond.Wait(ctx); err != nil {
			break
		}
	}

	// Kill the processing threads
	c.cancel()
	c.closed = true
	c.wg.Wait() // Wait for the processing threads to actually exit
}

// Loop that manages the batching and propagation of incoming messages according to the channel's batch policy.
func (c *Channel[T]) batchLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.ticker.C:
			c.propagateBatch(true)
		case m := <-c.input:
			c.messages = append(c.messages, m...)
			if len(c.messages) >= c.bp.Number {
				c.ticker.Reset(c.bp.Period)
				c.propagateBatch(false)
			}
		}
	}
}

// Batches pending messages and makes them available to receivers.
// If batchAll is true, all pending messages will be batched regardless of whether they fill full batches.
// If batchAll is false, messages are only batched if they can fill a full batch.
func (c *Channel[T]) propagateBatch(batchAll bool) {
	c.batchLock.Lock()
	defer c.batchLock.Unlock()

	batches := slices.Chunk(c.messages, c.bp.Number)
	for v := range batches {
		if len(v) == c.bp.Number || (len(v) > 0 && batchAll) {
			if len(c.batches) == 0 {
				c.batchCond.Signal()
			}
			c.batches = append(c.batches, v)
			c.messages = c.messages[len(v):]
		}
	}
}

// Loop that manages serving batches to receivers.
func (c *Channel[T]) serveLoop() {
	for {
		n, err := func() ([]*T, error) {
			c.batchLock.Lock()
			defer c.batchLock.Unlock()

			for len(c.batches) == 0 {
				if err := c.batchCond.Wait(c.ctx); err != nil {
					return nil, err
				}
			}
			next := c.batches[0]
			c.batches = c.batches[1:]
			return next, nil
		}()

		if err != nil {
			return
		}

		select {
		case <-c.ctx.Done():
			return
		case c.output <- n:
		}
	}
}
