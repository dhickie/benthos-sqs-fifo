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

	closeInitChan     chan struct{} // Channel that is sent to when Channel closure is being initiated
	stopInputChan     chan struct{} // Channel that is sent to when the input loop should stop
	stopOutputChan    chan struct{} // Channel that is sent to when the output loop should stop
	inputStoppedChan  chan struct{} // Channel that is sent to when the input loop has stopped
	outputStoppedChan chan struct{} // Channel that is sent to when the output loop has stopped
	closed            bool          // Set to true when the channel is closed

	batchLock *sync.Mutex       // The lock for providing thread safe access to the batch collection
	batchCond *util.ContextCond // Used for signalling a receiver thread that a new batch is available
	closeLock *sync.Mutex       // Protects access to the closed/closing bools
	wg        *sync.WaitGroup   // Used to wait for the batching and serving loops to exit
}

// NewChannel returns a pointer to a new channel that can send and receive messages of the specified type, and applies
// the provided batching policy to any sent messages before propagating messages to receivers.
func NewChannel[T any](bp *BatchPolicy) *Channel[T] {
	bLock := &sync.Mutex{}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Channel[T]{
		ctx:               ctx,
		cancel:            cancel,
		bp:                bp,
		input:             make(chan []*T),
		output:            make(chan []*T),
		messages:          make([]*T, 0),
		ticker:            time.NewTicker(bp.Period),
		batches:           make([][]*T, 0),
		batchLock:         bLock,
		batchCond:         util.NewContextCond(bLock),
		closeInitChan:     make(chan struct{}, 1),
		stopInputChan:     make(chan struct{}, 1),
		stopOutputChan:    make(chan struct{}, 1),
		inputStoppedChan:  make(chan struct{}, 1),
		outputStoppedChan: make(chan struct{}, 1),
		closed:            false,
		closeLock:         &sync.Mutex{},
		wg:                &sync.WaitGroup{},
		size:              new(atomic.Int32),
	}
	c.wg.Go(c.closeLoop)
	c.wg.Go(c.inputLoop)
	c.wg.Go(c.outputLoop)
	return c
}

// Send sends the provided message(s) to the Channel.
// Blocks indefinitely if the Channel is already closed.
// Returns an error if ctx is cancelled.
func (c *Channel[T]) Send(ctx context.Context, m ...*T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.input <- m:
		return nil
	}
}

// SendChan returns a go channel that can be used to send messages to the Channel, for example if needed for use with a
// select statement. If the channel is closed, it will asynchronously gracefully close the overall Channel.
// If the Channel is already closed when the channel is sent to, it will block indefinitely.
func (c *Channel[T]) SendChan() chan<- []*T {
	return c.input
}

// Receive receives the next batch of messages from the channel, blocking until one is available.
// Returns an error if the channel is already closed, or is forcefully closed while waiting for messages to be available.
// If the provided context is cancelled, any wait for a new batch is abandoned and an error is returned.
func (c *Channel[T]) Receive(ctx context.Context) ([]*T, error) {
	select {
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	case m, ok := <-c.output:
		if ok {
			return m, nil
		}

		return nil, errors.New("channel has closed")
	}
}

// ReceiveChan returns a channel that can be used to receive batches of messages from the Channel, for example if
// needed for use with a select statement. If the overall channel is closed, this channel will be closed when there are
// no more messages remaining.
func (c *Channel[T]) ReceiveChan() <-chan []*T {
	return c.output
}

// Close initiates a graceful closure of the channel, and blocks until any remaining messages in the channel have been
// received. Any attempts to send more messages while the channel is closing will return an error. If the context
// provided to Close is cancelled while waiting, the channel is forcefully closed and any pending messages are lost.
func (c *Channel[T]) Close(ctx context.Context) {
	c.closeLock.Lock()
	defer c.closeLock.Unlock()

	if c.closed {
		return
	}

	// Initiate the closure
	c.closeInitChan <- struct{}{}

	// Wait for either the closure to finish, or the context to be cancelled
	select {
	case <-ctx.Done():
	case <-c.outputStoppedChan:
	}

	// Cancel the Channel context
	c.cancel()

	// Wait for all the loops to exit
	c.wg.Wait()
	c.closed = true
}

// Loop that manages the closure of the Channel, ensuring graceful termination
func (c *Channel[T]) closeLoop() {
	for {
		select {
		case <-c.ctx.Done(): // The Channel is being force closed - kill the loop
			return
		case <-c.closeInitChan: // Graceful close has been initiated - tell the input loop to stop
			c.stopInputChan <- struct{}{}
		case <-c.inputStoppedChan: // The input loop has stopped, tell the output loop to stop when empty
			c.stopOutputChan <- struct{}{}
		case <-c.outputStoppedChan: // Graceful stop is complete, kill the loop
			return
		}
	}
}

// Loop that manages the batching and propagation of incoming messages according to the channel's batch policy.
func (c *Channel[T]) inputLoop() {
	for {
		select {
		case <-c.ctx.Done(): // Channel is being force closed
			return
		case <-c.stopInputChan: // Channel is closing, push any remaining messages and kill the input loop
			c.propagateBatch(true)
			<-c.inputStoppedChan
			return
		case <-c.ticker.C: // Batch period has expired
			c.propagateBatch(true)
		case m, ok := <-c.input: // Messages received on the input channel
			if ok {
				c.size.Add(int32(len(m)))
				c.messages = append(c.messages, m...)
				if len(c.messages) >= c.bp.Number {
					c.ticker.Reset(c.bp.Period)
					c.propagateBatch(false)
				}
			} else {
				// The input channel has been closed - gracefully close the Channel
				c.propagateBatch(true)
				<-c.inputStoppedChan
				return
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
func (c *Channel[T]) outputLoop() {
	closing := false
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
		case <-c.ctx.Done(): // Channel has been forcefully closed
			close(c.output)
			return
		case <-c.inputStoppedChan: // Channel is closing
			closing = true
		case c.output <- n: // A batch has been sent to a receiver
			c.size.Add(int32(len(n) * -1))
			if closing && c.size.Load() == 0 {
				c.outputStoppedChan <- struct{}{}
				close(c.output)
				return
			}
		}
	}
}
