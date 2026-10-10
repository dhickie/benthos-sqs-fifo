package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	logPropPipeName   = "pipeName"
	logPropNumMsgs    = "numMsgs"
	logPropNumBatches = "numBatches"
)

// Pipe represents a medium for asynchronously sending messages of any type to one or more receivers.
// They are fully threadsafe, and can be used by any number of goroutines simultaneously for both sending and receiving.
// Messages sent to the pipe are batched according to the batch policy provided upon creation. They are
// only propagated to any receivers when either the number of pending messages reaches the maximum batch size, or the
// duration since the previous pushed batch is greater than the maximum batch period.
//
// Pipes are strictly FIFO and ordering is guaranteed.
type Pipe[T any] struct {
	name string // A name for the pipe - used in logging

	ctx    context.Context    // The context for the lifetime of the pipe - cancelled when the pipe has closed
	cancel context.CancelFunc // The function to cancel the lifetime context

	bp   *BatchPolicy  // The batch policy that governs when batches are propagated to receivers
	size *atomic.Int32 // The number of messages waiting to be received in the pipe

	input  chan []*T // The input channel for incoming messages
	output chan []*T // The output channel for outgoing batches of messages

	ticker *time.Ticker // The ticker that governs the pushing of batches according to the batch policy

	messages []*T   // Collection of pending un-batched messages
	batches  [][]*T // Pending batches that haven't yet been received

	closeInitChan         chan struct{}      // Channel that is sent to when pipe closure is being initiated
	stopInputChan         chan struct{}      // Channel that is sent to when the input loop should stop
	stopOutputChan        chan struct{}      // Channel that is sent to when the output loop should stop
	inputStoppedChan      chan struct{}      // Channel that is sent to when the input loop has stopped
	inputStoppedCtx       context.Context    // Context that is cancelled when the input loop has stopped
	inputStoppedCtxCancel context.CancelFunc // Function for cancelling the input stopped ctx
	outputStoppedChan     chan struct{}      // Channel that is sent to when the output loop has stopped
	closed                bool               // Set to true when the pipe is closed

	batchLock *sync.Mutex       // The lock for providing thread safe access to the batch collection
	batchCond *util.ContextCond // Used for signalling a receiver thread that a new batch is available
	closeLock *sync.Mutex       // Ensures that a single call to Close is processed at a time, providing safe closure
	wg        *sync.WaitGroup   // Used to wait for the batching and serving loops to exit

	logger *slog.Logger
}

// NewPipe returns a pointer to a new pipe that can send and receive messages of the specified type, and applies
// the provided batching policy to any sent messages before propagating messages to receivers.
func NewPipe[T any](name string, bp *BatchPolicy, logger *slog.Logger) *Pipe[T] {
	bLock := &sync.Mutex{}
	ctx, cancel := context.WithCancel(context.Background())
	iCtx, iCancel := context.WithCancel(context.Background())
	c := &Pipe[T]{
		name:                  name,
		ctx:                   ctx,
		cancel:                cancel,
		bp:                    bp,
		input:                 make(chan []*T),
		output:                make(chan []*T),
		messages:              make([]*T, 0),
		ticker:                time.NewTicker(bp.Period),
		batches:               make([][]*T, 0),
		batchLock:             bLock,
		batchCond:             util.NewContextCond(bLock),
		closeInitChan:         make(chan struct{}, 1),
		stopInputChan:         make(chan struct{}, 1),
		stopOutputChan:        make(chan struct{}, 1),
		inputStoppedChan:      make(chan struct{}, 1),
		inputStoppedCtx:       iCtx,
		inputStoppedCtxCancel: iCancel,
		outputStoppedChan:     make(chan struct{}, 1),
		closed:                false,
		closeLock:             &sync.Mutex{},
		wg:                    &sync.WaitGroup{},
		size:                  new(atomic.Int32),
		logger:                logger,
	}
	c.wg.Go(c.closeLoop)
	c.wg.Go(c.inputLoop)
	c.wg.Go(c.outputLoop)
	return c
}

// Send sends the provided message(s) to the pipe.
// Blocks indefinitely if the pipe is already closed.
// Returns an error if ctx is cancelled.
func (c *Pipe[T]) Send(ctx context.Context, m ...*T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.input <- m:
		return nil
	}
}

// SendChan returns a channel that can be used to send messages to the pipe, for example if needed for use with a
// select statement. If the channel is closed, it will asynchronously gracefully close the pipe.
// If the pipe is already closed when the channel is sent to, it will block indefinitely.
func (c *Pipe[T]) SendChan() chan<- []*T {
	return c.input
}

// Receive receives the next batch of messages from the pipe, blocking until one is available.
// Returns an error if the pipe is already closed, or is forcefully closed while waiting for messages to be available.
// If the provided context is cancelled, any wait for a new batch is abandoned and an error is returned.
func (c *Pipe[T]) Receive(ctx context.Context) ([]*T, error) {
	select {
	case <-c.ctx.Done():
		c.logger.Debug("Pipe closed while waiting for batch", logPropPipeName, c.name)
		return nil, c.ctx.Err()
	case <-ctx.Done():
		c.logger.Debug("Context cancelled while waiting for batch", logPropPipeName, c.name)
		return nil, ctx.Err()
	case m, ok := <-c.output:
		if ok {
			return m, nil
		}

		return nil, errors.New("channel has closed")
	}
}

// ReceiveChan returns a channel that can be used to receive batches of messages from the pipe, for example if
// needed for use with a select statement. If the pipe is closed, this channel will be closed when there are
// no more messages remaining.
func (c *Pipe[T]) ReceiveChan() <-chan []*T {
	return c.output
}

// Close initiates a graceful closure of the pipe, and blocks until any remaining messages in the pipe have been
// received. Any attempts to send more messages while the pipe is closing will return an error. If the context
// provided to Close is cancelled while waiting, the pipe is forcefully closed and any pending messages are lost.
func (c *Pipe[T]) Close(ctx context.Context) {
	c.closeLock.Lock()
	defer c.closeLock.Unlock()

	c.logger.Debug("Pipe closing", logPropPipeName, c.name)

	if c.closed {
		c.logger.Debug("Pipe already closed", logPropPipeName, c.name)
		return
	}

	// Initiate the closure
	c.closeInitChan <- struct{}{}

	// Wait for either the closure to finish, or the context to be cancelled
	select {
	case <-ctx.Done():
		c.logger.Debug("Context cancelled while waiting for graceful closure", logPropPipeName, c.name)
	case <-c.outputStoppedChan:
		c.logger.Debug("Pipe closed gracefully", logPropPipeName, c.name)
	}

	// Cancel the lifetime context
	c.cancel()

	// Wait for all the loops to exit
	c.wg.Wait()
	c.closed = true
	c.logger.Debug("Pipe closure complete", logPropPipeName, c.name)
}

// Loop that manages the closure of the pipe, ensuring graceful termination
func (c *Pipe[T]) closeLoop() {
	for {
		select {
		case <-c.ctx.Done(): // Force closure - kill the loop
			c.logger.Debug("Close loop: lifetime context cancelled - killing loop", logPropPipeName, c.name)
			return
		case <-c.closeInitChan: // Tell the input loop to stop
			c.logger.Debug("Close loop: closure initiated", logPropPipeName, c.name)
			c.stopInputChan <- struct{}{}
		case <-c.inputStoppedChan: // Tell the output loop to stop when empty
			c.logger.Debug("Close loop: input loop stopped", logPropPipeName, c.name)
			c.stopOutputChan <- struct{}{}
		case <-c.outputStoppedChan:
			c.logger.Debug("Close loop: output loop stopped - killing loop", logPropPipeName, c.name)
			return
		}
	}
}

// Loop that manages the batching and propagation of incoming messages according to the channel's batch policy.
func (c *Pipe[T]) inputLoop() {
	for {
		select {
		case <-c.ctx.Done():
			c.logger.Debug("Input loop: lifetime context cancelled - killing loop", logPropPipeName, c.name)
			return
		case <-c.stopInputChan:
			c.logger.Debug("Input loop: stop instruction received, flushing pending messages", logPropPipeName, c.name)
			c.propagateBatch(true)
			c.inputStoppedChan <- struct{}{}
			c.inputStoppedCtxCancel() // Needed to tell the batch wait cond to stop waiting for any new batches
			return
		case <-c.ticker.C: // Batch period has expired
			c.logger.Debug("Input loop: batch period expired", logPropPipeName, c.name)
			c.propagateBatch(true)
		case m, ok := <-c.input: // Messages received on the input channel
			if ok {
				c.logger.Debug("Input loop: received new messages", logPropPipeName, c.name, logPropNumMsgs, len(m))
				c.size.Add(int32(len(m)))
				c.messages = append(c.messages, m...)
				if len(c.messages) >= c.bp.Number {
					c.ticker.Reset(c.bp.Period)
					c.propagateBatch(false)
				}
			} else {
				// The input channel has been closed - gracefully close the pipe
				c.logger.Debug("Input loop: input channel closed - killing loop", logPropPipeName, c.name)
				c.propagateBatch(true)
				c.inputStoppedChan <- struct{}{}
				c.inputStoppedCtxCancel()
				return
			}
		}
	}
}

// Batches pending messages and makes them available to receivers.
// If batchAll is true, all pending messages will be batched regardless of whether they fill full batches.
// If batchAll is false, messages are only batched if they can fill a full batch.
func (c *Pipe[T]) propagateBatch(batchAll bool) {
	c.batchLock.Lock()
	defer c.batchLock.Unlock()

	batches := slices.Chunk(c.messages, c.bp.Number)
	for v := range batches {
		if len(v) == c.bp.Number || (len(v) > 0 && batchAll) {
			if len(c.batches) == 0 {
				c.batchCond.Signal()
			}
			c.logger.Debug("Input loop: batch propagated", logPropPipeName, c.name, logPropNumMsgs, len(v))
			c.batches = append(c.batches, v)
			c.messages = c.messages[len(v):]
		}
	}
}

// Loop that manages serving batches to receivers.
func (c *Pipe[T]) outputLoop() {
	closing := false
	batchSent := false
	stopFunc := func() {
		c.logger.Debug("Output loop: stopping", logPropPipeName, c.name)
		c.outputStoppedChan <- struct{}{}
		close(c.output)
	}
	nextFunc := func() ([]*T, bool, error) { // Returns true if there are no more messages to serve
		c.batchLock.Lock()
		defer c.batchLock.Unlock()

		for len(c.batches) == 0 {
			if closing {
				if err := c.batchCond.Wait(c.ctx); err != nil { // No need to wait for a signal that the input loop has stopped
					c.logger.Debug("Output loop: lifetime context cancelled while waiting for batch", logPropPipeName, c.name)
					return nil, false, err
				}
			} else {
				if err1, err2 := c.batchCond.Wait2(c.ctx, c.inputStoppedCtx); err1 != nil {
					c.logger.Debug("Output loop: lifetime context cancelled while waiting for batch", logPropPipeName, c.name)
					return nil, false, err1
				} else if err2 != nil && c.size.Load() == 0 {
					c.logger.Debug("Output loop: received stop, batches empty", logPropPipeName, c.name)
					return nil, true, nil // The read loop has stopped and the pipe is empty
				} else if err2 != nil {
					c.logger.Debug("Output loop: received stop, batches remaining", logPropPipeName, c.name, logPropNumBatches, len(c.batches))
					closing = true // The read loop has stopped but there are more batches to serve
				}
			}
		}
		next := c.batches[0]
		c.batches = c.batches[1:]
		return next, false, nil
	}

	for {
		nextBatch, finished, err := nextFunc()
		if finished || err != nil {
			stopFunc()
			return
		}

		batchSent = false
		for !batchSent {
			select {
			case <-c.ctx.Done(): // Pipe has been forcefully closed
				c.logger.Debug("Output loop: lifetime context cancelled while waiting for receiver", logPropPipeName, c.name)
				stopFunc()
				return
			case <-c.inputStoppedChan:
				c.logger.Debug("Output loop: received stop, batches remaining", logPropPipeName, c.name, logPropNumBatches, len(c.batches))
				closing = true
			case c.output <- nextBatch:
				c.logger.Debug("Output loop: batch received", logPropPipeName, c.name, logPropNumMsgs, len(nextBatch))
				batchSent = true
				c.size.Add(int32(len(nextBatch) * -1))
				if closing && c.size.Load() == 0 {
					c.logger.Debug("Output loop: closure complete", logPropPipeName, c.name)
					stopFunc()
					return
				}
			}
		}
	}
}
