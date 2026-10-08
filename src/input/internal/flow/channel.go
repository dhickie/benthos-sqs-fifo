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

type Channel[T any] struct {
	ctx       context.Context
	cancel    context.CancelFunc
	bp        *BatchPolicy
	input     chan []*T
	output    chan []*T
	messages  []*T
	timer     *time.Timer
	batches   [][]*T
	batchLock *sync.Mutex
	batchCond *util.ContextCond
	closed    bool
	closing   bool
	closeLock *sync.RWMutex
	closeCond *util.ContextCond
	wg        *sync.WaitGroup
	size      *atomic.Int32
}

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
		timer:     time.NewTimer(bp.Period),
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

	// Kill the processing thread
	c.cancel()
	c.closed = true
	c.wg.Wait() // Wait for the processing thread to actually exit
}

func (c *Channel[T]) batchLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.timer.C:
			c.propagateBatch(true)
		case m := <-c.input:
			c.messages = append(c.messages, m...)
			if len(c.messages) >= c.bp.Number {
				c.propagateBatch(false)
			}
		}
	}
}

func (c *Channel[T]) propagateBatch(batchAll bool) {
	c.batchLock.Lock()
	defer c.batchLock.Unlock()

	batches := slices.Chunk(c.messages, c.bp.Number)
	for v := range batches {
		if len(v) == c.bp.Number || (len(v) > 0 && batchAll) {
			c.batches = append(c.batches, v)
			c.messages = c.messages[len(v):]
		}
	}
}

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
