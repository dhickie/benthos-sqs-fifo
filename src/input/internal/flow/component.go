package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"sync"
	"time"
)

type Component struct {
	inputMap  map[string]*handler        // Map of input pipe IDs to handlers
	outputMap map[string][]*Pipe         // Map of handler IDs to output pipes
	retryMap  map[string]*RetryPolicy    // Map of handler IDs to their retry policies
	inputs    []*Pipe                    // All the input pipes
	rChan     []*<-chan []any            // All of the receive channels for incoming pipes
	rChanMap  map[*<-chan []any]*handler // Map of receive channels to their corresponding handlers

	ctx     context.Context // Lifetime context
	wg      sync.WaitGroup
	stopped chan struct{} // Closed when the component has stopped

	workerPool *WorkerPool
	workWgMap  map[*handler]*util.ContextWaitGroup // For keeping track of work currently being carried out by the worker pool
}

type RetryPolicy struct {
	errPipe     *Pipe
	maxAttempts int           // 0 means retry indefinitely.
	interval    time.Duration // How long to wait between attempts
}

func NewRetryPolicy(errPipe *Pipe, maxAttempts int, interval time.Duration) *RetryPolicy {
	return &RetryPolicy{
		errPipe:     errPipe,
		maxAttempts: maxAttempts,
		interval:    interval,
	}
}

type HandlerCoupling struct {
	handler     *handler
	inputPipe   *Pipe
	outputPipe  []*Pipe
	retryPolicy *RetryPolicy
}

func NewHandlerCoupling(h *handler, in *Pipe, out []*Pipe, retryPolicy *RetryPolicy) *HandlerCoupling {
	return &HandlerCoupling{
		handler:     h,
		inputPipe:   in,
		outputPipe:  out,
		retryPolicy: retryPolicy,
	}
}

func NewComponent(ctx context.Context, couplings []*HandlerCoupling, maxConcurrency int) *Component {
	inputs := make([]*Pipe, 0, len(couplings))
	inputMap := make(map[string]*handler, len(couplings))
	outputMap := make(map[string][]*Pipe, len(couplings))
	retryMap := make(map[string]*RetryPolicy)
	rChan := make([]*<-chan []any, len(couplings))
	rChanMap := make(map[*<-chan []any]*handler, len(couplings))
	workWgMap := make(map[*handler]*util.ContextWaitGroup, maxConcurrency)

	pool := NewWorkerPool(ctx, maxConcurrency)

	for i, c := range couplings {
		inputs = append(inputs, c.inputPipe)
		inputMap[c.inputPipe.id] = c.handler
		outputMap[c.handler.id] = c.outputPipe
		ch := c.inputPipe.ReceiveChan()
		rChan[i] = &ch
		rChanMap[&ch] = c.handler
		workWgMap[c.handler] = util.NewContextWaitGroup()
		retryMap[c.handler.id] = c.retryPolicy
	}

	c := &Component{
		inputMap:   inputMap,
		outputMap:  outputMap,
		inputs:     inputs,
		rChan:      rChan,
		rChanMap:   rChanMap,
		ctx:        ctx,
		wg:         sync.WaitGroup{},
		stopped:    make(chan struct{}),
		workerPool: pool,
		workWgMap:  workWgMap,
		retryMap:   retryMap,
	}

	c.wg.Go(c.processLoop)
	go c.stopMonitor()
	return c
}

// Stopped returns a channel that is closed once the component has stopped operating
func (c *Component) Stopped() chan struct{} {
	return c.stopped
}

// Waits until the process loop has stopped, and closes the stopped channel when it has
func (c *Component) stopMonitor() {
	c.wg.Wait()
	close(c.stopped)
}

// Processes incoming messages for any handler in the component
func (c *Component) processLoop() {
	var i int
	var err error
	var v []any
	var ok bool
	var h *handler
	var out []*Pipe
	var retryPolicy *RetryPolicy
	var wg *util.ContextWaitGroup

	for {
		if i, err = util.Select(c.ctx, c.rChan, &v, &ok); err != nil {
			return
		}

		h = c.rChanMap[c.rChan[i]]
		out = c.outputMap[h.id]
		wg = c.workWgMap[h]
		retryPolicy = c.retryMap[h.id]

		if ok {
			// Message received - schedule the work
			_ = wg.Add(1)
			if err = c.workerPool.Do(wg, h, v, out, retryPolicy); err != nil {
				// Context cancelled - kill the loop
				return
			}
		} else {
			// Input pipe has closed - close all the output pipes when all work is finished
			if err = wg.Wait(c.ctx); err != nil {
				// Context cancelled - kill the loop
				return
			}
			for _, p := range out {
				close(p.SendChan())
			}
			if len(c.rChan) == 1 {
				// That was the last open channel, kill the loop
				return
			}

			// Remove the closed input channel from the slice of open channels
			c.rChan = append(c.rChan[0:i], c.rChan[i+1:]...)
		}
	}
}
