package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"log/slog"
	"sync"
	"time"
)

type component struct {
	inputMap  map[string]*handler              // Map of input pipe IDs to handlers
	outputMap map[string]map[string]*Pipe      // Map of handler IDs to output pipes
	retryMap  map[string]*RetryPolicy          // Map of handler IDs to their retry policies
	inputs    []*Pipe                          // All the input pipes
	rChan     []*<-chan *handlerJob            // All of the receive channels for incoming pipes
	rChanMap  map[*<-chan *handlerJob]*handler // Map of receive channels to their corresponding handlers

	ctx     context.Context // Lifetime context
	wg      sync.WaitGroup
	stopped chan struct{} // Closed when the component has stopped

	workerPool *WorkerPool
	workWgMap  map[*handler]*util.ContextWaitGroup // For keeping track of work currently being carried out by the worker pool

	logger *slog.Logger
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
	outputPipes map[string]*Pipe
	retryPolicy *RetryPolicy
}

func NewHandlerCoupling(h *handler, in *Pipe, out map[string]*Pipe, retryPolicy *RetryPolicy) *HandlerCoupling {
	return &HandlerCoupling{
		handler:     h,
		inputPipe:   in,
		outputPipes: out,
		retryPolicy: retryPolicy,
	}
}

type handlerJob struct {
	context handlerContext
	msgs    []*message
}

func NewComponent(ctx context.Context, logger *slog.Logger, couplings []*HandlerCoupling, maxConcurrency int) *component {
	inputs := make([]*Pipe, 0, len(couplings))
	inputMap := make(map[string]*handler, len(couplings))
	outputMap := make(map[string]map[string]*Pipe, len(couplings))
	retryMap := make(map[string]*RetryPolicy)
	rChan := make([]*<-chan *handlerJob, len(couplings))
	rChanMap := make(map[*<-chan *handlerJob]*handler, len(couplings))
	workWgMap := make(map[*handler]*util.ContextWaitGroup, maxConcurrency)

	pool := NewWorkerPool(ctx, logger, maxConcurrency)

	for i, c := range couplings {
		inputs = append(inputs, c.inputPipe)
		inputMap[c.inputPipe.id] = c.handler
		outputMap[c.handler.id] = c.outputPipes
		ch := c.inputPipe.ReceiveChan()
		rChan[i] = &ch
		rChanMap[&ch] = c.handler
		workWgMap[c.handler] = util.NewContextWaitGroup()
		retryMap[c.handler.id] = c.retryPolicy
	}

	c := &component{
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
		logger:     logger,
	}

	c.wg.Go(c.processLoop)
	go c.stopMonitor()
	return c
}

// Stopped returns a channel that is closed once the component has stopped operating
func (c *component) Stopped() chan struct{} {
	return c.stopped
}

// Waits until the process loop has stopped, and closes the stopped channel when it has
func (c *component) stopMonitor() {
	c.wg.Wait()
	close(c.stopped)
}

// Processes incoming messages for any handler in the component
func (c *component) processLoop() {
	var i int
	var err error
	var job *handlerJob
	var ok bool
	var h *handler
	var out map[string]*Pipe
	var retryPolicy *RetryPolicy
	var wg *util.ContextWaitGroup

	for {
		if i, err = Select(c.ctx, c.rChan, &job, &ok); err != nil {
			return
		}

		h = c.rChanMap[c.rChan[i]]
		out = c.outputMap[h.id]
		wg = c.workWgMap[h]
		retryPolicy = c.retryMap[h.id]

		if ok {
			// Message(s) received - schedule the work
			_ = wg.Add(1)
			if err = c.workerPool.Do(wg, h, job, out, retryPolicy); err != nil {
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
