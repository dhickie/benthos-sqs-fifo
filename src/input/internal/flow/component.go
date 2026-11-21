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
	outputMap map[string]map[string]*pipe      // Map of handler IDs to output pipes
	retryMap  map[string]*retryPolicy          // Map of handler IDs to their retry policies
	inputs    []*pipe                          // All the input pipes
	rChan     []*<-chan *handlerJob            // All of the receive channels for incoming pipes
	rChanMap  map[*<-chan *handlerJob]*handler // Map of receive channels to their corresponding handlers

	ctx     context.Context // Lifetime context
	wg      sync.WaitGroup
	stopped chan struct{} // Closed when the component has stopped

	workerPool *workerPool
	workWgMap  map[*handler]*util.ContextWaitGroup // For keeping track of work currently being carried out by the worker pool

	logger *slog.Logger
}

type retryPolicy struct {
	errPipe     *pipe
	maxAttempts int           // 0 means retry indefinitely.
	interval    time.Duration // How long to wait between attempts
}

func newRetryPolicy(errPipe *pipe, maxAttempts int, interval time.Duration) *retryPolicy {
	return &retryPolicy{
		errPipe:     errPipe,
		maxAttempts: maxAttempts,
		interval:    interval,
	}
}

type handlerCoupling struct {
	handler     *handler
	inputPipe   *pipe
	outputPipes map[string]*pipe
	retryPolicy *retryPolicy
}

func newHandlerCoupling(h *handler, in *pipe, out map[string]*pipe, retryPolicy *retryPolicy) *handlerCoupling {
	return &handlerCoupling{
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

func newComponent(ctx context.Context, logger *slog.Logger, couplings []*handlerCoupling, maxConcurrency int) *component {
	inputs := make([]*pipe, 0, len(couplings))
	inputMap := make(map[string]*handler, len(couplings))
	outputMap := make(map[string]map[string]*pipe, len(couplings))
	retryMap := make(map[string]*retryPolicy)
	rChan := make([]*<-chan *handlerJob, len(couplings))
	rChanMap := make(map[*<-chan *handlerJob]*handler, len(couplings))
	workWgMap := make(map[*handler]*util.ContextWaitGroup, maxConcurrency)

	pool := newWorkerPool(ctx, logger, maxConcurrency)

	for i, c := range couplings {
		inputs = append(inputs, c.inputPipe)
		inputMap[c.inputPipe.id] = c.handler
		outputMap[c.handler.id] = c.outputPipes
		ch := c.inputPipe.receiveChan()
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
	var out map[string]*pipe
	var retryPolicy *retryPolicy
	var wg *util.ContextWaitGroup

	for {
		if i, err = arraySelect(c.ctx, c.rChan, &job, &ok); err != nil {
			return
		}

		h = c.rChanMap[c.rChan[i]]
		out = c.outputMap[h.id]
		wg = c.workWgMap[h]
		retryPolicy = c.retryMap[h.id]

		if ok {
			// Message(s) received - schedule the work
			_ = wg.Add(1)
			if err = c.workerPool.do(wg, h, job, out, retryPolicy); err != nil {
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
				close(p.sendChan())
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
