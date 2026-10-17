package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"sync"
)

type Component struct {
	inputMap  map[string]*Handler        // Map of input pipe IDs to handlers
	outputMap map[string]*Pipe           // Map of handler IDs to output pipes
	inputs    []*Pipe                    // All the input pipes
	rChan     []*<-chan []any            // All of the receive channels for incoming pipes
	rChanMap  map[*<-chan []any]*Handler // Map of receive channels to their corresponding handlers

	ctx     context.Context // Lifetime context
	wg      sync.WaitGroup
	stopped chan struct{} // Closed when the component has stopped

	workerPool *WorkerPool
}

type HandlerCoupling struct {
	handler    *Handler
	inputPipe  *Pipe
	outputPipe *Pipe
}

func NewComponent(ctx context.Context, couplings []HandlerCoupling, maxConcurrency int) *Component {
	inputs := make([]*Pipe, 0, len(couplings))
	inputMap := make(map[string]*Handler, len(couplings))
	outputMap := make(map[string]*Pipe, len(couplings))
	rChan := make([]*<-chan []any, len(couplings))
	rChanMap := make(map[*<-chan []any]*Handler, len(couplings))

	pool := NewWorkerPool(ctx, maxConcurrency)

	for i, c := range couplings {
		inputs = append(inputs, c.inputPipe)
		inputMap[c.inputPipe.id] = c.handler
		outputMap[c.handler.id] = c.outputPipe
		ch := c.inputPipe.ReceiveChan()
		rChan[i] = &ch
		rChanMap[&ch] = c.handler
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
	var h *Handler
	var out *Pipe

	for {
		if i, err = util.Select(c.ctx, c.rChan, &v, &ok); err != nil {
			return
		}

		h = c.rChanMap[c.rChan[i]]
		out = c.outputMap[h.id]

		if ok {
			// Message received - schedule the work
			if err = c.workerPool.Do(h, v, out); err != nil {
				// Context cancelled - kill the loop
				return
			}
		} else {
			// Input pipe has closed - close the output pipe
			close(out.SendChan())
			if len(c.rChan) == 1 {
				// That was the last open channel, kill the loop
				return
			}

			// Remove the closed input channel from the slice of open channels
			c.rChan = append(c.rChan[0:i], c.rChan[i+1:]...)
		}
	}
}
