package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/test/wait"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"sync"
)

type WorkerPool struct {
	workers  []*Worker
	workChan chan *WorkItem

	ctx     context.Context
	wg      *sync.WaitGroup
	stopped chan struct{}
}

func NewWorkerPool(ctx context.Context, numWorkers int) *WorkerPool {
	workers := make([]*Worker, numWorkers)
	input := make(chan *WorkItem)
	wg := &sync.WaitGroup{}
	for i := range numWorkers {
		workers[i] = NewWorker(ctx, wg, input)
	}

	wp := &WorkerPool{
		workers:  workers,
		workChan: input,
		ctx:      ctx,
		wg:       wg,
		stopped:  make(chan struct{}),
	}
	go wp.stoppedMonitor()

	return wp
}

func (p *WorkerPool) Do(wg *util.ContextWaitGroup, h *Handler, w []any, o *Pipe, retryPolicy *RetryPolicy) error {
	f := func() error {
		numAttempts := 0
		var err error
		var r []any

		// Try until we succeed or hit the maximum number of attempts
		for {
			r, err = h.Handle(p.ctx, w)
			if err == nil {
				o.SendChan() <- r
				return nil
			}

			// Was the context cancelled?
			if p.ctx.Err() != nil {
				return p.ctx.Err()
			}

			// The handler returned an error, do we have a retry policy?
			numAttempts++

			retry := retryPolicy != nil && (retryPolicy.maxAttempts == 0 || numAttempts < retryPolicy.maxAttempts)

			if retry {
				if wErr := wait.For(p.ctx, retryPolicy.interval); wErr != nil {
					return p.ctx.Err()
				}

				continue
			}

			// We're out of retries, is there an error pipe to send to?
			if retryPolicy != nil && retryPolicy.errPipe != nil {
				retryPolicy.errPipe.SendChan() <- w
			}

			return nil
		}
	}
	wi := NewWorkItem(f, o, wg)

	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case p.workChan <- wi:
		return nil
	}
}

func (p *WorkerPool) stoppedMonitor() {
	p.wg.Wait()
	close(p.stopped)
	close(p.workChan)
}

type Worker struct {
	input chan *WorkItem
	ctx   context.Context
}

func NewWorker(ctx context.Context, wg *sync.WaitGroup, input chan *WorkItem) *Worker {
	w := &Worker{
		input: input,
		ctx:   ctx,
	}

	wg.Go(w.run)
	return w
}

func (w *Worker) run() {
	var i *WorkItem
	var err error

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		select {
		case <-w.ctx.Done():
			return
		case i = <-w.input:
			if err = i.f(); err != nil {
				// The work function only returns an error if the context is cancelled
				return
			}
			i.wg.Done()
		}
	}
}

type WorkItem struct {
	f      func() error
	output *Pipe
	wg     *util.ContextWaitGroup
}

func NewWorkItem(f func() error, output *Pipe, wg *util.ContextWaitGroup) *WorkItem {
	return &WorkItem{
		f:      f,
		output: output,
		wg:     wg,
	}
}
