package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/test/wait"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"log/slog"
	"sync"
)

type WorkerPool struct {
	workers  []*Worker
	workChan chan *WorkItem

	ctx     context.Context
	wg      *sync.WaitGroup
	stopped chan struct{}

	logger *slog.Logger
}

func NewWorkerPool(ctx context.Context, logger *slog.Logger, numWorkers int) *WorkerPool {
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
		logger:   logger,
	}
	go wp.stoppedMonitor()

	return wp
}

func (p *WorkerPool) Do(wg *util.ContextWaitGroup, h *handler, job *handlerJob, o map[string]*Pipe, retryPolicy *RetryPolicy) error {
	f := func() error {
		numAttempts := 0

		// Try until we succeed or hit the maximum number of attempts
		for {
			numAttempts++

			hCtx := job.context
			r := h.handle(p.ctx, hCtx, job.msgs)
			errors := make([]*handlerResult, 0, len(job.msgs))

			// Work through the results, sending successes on to subsequent handlers as appropriate
			for _, v := range r {
				if v.Error == nil {
					// Send the results to the correct pipes for that flow (in the case of split handlers)
					for _, w := range v.Results {
						if pipe, ok := o[w.FlowName]; !ok {
							// TODO log an error
						} else {
							pipe.Send(p.ctx, w.Value)
						}
					}
				} else if p.ctx.Err() != nil { // Was the context cancelled?
					return p.ctx.Err()
				} else {
					errors = append(errors, v)
				}
			}

			if len(errors) == 0 {
				return nil
			}

			if retryPolicy != nil {
				msgMap := make(map[string]*message)
				for _, v := range job.msgs {
					msgMap[v.id] = v
				}

				errorMsgs := make([]*message, len(errors))
				for i, v := range errors {
					errorMsgs[i] = msgMap[v.Id]
				}

				if retryPolicy.maxAttempts == 0 || numAttempts < retryPolicy.maxAttempts {
					// Retry the failures
					job.msgs = errorMsgs

					if err := wait.For(p.ctx, retryPolicy.interval); err != nil {
						return err // Return the error from the context if it was cancelled while waiting
					}
					continue
				} else if retryPolicy.errPipe != nil {
					// Send the messages to the error pipe
					errorMsgValues := make([]any, len(errorMsgs))
					for i, v := range errorMsgs {
						errorMsgValues[i] = v.value
					}
					if err := retryPolicy.errPipe.Send(p.ctx, errorMsgValues...); err != nil {
						return err
					}
				} else {
					// TODO log an error here - failed handle but no error handler
				}
			}

			return nil
		}
	}
	wi := NewWorkItem(f, wg)

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
	f  func() error
	wg *util.ContextWaitGroup
}

func NewWorkItem(f func() error, wg *util.ContextWaitGroup) *WorkItem {
	return &WorkItem{
		f:  f,
		wg: wg,
	}
}
