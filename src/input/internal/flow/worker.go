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

func (p *WorkerPool) Do(wg *util.ContextWaitGroup, h *handler, w []any, o []*Pipe, retryPolicy *RetryPolicy) error {
	f := func() error {
		numAttempts := 0
		var err error
		var r [][]InternalHandleResult

		// Try until we succeed or hit the maximum number of attempts
		for {
			numAttempts++
			r, err = h.Handle(p.ctx, w)
			if err == nil {
				// Send output messages to output pipes that the handler tells us are OK to send, and keep track of anything that needs to be retried
				retryMsgs := make([]any, 0)
				for i, v := range r {
					msgs := make([]any, 0, len(v))
					for _, q := range v {
						if q.ok && q.err == nil {
							msgs = append(msgs, q.output)
						} else if q.err != nil {
							retryMsgs = append(retryMsgs, q.input)
						}
					}

					// Only try to send to the output channel if there is one defined
					if len(msgs) > 0 && o != nil && len(o) > i {
						o[i].SendChan() <- msgs
					}
				}

				if len(retryMsgs) > 0 {
					// TODO at the moment we assume that each output from the handler will uniquely deal with one input message.
					// If this isn't the case, then this could result in duplicate handles. We need to wrap the messages in a message object
					// with a unique ID, so that we can a) idempotently handle messages inside handlers and b) not put the same message through
					// a retry multiple times from different failures on different outputs
					w = retryMsgs
					if retry, err := shouldRetry(p.ctx, retryPolicy, numAttempts); err != nil {
						return err
					} else if retry {
						continue
					}

					// We're out of retries, is there an error pipe to send to?
					if retryPolicy != nil && retryPolicy.errPipe != nil {
						retryPolicy.errPipe.SendChan() <- w
					}
				}

				return nil
			}

			// Was the context cancelled?
			if p.ctx.Err() != nil {
				return p.ctx.Err()
			}

			// The handler returned an error, do we have a retry policy that's within the max number of attempts?
			if retry, err := shouldRetry(p.ctx, retryPolicy, numAttempts); err != nil {
				return err
			} else if retry {
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

func shouldRetry(ctx context.Context, policy *RetryPolicy, numAttempts int) (bool, error) {
	retry := policy != nil && (policy.maxAttempts == 0 || numAttempts < policy.maxAttempts)

	if retry {
		if wErr := wait.For(ctx, policy.interval); wErr != nil {
			return false, ctx.Err()
		}

		return true, nil
	}

	return false, nil
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
	f       func() error
	outputs []*Pipe
	wg      *util.ContextWaitGroup
}

func NewWorkItem(f func() error, outputs []*Pipe, wg *util.ContextWaitGroup) *WorkItem {
	return &WorkItem{
		f:       f,
		outputs: outputs,
		wg:      wg,
	}
}
