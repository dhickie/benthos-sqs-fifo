package flow

import (
	"context"
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

func (p *WorkerPool) Do(h *Handler, w []any, o *Pipe) error {
	f := func() ([]any, error) {
		return h.Handle(p.ctx, w)
	}
	wi := NewWorkItem(f, o)

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
	var r []any
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
			if r, err = i.f(); err != nil { // TODO: Handle handler errors
				return
			}
			i.output.SendChan() <- r
		}
	}
}

type WorkItem struct {
	f      func() ([]any, error)
	output *Pipe
}

func NewWorkItem(f func() ([]any, error), output *Pipe) *WorkItem {
	return &WorkItem{
		f:      f,
		output: output,
	}
}
