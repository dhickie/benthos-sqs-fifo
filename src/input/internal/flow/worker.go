package flow

import "context"

type WorkerPool struct {
	workers  []*Worker
	workChan chan *WorkItem
}

func NewWorkerPool(numWorkers int) *WorkerPool {
	workers := make([]*Worker, numWorkers)
	input := make(chan *WorkItem)
	for i := range numWorkers {
		workers[i] = NewWorker(input)
	}
	return &WorkerPool{
		workers:  workers,
		workChan: input,
	}
}

func (p *WorkerPool) Do(ctx context.Context, h *Handler, w []any, o *Pipe) {
	f := func() ([]any, error) {
		return h.Handle(ctx, w)
	}
	wi := NewWorkItem(f, o)

	select {
	case <-ctx.Done():
	case p.workChan <- wi:
	}
}

type Worker struct {
	input chan *WorkItem
}

func NewWorker(input chan *WorkItem) *Worker {
	w := &Worker{
		input: input,
	}

	go w.run()
	return w
}

func (w *Worker) run() {
	var i *WorkItem
	var r []any
	//var err error
	for { // TODO: Handle context cancellation
		i = <-w.input
		r, _ = i.f() // TODO: Handle errors
		i.output.SendChan() <- r
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
