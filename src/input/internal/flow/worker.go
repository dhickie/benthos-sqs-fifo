package flow

type Worker struct {
	input  chan any
	output chan any
	f      func(any) (any, error)
}

func NewWorker(input chan any, output chan any, f func(any) (any, error)) *Worker {
	w := &Worker{
		input:  input,
		output: output,
		f:      f,
	}

	go w.Run()
	return w
}

type WorkerPool struct {
	workers []*Worker
}

func NewWorkerPool(input chan any, output chan any, f func(any) (any, error), numWorkers int) *WorkerPool {
	workers := make([]*Worker, numWorkers)
	for i := range workers {
		workers[i] = NewWorker(input, output, f)
	}

	return &WorkerPool{
		workers: workers,
	}
}

func (w *Worker) Run() {
	var i any
	var r any
	//var err error
	for { // TODO: Handle context cancellation
		i = <-w.input
		r, _ = w.f(i) // TODO: Handle errors
		w.output <- r
	}
}
