package flow

type Component struct {
	inputMap  map[string]*Handler // Map of input pipe IDs to handlers
	outputMap map[string]*Pipe    // Map of handler IDs to output pipes
	inputs    []*Pipe             // All the input pipes

	workerInput  chan any
	workerOutput chan any
	workerPool   *WorkerPool
}

type handlerCoupling struct {
	handler        *Handler
	inputPipe      *Pipe
	outputPipe     *Pipe
	maxConcurrency int
}

func NewComponent(couplings []handlerCoupling, maxConcurrency int) *Component {
	inputs := make([]*Pipe, 0, len(couplings))
	inputMap := make(map[string]*Handler, len(couplings))
	outputMap := make(map[string]*Pipe, len(couplings))

	workerInput := make(chan any)
	workerOutput := make(chan any)
	pool := NewWorkerPool()

	for _, c := range couplings {
		inputs = append(inputs, c.inputPipe)
		inputMap[c.inputPipe.id] = c.handler
		outputMap[c.handler.id] = c.outputPipe
	}
	return &Component{
		inputMap:       inputMap,
		outputMap:      outputMap,
		inputs:         inputs,
		maxConcurrency: maxConcurrency,
	}
}

func (c *Component) processLoop1(p1 *Pipe) (any, error) {
	select {
	case i := <-p1.ReceiveChan():
		for _, v := range i {
			h := c.inputMap[p1.id]
			o, _ := h.Handle(v) // TODO: What do we do with errored handlers?
			op := c.outputMap[h.id]
			op.SendChan() <- []*any{&o} // TODO - do proper pointer handling
		}
	}
}
