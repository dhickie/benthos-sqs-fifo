package flow

import (
	"context"
	"reflect"
	"strconv"
)

type Component struct {
	inputMap  map[string]*Handler // Map of input pipe IDs to handlers
	outputMap map[string]*Pipe    // Map of handler IDs to output pipes
	inputs    []*Pipe             // All the input pipes

	ctx    context.Context // Lifetime context
	cancel context.CancelFunc

	workerPool *WorkerPool
}

type HandlerCoupling struct {
	handler    *Handler
	inputPipe  *Pipe
	outputPipe *Pipe
}

func NewComponent(couplings []HandlerCoupling, maxConcurrency int) *Component {
	inputs := make([]*Pipe, 0, len(couplings))
	inputMap := make(map[string]*Handler, len(couplings))
	outputMap := make(map[string]*Pipe, len(couplings))

	pool := NewWorkerPool(maxConcurrency)

	for _, c := range couplings {
		inputs = append(inputs, c.inputPipe)
		inputMap[c.inputPipe.id] = c.handler
		outputMap[c.handler.id] = c.outputPipe
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Component{
		inputMap:   inputMap,
		outputMap:  outputMap,
		inputs:     inputs,
		ctx:        ctx,
		cancel:     cancel,
		workerPool: pool,
	}

	cValue := reflect.ValueOf(c)
	pMethod := cValue.MethodByName("ProcessLoop" + strconv.Itoa(len(couplings)))
	args := make([]reflect.Value, len(couplings))
	for i, v := range couplings {
		args[i] = reflect.ValueOf(v.inputPipe)
	}
	go pMethod.Call(args)

	return c
}

func (c *Component) processWork(ctx context.Context, work []any, pipe *Pipe) {
	handler := c.inputMap[pipe.id]
	outputChan := c.outputMap[handler.id]
	c.workerPool.Do(ctx, handler, work, outputChan)
}
