package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"
	"uuid"
)

// TODO - review all default values
// TODO - add validation when building
// TODO - replace public facing types with interfaces
// TODO - all the comments
// TODO - review variable naming for clarity
// TODO - termination of app

func App(name string) *AppBuilder {
	return &AppBuilder{
		n: name,
		f: make([]*flowBuilder, 0),
	}
}

func Flow[Tinit any](name string) *FlowBuilder[Tinit, Tinit] {
	return &FlowBuilder[Tinit, Tinit]{
		id:  uuid.New().String(),
		n:   name,
		inT: reflect.TypeFor[Tinit](),
		h:   make([]*completedHandlerBuilder, 0),
	}
}

func Handler[Tin, Tout any](name string) *HandlerBuilder[Tin, Tout] {
	return &HandlerBuilder[Tin, Tout]{
		n:           name,
		maxA:        3,
		retryPeriod: time.Second,
	}
}

func EndHandler[Tin any](name string) *EndHandlerBuilder[Tin] {
	return &EndHandlerBuilder[Tin]{
		n: name,
	}
}

func Component(name string) *ComponentBuilder {
	return &ComponentBuilder{
		n: name,
	}
}

type NilType struct{}

type AppBuilder struct {
	n string
	f []*flowBuilder
	c []*ComponentBuilder
	l *slog.Logger
}

func (a *AppBuilder) WithFlow[Tinit any](flow *FlowBuilder[Tinit, NilType]) *AppBuilder {
	a.f = append(a.f, (*flowBuilder)(flow))
	return a
}

func (a *AppBuilder) WithComponents(componentBuilders ...*ComponentBuilder) *AppBuilder {
	a.c = componentBuilders
	return a
}

func (a *AppBuilder) WithLogger(logger *slog.Logger) *AppBuilder {
	a.l = logger
	return a
}

func (a *AppBuilder) Build() *BuiltApp {
	builtCouplings := make(map[string][]*HandlerCoupling)
	handlerMap := make(map[string]*HandlerCoupling)
	inputMap := make(map[string][]*HandlerCoupling)
	inProg := util.NewSet[string]()

	if a.l == nil {
		a.l = slog.Default()
	}

	// Build all the couplings between input pipes, output pipes and handlers for all flows
	for _, v := range a.f {
		couplings := v.buildCouplings(a.l, &inputMap, inProg)
		builtCouplings[v.id] = couplings

		for _, c := range couplings {
			handlerMap[c.handler.name] = c
		}
	}

	// Build components where multiple handlers have been grouped together
	components := make([]*component, 0)
	for _, v := range a.c {
		couplings := make([]*HandlerCoupling, 0, len(v.h))
		for _, c := range v.h {
			coupling, ok := handlerMap[c]
			if !ok {
				err := fmt.Sprintf("Unable to find handler %v for component %v", c, v.n)
				panic(err)
			}
			couplings = append(couplings, coupling)
			delete(handlerMap, c)
		}

		component := NewComponent(context.TODO(), a.l, couplings, v.maxC) // TODO add actual context
		components = append(components, component)
	}

	// Build a single component per remaining handler
	for _, v := range handlerMap {
		component := NewComponent(context.TODO(), a.l, []*HandlerCoupling{v}, 1) // TODO review default concurrency
		components = append(components, component)
	}

	// Build the flow objects
	flows := make(map[string]*flow)
	for _, v := range a.f {
		inputPipe := builtCouplings[v.id][0].inputPipe
		flow := &flow{
			n: v.n,
			t: v.inT,
			p: inputPipe,
		}
		flows[v.n] = flow
	}

	return &BuiltApp{
		name:       a.n,
		flows:      flows,
		components: components,
	}
}

type flowBuilder struct {
	id  string
	n   string
	inT reflect.Type
	h   []*completedHandlerBuilder
	fb  *flowBuilder
}

type FlowBuilder[Tinit, Tnext any] flowBuilder

func (fb *FlowBuilder[Tinit, Tnext]) WithHandler[Tout any](hb *CompletedHandlerBuilder[Tnext, Tout]) *FlowBuilder[Tinit, Tout] {
	h := completedHandlerBuilder(*hb)
	fb.h = append(fb.h, &h)
	return fb.toNextBuilder[Tout]()
}

func (fb *FlowBuilder[Tinit, Tnext]) WithFlow(endingFb *FlowBuilder[Tnext, NilType]) *FlowBuilder[Tinit, NilType] {
	fb.fb = (*flowBuilder)(endingFb)
	return fb.toNextBuilder[NilType]()
}

func (fb *FlowBuilder[Tinit, Tnext]) toNextBuilder[Tnextnext any]() *FlowBuilder[Tinit, Tnextnext] {
	return &FlowBuilder[Tinit, Tnextnext]{
		id:  fb.id,
		n:   fb.n,
		inT: fb.inT,
		h:   fb.h,
		fb:  fb.fb,
	}
}

// Only builds the association between input pipes, handlers and output pipes - building components is done further up the stack
func (fb *flowBuilder) buildCouplings(logger *slog.Logger, flowMap *map[string][]*HandlerCoupling, inProg *util.Set[string]) []*HandlerCoupling {
	if f, ok := (*flowMap)[fb.id]; ok {
		// The flow's already been built, just return it
		return f
	}

	// Add this flow to the set of flows currently being built
	if !inProg.Add(fb.id) {
		// Circular reference
		err := fmt.Sprintf("Unable to build flow %v - circular reference", fb.n)
		panic(err)
	}

	couplings := make([]*HandlerCoupling, 0)

	// Create all the input pipes and associate them with their handlers
	for _, v := range fb.h {
		var errPipe *Pipe
		if v.eFb != nil {
			errPipe = v.eFb.buildCouplings(logger, flowMap, inProg)[0].inputPipe
		}
		retryPolicy := NewRetryPolicy(errPipe, v.maxA, v.retryPeriod)

		coupling := &HandlerCoupling{
			handler:     v.build(),
			inputPipe:   NewPipe(fmt.Sprintf("%v_input", fb.n), v.bp, logger, fb.n, []string{}), // The following flow names are added later
			retryPolicy: retryPolicy,
		}
		couplings = append(couplings, coupling)
	}

	// If the flow is completed by another flow, build that flow as well
	var nextFlowPipe *Pipe
	if fb.fb != nil {
		nextFlowPipe = fb.fb.buildCouplings(logger, flowMap, inProg)[0].inputPipe
	}

	// Associate input pipes as the previous handler's output
	for i, v := range couplings {
		v.outputPipes = make(map[string]*Pipe)
		if i == len(fb.h)-1 { // Final coupling
			if nextFlowPipe != nil {
				// Use the input from the following flow, if this flow is completed by another one
				v.outputPipes[nextFlowPipe.flowName] = nextFlowPipe
				v.inputPipe.nextFlows = append(v.inputPipe.nextFlows, nextFlowPipe.flowName)
			}
		} else {
			nextInput := couplings[i+1].inputPipe
			v.outputPipes[nextInput.flowName] = nextInput
			v.inputPipe.nextFlows = append(v.inputPipe.nextFlows, nextInput.flowName)
		}
	}

	(*flowMap)[fb.id] = couplings
	inProg.Remove(fb.id)
	return couplings
}

type completedHandlerBuilder struct {
	n           string
	bp          *BatchPolicy
	maxA        int
	retryPeriod time.Duration
	eFb         *flowBuilder
	build       func() *handler
}

type CompletedHandlerBuilder[Tin, Tout any] completedHandlerBuilder

type HandlerBuilder[Tin, Tout any] struct {
	n           string
	maxA        int
	retryPeriod time.Duration
	eFb         *flowBuilder
}

func (h *HandlerBuilder[Tin, Tout]) WithBatching(batchSize int, period time.Duration) *BatchHandlerBuilder[Tin, Tout] {
	return &BatchHandlerBuilder[Tin, Tout]{
		n:           h.n,
		maxA:        h.maxA,
		retryPeriod: h.retryPeriod,
		eFb:         h.eFb,
		bs:          batchSize,
		bp:          period,
	}
}

func (h *HandlerBuilder[Tin, Tout]) WithMaxAttempts(maxAttempts int) *HandlerBuilder[Tin, Tout] {
	h.maxA = maxAttempts
	return h
}

func (h *HandlerBuilder[Tin, Tout]) WithRetryPeriod(period time.Duration) *HandlerBuilder[Tin, Tout] {
	h.retryPeriod = period
	return h
}

func (h *HandlerBuilder[Tin, Tout]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *HandlerBuilder[Tin, Tout] {
	efb := Flow[Tin](h.n + "_Error").
		WithHandler(
			errHandler,
		)
	h.eFb = (*flowBuilder)(efb)
	return h
}

func (h *HandlerBuilder[Tin, Tout]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *HandlerBuilder[Tin, Tout] {
	h.eFb = (*flowBuilder)(errFlow)
	return h
}

func (h *HandlerBuilder[Tin, Tout]) WithHandleFunc(f func(context.Context, HandlerContext[Tin, Tout], *Message[Tin]) *HandlerResult[Tout]) *CompletedHandlerBuilder[Tin, Tout] {
	return &CompletedHandlerBuilder[Tin, Tout]{
		n:           h.n,
		bp:          nil,
		maxA:        h.maxA,
		retryPeriod: h.retryPeriod,
		eFb:         h.eFb,
		build: func() *handler {
			return newHandler(h.n, f)
		},
	}
}

type BatchHandlerBuilder[Tin, Tout any] struct {
	n           string
	bs          int
	bp          time.Duration
	maxA        int
	retryPeriod time.Duration
	eFb         *flowBuilder
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithMaxAttempts(maxAttempts int) *BatchHandlerBuilder[Tin, Tout] {
	h.maxA = maxAttempts
	return h
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithRetryPeriod(period time.Duration) *BatchHandlerBuilder[Tin, Tout] {
	h.retryPeriod = period
	return h
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *BatchHandlerBuilder[Tin, Tout] {
	efb := Flow[Tin](h.n + "_Error").
		WithHandler(
			errHandler,
		)
	h.eFb = (*flowBuilder)(efb)
	return h
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *BatchHandlerBuilder[Tin, Tout] {
	h.eFb = (*flowBuilder)(errFlow)
	return h
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithHandleFunc(f func(context.Context, HandlerContext[Tin, Tout], []*Message[Tin]) []*HandlerResult[Tout]) *CompletedHandlerBuilder[Tin, Tout] {
	return &CompletedHandlerBuilder[Tin, Tout]{
		n:           h.n,
		bp:          NewBatchPolicy(h.bs, h.bp),
		maxA:        h.maxA,
		retryPeriod: h.retryPeriod,
		eFb:         h.eFb,
		build: func() *handler {
			return newBatchHandler(h.n, f)
		},
	}
}

type EndHandlerBuilder[Tin any] struct {
	n           string
	f           func(context.Context, Tin) error
	maxA        int
	retryPeriod time.Duration
	eFb         *flowBuilder
}

func (h *EndHandlerBuilder[Tin]) WithBatching(batchSize int, period time.Duration) *BatchEndHandlerBuilder[Tin] {
	return &BatchEndHandlerBuilder[Tin]{
		n:           h.n,
		maxA:        h.maxA,
		retryPeriod: h.retryPeriod,
		bs:          batchSize,
		bp:          period,
	}
}

func (h *EndHandlerBuilder[Tin]) WithMaxAttempts(maxAttempts int) *EndHandlerBuilder[Tin] {
	h.maxA = maxAttempts
	return h
}

func (h *EndHandlerBuilder[Tin]) WithRetryPeriod(period time.Duration) *EndHandlerBuilder[Tin] {
	h.retryPeriod = period
	return h
}

func (h *EndHandlerBuilder[Tin]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *EndHandlerBuilder[Tin] {
	efb := Flow[Tin](h.n + "_Error").
		WithHandler(
			errHandler,
		)
	h.eFb = (*flowBuilder)(efb)
	return h
}

func (h *EndHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *EndHandlerBuilder[Tin] {
	h.eFb = (*flowBuilder)(errFlow)
	return h
}

func (h *EndHandlerBuilder[Tin]) WithHandleFunc(f func(context.Context, EndHandlerContext[Tin], *Message[Tin]) *EndHandlerResult) *CompletedHandlerBuilder[Tin, NilType] {
	return &CompletedHandlerBuilder[Tin, NilType]{
		n:           h.n,
		bp:          nil,
		maxA:        h.maxA,
		retryPeriod: h.retryPeriod,
		eFb:         h.eFb,
		build: func() *handler {
			return newEndHandler(h.n, f)
		},
	}
}

type BatchEndHandlerBuilder[Tin any] struct {
	n           string
	bs          int
	bp          time.Duration
	maxA        int
	retryPeriod time.Duration
	eFb         *flowBuilder
}

func (h *BatchEndHandlerBuilder[Tin]) WithMaxAttempts(maxAttempts int) *BatchEndHandlerBuilder[Tin] {
	h.maxA = maxAttempts
	return h
}

func (h *BatchEndHandlerBuilder[Tin]) WithRetryPeriod(period time.Duration) *BatchEndHandlerBuilder[Tin] {
	h.retryPeriod = period
	return h
}

func (h *BatchEndHandlerBuilder[Tin]) WithErrorHandler(errHandler *CompletedHandlerBuilder[Tin, NilType]) *BatchEndHandlerBuilder[Tin] {
	efb := Flow[Tin](h.n + "_Error").
		WithHandler(
			errHandler,
		)
	h.eFb = (*flowBuilder)(efb)
	return h
}

func (h *BatchEndHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[Tin, NilType]) *BatchEndHandlerBuilder[Tin] {
	h.eFb = (*flowBuilder)(errFlow)
	return h
}

func (h *BatchEndHandlerBuilder[Tin]) WithHandleFunc(f func(context.Context, EndHandlerContext[Tin], []*Message[Tin]) []*EndHandlerResult) *CompletedHandlerBuilder[Tin, NilType] {
	return &CompletedHandlerBuilder[Tin, NilType]{
		n:           h.n,
		bp:          NewBatchPolicy(h.bs, h.bp),
		maxA:        h.maxA,
		retryPeriod: h.retryPeriod,
		eFb:         h.eFb,
		build: func() *handler {
			return newBatchEndHandler(h.n, f)
		},
	}
}

type ComponentBuilder struct {
	n    string
	h    []string
	maxC int
}

func (c *ComponentBuilder) WithHandlers(handlerNames ...string) *ComponentBuilder {
	c.h = handlerNames
	return c
}

func (c *ComponentBuilder) WithConcurrency(maxConcurrency int) *ComponentBuilder {
	c.maxC = maxConcurrency
	return c
}

type BuiltApp struct {
	name       string
	flows      map[string]*flow
	components []*component
}

func (a *BuiltApp) GetFlow[T any](name string) (*BuiltFlow[T], error) {
	if f, ok := a.flows[name]; !ok {
		return nil, errors.New("Flow doesn't exist")
	} else if f.t != reflect.TypeFor[T]() {
		return nil, errors.New("Flow doesn't have requested type")
	} else {
		return &BuiltFlow[T]{
			f: f,
		}, nil
	}
}

type flow struct {
	t reflect.Type
	n string
	p *Pipe
}

func (f *flow) trigger(ctx context.Context, data ...any) error {
	return f.p.Send(ctx, data...)
}

type BuiltFlow[T any] struct {
	f *flow
}

func (f *BuiltFlow[T]) Trigger(ctx context.Context, data ...T) error {
	conv := make([]any, len(data))
	for i, v := range data {
		conv[i] = v
	}
	return f.f.trigger(ctx, conv...)
}
