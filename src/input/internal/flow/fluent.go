package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"errors"
	"fmt"
	"reflect"
	"time"
	"uuid"
)

// TODO - review all default values
// TODO - add validation when building
// TODO - replace public facing types with interfaces
// TODO - all the comments

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

type NilType struct{}

type AppBuilder struct {
	n string
	f []*flowBuilder
}

func (a *AppBuilder) WithFlow[Tinit any](flow *FlowBuilder[Tinit, NilType]) *AppBuilder {
	a.f = append(a.f, (*flowBuilder)(flow))
	return a
}

func (a *AppBuilder) Build() *BuiltApp {
	builtFlows := make(map[string]*flow)
	inputMap := make(map[string]*flow)
	inProg := util.NewSet[string]()
	for _, v := range a.f {
		built := v.build(&inputMap, inProg)
		builtFlows[built.n] = built
	}

	return &BuiltApp{
		name:  a.n,
		flows: builtFlows,
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

func (fb *flowBuilder) build(flowMap *map[string]*flow, inProg *util.Set[string]) *flow {
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
	components := make([]*Component, 0)

	// Create all the input pipes and associate them with their handlers
	for _, v := range fb.h {
		var errPipe *Pipe
		if v.eFb != nil {
			errPipe = v.eFb.build(flowMap, inProg).p
		}
		retryPolicy := NewRetryPolicy(errPipe, v.maxA, time.Second) // TODO replace with actual time period

		coupling := &HandlerCoupling{
			handler:     v.build(),
			inputPipe:   NewPipe(fmt.Sprintf("%v_input", fb.n), v.bp, nil),
			retryPolicy: retryPolicy,
		}
		couplings = append(couplings, coupling)
	}

	// If the flow is completed by another flow, build that flow as well
	var nextFlowPipe *Pipe
	if fb.fb != nil {
		nextFlowPipe = fb.fb.build(flowMap, inProg).p
	}

	// Associate input pipes as the previous handler's output & create a component (TODO one handler per component for now)
	for i, v := range couplings {
		if i == len(fb.h)-1 { // Final coupling
			if nextFlowPipe != nil {
				// Use the input from the following flow, if this flow is completed by another one
				v.outputPipe = []*Pipe{nextFlowPipe}
			} else {
				v.outputPipe = make([]*Pipe, 0)
			}
		} else {
			v.outputPipe = []*Pipe{
				couplings[i+1].inputPipe,
			}
		}

		components = append(components, NewComponent(context.TODO(), []*HandlerCoupling{v}, 1)) // TODO - review default concurrency, add context
	}

	inProg.Remove(fb.id)
	return &flow{
		n: fb.n,
		t: fb.inT,
		p: couplings[0].inputPipe,
		c: components,
	}
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
	f           func(context.Context, Tin) (*ValueHandleResult[Tout], error)
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

func (h *HandlerBuilder[Tin, Tout]) WithHandleFunc(f func(context.Context, Tin) (*ValueHandleResult[Tout], error)) *CompletedHandlerBuilder[Tin, Tout] {
	return &CompletedHandlerBuilder[Tin, Tout]{
		n:           h.n,
		bp:          nil,
		maxA:        h.maxA,
		retryPeriod: h.retryPeriod,
		eFb:         h.eFb,
		build: func() *handler {
			return NewHandler_1(f)
		},
	}
}

type BatchHandlerBuilder[Tin, Tout any] struct {
	n           string
	f           func(context.Context, []Tin) (*ValueBatchHandleResult[Tin, Tout], error)
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

func (h *BatchHandlerBuilder[Tin, Tout]) WithHandleFunc(f func(context.Context, []Tin) (*ValueBatchHandleResult[Tin, Tout], error)) *CompletedHandlerBuilder[Tin, Tout] {
	return &CompletedHandlerBuilder[Tin, Tout]{
		n:           h.n,
		bp:          NewBatchPolicy(h.bs, h.bp),
		maxA:        h.maxA,
		retryPeriod: h.retryPeriod,
		eFb:         h.eFb,
		build: func() *handler {
			return NewBatchHandler_1(f)
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

func (h *EndHandlerBuilder[Tin]) WithHandleFunc(f func(context.Context, Tin) error) *CompletedHandlerBuilder[Tin, NilType] {
	return &CompletedHandlerBuilder[Tin, NilType]{
		n:           h.n,
		bp:          nil,
		maxA:        h.maxA,
		retryPeriod: h.retryPeriod,
		eFb:         h.eFb,
		build: func() *handler {
			return NewHandler_0(f)
		},
	}
}

type BatchEndHandlerBuilder[Tin any] struct {
	n           string
	f           func(context.Context, []Tin) (*BatchHandleResult[Tin], error)
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

func (h *BatchEndHandlerBuilder[Tin]) WithHandleFunc(f func(context.Context, []Tin) (*BatchHandleResult[Tin], error)) *CompletedHandlerBuilder[Tin, NilType] {
	return &CompletedHandlerBuilder[Tin, NilType]{
		n:           h.n,
		bp:          NewBatchPolicy(h.bs, h.bp),
		maxA:        h.maxA,
		retryPeriod: h.retryPeriod,
		eFb:         h.eFb,
		build: func() *handler {
			return NewBatchHandler_0(f)
		},
	}
}

type BuiltApp struct {
	name  string
	flows map[string]*flow
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
	c []*Component
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
