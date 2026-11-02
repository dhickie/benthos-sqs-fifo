package flow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

func App(name string) *AppBuilder {
	return &AppBuilder{
		n: name,
		f: make([]*FlowBuilder[NilType], 0),
	}
}

func Flow[Tinit any](name string) *FlowBuilder[Tinit] {
	return &FlowBuilder[Tinit]{
		inT: reflect.TypeFor[Tinit](),
		n:   name,
		h:   make([]*completedHandlerBuilder, 0),
	}
}

func Handler[Tin, Tout any](name string) *HandlerBuilder[Tin, Tout] {
	return &HandlerBuilder[Tin, Tout]{
		n: name,
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
	f []*FlowBuilder[NilType]
}

func (a *AppBuilder) WithFlows(flows ...*FlowBuilder[NilType]) *AppBuilder {
	a.f = flows
	return a
}

func (a *AppBuilder) Build() *BuiltApp {
	builtFlows := make(map[string]*flow)
	inputMap := make(map[string]*Pipe)
	for _, v := range a.f {
		built := v.build(&inputMap)
		builtFlows[built.n] = built
	}

	return &BuiltApp{
		name:  a.n,
		flows: builtFlows,
	}
}

type FlowBuilder[Tprev any] struct {
	inT reflect.Type
	n   string
	h   []*completedHandlerBuilder
}

func (fb *FlowBuilder[Tprev]) WithHandler[Tout any](hb *CompletedHandlerBuilder[Tprev, Tout]) *FlowBuilder[Tout] {
	h := completedHandlerBuilder(*hb)
	fb.h = append(fb.h, &h)
	return fb.toNextBuilder[Tout]()
}

func (fb *FlowBuilder[Tprev]) toNextBuilder[Tnext any]() *FlowBuilder[Tnext] {
	return &FlowBuilder[Tnext]{
		inT: fb.inT,
		n:   fb.n,
		h:   fb.h,
	}
}

func (fb *FlowBuilder[NilType]) build(hbMap *map[string]*Pipe) *flow {
	couplings := make([]*HandlerCoupling, 0)
	components := make([]*Component, 0)

	// Create all the input pipes and associate them with their handlers
	var termPipe *Pipe // The pipe after which handlers in this flow already exist in other components
	var ok bool
	for _, v := range fb.h {
		if termPipe, ok = (*hbMap)[v.n]; ok {
			// Handler has already been built - by definition it must be part of a component already
			// Break from this loop and build the components for handlers that haven't been built yet
			break
		}

		inputPipe := NewPipe(fmt.Sprintf("%v_input", fb.n), v.bp, nil)
		(*hbMap)[v.n] = inputPipe
		var errPipe *Pipe
		if v.eFb != nil {
			errPipe = v.eFb.build(hbMap).p
		}
		retryPolicy := NewRetryPolicy(errPipe, v.maxA, time.Second) // TODO replace with actual time period

		coupling := &HandlerCoupling{
			handler:     v.build(),
			inputPipe:   NewPipe(fmt.Sprintf("%v_input", fb.n), v.bp, nil),
			retryPolicy: retryPolicy,
		}
		couplings = append(couplings, coupling)
	}

	// Associate input pipes as the previous handler's output & create a component (TODO one handler per component for now)
	for i, v := range couplings {
		if i == len(fb.h)-1 && termPipe == nil { // This is the final coupling but there are more handlers built for a different flow
			v.outputPipe = make([]*Pipe, 0)
		} else if i == len(fb.h)-1 { // This is the final coupling and there are no further handlers in the flow
			v.outputPipe = []*Pipe{termPipe}
		} else { // This isn't the final coupling in the flow
			v.outputPipe = []*Pipe{
				couplings[i+1].inputPipe,
			}
		}

		components = append(components, NewComponent(context.TODO(), []*HandlerCoupling{v}, 1)) // TODO - review default concurrency
	}

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
	eFb         *FlowBuilder[NilType]
	build       func() *handler
}

type CompletedHandlerBuilder[Tin, Tout any] completedHandlerBuilder

type HandlerBuilder[Tin, Tout any] struct {
	n           string
	f           func(context.Context, Tin) (*ValueHandleResult[Tout], error)
	maxA        int
	retryPeriod time.Duration
	eFb         *FlowBuilder[NilType]
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
	h.eFb = Flow[Tin](h.n + "_Error").
		WithHandler(
			errHandler,
		)
	return h
}

func (h *HandlerBuilder[Tin, Tout]) WithErrorFlow(errFlow *FlowBuilder[NilType]) *HandlerBuilder[Tin, Tout] {
	h.eFb = errFlow
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
	eFb         *FlowBuilder[NilType]
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
	h.eFb = Flow[Tin](h.n + "_Error").
		WithHandler(
			errHandler,
		)
	return h
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithErrorFlow(errFlow *FlowBuilder[NilType]) *BatchHandlerBuilder[Tin, Tout] {
	h.eFb = errFlow
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
	eFb         *FlowBuilder[NilType]
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
	h.eFb = Flow[Tin](h.n + "_Error").
		WithHandler(
			errHandler,
		)
	return h
}

func (h *EndHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[NilType]) *EndHandlerBuilder[Tin] {
	h.eFb = errFlow
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
	eFb         *FlowBuilder[NilType]
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
	h.eFb = Flow[Tin](h.n + "_Error").
		WithHandler(
			errHandler,
		)
	return h
}

func (h *BatchEndHandlerBuilder[Tin]) WithErrorFlow(errFlow *FlowBuilder[NilType]) *BatchEndHandlerBuilder[Tin] {
	h.eFb = errFlow
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
