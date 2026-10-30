package flow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

func App(name string) *AppBuilder {
	return &AppBuilder{
		n: name,
		f: make([]*CompletedFlowBuilder, 0),
	}
}

func Flow[Tinit any](name string) *FlowBuilder[Tinit] {
	return &FlowBuilder[Tinit]{
		inT: reflect.TypeFor[Tinit](),
		n:   name,
		h:   make([]*handler, 0),
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

type AppBuilder struct {
	n string
	f []*CompletedFlowBuilder
}

func (a *AppBuilder) WithFlows(flows ...*CompletedFlowBuilder) *AppBuilder {
	a.f = flows
	return a
}

func (a *AppBuilder) Build() *BuiltApp {
	builtFlows := make(map[string]*flow)
	for _, v := range a.f {
		built := v.build()
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
	h   []*handler
}

type CompletedFlowBuilder FlowBuilder[any]

func (fb *FlowBuilder[Tprev]) WithHandler[Tout any](hb *HandlerBuilder[Tprev, Tout]) *FlowBuilder[Tout] {
	fb.h = append(fb.h, hb.build())
	return fb.toNextBuilder[Tout]()
}

func (fb *FlowBuilder[Tprev]) WithEndHandler(hb *EndHandlerBuilder[Tprev]) *CompletedFlowBuilder {
	fb.h = append(fb.h, hb.build())
	return fb.toCompletedBuilder()
}

func (fb *FlowBuilder[Tprev]) toNextBuilder[Tnext any]() *FlowBuilder[Tnext] {
	return &FlowBuilder[Tnext]{
		inT: fb.inT,
		n:   fb.n,
		h:   fb.h,
	}
}

func (fb *FlowBuilder[Tprev]) toCompletedBuilder() *CompletedFlowBuilder {
	return &CompletedFlowBuilder{
		inT: fb.inT,
		n:   fb.n,
		h:   fb.h,
	}
}

func (fb *CompletedFlowBuilder) build() *flow {
	components := make([]*Component, 0)                     // TODO one handler per component for now
	inP := NewPipe(fmt.Sprintf("%v_input", fb.n), nil, nil) // TODO add logger
	nextP := inP
	for i, v := range fb.h {
		if i == len(fb.h)-1 {
			// Last handler, dont need an output pipe
			coupling := NewHandlerCoupling(v, nextP, nil, nil)
			component := NewComponent(context.TODO(), []*HandlerCoupling{coupling}, 1) // TODO - review default concurrency
			components = append(components, component)
		} else {
			outP := NewPipe(v.id, nil, nil) // TODO sort out the name
			coupling := NewHandlerCoupling(v, nextP, []*Pipe{outP}, nil)
			component := NewComponent(context.TODO(), []*HandlerCoupling{coupling}, 1) // TODO - review default concurrency
			components = append(components, component)

			nextP = outP
		}
	}

	return &flow{
		n: fb.n,
		t: fb.inT,
		p: inP,
		c: components,
	}
}

type HandlerBuilder[Tin any, Tout any] struct {
	n string
	f func(context.Context, Tin) (*ValueHandleResult[Tout], error)
}

func (h *HandlerBuilder[Tin, Tout]) WithHandleFunc(f func(context.Context, Tin) (*ValueHandleResult[Tout], error)) *HandlerBuilder[Tin, Tout] {
	h.f = f
	return h
}

func (h *HandlerBuilder[Tin, Tout]) build() *handler {
	return NewHandler_1(h.f)
}

type EndHandlerBuilder[Tin any] struct {
	n string
	f func(context.Context, Tin) error
}

func (h *EndHandlerBuilder[Tin]) WithHandleFunc(f func(context.Context, Tin) error) *EndHandlerBuilder[Tin] {
	h.f = f
	return h
}

func (h *EndHandlerBuilder[Tin]) build() *handler {
	return NewHandler_0(h.f)
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
