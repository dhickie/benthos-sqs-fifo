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
		f: make([]*CompletedFlowBuilder, 0),
	}
}

func Flow[Tinit any](name string) *FlowBuilder[Tinit] {
	return &FlowBuilder[Tinit]{
		inT: reflect.TypeFor[Tinit](),
		n:   name,
		h:   make([]handlerInfo, 0),
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
	h   []handlerInfo
}

type handlerInfo struct {
	bp *BatchPolicy
	h  *handler
}

type CompletedFlowBuilder FlowBuilder[any]

func (fb *FlowBuilder[Tprev]) WithHandler[Tout any](hb *CompletedHandlerBuilder[Tprev, Tout]) *FlowBuilder[Tout] {
	h := hb.build()
	hi := handlerInfo{
		bp: hb.bp,
		h:  h,
	}

	fb.h = append(fb.h, hi)
	return fb.toNextBuilder[Tout]()
}

func (fb *FlowBuilder[Tprev]) WithEndHandler(hb *CompletedEndHandlerBuilder[Tprev]) *CompletedFlowBuilder {
	h := hb.build()
	hi := handlerInfo{
		bp: hb.bp,
		h:  h,
	}

	fb.h = append(fb.h, hi)
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
	couplings := make([]*HandlerCoupling, 0)
	components := make([]*Component, 0)

	// Create all the input pipes and associate them with their handlers
	for _, v := range fb.h {
		coupling := &HandlerCoupling{
			handler:   v.h,
			inputPipe: NewPipe(fmt.Sprintf("%v_input", fb.n), v.bp, nil),
		}
		couplings = append(couplings, coupling)
	}

	// Associate input pipes as the previous handler's output & create a component (TODO one handler per component for now)
	for i, v := range couplings {
		if i == len(fb.h)-1 {
			v.outputPipe = make([]*Pipe, 0)
		} else {
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
	n     string
	build func() *handler
	bp    *BatchPolicy
}

type CompletedHandlerBuilder[Tin, Tout any] completedHandlerBuilder
type CompletedEndHandlerBuilder[Tin any] completedHandlerBuilder

type HandlerBuilder[Tin, Tout any] struct {
	n string
	f func(context.Context, Tin) (*ValueHandleResult[Tout], error)
}

func (h *HandlerBuilder[Tin, Tout]) WithBatching(batchSize int, period time.Duration) *BatchHandlerBuilder[Tin, Tout] {
	return &BatchHandlerBuilder[Tin, Tout]{
		n:  h.n,
		bs: batchSize,
		bp: period,
	}
}

func (h *HandlerBuilder[Tin, Tout]) WithHandleFunc(f func(context.Context, Tin) (*ValueHandleResult[Tout], error)) *CompletedHandlerBuilder[Tin, Tout] {
	return &CompletedHandlerBuilder[Tin, Tout]{
		n:  h.n,
		bp: nil,
		build: func() *handler {
			return NewHandler_1(f)
		},
	}
}

type BatchHandlerBuilder[Tin, Tout any] struct {
	n  string
	f  func(context.Context, []Tin) (*ValueBatchHandleResult[Tin, Tout], error)
	bs int
	bp time.Duration
}

func (h *BatchHandlerBuilder[Tin, Tout]) WithHandleFunc(f func(context.Context, []Tin) (*ValueBatchHandleResult[Tin, Tout], error)) *CompletedHandlerBuilder[Tin, Tout] {
	return &CompletedHandlerBuilder[Tin, Tout]{
		n:  h.n,
		bp: NewBatchPolicy(h.bs, h.bp),
		build: func() *handler {
			return NewBatchHandler_1(f)
		},
	}
}

type EndHandlerBuilder[Tin any] struct {
	n string
	f func(context.Context, Tin) error
}

func (h *EndHandlerBuilder[Tin]) WithBatching(batchSize int, period time.Duration) *BatchEndHandlerBuilder[Tin] {
	return &BatchEndHandlerBuilder[Tin]{
		n:  h.n,
		bs: batchSize,
		bp: period,
	}
}

func (h *EndHandlerBuilder[Tin]) WithHandleFunc(f func(context.Context, Tin) error) *CompletedEndHandlerBuilder[Tin] {
	return &CompletedEndHandlerBuilder[Tin]{
		n:  h.n,
		bp: nil,
		build: func() *handler {
			return NewHandler_0(f)
		},
	}
}

type BatchEndHandlerBuilder[Tin any] struct {
	n  string
	f  func(context.Context, []Tin) (*BatchHandleResult[Tin], error)
	bs int
	bp time.Duration
}

func (h *BatchEndHandlerBuilder[Tin]) WithHandleFunc(f func(context.Context, []Tin) (*BatchHandleResult[Tin], error)) *CompletedEndHandlerBuilder[Tin] {
	return &CompletedEndHandlerBuilder[Tin]{
		n:  h.n,
		bp: NewBatchPolicy(h.bs, h.bp),
		build: func() *handler {
			return NewBatchHandler_0(f)
		},
	}
}

func (h *BatchEndHandlerBuilder[Tin]) buildHandler() *handler {
	return NewBatchHandler_0(h.f)
}

func (h *BatchEndHandlerBuilder[Tin]) buildBatchPolicy() *BatchPolicy {
	return NewBatchPolicy(h.bs, h.bp)
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
