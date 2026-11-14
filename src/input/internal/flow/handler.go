//go:generate go run generate.go

package flow

import (
	"context"
	"log/slog"
	"uuid"
)

type handler struct {
	id         string
	name       string
	handleFunc func(context.Context, handlerContext, []*message) []*handlerResult
}

func (h *handler) handle(ctx context.Context, hCtx handlerContext, msgs []*message) []*handlerResult {
	return h.handleFunc(ctx, hCtx, msgs)
}

type EndHandleFunc[Tin any] func(context.Context, EndHandlerContext[Tin], *Message[Tin]) *EndHandlerResult
type BatchEndHandleFunc[Tin any] func(context.Context, EndHandlerContext[Tin], []*Message[Tin]) []*EndHandlerResult
type HandleFunc[Tin, Tout any] func(context.Context, HandlerContext[Tin, Tout], *Message[Tin]) *HandlerResult[Tout]
type BatchHandleFunc[Tin, Tout any] func(context.Context, HandlerContext[Tin, Tout], []*Message[Tin]) []*HandlerResult[Tout]
type SplitHandleFunc[Tin any] func(context.Context, SplitHandlerContext[Tin], *Message[Tin]) *SplitHandlerResult
type BatchSplitHandleFunc[Tin any] func(context.Context, SplitHandlerContext[Tin], []*Message[Tin]) []*SplitHandlerResult

func newEndHandler[Tin any](name string, f func(context.Context, EndHandlerContext[Tin], *Message[Tin]) *EndHandlerResult) *handler {
	return &handler{
		id:   uuid.New().String(),
		name: name,
		handleFunc: func(ctx context.Context, hCtx handlerContext, msgs []*message) []*handlerResult {
			if result := f(ctx, (EndHandlerContext[Tin])(hCtx), (*Message[Tin])(msgs[0])); result == nil {
				return make([]*handlerResult, 0)
			} else {
				return []*handlerResult{
					(*handlerResult)(result),
				}
			}
		},
	}
}

func newBatchEndHandler[Tin any](name string, f func(context.Context, EndHandlerContext[Tin], []*Message[Tin]) []*EndHandlerResult) *handler {
	return &handler{
		id:   uuid.New().String(),
		name: name,
		handleFunc: func(ctx context.Context, hCtx handlerContext, msgs []*message) []*handlerResult {
			conv := make([]*Message[Tin], len(msgs))
			for i := range len(msgs) {
				conv[i] = (*Message[Tin])(msgs[i])
			}

			if result := f(ctx, (EndHandlerContext[Tin])(hCtx), conv); result != nil {
				resConv := make([]*handlerResult, len(result))
				for i := range len(result) {
					resConv[i] = (*handlerResult)(result[i])
				}
				return resConv
			} else {
				return make([]*handlerResult, 0)
			}
		},
	}
}

func newHandler[Tin, Tout any](name string, f func(context.Context, HandlerContext[Tin, Tout], *Message[Tin]) *HandlerResult[Tout]) *handler {
	return &handler{
		id:   uuid.New().String(),
		name: name,
		handleFunc: func(ctx context.Context, hCtx handlerContext, msgs []*message) []*handlerResult {
			if result := f(ctx, (HandlerContext[Tin, Tout])(hCtx), (*Message[Tin])(msgs[0])); result != nil {
				return []*handlerResult{
					(*handlerResult)(result),
				}
			} else {
				return make([]*handlerResult, 0)
			}
		},
	}
}

func newBatchHandler[Tin, Tout any](name string, f func(context.Context, HandlerContext[Tin, Tout], []*Message[Tin]) []*HandlerResult[Tout]) *handler {
	return &handler{
		id:   uuid.New().String(),
		name: name,
		handleFunc: func(ctx context.Context, hCtx handlerContext, msgs []*message) []*handlerResult {
			conv := make([]*Message[Tin], len(msgs))
			for i := range len(msgs) {
				conv[i] = (*Message[Tin])(msgs[i])
			}

			if result := f(ctx, (HandlerContext[Tin, Tout])(hCtx), conv); result != nil {
				resConv := make([]*handlerResult, len(result))
				for i := range len(result) {
					resConv[i] = (*handlerResult)(result[i])
				}
				return resConv
			} else {
				return make([]*handlerResult, 0)
			}
		},
	}
}

func newSplitHandler[Tin, Tout any](name string, f func(context.Context, SplitHandlerContext[Tin], *Message[Tin]) *SplitHandlerResult) *handler {
	return &handler{
		id:   uuid.New().String(),
		name: name,
		handleFunc: func(ctx context.Context, hCtx handlerContext, msgs []*message) []*handlerResult {
			if result := f(ctx, (SplitHandlerContext[Tin])(hCtx), (*Message[Tin])(msgs[0])); result != nil {
				return []*handlerResult{
					(*handlerResult)(result),
				}
			} else {
				return make([]*handlerResult, 0)
			}
		},
	}
}

func newBatchSplitHandler[Tin any](name string, f func(context.Context, SplitHandlerContext[Tin], []*Message[Tin]) []*SplitHandlerResult) *handler {
	return &handler{
		id:   uuid.New().String(),
		name: name,
		handleFunc: func(ctx context.Context, hCtx handlerContext, msgs []*message) []*handlerResult {
			conv := make([]*Message[Tin], len(msgs))
			for i := range len(msgs) {
				conv[i] = (*Message[Tin])(msgs[i])
			}
			if result := f(ctx, (SplitHandlerContext[Tin])(hCtx), conv); result != nil {
				resConv := make([]*handlerResult, len(result))
				for i := range len(result) {
					resConv[i] = (*handlerResult)(result[i])
				}
				return resConv
			} else {
				return make([]*handlerResult, 0)
			}
		},
	}
}

type message struct {
	id    string
	value any
}

type Message[T any] message

func (m *Message[T]) Value() T {
	return m.value.(T)
}

type ResultValue struct {
	Value    any
	FlowName string
}

type handlerResult struct {
	Id      string
	Error   error
	Results []ResultValue
}

type EndHandlerResult handlerResult
type HandlerResult[Tout any] handlerResult
type SplitHandlerResult handlerResult

type handlerContext struct {
	Logger *slog.Logger

	currentFlow string
	nextFlows   []string
}

type EndHandlerContext[Tin any] handlerContext

func (c *EndHandlerContext[Tin]) CurrentFlow() string {
	return c.currentFlow
}

func (c *EndHandlerContext[Tin]) Error(m *Message[Tin], err error) *EndHandlerResult {
	return &EndHandlerResult{
		Id:      m.id,
		Error:   err,
		Results: make([]ResultValue, 0),
	}
}

type HandlerContext[Tin, Tout any] handlerContext

func (c *HandlerContext[Tin, Tout]) CurrentFlow() string {
	return c.currentFlow
}

func (c *HandlerContext[Tin, Tout]) Success(m *Message[Tin], value Tout) *HandlerResult[Tout] {
	return &HandlerResult[Tout]{
		Id:    m.id,
		Error: nil,
		Results: []ResultValue{
			{Value: value, FlowName: c.nextFlows[0]},
		},
	}
}

func (c *HandlerContext[Tin, Tout]) Error(m *Message[Tin], err error) *HandlerResult[Tout] {
	return &HandlerResult[Tout]{
		Id:      m.id,
		Error:   err,
		Results: []ResultValue{},
	}
}

type SplitHandlerContext[Tin any] handlerContext

func (c *SplitHandlerContext[Tin]) CurrentFlow() string {
	return c.currentFlow
}

func (c *SplitHandlerContext[Tin]) NextFlows() []string {
	return c.nextFlows
}

func (c *SplitHandlerContext[Tin]) ResultValue[Tout any](value Tout, flowName string) ResultValue {
	return ResultValue{
		Value:    value,
		FlowName: flowName,
	}
}

func (c *SplitHandlerContext[Tin]) Success[Tout any](m *Message[Tin], value Tout, flowName string) *SplitHandlerResult {
	return &SplitHandlerResult{
		Id:    m.id,
		Error: nil,
		Results: []ResultValue{
			{Value: value, FlowName: flowName},
		},
	}
}

func (c *SplitHandlerContext[Tin]) FanOut[Tout any](m *Message[Tin], value Tout, flowNames []string) *SplitHandlerResult {
	result := &SplitHandlerResult{
		Id:      m.id,
		Error:   nil,
		Results: make([]ResultValue, len(flowNames)),
	}

	for i, v := range flowNames {
		result.Results[i] = ResultValue{Value: value, FlowName: v}
	}

	return result
}

func (c *SplitHandlerContext[Tin]) Split[Tout any](m *Message[Tin], values []ResultValue) *SplitHandlerResult {
	return &SplitHandlerResult{
		Id:      m.id,
		Error:   nil,
		Results: values,
	}
}

func (c *SplitHandlerContext[Tin]) Error(m *Message[Tin], err error) *SplitHandlerResult {
	return &SplitHandlerResult{
		Id:      m.id,
		Error:   err,
		Results: make([]ResultValue, 0),
	}
}
