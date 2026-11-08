package flow

import "log/slog"

type Message[T any] struct {
	id    string
	Value T
}

type handlerResult struct {
	Id      string
	Error   error
	Results []ResultValue
}

type EndHandlerResult handlerResult
type HandlerResult[Tout any] handlerResult
type SplitHandlerResult handlerResult

type ResultValue struct {
	Value    any
	FlowName string
}

type EndHandlerContext[Tin any] struct {
	Logger      slog.Logger
	CurrentFlow string
}

func (c *EndHandlerContext[Tin]) Error(m *Message[Tin], err error) *EndHandlerResult {
	return &EndHandlerResult{
		Id:      m.id,
		Error:   err,
		Results: make([]ResultValue, 0),
	}
}

type HandlerContext[Tin, Tout any] struct {
	EndHandlerContext[Tin]
}

func (c *HandlerContext[Tin, Tout]) Success(m *Message[Tin], value Tout) *HandlerResult[Tout] {
	return &HandlerResult[Tout]{
		Id:    m.id,
		Error: nil,
		Results: []ResultValue{
			{Value: value, FlowName: c.CurrentFlow},
		},
	}
}

func (c *HandlerContext[Tin, Tout]) Error(m *Message[Tin], err error) *HandlerResult[Tout] {
	return &HandlerResult[Tout]{
		Id:    m.id,
		Error: err,
		Results: []ResultValue{
			{Value: nil, FlowName: c.CurrentFlow},
		},
	}
}

type SplitHandlerContext[Tin any] struct {
	EndHandlerContext[Tin]
	NextFlows []string
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
