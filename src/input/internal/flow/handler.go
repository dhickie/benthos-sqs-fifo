//go:generate go run generate.go

package flow

import (
	"context"
)

type handler struct {
	id         string
	handleFunc func(context.Context, []any) ([][]InternalHandleResult, error)
}

type ValueHandleResult[Tout any] struct {
	output Tout
	ok     bool
}

func NewValueHandleResult[Tout any](result Tout, ok bool) *ValueHandleResult[Tout] {
	return &ValueHandleResult[Tout]{
		output: result,
		ok:     ok,
	}
}

type BatchHandleResult[Tin any] struct {
	failures []*BatchFailure[Tin]
}

func NewBatchHandleResult[Tin any](failures []*BatchFailure[Tin]) *BatchHandleResult[Tin] {
	var f []*BatchFailure[Tin]
	if failures == nil {
		f = make([]*BatchFailure[Tin], 0)
	} else {
		f = failures
	}

	return &BatchHandleResult[Tin]{
		failures: f,
	}
}

type ValueBatchHandleResult[Tin any, Tout any] struct {
	results []Tout
	BatchHandleResult[Tin]
}

func NewValueBatchHandleResult[Tin any, Tout any](results []Tout, failures []*BatchFailure[Tin]) *ValueBatchHandleResult[Tin, Tout] {
	var r []Tout
	var f []*BatchFailure[Tin]
	if results == nil {
		r = make([]Tout, 0)
	} else {
		r = results
	}
	if failures == nil {
		f = make([]*BatchFailure[Tin], 0)
	} else {
		f = failures
	}

	return &ValueBatchHandleResult[Tin, Tout]{
		results:  r,
		failures: f,
	}
}

type BatchFailure[Tin any] struct {
	input Tin
	err   error
}

func NewBatchFailure[Tin any](input Tin, err error) *BatchFailure[Tin] {
	return &BatchFailure[Tin]{
		input: input,
		err:   err,
	}
}

type InternalHandleResult struct {
	input  any
	output any
	ok     bool
	err    error
}

func (h *handler) Handle(ctx context.Context, in []any) ([][]InternalHandleResult, error) {
	return h.handleFunc(ctx, in)
}
