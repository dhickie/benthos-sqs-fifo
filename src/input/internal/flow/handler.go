//go:generate go run generate.go

package flow

import (
	"context"
)

type Handler struct {
	id         string
	handleFunc func(context.Context, []any) ([][]InternalHandleResult, error)
}

type HandleResult[Tin any] struct {
	input Tin
	err   error
}

type ValueHandleResult[Tin any, Tout any] struct {
	HandleResult[Tin]
	ok     bool
	output Tout
}

type InternalHandleResult struct {
	input  any
	ok     bool
	err    error
	output any
}

func (h *Handler) Handle(ctx context.Context, in []any) ([][]InternalHandleResult, error) {
	return h.handleFunc(ctx, in)
}
