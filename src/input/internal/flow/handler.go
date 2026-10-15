package flow

import (
	"context"
	"uuid"
)

type Handler struct {
	id         string
	handleFunc func(context.Context, []any) ([]any, error)
}

type HandleFunc[Tin any, Tout any] func(context.Context, []Tin) ([]Tout, error)

func NewHandler[Tin any, Tout any](handleFunc HandleFunc[Tin, Tout]) *Handler {
	f := func(ctx context.Context, in []any) ([]any, error) {
		inConv := make([]Tin, len(in))
		for i, v := range in {
			inConv[i] = v.(Tin)
		}

		out, err := handleFunc(ctx, inConv)
		outConv := make([]any, len(out))
		for i, v := range out {
			outConv[i] = v
		}
		return outConv, err
	}
	return &Handler{
		id:         uuid.New().String(),
		handleFunc: f,
	}
}

func (h *Handler) Handle(ctx context.Context, in []any) ([]any, error) {
	return h.handleFunc(ctx, in)
}
