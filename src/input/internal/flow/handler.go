package flow

import (
	"uuid"
)

type Handler struct {
	id         string
	handleFunc func([]any) ([]any, error)
}

func NewHandler[Tin any, Tout any](handleFunc func([]Tin) ([]Tout, error)) *Handler {
	f := func(in []any) ([]any, error) {
		inConv := make([]Tin, len(in))
		for i, v := range in {
			inConv[i] = v.(Tin)
		}

		out, err := handleFunc(inConv)
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

func (h *Handler) Handle(in []any) ([]any, error) {
	return h.handleFunc(in)
}
