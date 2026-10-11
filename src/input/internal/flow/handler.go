package flow

import (
	"uuid"
)

type Handler struct {
	id         string
	handleFunc func(any) (any, error)
}

func NewHandler[Tin any, Tout any](handleFunc func(Tin) (Tout, error)) *Handler {
	f := func(in any) (any, error) {
		return handleFunc(in.(Tin))
	}
	return &Handler{
		id:         uuid.New().String(),
		handleFunc: f,
	}
}

func (h *Handler) Handle(in any) (any, error) {
	return h.handleFunc(in)
}
