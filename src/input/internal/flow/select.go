//go:generate go run generate.go

package flow

import "context"

type selectFunc[T any] func(context.Context, []*<-chan T, *T, *bool) (int, error)
