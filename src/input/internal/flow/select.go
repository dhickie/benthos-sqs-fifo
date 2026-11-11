//go:generate go run generate.go

package flow

import "context"

type selectFunc func(context.Context, []*<-chan *handlerJob, **handlerJob, *bool) (int, error)
