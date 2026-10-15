//go:generate go run generate.go

package util

import "context"

type selectFunc func(context.Context, []chan any, *any, *bool) (int, bool)
