package flow

import "time"

type batchPolicy struct {
	number int
	period time.Duration
}

func newBatchPolicy(number int, period time.Duration) *batchPolicy {
	return &batchPolicy{
		number: number,
		period: period,
	}
}
