package flow

import "time"

type BatchPolicy struct {
	Number int
	Period time.Duration
}

func NewBatchPolicy(number int, period time.Duration) *BatchPolicy {
	return &BatchPolicy{
		Number: number,
		Period: period,
	}
}
