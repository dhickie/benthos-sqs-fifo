package test

import (
	"math"
	"math/rand/v2"
	"time"
)

// Rng is a helper struct for generating random values in tests
type Rng struct {
	rng *rand.Rand
}

// NewRng returns a new Rng instance using the specified seed.
// Providing a value of 0 uses the current time as the seed.
func NewRng(seed int64) *Rng {
	return &Rng{
		rng: rand.New(newTestSource(seed)),
	}
}

// RandRange returns a new integer within the specified range, inclusive
func (r *Rng) RandRange(min, max int) int {
	return min + r.rng.IntN(max-min)
}

// RandFloatRange returns a 32-bit float with the specified number of decimals
func (r *Rng) RandFloatRange(min, max, decimals int) float32 {
	m := int(math.Pow(10, float64(decimals)))
	mMin := m * min
	mMax := m * max
	mV := r.RandRange(mMin, mMax)
	return float32(mV) / float32(m)
}

// RandBool returns a random boolean value
func (r *Rng) RandBool() bool {
	n := r.RandRange(0, 1)
	if n == 1 {
		return true
	}

	return false
}

type testSource struct {
	seed int64
}

func newTestSource(seed int64) *testSource {
	return &testSource{
		seed: seed,
	}
}

func (t *testSource) Uint64() uint64 {
	if t.seed == 0 {
		return uint64(time.Now().UnixNano())
	}

	return uint64(t.seed)
}
