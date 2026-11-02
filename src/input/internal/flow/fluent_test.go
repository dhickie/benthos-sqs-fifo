package flow

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"
)

func TestFluentApi_CanCreateSimpleHandler(t *testing.T) {
	// Arrange
	in := []any{1}
	expectedOut := "1"
	n := uuid.New().String()
	f := func(_ context.Context, i int) (*ValueHandleResult[string], error) {
		return NewValueHandleResult(strconv.Itoa(i), true), nil
	}
	h := Handler[int, string](n).
		WithHandleFunc(f)
	built := h.build()

	// Act
	result, err := built.Handle(t.Context(), in)

	// Assert
	assert.Equal(t, n, h.n)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Len(t, result[0], 1)
	assert.Equal(t, expectedOut, result[0][0].output)
	assert.Equal(t, true, result[0][0].ok)
	assert.NoError(t, result[0][0].err)
}

func TestFluentApi_CanCreateBatchHandler(t *testing.T) {
	// Arrange
	in := []any{1, 2, 3}
	expectedOut := []string{"1", "2", "3"}
	n := uuid.New().String()
	f := func(_ context.Context, i []int) (*ValueBatchHandleResult[int, string], error) {
		results := make([]string, 0)
		for _, v := range i {
			results = append(results, strconv.Itoa(v))
		}

		return NewValueBatchHandleResult[int](results, nil), nil
	}
	h := Handler[int, string](n).
		WithBatching(3, time.Second).
		WithHandleFunc(f)
	built := h.build()

	// Act
	result, err := built.Handle(t.Context(), in)

	// Assert
	assert.Equal(t, n, h.n)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Len(t, result[0], 3)
	for i := range in {
		assert.Equal(t, expectedOut[i], result[0][i].output)
		assert.Equal(t, true, result[0][i].ok)
		assert.NoError(t, result[0][i].err)
	}
}

func TestFluentApi_CanCreateSimpleFlow(t *testing.T) {
	// Arrange
	inputMap := make(map[string]*Pipe)
	fName := uuid.New().String()
	hName1 := uuid.New().String()
	hName2 := uuid.New().String()
	in := 123
	expectedOut := "321"

	var actualOut string
	mu := &sync.Mutex{}
	cond := sync.NewCond(mu)
	mu.Lock()
	defer mu.Unlock()

	f := Flow[int](fName).
		WithHandler(
			Handler[int, string](hName1).
				WithHandleFunc(itoa),
		).
		WithHandler(
			EndHandler[string](hName2).
				WithHandleFunc(func(c context.Context, i string) error {
					mu.Lock()
					defer mu.Unlock()

					actualOut = reverse(i)
					cond.Signal()

					return nil
				}),
		)
	built := f.build(&inputMap)

	// Act
	err := built.p.Send(t.Context(), in)
	cond.Wait()

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedOut, actualOut)
	assert.Equal(t, fName, f.n)
}

func TestFluentApi_CanCreateBatchedFlow(t *testing.T) {
	// Arrange
	inputMap := make(map[string]*Pipe)
	fName := uuid.New().String()
	hName1 := uuid.New().String()
	hName2 := uuid.New().String()
	in := []any{123, 456, 789}
	expectedOut := []string{"321", "654", "987"}
	actualOut := make([]string, 3)

	mu := &sync.Mutex{}
	cond := sync.NewCond(mu)
	mu.Lock()
	defer mu.Unlock()

	f := Flow[int](fName).
		WithHandler(
			Handler[int, string](hName1).
				WithBatching(3, time.Second).
				WithHandleFunc(batchItoa),
		).
		WithHandler[NilType](
		EndHandler[string](hName2).
			WithBatching(3, time.Second).
			WithHandleFunc(func(c context.Context, in []string) (*BatchHandleResult[string], error) {
				mu.Lock()
				defer mu.Unlock()

				for i := range in {
					actualOut[i] = reverse(in[i])
				}
				cond.Signal()

				return NewBatchHandleResult[string](nil), nil
			}),
	)
	built := f.build(&inputMap)

	// Act
	err := built.p.Send(t.Context(), in...)
	cond.Wait()

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedOut, actualOut)
	assert.Equal(t, fName, f.n)
}

func TestFluentApi_CanCreateSimpleApp(t *testing.T) {
	// Arrange
	aName := uuid.New().String()
	fName := uuid.New().String()
	hName1 := uuid.New().String()
	hName2 := uuid.New().String()

	in := 123
	expectedOut := "321"

	var actualOut string
	mu := &sync.Mutex{}
	cond := sync.NewCond(mu)
	mu.Lock()
	defer mu.Unlock()

	a := App(aName).
		WithFlows(
			Flow[int](fName).
				WithHandler(
					Handler[int, string](hName1).
						WithHandleFunc(itoa),
				).
				WithHandler(
					EndHandler[string](hName2).
						WithHandleFunc(func(c context.Context, i string) error {
							mu.Lock()
							defer mu.Unlock()

							actualOut = reverse(i)
							cond.Signal()

							return nil
						}),
				),
		)
	built := a.Build()

	// Act
	if f, err := built.GetFlow[int](fName); err != nil {
		t.Fatal("Error getting flow from app")
	} else {
		f.Trigger(t.Context(), in)
		cond.Wait()
	}

	// Assert
	assert.Equal(t, expectedOut, actualOut)
	assert.Equal(t, aName, a.n)
}

func TestFluentApi_ReturnsErrorIfFlowDoesntExist(t *testing.T) {
	// Arrange
	aName := uuid.New().String()
	fName := uuid.New().String()
	hName := uuid.New().String()

	a := App(aName).
		WithFlows(
			Flow[string](fName).
				WithHandler(
					EndHandler[string](hName).
						WithHandleFunc(func(c context.Context, i string) error {
							return nil
						}),
				),
		)
	built := a.Build()

	// Act
	_, err := built.GetFlow[string](fName + "1")

	// Assert
	assert.Error(t, err)
}

func TestFluentApi_ReturnsErrorIfFlowHasDifferentTriggerType(t *testing.T) {
	// Arrange
	aName := uuid.New().String()
	fName := uuid.New().String()
	hName := uuid.New().String()

	a := App(aName).
		WithFlows(
			Flow[string](fName).
				WithHandler(
					EndHandler[string](hName).
						WithHandleFunc(func(c context.Context, i string) error {
							return nil
						}),
				),
		)
	built := a.Build()

	// Act
	_, err := built.GetFlow[int](fName)

	// Assert
	assert.Error(t, err)
}

func itoa(_ context.Context, i int) (*ValueHandleResult[string], error) {
	return NewValueHandleResult(strconv.Itoa(i), true), nil
}

func batchItoa(_ context.Context, i []int) (*ValueBatchHandleResult[int, string], error) {
	results := make([]string, 0)
	for _, v := range i {
		results = append(results, strconv.Itoa(v))
	}

	return NewValueBatchHandleResult[int](results, nil), nil
}

func reverse(i string) string {
	r := []rune(i)
	for x, y := 0, len(r)-1; x < y; x, y = x+1, y-1 {
		r[x], r[y] = r[y], r[x]
	}
	return string(r)
}
