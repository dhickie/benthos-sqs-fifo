package flow

import (
	"context"
	"strconv"
	"sync"
	"testing"
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
	assert.Equal(t, true, result[0][0].ok)
	assert.NoError(t, result[0][0].err)
}

func TestFluentApi_CanCreateSimpleFlow(t *testing.T) {
	// Arrange
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
		WithEndHandler(
			EndHandler[string](hName2).
				WithHandleFunc(func(c context.Context, i string) error {
					mu.Lock()
					defer mu.Unlock()

					actualOut = reverse(i)
					cond.Signal()

					return nil
				}),
		)
	built := f.build()

	// Act
	err := built.p.Send(t.Context(), in)
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
				WithEndHandler(
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
				WithEndHandler(
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
				WithEndHandler(
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

func reverse(i string) string {
	r := []rune(i)
	for x, y := 0, len(r)-1; x < y; x, y = x+1, y-1 {
		r[x], r[y] = r[y], r[x]
	}
	return string(r)
}
