package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/test/wait"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"
)

func TestFluentApi_CanCreateSimpleHandler(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1})
	expectedOut := "1"
	n := uuid.New().String()
	f := func(_ context.Context, c HandlerContext[int, string], i *Message[int]) *HandlerResult[string] {
		return c.Success(i, strconv.Itoa(i.Value()))
	}
	h := Handler[int, string](n).
		WithHandleFunc(f)
	built := h.build()

	// Act
	result := built.handle(t.Context(), hCtx, in)

	// Assert
	assert.Equal(t, n, h.n)
	assert.Len(t, result, 1)
	assert.Equal(t, in[0].id, result[0].Id)
	assert.Equal(t, []ResultValue{{Value: expectedOut, FlowName: hCtx.currentFlow}}, result[0].Results)
	assert.NoError(t, result[0].Error)
}

func TestFluentApi_CanCreateBatchHandler(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1, 2, 3})
	expectedOut := []string{"1", "2", "3"}
	n := uuid.New().String()
	f := func(_ context.Context, c HandlerContext[int, string], i []*Message[int]) []*HandlerResult[string] {
		results := make([]*HandlerResult[string], 0)
		for _, v := range i {
			results = append(results, c.Success(v, strconv.Itoa(v.Value())))
		}

		return results
	}
	h := Handler[int, string](n).
		WithBatching(3, time.Second).
		WithHandleFunc(f)
	built := h.build()

	// Act
	result := built.handle(t.Context(), hCtx, in)

	// Assert
	assert.Equal(t, n, h.n)
	assert.Len(t, result, 3)
	for i := range in {
		assert.Equal(t, in[i].id, result[i].Id)
		assert.Equal(t, []ResultValue{{Value: expectedOut[i], FlowName: hCtx.currentFlow}}, result[i].Results)
		assert.NoError(t, result[i].Error)
	}
}

func TestFluentApi_CanCreateRetriedHandler(t *testing.T) {
	// Arrange
	n := uuid.New().String()
	maxA := 2
	retryPeriod := time.Second

	// Act
	h := Handler[int, string](n).
		WithMaxAttempts(maxA).
		WithRetryPeriod(retryPeriod).
		WithErrorFlow(
			Flow[int]("errorFlow").
				WithHandler(
					EndHandler[int]("errorHandler").
						WithHandleFunc(nullHandler[int]("nullErrorHandler")),
				),
		).
		WithHandleFunc(itoa)

	// Assert
	assert.Equal(t, maxA, h.maxA)
	assert.Equal(t, retryPeriod, h.retryPeriod)
	assert.NotNil(t, h.eFb)
}

func TestFluentApi_CanCreateRetriedHandler_WhenMixedWithBatchConfig(t *testing.T) {
	// Arrange
	n := uuid.New().String()
	maxA := 2
	retryPeriod := time.Second

	// Act
	h := Handler[int, string](n).
		WithMaxAttempts(maxA).
		WithRetryPeriod(retryPeriod).
		WithBatching(2, time.Minute).
		WithErrorFlow(
			Flow[int]("errorFlow").
				WithHandler(
					EndHandler[int]("errorHandler").
						WithHandleFunc(nullHandler[int]("nullErrorHandler")),
				),
		).
		WithHandleFunc(batchItoa)

	// Assert
	assert.Equal(t, maxA, h.maxA)
	assert.Equal(t, retryPeriod, h.retryPeriod)
	assert.NotNil(t, h.eFb)
}

func TestFluentApi_CanCreateApp_WithBatchedFlow(t *testing.T) {
	// Arrange
	aName := uuid.New().String()
	fName := uuid.New().String()
	hName1 := uuid.New().String()
	hName2 := uuid.New().String()
	in := []int{123, 456, 789}
	expectedOut := []string{"321", "654", "987"}
	actualOut := make([]string, 3)

	finishedChan := make(chan struct{}, 1)

	f := Flow[int](fName).
		WithHandler(
			Handler[int, string](hName1).
				WithBatching(3, time.Second).
				WithHandleFunc(batchItoa),
		).
		WithHandler(
			EndHandler[string](hName2).
				WithBatching(3, time.Second).
				WithHandleFunc(func(_ context.Context, c EndHandlerContext[string], in []*Message[string]) []*EndHandlerResult {
					for i := range in {
						actualOut[i] = reverse(in[i].Value())
					}
					finishedChan <- struct{}{}

					return nil
				}),
		)
	a := App(aName).
		WithFlow(f).
		Build()
	bF, err := a.GetFlow[int](fName)
	if err != nil {
		t.Fatal(err)
	}

	// Act
	bF.Trigger(t.Context(), in...)
	<-finishedChan

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedOut, actualOut)
	assert.Equal(t, fName, f.n)
}

func TestFluentApi_CanCreateApp_WithSimpleFlow(t *testing.T) {
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
		WithFlow(
			Flow[int](fName).
				WithHandler(
					Handler[int, string](hName1).
						WithHandleFunc(itoa),
				).
				WithHandler(
					EndHandler[string](hName2).
						WithHandleFunc(func(_ context.Context, c EndHandlerContext[string], i *Message[string]) *EndHandlerResult {
							mu.Lock()
							defer mu.Unlock()

							actualOut = reverse(i.Value())
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

func TestFluentApi_CanCreateApp_WhichRetriesFailures(t *testing.T) {
	// Arrange
	aName := uuid.New().String()
	fName := uuid.New().String()
	hName1 := uuid.New().String()
	hName2 := uuid.New().String()

	attemptNo := 0
	in := 123
	expectedOut := "321"

	var actualOut string
	mu := &sync.Mutex{}
	cond := sync.NewCond(mu)
	mu.Lock()
	defer mu.Unlock()

	a := App(aName).
		WithFlow(
			Flow[int](fName).
				WithHandler(
					Handler[int, string](hName1).
						WithHandleFunc(itoa),
				).
				WithHandler(
					EndHandler[string](hName2).
						WithMaxAttempts(2).
						WithRetryPeriod(time.Microsecond).
						WithHandleFunc(func(_ context.Context, c EndHandlerContext[string], i *Message[string]) *EndHandlerResult {
							mu.Lock()
							defer mu.Unlock()

							if attemptNo == 0 {
								attemptNo++
								return c.Error(i, errors.New("uh oh"))
							}
							attemptNo++

							actualOut = reverse(i.Value())
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
	assert.Equal(t, 2, attemptNo)
}

func TestFluentApi_CanCreateApp_WhichTriggersErrorFlow(t *testing.T) {
	// Arrange
	in := 123
	expectedOut := "321"

	var actualOut string
	mu := &sync.Mutex{}
	cond := sync.NewCond(mu)
	mu.Lock()
	defer mu.Unlock()

	errFlow := Flow[int]("errorFlow").
		WithHandler(
			Handler[int, string]("itoa").
				WithHandleFunc(itoa),
		).
		WithHandler(
			EndHandler[string]("reverse").
				WithHandleFunc(func(_ context.Context, c EndHandlerContext[string], i *Message[string]) *EndHandlerResult {
					mu.Lock()
					defer mu.Unlock()

					actualOut = reverse(i.Value())
					cond.Signal()
					return nil
				}),
		)

	a := App("testApp").
		WithFlow(
			Flow[int]("mainFlow").
				WithHandler(
					EndHandler[int]("throwingHandler").
						WithMaxAttempts(1).
						WithRetryPeriod(time.Microsecond).
						WithErrorFlow(errFlow).
						WithHandleFunc(func(_ context.Context, c EndHandlerContext[int], i *Message[int]) *EndHandlerResult {
							return c.Error(i, errors.New("uh oh"))
						}),
				),
		).
		WithFlow(errFlow) // Also test that we can use the same flow multiple times in an app
	built := a.Build()

	// Act & Assert
	f1, err := built.GetFlow[int]("mainFlow")
	if err != nil {
		t.Fatal("Error getting main flow from app")
	}
	f1.Trigger(t.Context(), in)
	cond.Wait()
	actualOut1 := actualOut

	f2, err := built.GetFlow[int]("errorFlow")
	if err != nil {
		t.Fatal("Error getting error flow from app")
	}
	f2.Trigger(t.Context(), in)
	cond.Wait()
	actualOut2 := actualOut

	// Assert
	assert.Equal(t, expectedOut, actualOut1)
	assert.Equal(t, expectedOut, actualOut2)
}

func TestFluentApi_CanCreateApp_WithFlowThatTriggersOtherFlow(t *testing.T) {
	// Arrange
	input := 123
	expectedOutput := "321"
	outputChan := make(chan string, 1)

	endFlow := Flow[string]("reverseFlow").
		WithHandler(
			EndHandler[string]("reverseHandler").
				WithHandleFunc(func(_ context.Context, c EndHandlerContext[string], i *Message[string]) *EndHandlerResult {
					outputChan <- reverse(i.Value())
					return nil
				}),
		)
	startFlow := Flow[int]("itoaFlow").
		WithHandler(
			Handler[int, string]("itoaHandler").
				WithHandleFunc(itoa),
		).
		WithFlow(endFlow)

	app := App("app").
		WithFlow(startFlow).
		Build()
	flow, err := app.GetFlow[int]("itoaFlow")
	if err != nil {
		t.Fatal(err)
	}

	// Act
	flow.Trigger(t.Context(), input)
	output := <-outputChan

	// Assert
	assert.Equal(t, expectedOutput, output)
}

func TestFluentApi_CanCreateApp_WithHandlersGroupedIntoComponents(t *testing.T) {
	// Arrange
	f1Started := make(chan struct{})
	f1Wait := make(chan struct{})
	f1Finish := make(chan struct{})
	f2Finish := make(chan struct{})
	var handler2Finish time.Time
	var handler4Finish time.Time

	f1 := Flow[int]("flow1").
		WithHandler(
			Handler[int, string]("handler1").
				WithHandleFunc(itoa),
		).
		WithHandler(
			EndHandler[string]("handler2").
				WithHandleFunc(func(_ context.Context, c EndHandlerContext[string], i *Message[string]) *EndHandlerResult {
					f1Started <- struct{}{}
					<-f1Wait
					handler2Finish = time.Now()
					f1Finish <- struct{}{}
					return nil
				}),
		)
	f2 := Flow[int]("flow2").
		WithHandler(
			Handler[int, string]("handler3").
				WithHandleFunc(itoa),
		).
		WithHandler(
			EndHandler[string]("handler4").
				WithHandleFunc(func(_ context.Context, c EndHandlerContext[string], i *Message[string]) *EndHandlerResult {
					handler4Finish = time.Now()
					f2Finish <- struct{}{}
					return nil
				}),
		)
	a := App("myApp").
		WithFlow(f1).
		WithFlow(f2).
		WithComponents(
			Component("myComponent").
				WithHandlers("handler2", "handler4").
				WithConcurrency(1),
		).
		Build()

	f1Built, err := a.GetFlow[int]("flow1")
	if err != nil {
		t.Fatal(err)
	}
	f2Built, err := a.GetFlow[int]("flow2")
	if err != nil {
		t.Fatal(err)
	}

	// Act
	if err = f1Built.Trigger(t.Context(), 123); err != nil { // Trigger the first flow and wait for handler2 to start
		t.Fatal(err)
	}
	<-f1Started

	if err = f2Built.Trigger(t.Context(), 456); err != nil { // Trigger the second flow, and allow time for handler4 to fire (if something isn't working)
		t.Fatal(err)
	}
	wait.For(t.Context(), time.Second)
	f1Wait <- struct{}{}
	<-f1Finish
	<-f2Finish

	// Assert
	assert.Greater(t, handler4Finish, handler2Finish) // Assert handler 4 didn't fire while handler 2 was waiting
}

func TestFluentApi_Panics_IfComponentReferencesNonExistentHandler(t *testing.T) {
	// Arrange
	a := App("myApp").
		WithFlow(
			Flow[string]("myFlow").
				WithHandler(
					EndHandler[string]("myHandler").
						WithHandleFunc(func(_ context.Context, c EndHandlerContext[string], i *Message[string]) *EndHandlerResult {
							return nil
						}),
				),
		).
		WithComponents(
			Component("myComponent").
				WithHandlers("otherHandler").
				WithConcurrency(1),
		)

	// Act & Assert
	assert.Panics(t, func() {
		a.Build()
	})
}

func TestFluentApi_ReturnsErrorIfFlowDoesntExist(t *testing.T) {
	// Arrange
	aName := uuid.New().String()
	fName := uuid.New().String()
	hName := uuid.New().String()

	a := App(aName).
		WithFlow(
			Flow[string](fName).
				WithHandler(
					EndHandler[string](hName).
						WithHandleFunc(func(_ context.Context, c EndHandlerContext[string], i *Message[string]) *EndHandlerResult {
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
		WithFlow(
			Flow[string](fName).
				WithHandler(
					EndHandler[string](hName).
						WithHandleFunc(func(_ context.Context, c EndHandlerContext[string], i *Message[string]) *EndHandlerResult {
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

func itoa(_ context.Context, c HandlerContext[int, string], i *Message[int]) *HandlerResult[string] {
	return c.Success(i, strconv.Itoa(i.Value()))
}

func batchItoa(_ context.Context, c HandlerContext[int, string], i []*Message[int]) []*HandlerResult[string] {
	results := make([]*HandlerResult[string], 0)
	for _, v := range i {
		results = append(results, c.Success(v, strconv.Itoa(v.Value())))
	}

	return results
}

func reverse(i string) string {
	r := []rune(i)
	for x, y := 0, len(r)-1; x < y; x, y = x+1, y-1 {
		r[x], r[y] = r[y], r[x]
	}
	return string(r)
}

func nullHandler[Tin any](name string) EndHandleFunc[Tin] {
	return func(_ context.Context, c EndHandlerContext[Tin], i *Message[Tin]) *EndHandlerResult {
		return nil
	}
}
