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
	assert.Equal(t, n, h.name)
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
	assert.Equal(t, n, h.name)
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
	assert.Equal(t, maxA, *h.maxAttempts)
	assert.Equal(t, retryPeriod, *h.retryPeriod)
	assert.NotNil(t, h.errorFlow)
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
	assert.Equal(t, maxA, *h.maxAttempts)
	assert.Equal(t, retryPeriod, *h.retryPeriod)
	assert.NotNil(t, h.errorFlow)
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
	a, err := App(aName).
		WithFlow(f).
		Build()
	if err != nil {
		t.Fatal(err)
	}

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
	assert.Equal(t, fName, f.name)
}

func TestFluentApi_CanCreateApp_WithSimpleFlow(t *testing.T) {
	// Arrange
	in := 123
	expectedOut := "321"

	var actualOut string
	mu := &sync.Mutex{}
	cond := sync.NewCond(mu)
	mu.Lock()
	defer mu.Unlock()

	a := App("myApp").
		WithFlow(
			Flow[int]("myFlow").
				WithHandler(
					Handler[int, string]("handler1").
						WithHandleFunc(itoa),
				).
				WithHandler(
					EndHandler[string]("handler2").
						WithHandleFunc(func(_ context.Context, c EndHandlerContext[string], i *Message[string]) *EndHandlerResult {
							mu.Lock()
							defer mu.Unlock()

							actualOut = reverse(i.Value())
							cond.Signal()

							return nil
						}),
				),
		)
	built, err := a.Build()
	if err != nil {
		t.Fatal(err)
	}

	// Act
	if f, err := built.GetFlow[int]("myFlow"); err != nil {
		t.Fatal("Error getting flow from app")
	} else {
		f.Trigger(t.Context(), in)
		cond.Wait()
	}

	// Assert
	assert.Equal(t, expectedOut, actualOut)
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
	built, err := a.Build()
	if err != nil {
		t.Fatal(err)
	}

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

	built, err := a.Build()
	if err != nil {
		t.Fatal(err)
	}

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

	app, err := App("app").
		WithFlow(startFlow).
		Build()
	if err != nil {
		t.Fatal(err)
	}

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
	a, err := App("myApp").
		WithFlow(f1).
		WithFlow(f2).
		WithComponents(
			Component("myComponent").
				WithHandlers("handler2", "handler4").
				WithConcurrency(1),
		).
		Build()
	if err != nil {
		t.Fatal(err)
	}

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

func TestFluentApi_CanCreateApp_WithSplitFlow(t *testing.T) {
	// Arrange
	input := []int{1, 2}
	expectedItoaOutput := "2"
	expectedAddOutput := 2
	itoaChan := make(chan string, 1)
	addChan := make(chan int, 1)

	branch1 := Flow[int]("itoaBranch").
		WithHandler(
			EndHandler[int]("itoaHandler").
				WithHandleFunc(func(_ context.Context, c EndHandlerContext[int], m *Message[int]) *EndHandlerResult {
					itoaChan <- strconv.Itoa(m.Value())
					return nil
				},
				),
		)
	branch2 := Flow[int]("addBranch").
		WithHandler(
			EndHandler[int]("addHandler").
				WithHandleFunc(func(_ context.Context, c EndHandlerContext[int], m *Message[int]) *EndHandlerResult {
					addChan <- m.Value() + 1
					return nil
				},
				),
		)
	f := Flow[int]("flow1").
		WithHandler(
			SplitHandler[int]("splitHandler").
				WithFlow(branch1).
				WithFlow(branch2).
				WithHandleFunc(func(_ context.Context, c SplitHandlerContext[int], m *Message[int]) *SplitHandlerResult {
					results := make([]ResultValue, 0)
					if m.Value() > 1 {
						results = append(results, c.ResultValue(m.Value(), "itoaBranch"))
					} else {
						results = append(results, c.ResultValue(m.Value(), "addBranch"))
					}

					return c.Split(m, results)
				}),
		)
	app, err := App("splitApp").
		WithFlow(f).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	flow, err := app.GetFlow[int]("flow1")
	if err != nil {
		t.Fatal(err)
	}

	// Act
	flow.Trigger(t.Context(), input...)
	itoaOutput := <-itoaChan
	addOutput := <-addChan

	// Assert
	assert.Equal(t, expectedItoaOutput, itoaOutput)
	assert.Equal(t, expectedAddOutput, addOutput)
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
	built, err := a.Build()
	if err != nil {
		t.Fatal(err)
	}

	// Act
	_, err = built.GetFlow[string](fName + "1")

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
	built, err := a.Build()
	if err != nil {
		t.Fatal(err)
	}

	// Act
	_, err = built.GetFlow[int](fName)

	// Assert
	assert.Error(t, err)
}

func TestFluentApi_ReturnsError_IfAppContainsHandlersWithDuplicateNames(t *testing.T) {
	// Arrange
	builder := App("myApp").
		WithFlow(
			Flow[int]("flow1").
				WithHandler(
					EndHandler[int]("handler").
						WithHandleFunc(NilHandler),
				),
		).
		WithFlow(
			Flow[int]("flow2").
				WithHandler(
					EndHandler[int]("handler").
						WithHandleFunc(NilHandler),
				),
		)

	// Act
	app, err := builder.Build()

	// Assert
	assert.Nil(t, app)
	assert.Error(t, err)
}

func TestFluentApi_ReturnsError_IfAppContainsCircularDependency(t *testing.T) {
	// Arrange
	handlerBuilder := EndHandler[int]("handler1").
		WithHandleFunc(NilHandler)
	flow1 := Flow[int]("flow1").
		WithHandler(
			handlerBuilder,
		)
	*handlerBuilder = *EndHandler[int]("handler1").
		WithErrorFlow(flow1).
		WithHandleFunc(NilHandler)

	builder := App("app").
		WithFlow(flow1)

	// Act
	app, err := builder.Build()

	// Assert
	assert.Nil(t, app)
	assert.Error(t, err)
}

func TestFluentApi_ReturnsError_IfAppContainsNoFlows(t *testing.T) {
	// Arrange
	builder := App("app")

	// Act
	app, err := builder.Build()

	// Assert
	assert.Nil(t, app)
	assert.Error(t, err)
}

func TestFluentApi_ReturnsError_IfFlowContainsNoHandlers(t *testing.T) {
	// Arrange
	flow := Flow[int]("flow1")
	builder := App("myApp").
		WithFlow(
			(*FlowBuilder[int, NilType])(flow),
		)

	// Act
	app, err := builder.Build()

	// Assert
	assert.Nil(t, app)
	assert.Error(t, err)
}

func TestFluentApi_ReturnsError_IfHandlerHasNoName(t *testing.T) {
	// Arrange
	builder := App("myApp").
		WithFlow(
			Flow[int]("flow1").
				WithHandler(
					EndHandler[int]("").
						WithHandleFunc(NilHandler),
				),
		)

	// Act
	app, err := builder.Build()

	// Assert
	assert.Nil(t, app)
	assert.Error(t, err)
}

func TestFluentApi_ReturnsError_IfComponentHasNonExistentHandler(t *testing.T) {
	// Arrange
	builder := App("myApp").
		WithFlow(
			Flow[int]("flow1").
				WithHandler(
					EndHandler[int]("handler1").
						WithHandleFunc(NilHandler),
				),
		).
		WithComponents(
			Component("component1").
				WithHandlers("handler1", "handler2"),
		)

	// Act
	app, err := builder.Build()

	// Assert
	assert.Nil(t, app)
	assert.Error(t, err)
}

func TestFluentApi_ReturnsError_IfComponentHasNoHandlers(t *testing.T) {
	// Arrange
	builder := App("myApp").
		WithFlow(
			Flow[int]("flow1").
				WithHandler(
					EndHandler[int]("handler1").
						WithHandleFunc(NilHandler),
				),
		).
		WithComponents(
			Component("component1"),
		)

	// Act
	app, err := builder.Build()

	// Assert
	assert.Nil(t, app)
	assert.Error(t, err)
}

func TestFluentApi_ReturnsError_IfHandlerIsPartOfMultipleComponents(t *testing.T) {
	// Arrange
	builder := App("myApp").
		WithFlow(
			Flow[int]("flow1").
				WithHandler(
					EndHandler[int]("handler1").
						WithHandleFunc(NilHandler),
				),
		).
		WithComponents(
			Component("component1").WithHandlers("handler1"),
			Component("component2").WithHandlers("handler1"),
		)

	// Act
	app, err := builder.Build()

	// Assert
	assert.Nil(t, app)
	assert.Error(t, err)
}

func TestBuiltApp_WaitsForRunningHandlersToFinish_BeforeClosingApp(t *testing.T) {
	// Arrange
	startChan := make(chan struct{}, 1)
	continueChan := make(chan struct{}, 1)

	flow := Flow[int]("flow1").
		WithHandler(
			EndHandler[int]("handler1").
				WithHandleFunc(func(_ context.Context, c EndHandlerContext[int], msg *Message[int]) *EndHandlerResult {
					startChan <- struct{}{}
					<-continueChan
					return nil
				}),
		)

	app, err := App("app").
		WithFlow(flow).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	// Act
	f, err := app.GetFlow[int]("flow1")
	if err != nil {
		t.Fatal(err)
	}
	f.Trigger(t.Context(), 1)
	<-startChan

	var result bool
	stopChan := make(chan struct{}, 1)
	stoppedChan := make(chan struct{}, 1)
	go func() {
		stopChan <- struct{}{}
		result = app.Stop(t.Context())
		stoppedChan <- struct{}{}
	}()
	<-stopChan
	continueChan <- struct{}{}
	<-stoppedChan

	// Assert
	assert.True(t, result)
}

func TestBuiltApp_ForceCloses_WhenContextIsCancelled(t *testing.T) {
	// Arrange
	startedChan := make(chan struct{}, 1)
	continueChan := make(chan struct{}, 1)
	flow := Flow[int]("flow1").
		WithHandler(
			EndHandler[int]("handler1").
				WithHandleFunc(func(_ context.Context, c EndHandlerContext[int], msg *Message[int]) *EndHandlerResult {
					startedChan <- struct{}{}
					<-continueChan
					return nil
				}),
		)

	app, err := App("app").
		WithFlow(flow).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	// Act
	f, err := app.GetFlow[int]("flow1")
	if err != nil {
		t.Fatal(err)
	}
	f.Trigger(t.Context(), 1)
	<-startedChan

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result := app.Stop(ctx)

	// Assert
	assert.False(t, result)
}

func TestBuiltApp_WaitsForBothHandlersBeforeStopping_WhenTheyArePartOfSameComponent(t *testing.T) {
	// Arrange
	startedChan1 := make(chan struct{}, 1)
	continueChan1 := make(chan struct{}, 1)
	startedChan2 := make(chan struct{}, 1)
	continueChan2 := make(chan struct{}, 1)
	flow1 := Flow[int]("flow1").
		WithHandler(
			EndHandler[int]("handler1").
				WithHandleFunc(func(_ context.Context, c EndHandlerContext[int], msg *Message[int]) *EndHandlerResult {
					startedChan1 <- struct{}{}
					<-continueChan1
					return nil
				}),
		)
	flow2 := Flow[int]("flow2").
		WithHandler(
			EndHandler[int]("handler2").
				WithHandleFunc(func(_ context.Context, c EndHandlerContext[int], msg *Message[int]) *EndHandlerResult {
					startedChan2 <- struct{}{}
					<-continueChan2
					return nil
				}),
		)

	app, err := App("app").
		WithFlow(flow1).
		WithFlow(flow2).
		WithComponents(
			Component("component1").
				WithConcurrency(2).
				WithHandlers("handler1", "handler2"),
		).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	f1, err := app.GetFlow[int]("flow1")
	if err != nil {
		t.Fatal(err)
	}
	f2, err := app.GetFlow[int]("flow2")
	if err != nil {
		t.Fatal(err)
	}

	// Act
	f1.Trigger(t.Context(), 1)
	f2.Trigger(t.Context(), 1)
	<-startedChan1
	<-startedChan2

	var result bool
	stopChan := make(chan struct{}, 1)
	stoppedChan := make(chan struct{}, 1)
	go func() {
		stopChan <- struct{}{}
		result = app.Stop(t.Context())
		stoppedChan <- struct{}{}
	}()
	<-stopChan

	continueChan1 <- struct{}{}
	continueChan2 <- struct{}{}
	<-stoppedChan

	// Assert
	assert.True(t, result)
}

func NilHandler(_ context.Context, c EndHandlerContext[int], m *Message[int]) *EndHandlerResult {
	return nil
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

func nullHandler[Tin any](name string) endHandleFunc[Tin] {
	return func(_ context.Context, c EndHandlerContext[Tin], i *Message[Tin]) *EndHandlerResult {
		return nil
	}
}
