package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/test/wait"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"errors"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var bp = NewBatchPolicy(1, time.Second)
var l = slog.Default()

func TestComponent_CanReceiveAndCarryOutWork(t *testing.T) {
	// Arrange
	input := []int{1, 2, 3, 4, 5}
	expectedOutput := []string{"1", "2", "3", "4", "5"}
	_, inP, outP := createComponent(t.Context(), itoa)

	// Act
	_ = inP.Send(t.Context(), input...)
	result, _ := outP.Receive(t.Context())

	// Assert
	for i, v := range result {
		vStr, ok := v.(string)
		if !ok {
			t.Fatal("Result type was not a string")
		}

		assert.Equal(t, expectedOutput[i], vStr)
	}
}

func TestComponents_CanBeChainedTogether(t *testing.T) {
	// Arrange
	input := []int{123, 456}
	expectedOutput := []string{"321", "654"}
	_, inP, outP := createComponentChain(t.Context(), itoa, reverse)

	// Act
	_ = inP.Send(t.Context(), input...)
	result, _ := outP.Receive(t.Context())

	// Assert
	for i, v := range result {
		vStr, ok := v.(string)
		if !ok {
			t.Fatal("Result type was not a string")
		}

		assert.Equal(t, expectedOutput[i], vStr)
	}
}

func TestComponents_CanContainMultipleHandlers(t *testing.T) {
	// Arrange
	in1 := []int{123, 456}
	in2 := []string{"123", "456"}
	expOut1 := []string{"123", "456"}
	expOut2 := []string{"321", "654"}
	_, inPipes, outPipes := createMultiHandlerComponent(t.Context(), itoa, reverse)

	// Act
	_ = inPipes[0].Send(t.Context(), in1...)
	_ = inPipes[1].Send(t.Context(), in2...)
	result1, _ := outPipes[0].Receive(t.Context())
	result2, _ := outPipes[1].Receive(t.Context())

	// Assert
	for i, v := range result1 {
		vStr, ok := v.(string)
		if !ok {
			t.Fatal("Result type was not a string")
		}

		assert.Equal(t, expOut1[i], vStr)
	}
	for i, v := range result2 {
		vStr, ok := v.(string)
		if !ok {
			t.Fatal("Result type was not a string")
		}

		assert.Equal(t, expOut2[i], vStr)
	}
}

func TestComponent_Stops_AndClosesOutputPipe_WhenInputPipeHasClosed(t *testing.T) {
	// Arrange
	c, inP, outP := createComponent(t.Context(), itoa)

	// Act
	close(inP.SendChan())

	// Assert
	_, ok := <-c.Stopped()
	assert.False(t, ok)
	err := wait.Until(t.Context(), func(_ context.Context) (bool, error) {
		_, ok := <-outP.ReceiveChan()
		return !ok, nil
	}, time.Second)
	assert.NoError(t, err)
}

func TestComponent_Stops_WhenContextIsCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c, _, _ := createComponent(ctx, itoa)

	// Act
	cancel()

	// Assert
	_, ok := <-c.Stopped()
	assert.False(t, ok)
}

func TestComponent_WaitsToStop_UntilAllInputPipesAreClosed(t *testing.T) {
	// Arrange
	input := []string{"123", "456"}
	expectedOutput := []string{"321", "654"}
	c, inP, outP := createMultiHandlerComponent(t.Context(), itoa, reverse)

	// Act
	close(inP[0].SendChan())
	_ = inP[1].Send(t.Context(), input...)
	result, _ := outP[1].Receive(t.Context())
	close(inP[1].SendChan())

	// Assert
	for i, v := range result {
		vStr, ok := v.(string)
		if !ok {
			t.Fatal("Result type was not a string")
		}

		assert.Equal(t, expectedOutput[i], vStr)
	}
	_, ok := <-c.Stopped()
	assert.False(t, ok)
}

func TestComponent_WaitsForWorkToFinish_BeforeStopping(t *testing.T) {
	// Arrange
	started := util.NewSyncValue(false)
	cont := util.NewSyncValue(false)
	workFunc := bigWorkFactory(started, cont)
	c, inP, _ := createComponent(t.Context(), workFunc)
	input := []int{123}

	// Act
	_ = inP.Send(t.Context(), input...)
	started.WaitForValue(t.Context(), true)
	close(inP.SendChan())
	err := wait.Until(t.Context(), func(_ context.Context) (bool, error) {
		<-c.Stopped()
		return true, nil
	}, time.Duration(10)*time.Millisecond)
	cont.Set(true)
	_, ok := <-c.Stopped()

	// Assert
	assert.Error(t, err)
	assert.False(t, ok)
}

func TestComponent_SendsToErrorPipe_AfterFailure(t *testing.T) {
	// Arrange
	input := []int{123}
	f := func(_ context.Context, i []int) (*ValueBatchHandleResult[int, string], error) {
		return nil, errors.New("test error")
	}
	_, inP, errorP := createErrorComponent(t.Context(), f, 1)
	defer close(inP.SendChan())

	// Act
	_ = inP.Send(t.Context(), input...)
	result, err := errorP.Receive(t.Context())

	// Assert
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, input[0], result[0].(int))
}

func TestComponent_RetriesHandler_UpToMaxAttempts(t *testing.T) {
	// Arrange
	input := []int{123}
	attempts := 0
	maxAttempts := 2
	f := func(_ context.Context, i []int) (*ValueBatchHandleResult[int, string], error) {
		attempts++
		return nil, errors.New("test error")
	}
	_, inP, errorP := createErrorComponent(t.Context(), f, maxAttempts)
	defer close(inP.SendChan())

	// Act
	_ = inP.Send(t.Context(), input...)
	errorP.Receive(t.Context()) // Ensure the work is completed

	// Assert
	assert.Equal(t, maxAttempts, attempts)
}

func TestComponent_IgnoresErrors_IfNoErrorPipeIsDefined(t *testing.T) {
	// Arrange
	input := []int{123}
	attempts := 0
	maxAttempts := 2
	f := func(_ context.Context, i []int) (*ValueBatchHandleResult[int, string], error) {
		attempts++
		return nil, errors.New("test error")
	}
	c, inP := createErrorComponentWithNoErrorPipe(t.Context(), f, maxAttempts)

	// Act
	_ = inP.Send(t.Context(), input...)
	close(inP.SendChan())
	<-c.Stopped()

	// Assert
	assert.Equal(t, maxAttempts, attempts)
}

func itoa(_ context.Context, i []int) (*ValueBatchHandleResult[int, string], error) {
	o := make([]string, len(i))
	for i, v := range i {
		o[i] = strconv.Itoa(v)
	}
	return NewValueBatchHandleResult[int](o, nil), nil
}

func reverse(_ context.Context, str []string) (*ValueBatchHandleResult[string, string], error) {
	o := make([]string, len(str))
	for i, v := range str {
		r := []rune(v)
		for x, y := 0, len(r)-1; x < y; x, y = x+1, y-1 {
			r[x], r[y] = r[y], r[x]
		}
		o[i] = string(r)
	}

	return NewValueBatchHandleResult[string](o, nil), nil
}

func bigWorkFactory(started *util.SyncValue[bool], cont *util.SyncValue[bool]) func(context.Context, []int) (*ValueBatchHandleResult[int, string], error) {
	return func(ctx context.Context, input []int) (*ValueBatchHandleResult[int, string], error) {
		started.Set(true)
		cont.WaitForValue(ctx, true)
		return itoa(ctx, input)
	}
}

func createComponent(ctx context.Context, handleFunc func(context.Context, []int) (*ValueBatchHandleResult[int, string], error)) (*Component, *Pipe, *Pipe) {
	l := slog.Default()
	bp := NewBatchPolicy(5, time.Second)
	inP := NewPipe("input", bp, l)
	outP := NewPipe("output", bp, l)
	handler := NewBatchHandler_1(handleFunc)
	coupling := NewHandlerCoupling(handler, inP, []*Pipe{outP}, nil)

	return NewComponent(ctx, []*HandlerCoupling{coupling}, 1), inP, outP
}

func createComponentChain(ctx context.Context, f1 func(context.Context, []int) (*ValueBatchHandleResult[int, string], error), f2 func(context.Context, []string) (*ValueBatchHandleResult[string, string], error)) ([]*Component, *Pipe, *Pipe) {
	p1 := NewPipe("input", bp, l)
	p2 := NewPipe("intermediate", bp, l)
	p3 := NewPipe("output", bp, l)
	handler1 := NewBatchHandler_1(f1)
	handler2 := NewBatchHandler_1(f2)
	couplings1 := []*HandlerCoupling{NewHandlerCoupling(handler1, p1, []*Pipe{p2}, nil)}
	couplings2 := []*HandlerCoupling{NewHandlerCoupling(handler2, p2, []*Pipe{p3}, nil)}
	c1 := NewComponent(ctx, couplings1, 1)
	c2 := NewComponent(ctx, couplings2, 1)

	return []*Component{c1, c2}, p1, p3
}

func createMultiHandlerComponent(ctx context.Context, f1 func(context.Context, []int) (*ValueBatchHandleResult[int, string], error), f2 func(context.Context, []string) (*ValueBatchHandleResult[string, string], error)) (*Component, []*Pipe, []*Pipe) {
	inP1 := NewPipe("input1", bp, l)
	inP2 := NewPipe("input2", bp, l)
	outP1 := NewPipe("output1", bp, l)
	outP2 := NewPipe("output2", bp, l)
	handler1 := NewBatchHandler_1(f1)
	handler2 := NewBatchHandler_1(f2)
	couplings := []*HandlerCoupling{
		NewHandlerCoupling(handler1, inP1, []*Pipe{outP1}, nil),
		NewHandlerCoupling(handler2, inP2, []*Pipe{outP2}, nil),
	}
	c := NewComponent(ctx, couplings, 1)
	return c, []*Pipe{inP1, inP2}, []*Pipe{outP1, outP2}
}

func createErrorComponent(ctx context.Context, f func(context.Context, []int) (*ValueBatchHandleResult[int, string], error), maxAttempts int) (*Component, *Pipe, *Pipe) {
	inP := NewPipe("input", bp, l)
	outP := NewPipe("output", bp, l)
	errorP := NewPipe("error", bp, l)
	retryPolicy := NewRetryPolicy(errorP, maxAttempts, time.Microsecond)
	handler := NewBatchHandler_1(f)
	coupling := NewHandlerCoupling(handler, inP, []*Pipe{outP}, retryPolicy)
	return NewComponent(ctx, []*HandlerCoupling{coupling}, 1), inP, errorP
}

func createErrorComponentWithNoErrorPipe(ctx context.Context, f func(context.Context, []int) (*ValueBatchHandleResult[int, string], error), maxAttempts int) (*Component, *Pipe) {
	inP := NewPipe("input", bp, l)
	outP := NewPipe("output", bp, l)
	retryPolicy := NewRetryPolicy(nil, maxAttempts, time.Microsecond)
	handler := NewBatchHandler_1(f)
	coupling := NewHandlerCoupling(handler, inP, []*Pipe{outP}, retryPolicy)
	return NewComponent(ctx, []*HandlerCoupling{coupling}, 1), inP
}
