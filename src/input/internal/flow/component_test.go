package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/test/wait"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"errors"
	"log/slog"
	"strconv"
	"sync"
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
	_, inP, outP := createComponent(t.Context(), itoaBatch)

	// Act
	_ = inP.Send(t.Context(), input...)
	result, _ := outP.Receive(t.Context())

	// Assert
	for i, v := range result.msgs {
		vStr, ok := v.value.(string)
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
	_, inP, outP := createComponentChain(t.Context(), itoaBatch, reverseBatch)

	// Act
	_ = inP.Send(t.Context(), input...)
	result, _ := outP.Receive(t.Context())

	// Assert
	for i, v := range result.msgs {
		vStr, ok := v.value.(string)
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
	_, inPipes, outPipes := createMultiHandlerComponent(t.Context(), itoaBatch, reverseBatch)

	// Act
	_ = inPipes[0].Send(t.Context(), in1...)
	_ = inPipes[1].Send(t.Context(), in2...)
	result1, _ := outPipes[0].Receive(t.Context())
	result2, _ := outPipes[1].Receive(t.Context())

	// Assert
	for i, v := range result1.msgs {
		vStr, ok := v.value.(string)
		if !ok {
			t.Fatal("Result type was not a string")
		}

		assert.Equal(t, expOut1[i], vStr)
	}
	for i, v := range result2.msgs {
		vStr, ok := v.value.(string)
		if !ok {
			t.Fatal("Result type was not a string")
		}

		assert.Equal(t, expOut2[i], vStr)
	}
}

func TestComponents_CanHaveNoOutput(t *testing.T) {
	// Arrange
	in := 1
	mu := &sync.Mutex{}
	cond := sync.NewCond(mu)
	c, inP := createZeroOutputComponent(t.Context(), func(ctx context.Context, c EndHandlerContext[int], i *Message[int]) *EndHandlerResult {
		mu.Lock()
		defer mu.Unlock()

		cond.Signal()
		return &EndHandlerResult{Id: i.id}
	})

	// Act
	mu.Lock()
	defer mu.Unlock()
	inP.Send(t.Context(), in)
	cond.Wait()
	close(inP.SendChan())

	// Assert
	<-c.Stopped()
}

func TestComponent_CanHaveOptionalOutputs(t *testing.T) {
	// Arrange
	in := []int{1, 2}
	expectedOut := "2"
	_, inP, outP := createComponent(t.Context(), func(_ context.Context, c HandlerContext[int, string], in []*Message[int]) []*HandlerResult[string] {
		results := make([]*HandlerResult[string], 0)
		for _, v := range in {
			if v.Value() == 2 {
				results = append(results, c.Success(v, strconv.Itoa(v.Value())))
			}
		}

		return results
	})
	defer close(inP.SendChan())

	// Act
	inP.Send(t.Context(), in...)
	out, err := outP.Receive(t.Context())

	// Assert
	assert.NoError(t, err)
	assert.Len(t, out.msgs, 1)
	assert.Equal(t, expectedOut, out.msgs[0].value)
}

func TestComponent_CanHaveMultipleOutputs(t *testing.T) {
	// Arrange
	in := []int{1, 2}
	expectedOut1 := "1"
	expectedOut2 := "2"
	f := func(_ context.Context, c SplitHandlerContext[int], in []*Message[int]) []*SplitHandlerResult {
		out := make([]*SplitHandlerResult, len(in))
		for i, v := range in {
			if v.Value() == 1 {
				out[i] = c.Success(v, strconv.Itoa(v.Value()), "first")
			} else {
				out[i] = c.Success(v, strconv.Itoa(v.Value()), "second")
			}
		}

		return out
	}
	_, inP, outP1, outP2 := createMultiOutputComponent(t.Context(), f)
	defer close(inP.SendChan())

	// Act
	inP.Send(t.Context(), in...)
	out1, err1 := outP1.Receive(t.Context())
	out2, err2 := outP2.Receive(t.Context())

	// Assert
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Len(t, out1.msgs, 1)
	assert.Len(t, out2.msgs, 1)
	assert.Equal(t, expectedOut1, out1.msgs[0].value)
	assert.Equal(t, expectedOut2, out2.msgs[0].value)
}

func TestComponent_Stops_AndClosesOutputPipe_WhenInputPipeHasClosed(t *testing.T) {
	// Arrange
	c, inP, outP := createComponent(t.Context(), itoaBatch)

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
	c, _, _ := createComponent(ctx, itoaBatch)

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
	c, inP, outP := createMultiHandlerComponent(t.Context(), itoaBatch, reverseBatch)

	// Act
	close(inP[0].SendChan())
	_ = inP[1].Send(t.Context(), input...)
	result, _ := outP[1].Receive(t.Context())
	close(inP[1].SendChan())

	// Assert
	for i, v := range result.msgs {
		vStr, ok := v.value.(string)
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
	f := func(_ context.Context, c HandlerContext[int, string], i []*Message[int]) []*HandlerResult[string] {
		return []*HandlerResult[string]{c.Error(i[0], errors.New("test error"))}
	}
	_, inP, _, errorP := createErrorComponent(t.Context(), f, 1, 1)
	defer close(inP.SendChan())

	// Act
	_ = inP.Send(t.Context(), input...)
	result, err := errorP.Receive(t.Context())

	// Assert
	assert.NoError(t, err)
	assert.Len(t, result.msgs, 1)
	assert.Equal(t, input[0], result.msgs[0].value)
}

func TestComponent_RetriesHandler_UpToMaxAttempts(t *testing.T) {
	// Arrange
	input := []int{123}
	attempts := 0
	maxAttempts := 2
	f := func(_ context.Context, c HandlerContext[int, string], i []*Message[int]) []*HandlerResult[string] {
		attempts++
		return []*HandlerResult[string]{c.Error(i[0], errors.New("test error"))}
	}
	_, inP, _, errorP := createErrorComponent(t.Context(), f, maxAttempts, 1)
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
	f := func(_ context.Context, c HandlerContext[int, string], i []*Message[int]) []*HandlerResult[string] {
		attempts++
		return []*HandlerResult[string]{c.Error(i[0], errors.New("test error"))}
	}
	c, inP := createErrorComponentWithNoErrorPipe(t.Context(), f, maxAttempts)

	// Act
	_ = inP.Send(t.Context(), input...)
	close(inP.SendChan())
	<-c.Stopped()

	// Assert
	assert.Equal(t, maxAttempts, attempts)
}

func TestComponent_OnlyRetriesFailedMessagesFromBatch(t *testing.T) {
	// Arrange
	input := []int{1, 2}
	attempts1 := 0
	attempts2 := 0
	maxAttempts := 2
	expectedOut := []string{"1", "2"}
	f := func(_ context.Context, c HandlerContext[int, string], in []*Message[int]) []*HandlerResult[string] {
		result := make([]*HandlerResult[string], len(in))
		for i, v := range in {
			if v.Value() == 1 {
				attempts1++
				result[i] = c.Success(v, strconv.Itoa(v.Value()))
			} else {
				attempts2++
				if attempts2 == 2 {
					result[i] = c.Success(v, strconv.Itoa(v.Value()))
				} else {
					result[i] = c.Error(v, errors.New("oh no"))
				}
			}
		}

		return result
	}
	_, inP, outP, _ := createErrorComponent(t.Context(), f, maxAttempts, 2)

	// Act
	inP.Send(t.Context(), input...)
	out, err := outP.Receive(t.Context())

	// Assert
	assert.NoError(t, err)
	assert.Len(t, out.msgs, 2)
	assert.Equal(t, attempts1, 1)
	assert.Equal(t, attempts2, 2)
	for i, v := range out.msgs {
		assert.Equal(t, expectedOut[i], v.value)
	}
}

func itoaBatch(_ context.Context, c HandlerContext[int, string], i []*Message[int]) []*HandlerResult[string] {
	o := make([]*HandlerResult[string], len(i))
	for i, v := range i {
		o[i] = c.Success(v, strconv.Itoa(v.Value()))
	}
	return o
}

func reverseBatch(_ context.Context, c HandlerContext[string, string], str []*Message[string]) []*HandlerResult[string] {
	o := make([]*HandlerResult[string], len(str))
	for i, v := range str {
		r := []rune(v.Value())
		for x, y := 0, len(r)-1; x < y; x, y = x+1, y-1 {
			r[x], r[y] = r[y], r[x]
		}
		o[i] = c.Success(v, string(r))
	}

	return o
}

func bigWorkFactory(started *util.SyncValue[bool], cont *util.SyncValue[bool]) BatchHandleFunc[int, string] {
	return func(ctx context.Context, c HandlerContext[int, string], input []*Message[int]) []*HandlerResult[string] {
		started.Set(true)
		cont.WaitForValue(ctx, true)
		return itoaBatch(ctx, c, input)
	}
}

func createComponent(ctx context.Context, handleFunc BatchHandleFunc[int, string]) (*component, *Pipe, *Pipe) {
	bp := NewBatchPolicy(5, time.Second)
	inP := NewPipe("input", bp, l, "", []string{""})
	outP := NewPipe("output", bp, l, "", []string{""})
	handler := newBatchHandler("", handleFunc)
	coupling := NewHandlerCoupling(handler, inP, map[string]*Pipe{"": outP}, nil)

	return NewComponent(ctx, l, []*HandlerCoupling{coupling}, 1), inP, outP
}

func createComponentChain(ctx context.Context, f1 BatchHandleFunc[int, string], f2 BatchHandleFunc[string, string]) ([]*component, *Pipe, *Pipe) {
	p1 := NewPipe("input", bp, l, "", []string{""})
	p2 := NewPipe("intermediate", bp, l, "", []string{""})
	p3 := NewPipe("output", bp, l, "", []string{""})
	handler1 := newBatchHandler("", f1)
	handler2 := newBatchHandler("", f2)
	couplings1 := []*HandlerCoupling{NewHandlerCoupling(handler1, p1, map[string]*Pipe{"": p2}, nil)}
	couplings2 := []*HandlerCoupling{NewHandlerCoupling(handler2, p2, map[string]*Pipe{"": p3}, nil)}
	c1 := NewComponent(ctx, l, couplings1, 1)
	c2 := NewComponent(ctx, l, couplings2, 1)

	return []*component{c1, c2}, p1, p3
}

func createMultiHandlerComponent(ctx context.Context, f1 BatchHandleFunc[int, string], f2 BatchHandleFunc[string, string]) (*component, []*Pipe, []*Pipe) {
	inP1 := NewPipe("input1", bp, l, "", []string{""})
	inP2 := NewPipe("input2", bp, l, "", []string{""})
	outP1 := NewPipe("output1", bp, l, "", []string{""})
	outP2 := NewPipe("output2", bp, l, "", []string{""})
	handler1 := newBatchHandler("", f1)
	handler2 := newBatchHandler("", f2)
	couplings := []*HandlerCoupling{
		NewHandlerCoupling(handler1, inP1, map[string]*Pipe{"": outP1}, nil),
		NewHandlerCoupling(handler2, inP2, map[string]*Pipe{"": outP2}, nil),
	}
	c := NewComponent(ctx, l, couplings, 1)
	return c, []*Pipe{inP1, inP2}, []*Pipe{outP1, outP2}
}

func createErrorComponent(ctx context.Context, f BatchHandleFunc[int, string], maxAttempts int, batchSize int) (*component, *Pipe, *Pipe, *Pipe) {
	bp := NewBatchPolicy(batchSize, time.Minute)
	inP := NewPipe("input", bp, l, "", []string{""})
	outP := NewPipe("output", bp, l, "", []string{""})
	errorP := NewPipe("error", bp, l, "", []string{""})
	retryPolicy := NewRetryPolicy(errorP, maxAttempts, time.Microsecond)
	handler := newBatchHandler("", f)
	coupling := NewHandlerCoupling(handler, inP, map[string]*Pipe{"": outP}, retryPolicy)
	return NewComponent(ctx, l, []*HandlerCoupling{coupling}, 1), inP, outP, errorP
}

func createErrorComponentWithNoErrorPipe(ctx context.Context, f BatchHandleFunc[int, string], maxAttempts int) (*component, *Pipe) {
	inP := NewPipe("input", bp, l, "", []string{""})
	outP := NewPipe("output", bp, l, "", []string{""})
	retryPolicy := NewRetryPolicy(nil, maxAttempts, time.Microsecond)
	handler := newBatchHandler("", f)
	coupling := NewHandlerCoupling(handler, inP, map[string]*Pipe{"": outP}, retryPolicy)
	return NewComponent(ctx, l, []*HandlerCoupling{coupling}, 1), inP
}

func createZeroOutputComponent(ctx context.Context, f EndHandleFunc[int]) (*component, *Pipe) {
	bp := NewBatchPolicy(5, time.Second)
	inP := NewPipe("input", bp, l, "", []string{""})
	handler := newEndHandler("", f)
	coupling := NewHandlerCoupling(handler, inP, nil, nil)

	return NewComponent(ctx, l, []*HandlerCoupling{coupling}, 1), inP
}

func createMultiOutputComponent(ctx context.Context, f BatchSplitHandleFunc[int]) (*component, *Pipe, *Pipe, *Pipe) {
	inP := NewPipe("input", bp, l, "", []string{""})
	outP1 := NewPipe("output1", bp, l, "", []string{""})
	outP2 := NewPipe("output2", bp, l, "", []string{""})
	handler := newBatchSplitHandler("", f)
	coupling := NewHandlerCoupling(handler, inP, map[string]*Pipe{"first": outP1, "second": outP2}, nil)

	return NewComponent(ctx, l, []*HandlerCoupling{coupling}, 1), inP, outP1, outP2
}
