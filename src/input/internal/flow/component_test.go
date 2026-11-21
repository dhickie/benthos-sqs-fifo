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

var bp = newBatchPolicy(1, time.Second)
var l = slog.Default()

func TestComponent_CanReceiveAndCarryOutWork(t *testing.T) {
	// Arrange
	input := []int{1, 2, 3, 4, 5}
	expectedOutput := []string{"1", "2", "3", "4", "5"}
	_, inP, outP := createComponent(t.Context(), itoaBatch)

	// Act
	_ = inP.send(t.Context(), input...)
	result, _ := outP.receive(t.Context())

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
	_ = inP.send(t.Context(), input...)
	result, _ := outP.receive(t.Context())

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
	_ = inPipes[0].send(t.Context(), in1...)
	_ = inPipes[1].send(t.Context(), in2...)
	result1, _ := outPipes[0].receive(t.Context())
	result2, _ := outPipes[1].receive(t.Context())

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
		return nil
	})

	// Act
	mu.Lock()
	defer mu.Unlock()
	inP.send(t.Context(), in)
	cond.Wait()
	close(inP.sendChan())

	// Assert
	<-c.stopped
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
	defer close(inP.sendChan())

	// Act
	inP.send(t.Context(), in...)
	out, err := outP.receive(t.Context())

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
	defer close(inP.sendChan())

	// Act
	inP.send(t.Context(), in...)
	out1, err1 := outP1.receive(t.Context())
	out2, err2 := outP2.receive(t.Context())

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
	close(inP.sendChan())

	// Assert
	_, ok := <-c.stopped
	assert.False(t, ok)
	err := wait.Until(t.Context(), func(_ context.Context) (bool, error) {
		_, ok := <-outP.receiveChan()
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
	_, ok := <-c.stopped
	assert.False(t, ok)
}

func TestComponent_WaitsToStop_UntilAllInputPipesAreClosed(t *testing.T) {
	// Arrange
	input := []string{"123", "456"}
	expectedOutput := []string{"321", "654"}
	c, inP, outP := createMultiHandlerComponent(t.Context(), itoaBatch, reverseBatch)

	// Act
	close(inP[0].sendChan())
	_ = inP[1].send(t.Context(), input...)
	result, _ := outP[1].receive(t.Context())
	close(inP[1].sendChan())

	// Assert
	for i, v := range result.msgs {
		vStr, ok := v.value.(string)
		if !ok {
			t.Fatal("Result type was not a string")
		}

		assert.Equal(t, expectedOutput[i], vStr)
	}
	_, ok := <-c.stopped
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
	_ = inP.send(t.Context(), input...)
	started.WaitForValue(t.Context(), true)
	close(inP.sendChan())
	err := wait.Until(t.Context(), func(_ context.Context) (bool, error) {
		<-c.stopped
		return true, nil
	}, time.Duration(10)*time.Millisecond)
	cont.Set(true)
	_, ok := <-c.stopped

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
	defer close(inP.sendChan())

	// Act
	_ = inP.send(t.Context(), input...)
	result, err := errorP.receive(t.Context())

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
	defer close(inP.sendChan())

	// Act
	_ = inP.send(t.Context(), input...)
	errorP.receive(t.Context()) // Ensure the work is completed

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
	_ = inP.send(t.Context(), input...)
	close(inP.sendChan())
	<-c.stopped

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
	inP.send(t.Context(), input...)
	out, err := outP.receive(t.Context())

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

func bigWorkFactory(started *util.SyncValue[bool], cont *util.SyncValue[bool]) batchHandleFunc[int, string] {
	return func(ctx context.Context, c HandlerContext[int, string], input []*Message[int]) []*HandlerResult[string] {
		started.Set(true)
		cont.WaitForValue(ctx, true)
		return itoaBatch(ctx, c, input)
	}
}

func createComponent(ctx context.Context, handleFunc batchHandleFunc[int, string]) (*component, *pipe, *pipe) {
	bp := newBatchPolicy(5, time.Second)
	inP := newPipe("input", bp, l, "", []string{""})
	outP := newPipe("output", bp, l, "", []string{""})
	handler := newBatchHandler("", handleFunc)
	coupling := newHandlerCoupling(handler, inP, map[string]*pipe{"": outP}, nil)

	return newComponent(ctx, l, []*handlerCoupling{coupling}, 1), inP, outP
}

func createComponentChain(ctx context.Context, f1 batchHandleFunc[int, string], f2 batchHandleFunc[string, string]) ([]*component, *pipe, *pipe) {
	p1 := newPipe("input", bp, l, "", []string{""})
	p2 := newPipe("intermediate", bp, l, "", []string{""})
	p3 := newPipe("output", bp, l, "", []string{""})
	handler1 := newBatchHandler("", f1)
	handler2 := newBatchHandler("", f2)
	couplings1 := []*handlerCoupling{newHandlerCoupling(handler1, p1, map[string]*pipe{"": p2}, nil)}
	couplings2 := []*handlerCoupling{newHandlerCoupling(handler2, p2, map[string]*pipe{"": p3}, nil)}
	c1 := newComponent(ctx, l, couplings1, 1)
	c2 := newComponent(ctx, l, couplings2, 1)

	return []*component{c1, c2}, p1, p3
}

func createMultiHandlerComponent(ctx context.Context, f1 batchHandleFunc[int, string], f2 batchHandleFunc[string, string]) (*component, []*pipe, []*pipe) {
	inP1 := newPipe("input1", bp, l, "", []string{""})
	inP2 := newPipe("input2", bp, l, "", []string{""})
	outP1 := newPipe("output1", bp, l, "", []string{""})
	outP2 := newPipe("output2", bp, l, "", []string{""})
	handler1 := newBatchHandler("", f1)
	handler2 := newBatchHandler("", f2)
	couplings := []*handlerCoupling{
		newHandlerCoupling(handler1, inP1, map[string]*pipe{"": outP1}, nil),
		newHandlerCoupling(handler2, inP2, map[string]*pipe{"": outP2}, nil),
	}
	c := newComponent(ctx, l, couplings, 1)
	return c, []*pipe{inP1, inP2}, []*pipe{outP1, outP2}
}

func createErrorComponent(ctx context.Context, f batchHandleFunc[int, string], maxAttempts int, batchSize int) (*component, *pipe, *pipe, *pipe) {
	bp := newBatchPolicy(batchSize, time.Minute)
	inP := newPipe("input", bp, l, "", []string{""})
	outP := newPipe("output", bp, l, "", []string{""})
	errorP := newPipe("error", bp, l, "", []string{""})
	retryPolicy := newRetryPolicy(errorP, maxAttempts, time.Microsecond)
	handler := newBatchHandler("", f)
	coupling := newHandlerCoupling(handler, inP, map[string]*pipe{"": outP}, retryPolicy)
	return newComponent(ctx, l, []*handlerCoupling{coupling}, 1), inP, outP, errorP
}

func createErrorComponentWithNoErrorPipe(ctx context.Context, f batchHandleFunc[int, string], maxAttempts int) (*component, *pipe) {
	inP := newPipe("input", bp, l, "", []string{""})
	outP := newPipe("output", bp, l, "", []string{""})
	retryPolicy := newRetryPolicy(nil, maxAttempts, time.Microsecond)
	handler := newBatchHandler("", f)
	coupling := newHandlerCoupling(handler, inP, map[string]*pipe{"": outP}, retryPolicy)
	return newComponent(ctx, l, []*handlerCoupling{coupling}, 1), inP
}

func createZeroOutputComponent(ctx context.Context, f endHandleFunc[int]) (*component, *pipe) {
	bp := newBatchPolicy(5, time.Second)
	inP := newPipe("input", bp, l, "", []string{""})
	handler := newEndHandler("", f)
	coupling := newHandlerCoupling(handler, inP, nil, nil)

	return newComponent(ctx, l, []*handlerCoupling{coupling}, 1), inP
}

func createMultiOutputComponent(ctx context.Context, f batchSplitHandleFunc[int]) (*component, *pipe, *pipe, *pipe) {
	inP := newPipe("input", bp, l, "", []string{""})
	outP1 := newPipe("output1", bp, l, "", []string{""})
	outP2 := newPipe("output2", bp, l, "", []string{""})
	handler := newBatchSplitHandler("", f)
	coupling := newHandlerCoupling(handler, inP, map[string]*pipe{"first": outP1, "second": outP2}, nil)

	return newComponent(ctx, l, []*handlerCoupling{coupling}, 1), inP, outP1, outP2
}
