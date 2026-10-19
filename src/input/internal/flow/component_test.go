package flow

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/test/wait"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
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

func itoa(_ context.Context, i []int) ([]string, error) {
	o := make([]string, len(i))
	for i, v := range i {
		o[i] = strconv.Itoa(v)
	}
	return o, nil
}

func reverse(_ context.Context, str []string) ([]string, error) {
	o := make([]string, len(str))
	for i, v := range str {
		r := []rune(v)
		for x, y := 0, len(r)-1; x < y; x, y = x+1, y-1 {
			r[x], r[y] = r[y], r[x]
		}
		o[i] = string(r)
	}

	return o, nil
}

func bigWorkFactory(started *util.SyncValue[bool], cont *util.SyncValue[bool]) func(context.Context, []int) ([]string, error) {
	return func(ctx context.Context, input []int) ([]string, error) {
		started.Set(true)
		cont.WaitForValue(ctx, true)
		return itoa(ctx, input)
	}
}

func createComponent(ctx context.Context, handleFunc func(context.Context, []int) ([]string, error)) (*Component, *Pipe, *Pipe) {
	l := slog.Default()
	bp := NewBatchPolicy(5, time.Second)
	inP := NewPipe("input", bp, l)
	outP := NewPipe("output", bp, l)
	handler := NewHandler(handleFunc)
	coupling := HandlerCoupling{
		handler:    handler,
		inputPipe:  inP,
		outputPipe: outP,
	}

	return NewComponent(ctx, []HandlerCoupling{coupling}, 1), inP, outP
}

func createComponentChain(ctx context.Context, f1 func(context.Context, []int) ([]string, error), f2 func(context.Context, []string) ([]string, error)) ([]*Component, *Pipe, *Pipe) {
	p1 := NewPipe("input", bp, l)
	p2 := NewPipe("intermediate", bp, l)
	p3 := NewPipe("output", bp, l)
	handler1 := NewHandler(f1)
	handler2 := NewHandler(f2)
	couplings1 := []HandlerCoupling{{handler1, p1, p2}}
	couplings2 := []HandlerCoupling{{handler2, p2, p3}}
	c1 := NewComponent(ctx, couplings1, 1)
	c2 := NewComponent(ctx, couplings2, 1)

	return []*Component{c1, c2}, p1, p3
}

func createMultiHandlerComponent(ctx context.Context, f1 func(context.Context, []int) ([]string, error), f2 func(context.Context, []string) ([]string, error)) (*Component, []*Pipe, []*Pipe) {
	inP1 := NewPipe("input1", bp, l)
	inP2 := NewPipe("input2", bp, l)
	outP1 := NewPipe("output1", bp, l)
	outP2 := NewPipe("output2", bp, l)
	handler1 := NewHandler(f1)
	handler2 := NewHandler(f2)
	couplings := []HandlerCoupling{
		{handler1, inP1, outP1},
		{handler2, inP2, outP2},
	}
	c := NewComponent(ctx, couplings, 1)
	return c, []*Pipe{inP1, inP2}, []*Pipe{outP1, outP2}
}
