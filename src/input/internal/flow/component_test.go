package flow

import (
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestComponent_CanReceiveAndCarryOutWork(t *testing.T) {
	// Arrange
	input := []int{1, 2, 3, 4, 5}
	expectedOutput := []string{"1", "2", "3", "4", "5"}
	_, inP, outP := createComponent(itoa)

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
	_, inP, outP := createComponentChain(itoa, reverse)

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
	input := []int{123, 456}
	expectedOutput := []string{"321", "654"}
	_, inP, outP := createMultiHandlerComponent(itoa, reverse)

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

func itoa(i []int) ([]string, error) {
	o := make([]string, len(i))
	for i, v := range i {
		o[i] = strconv.Itoa(v)
	}
	return o, nil
}

func reverse(str []string) ([]string, error) {
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

func createComponent(handleFunc func([]int) ([]string, error)) (*Component, *Pipe, *Pipe) {
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

	return NewComponent([]HandlerCoupling{coupling}, 1), inP, outP
}

func createComponentChain(f1 func([]int) ([]string, error), f2 func([]string) ([]string, error)) ([]*Component, *Pipe, *Pipe) {
	hc, pIn, pOut := createHandlerCouplings(f1, f2)

	c := []*Component{
		NewComponent([]HandlerCoupling{hc[0]}, 1),
		NewComponent([]HandlerCoupling{hc[1]}, 1),
	}
	return c, pIn, pOut
}

func createMultiHandlerComponent(f1 func([]int) ([]string, error), f2 func([]string) ([]string, error)) (*Component, *Pipe, *Pipe) {
	hc, pIn, pOut := createHandlerCouplings(f1, f2)

	c := NewComponent([]HandlerCoupling{hc[0], hc[1]}, 1)
	return c, pIn, pOut
}

func createHandlerCouplings(f1 func([]int) ([]string, error), f2 func([]string) ([]string, error)) ([]HandlerCoupling, *Pipe, *Pipe) {
	l := slog.Default()
	bp := NewBatchPolicy(1, time.Second)
	p1 := NewPipe("input", bp, l)
	p2 := NewPipe("intermediate", bp, l)
	p3 := NewPipe("output", bp, l)

	h1 := NewHandler(f1)
	h2 := NewHandler(f2)

	hc1 := HandlerCoupling{
		handler:    h1,
		inputPipe:  p1,
		outputPipe: p2,
	}
	hc2 := HandlerCoupling{
		handler:    h2,
		inputPipe:  p2,
		outputPipe: p3,
	}

	return []HandlerCoupling{hc1, hc2}, p1, p3
}
