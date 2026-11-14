package flow

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"reflect"
	"strconv"
	"testing"
	"uuid"

	"github.com/stretchr/testify/assert"
)

var hCtx = handlerContext{
	Logger: slog.Default(),

	currentFlow: "",
	nextFlows:   []string{""},
}

func TestHandle_PassesValuesCorrectly_ForMinNumericalValues(t *testing.T) {
	testNumericalExtremes(t, minNumericalValues)
}

func TestHandle_PassesValuesCorrectly_ForMaxNumericalValues(t *testing.T) {
	testNumericalExtremes(t, maxNumericalValues)
}

func TestHandle_PassesValuesCorrectly_ForNonNumericalValues(t *testing.T) {
	v := otherValues
	testValue[bool, string](t, v)
	testValue[string, testStruct](t, v)
	testValue[testStruct, testInterface](t, v)
	testValue[testInterface, *testStruct](t, v)
	testValue[*testStruct, [5]int](t, v)
	testValue[[5]int, []*testStruct](t, v)
	testValue[[]*testStruct, map[string]testStruct](t, v)
	testValue[map[string]testStruct, func(string) *testStruct](t, v)
	testValue[func(string) *testStruct, chan func(string) *testStruct](t, v)
	testValue[chan func(string) *testStruct, bool](t, v)
}

func buildHandlerMessages[T any](values []T) []*message {
	msgs := make([]*message, len(values))
	for i, v := range values {
		msgs[i] = &message{
			id:    uuid.New().String(),
			value: v,
		}
	}
	return msgs
}

func TestHandle_PassesMultipleValuesCorrectly(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1, 2, 3, 4, 5})
	expectedOut := []string{
		"1", "2", "3", "4", "5",
	}
	handleFunc := func(_ context.Context, c HandlerContext[int, string], i []*Message[int]) []*HandlerResult[string] {
		out := make([]*HandlerResult[string], len(i))
		for i, v := range i {
			r := strconv.Itoa(v.Value())
			out[i] = c.Success(v, r)
		}
		return out
	}
	handler := newBatchHandler("myHandler", handleFunc)

	// Act
	result := handler.handle(t.Context(), hCtx, in)

	// Assert
	for i, v := range result {
		conv, ok := v.Results[0].Value.(string)
		assert.True(t, ok)
		assert.Equal(t, expectedOut[i], conv)
	}
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithZeroOutputs_WithNoError(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1})
	handleFunc := func(_ context.Context, c EndHandlerContext[int], i *Message[int]) *EndHandlerResult {
		return nil
	}
	handler := newEndHandler("myHandler", handleFunc)

	// Act
	result := handler.handle(t.Context(), hCtx, in)

	// Assert
	assert.Len(t, result, 0)
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithZeroOutputs_WithError(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1})
	expectedErr := errors.New("test error")
	handleFunc := func(_ context.Context, c EndHandlerContext[int], i *Message[int]) *EndHandlerResult {
		return c.Error(i, expectedErr)
	}
	handler := newEndHandler("myHandler", handleFunc)

	// Act
	result := handler.handle(t.Context(), hCtx, in)

	// Assert
	assert.Len(t, result, 1)
	assert.Error(t, result[0].Error)
	assert.Equal(t, expectedErr, result[0].Error)
	assert.Len(t, result[0].Results, 0)
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithZeroOutputs_WithNoErrors(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1, 2, 3, 4, 5})
	handleFunc := func(_ context.Context, c EndHandlerContext[int], i []*Message[int]) []*EndHandlerResult {
		return nil
	}
	handler := newBatchEndHandler("myHandler", handleFunc)

	// Act
	result := handler.handle(t.Context(), hCtx, in)

	// Assert
	assert.Len(t, result, 0)
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithZeroOutputs_WithAllErrors(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1, 2, 3, 4, 5})
	expectedErr := errors.New("test error")
	handleFunc := func(_ context.Context, c EndHandlerContext[int], i []*Message[int]) []*EndHandlerResult {
		failures := make([]*EndHandlerResult, len(i))
		for i, v := range i {
			failures[i] = c.Error(v, expectedErr)
		}
		return failures
	}
	handler := newBatchEndHandler("myHandler", handleFunc)

	// Act
	result := handler.handle(t.Context(), hCtx, in)

	// Assert
	assert.Len(t, result, 5)

	for i, v := range in {
		assert.Equal(t, v.id, result[i].Id)
		assert.Equal(t, expectedErr, result[i].Error)
		assert.Empty(t, result[i].Results)
	}
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithZeroOutputs_WithPartialErrors(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1, 2, 3, 4})
	expectedErr := errors.New("test error")
	handleFunc := func(_ context.Context, c EndHandlerContext[int], i []*Message[int]) []*EndHandlerResult {
		failures := make([]*EndHandlerResult, 0)
		for _, v := range i {
			if v.Value() <= 2 {
				failures = append(failures, c.Error(v, expectedErr))
			}
		}
		return failures
	}
	handler := newBatchEndHandler("myHandler", handleFunc)

	// Act
	result := handler.handle(t.Context(), hCtx, in)

	// Assert
	assert.Len(t, result, 2)

	for i := range 2 {
		assert.Equal(t, in[i].id, result[i].Id)
		assert.Equal(t, expectedErr, result[i].Error)
		assert.Empty(t, result[i].Results)
	}
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithSingleOutput_WithNoError(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1})
	expectedOut := "1"
	handleFunc := func(_ context.Context, c HandlerContext[int, string], i *Message[int]) *HandlerResult[string] {
		return c.Success(i, strconv.Itoa(i.Value()))
	}
	handler := newHandler("myHandler", handleFunc)

	// Act
	result := handler.handle(t.Context(), hCtx, in)

	// Assert
	assert.Len(t, result, 1)
	assert.Equal(t, in[0].id, result[0].Id)
	assert.Equal(t, []ResultValue{{Value: expectedOut, FlowName: hCtx.currentFlow}}, result[0].Results)
	assert.NoError(t, result[0].Error)
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithSingleOutput_WithError(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1})
	expectedErr := errors.New("test error")
	handleFunc := func(_ context.Context, c HandlerContext[int, string], i *Message[int]) *HandlerResult[string] {
		return c.Error(i, expectedErr)
	}
	handler := newHandler("myHandler", handleFunc)

	// Act
	result := handler.handle(t.Context(), hCtx, in)

	// Assert
	assert.Len(t, result, 1)
	assert.Equal(t, in[0].id, result[0].Id)
	assert.Equal(t, expectedErr, result[0].Error)
	assert.Len(t, result[0].Results, 0)
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithSingleOutput_WithNoErrors(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1, 2, 3, 4, 5})
	expectedOut := []string{"1", "2", "3", "4", "5"}
	handleFunc := func(_ context.Context, c HandlerContext[int, string], i []*Message[int]) []*HandlerResult[string] {
		result := make([]*HandlerResult[string], len(i))
		for i, v := range i {
			result[i] = c.Success(v, strconv.Itoa(v.Value()))
		}
		return result
	}
	handler := newBatchHandler("myHandler", handleFunc)

	// Act
	result := handler.handle(t.Context(), hCtx, in)

	// Assert
	assert.Len(t, result, 5)
	for i, v := range result {
		assert.Equal(t, in[i].id, v.Id)
		assert.Equal(t, []ResultValue{{Value: expectedOut[i], FlowName: hCtx.currentFlow}}, v.Results)
		assert.NoError(t, v.Error)
	}
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithSingleOutput_WithAllErrors(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1, 2, 3, 4, 5})
	expectedErr := errors.New("test error")
	handleFunc := func(_ context.Context, c HandlerContext[int, string], i []*Message[int]) []*HandlerResult[string] {
		failures := make([]*HandlerResult[string], len(i))
		for i, v := range i {
			failures[i] = c.Error(v, expectedErr)
		}
		return failures
	}
	handler := newBatchHandler("myHandler", handleFunc)

	// Act
	result := handler.handle(t.Context(), hCtx, in)

	// Assert
	assert.Len(t, result, 5)
	for i, v := range result {
		assert.Equal(t, expectedErr, v.Error)
		assert.Equal(t, in[i].id, v.Id)
		assert.Len(t, v.Results, 0)
	}
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithSingleOutput_WithPartialErrors(t *testing.T) {
	// Arrange
	in := buildHandlerMessages([]int{1, 2, 3, 4})
	expectedErr := errors.New("test error")
	expectedOut := []string{"3", "4"}
	handleFunc := func(_ context.Context, c HandlerContext[int, string], i []*Message[int]) []*HandlerResult[string] {
		result := make([]*HandlerResult[string], len(i))
		for i, v := range i {
			if v.Value() <= 2 {
				result[i] = c.Error(v, expectedErr)
			} else {
				result[i] = c.Success(v, strconv.Itoa(v.Value()))
			}
		}
		return result
	}
	handler := newBatchHandler("myHandler", handleFunc)

	// Act
	result := handler.handle(t.Context(), hCtx, in)

	// Assert
	assert.Len(t, result, 4)
	for i := range 2 {
		assert.Equal(t, in[i].id, result[i].Id)
		assert.Equal(t, expectedErr, result[i].Error)
		assert.Len(t, result[i].Results, 0)
	}
	for i := range 2 {
		assert.Equal(t, in[i+2].id, result[i+2].Id)
		assert.NoError(t, result[i+2].Error)
		assert.Equal(t, []ResultValue{{Value: expectedOut[i], FlowName: hCtx.currentFlow}}, result[i+2].Results)
	}
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithMultipleOutputs_WithNoError(t *testing.T) {
	// Arrange
	in1 := buildHandlerMessages([]int{1})
	in2 := buildHandlerMessages([]int{2})
	expectedOut1 := 1
	expectedOut2 := "2"
	handleFunc := func(_ context.Context, c SplitHandlerContext[int], i *Message[int]) *SplitHandlerResult {
		if i.Value() == 1 {
			return c.Success(i, i.Value(), "integers")
		} else {
			return c.Success(i, strconv.Itoa(i.Value()), "strings")
		}
	}
	handler := newSplitHandler[int, any]("myHandler", handleFunc)

	// Act
	result1 := handler.handle(t.Context(), hCtx, in1)
	result2 := handler.handle(t.Context(), hCtx, in2)

	// Assert
	assert.Len(t, result1, 1)
	assert.Equal(t, in1[0].id, result1[0].Id)
	assert.NoError(t, result1[0].Error)
	assert.Equal(t, []ResultValue{{Value: expectedOut1, FlowName: "integers"}}, result1[0].Results)

	assert.Len(t, result2, 1)
	assert.Equal(t, in2[0].id, result2[0].Id)
	assert.NoError(t, result2[0].Error)
	assert.Equal(t, []ResultValue{{Value: expectedOut2, FlowName: "strings"}}, result2[0].Results)
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithMultipleOutputs_WithError(t *testing.T) {
	// Arrange
	in1 := buildHandlerMessages([]int{1})
	in2 := buildHandlerMessages([]int{2})
	expectedErr1 := errors.New("test error 1")
	expectedErr2 := errors.New("test error 2")
	handleFunc := func(_ context.Context, c SplitHandlerContext[int], i *Message[int]) *SplitHandlerResult {
		if i.Value() == 1 {
			return c.Error(i, expectedErr1)
		} else {
			return c.Error(i, expectedErr2)
		}
	}
	handler := newSplitHandler[int, any]("myHandler", handleFunc)

	// Act
	result1 := handler.handle(t.Context(), hCtx, in1)
	result2 := handler.handle(t.Context(), hCtx, in2)

	// Assert
	assert.Len(t, result1, 1)
	assert.Equal(t, expectedErr1, result1[0].Error)
	assert.Equal(t, in1[0].id, result1[0].Id)
	assert.Empty(t, result1[0].Results)

	assert.Len(t, result2, 1)
	assert.Equal(t, expectedErr2, result2[0].Error)
	assert.Equal(t, in2[0].id, result2[0].Id)
	assert.Empty(t, result2[0].Results)
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithMultipleOutputs_WithNoError(t *testing.T) {
	// Arrange
	in1 := buildHandlerMessages([]int{1, 2, 3, 4, 5})
	in2 := buildHandlerMessages([]int{6, 7, 8, 9, 10})
	expectedOut1 := []int{1, 2, 3, 4, 5}
	expectedOut2 := []string{"6", "7", "8", "9", "10"}
	handleFunc := func(_ context.Context, c SplitHandlerContext[int], i []*Message[int]) []*SplitHandlerResult {
		result := make([]*SplitHandlerResult, len(i))
		for i, v := range i {
			if v.Value() <= 5 {
				result[i] = c.Success(v, v.Value(), "integers")
			} else {
				result[i] = c.Success(v, strconv.Itoa(v.Value()), "strings")
			}
		}

		return result
	}
	handler := newBatchSplitHandler("myHandler", handleFunc)

	// Act
	result1 := handler.handle(t.Context(), hCtx, in1)
	result2 := handler.handle(t.Context(), hCtx, in2)

	// Assert
	assert.Len(t, result1, 5)
	for i := range in1 {
		assert.Equal(t, in1[i].id, result1[i].Id)
		assert.Equal(t, []ResultValue{{Value: expectedOut1[i], FlowName: "integers"}}, result1[i].Results)
		assert.NoError(t, result1[i].Error)
	}

	assert.Len(t, result2, 5)
	for i := range in2 {
		assert.Equal(t, in2[i].id, result2[i].Id)
		assert.Equal(t, []ResultValue{{Value: expectedOut2[i], FlowName: "strings"}}, result2[i].Results)
		assert.NoError(t, result2[i].Error)
	}
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithMultipleOutputs_WithAllErrors(t *testing.T) {
	// Arrange
	in1 := buildHandlerMessages([]int{1, 2, 3, 4, 5})
	in2 := buildHandlerMessages([]int{6, 7, 8, 9, 10})
	expectedErr1 := errors.New("test error 1")
	expectedErr2 := errors.New("test error 2")
	handleFunc := func(_ context.Context, c SplitHandlerContext[int], i []*Message[int]) []*SplitHandlerResult {
		result := make([]*SplitHandlerResult, len(i))
		for i, v := range i {
			if v.Value() <= 5 {
				result[i] = c.Error(v, expectedErr1)
			} else {
				result[i] = c.Error(v, expectedErr2)
			}
		}

		return result
	}
	handler := newBatchSplitHandler("myHandler", handleFunc)

	// Act
	result1 := handler.handle(t.Context(), hCtx, in1)
	result2 := handler.handle(t.Context(), hCtx, in2)

	// Assert
	assert.Len(t, result1, 5)
	for i := range in1 {
		assert.Equal(t, expectedErr1, result1[i].Error)
		assert.Equal(t, in1[i].id, result1[i].Id)
		assert.Empty(t, result1[i].Results)
	}

	assert.Len(t, result2, 5)
	for i := range in2 {
		assert.Equal(t, expectedErr2, result2[i].Error)
		assert.Equal(t, in2[i].id, result2[i].Id)
		assert.Empty(t, result2[i].Results)
	}
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithMultipleOutputs_WithPartialErrors(t *testing.T) {
	// Arrange
	in1 := buildHandlerMessages([]int{1, 2, 3, 4})
	in2 := buildHandlerMessages([]int{6, 7, 8, 9})
	expectedErr1 := errors.New("test error 1")
	expectedOut1 := []int{3, 4}
	expectedErr2 := errors.New("test error 2")
	expectedOut2 := []string{"8", "9"}
	handleFunc := func(_ context.Context, c SplitHandlerContext[int], i []*Message[int]) []*SplitHandlerResult {
		result := make([]*SplitHandlerResult, len(i))
		for i, v := range i {
			if v.Value() <= 4 {
				if v.Value() <= 2 {
					result[i] = c.Error(v, expectedErr1)
				} else {
					result[i] = c.Success(v, v.Value(), "integers")
				}
			} else {
				if v.Value() <= 7 {
					result[i] = c.Error(v, expectedErr2)
				} else {
					result[i] = c.Success(v, strconv.Itoa(v.Value()), "strings")
				}
			}
		}

		return result
	}
	handler := newBatchSplitHandler("myHandler", handleFunc)

	// Act
	result1 := handler.handle(t.Context(), hCtx, in1)
	result2 := handler.handle(t.Context(), hCtx, in2)

	// Assert
	assert.Len(t, result1, 4)
	for i := range 2 {
		assert.Equal(t, in1[i].id, result1[i].Id)
		assert.Equal(t, expectedErr1, result1[i].Error)
		assert.Empty(t, result1[i].Results)
	}
	for i := range 2 {
		assert.Equal(t, in1[i+2].id, result1[i+2].Id)
		assert.NoError(t, result1[i+2].Error)
		assert.Equal(t, []ResultValue{{Value: expectedOut1[i], FlowName: "integers"}}, result1[i+2].Results)
	}

	assert.Len(t, result2, 4)
	for i := range 2 {
		assert.Equal(t, in2[i].id, result2[i].Id)
		assert.Equal(t, expectedErr2, result2[i].Error)
		assert.Empty(t, result2[i].Results)
	}
	for i := range 2 {
		assert.Equal(t, in2[i+2].id, result2[i+2].Id)
		assert.NoError(t, result2[i+2].Error)
		assert.Equal(t, []ResultValue{{Value: expectedOut2[i], FlowName: "strings"}}, result2[i+2].Results)
	}
}

func testNumericalExtremes(t *testing.T, v map[reflect.Type]any) {
	testValue[uint, uint8](t, v)
	testValue[uint8, uint16](t, v)
	testValue[uint16, uint32](t, v)
	testValue[uint32, uint64](t, v)
	testValue[uint64, int](t, v)
	testValue[int, int8](t, v)
	testValue[int8, int16](t, v)
	testValue[int16, int32](t, v)
	testValue[int32, int64](t, v)
	testValue[int64, float32](t, v)
	testValue[float32, float64](t, v)
	testValue[float64, uint](t, v)
}

func testValue[Tin any, Tout any](t *testing.T, values map[reflect.Type]any) {
	// Arrange
	input := values[reflect.TypeFor[Tin]()].(Tin)
	expectedOutput := values[reflect.TypeFor[Tout]()].(Tout)

	h := newTestHandler[Tin, Tout](t, values)
	h.expectedInput = input
	if reflect.TypeFor[Tin]().Kind() == reflect.Func {
		// Don't assert function equality - it can't be done
		h.assertInputValue = false
	}
	handler := newHandler("myHandler", h.handleTestInput)
	in := buildHandlerMessages([]Tin{input})

	// Act
	output := handler.handle(t.Context(), hCtx, in)

	// Assert
	assert.Len(t, output, 1)
	assert.Equal(t, in[0].id, output[0].Id)
	assert.NoError(t, output[0].Error)
	assert.Len(t, output[0].Results, 1)
	assert.Equal(t, hCtx.currentFlow, output[0].Results[0].FlowName)
	oConv, ok := output[0].Results[0].Value.(Tout)
	assert.True(t, ok)
	if reflect.TypeFor[Tout]().Kind() != reflect.Func {
		assert.True(t, reflect.DeepEqual(expectedOutput, oConv))
	}
}

// ------------------------------------------------------------------------------------------------------------------ //

type testHandler[Tin any, Tout any] struct {
	t                *testing.T
	values           map[reflect.Type]any
	expectedInput    Tin
	assertInputValue bool
}

func newTestHandler[Tin any, Tout any](t *testing.T, values map[reflect.Type]any) *testHandler[Tin, Tout] {
	return &testHandler[Tin, Tout]{
		t:                t,
		values:           values,
		assertInputValue: true,
	}
}

func (h *testHandler[Tin, Tout]) handleTestInput(_ context.Context, c HandlerContext[Tin, Tout], in *Message[Tin]) *HandlerResult[Tout] {
	assert.Equal(h.t, reflect.TypeOf(h.expectedInput), reflect.TypeOf(in.Value()))
	if h.assertInputValue {
		assert.Equal(h.t, h.expectedInput, in.Value())
	}

	return c.Success(in, h.values[reflect.TypeFor[Tout]()].(Tout))
}

// ------------------------------------------------------------------------------------------------------------------ //

var minNumericalValues = map[reflect.Type]any{
	reflect.TypeFor[uint]():    uint(0),
	reflect.TypeFor[uint8]():   uint8(0),
	reflect.TypeFor[uint16]():  uint16(0),
	reflect.TypeFor[uint32]():  uint32(0),
	reflect.TypeFor[uint64]():  uint64(0),
	reflect.TypeFor[int]():     int(math.MinInt),
	reflect.TypeFor[int8]():    int8(math.MinInt8),
	reflect.TypeFor[int16]():   int16(math.MinInt16),
	reflect.TypeFor[int32]():   int32(math.MinInt32),
	reflect.TypeFor[int64]():   int64(math.MinInt64),
	reflect.TypeFor[float32](): float32(-math.MaxFloat32),
	reflect.TypeFor[float64](): float64(-math.MaxFloat64),
}

var maxNumericalValues = map[reflect.Type]any{
	reflect.TypeFor[uint]():    uint(math.MaxUint),
	reflect.TypeFor[uint8]():   uint8(math.MaxUint8),
	reflect.TypeFor[uint16]():  uint16(math.MaxUint16),
	reflect.TypeFor[uint32]():  uint32(math.MaxUint32),
	reflect.TypeFor[uint64]():  uint64(math.MaxUint64),
	reflect.TypeFor[int]():     int(math.MaxInt),
	reflect.TypeFor[int8]():    int8(math.MaxInt8),
	reflect.TypeFor[int16]():   int16(math.MaxInt16),
	reflect.TypeFor[int32]():   int32(math.MaxInt32),
	reflect.TypeFor[int64]():   int64(math.MaxInt64),
	reflect.TypeFor[float32](): float32(math.MaxFloat32),
	reflect.TypeFor[float64](): float64(math.MaxFloat64),
}

var structValue = testStruct{"foo"}
var pointerValue = &structValue
var arrayValue = [5]int{1, 2, 3, 4, 5}
var sliceValue = []*testStruct{
	{"foo"},
	{"bar"},
}
var mapValue = map[string]testStruct{
	"foo": {"foo"},
	"bar": {"bar"},
}
var funcValue = func(s string) *testStruct {
	return &testStruct{s}
}
var chanValue = make(chan func(string) *testStruct)
var otherValues = map[reflect.Type]any{
	reflect.TypeFor[bool]():                          true,
	reflect.TypeFor[string]():                        uuid.New().String(),
	reflect.TypeFor[testStruct]():                    structValue,
	reflect.TypeFor[testInterface]():                 &structValue,
	reflect.TypeFor[*testStruct]():                   pointerValue,
	reflect.TypeFor[[5]int]():                        arrayValue,
	reflect.TypeFor[[]*testStruct]():                 sliceValue,
	reflect.TypeFor[map[string]testStruct]():         mapValue,
	reflect.TypeFor[func(string) *testStruct]():      funcValue,
	reflect.TypeFor[chan func(string) *testStruct](): chanValue,
}

type testStruct struct {
	str string
}

func (t *testStruct) String() string {
	return t.str
}

type testInterface interface {
	String() string
}
