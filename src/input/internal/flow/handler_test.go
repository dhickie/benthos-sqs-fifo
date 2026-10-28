package flow

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"testing"
	"uuid"

	"github.com/stretchr/testify/assert"
)

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

func TestHandle_PassesMultipleValuesCorrectly(t *testing.T) {
	// Arrange
	in := []any{
		1, 2, 3, 4, 5,
	}
	expectedOut := []string{
		"1", "2", "3", "4", "5",
	}
	handleFunc := func(_ context.Context, i []int) (*ValueBatchHandleResult[int, string], error) {
		out := make([]string, len(i))
		for i, v := range i {
			out[i] = strconv.Itoa(v)
		}
		return NewValueBatchHandleResult[int, string](out, nil), nil
	}
	handler := NewBatchHandler_1(handleFunc)

	// Act
	result, _ := handler.Handle(t.Context(), in)

	// Assert
	for i, v := range result[0] {
		conv, ok := v.output.(string)
		assert.True(t, ok)
		assert.Equal(t, expectedOut[i], conv)
	}
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithZeroOutputs_WithNoError(t *testing.T) {
	// Arrange
	in := []any{1}
	handleFunc := func(_ context.Context, i int) error {
		return nil
	}
	handler := NewHandler_0(handleFunc)

	// Act
	result, err := handler.Handle(t.Context(), in)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 1, len(result[0]))
	assert.Equal(t, false, result[0][0].ok)
	assert.Equal(t, 1, result[0][0].input)
	assert.Nil(t, result[0][0].output)
	assert.Nil(t, result[0][0].err)
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithZeroOutputs_WithError(t *testing.T) {
	// Arrange
	in := []any{1}
	expectedErr := errors.New("test error")
	handleFunc := func(_ context.Context, i int) error {
		return expectedErr
	}
	handler := NewHandler_0(handleFunc)

	// Act
	result, err := handler.Handle(t.Context(), in)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 1, len(result[0]))
	assert.Equal(t, expectedErr, result[0][0].err)
	assert.Equal(t, in[0], result[0][0].input)
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithZeroOutputs_WithNoErrors(t *testing.T) {
	// Arrange
	in := []any{1, 2, 3, 4, 5}
	handleFunc := func(_ context.Context, i []int) (*BatchHandleResult[int], error) {
		return NewBatchHandleResult[int](nil), nil
	}
	handler := NewBatchHandler_0(handleFunc)

	// Act
	result, err := handler.Handle(t.Context(), in)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 0, len(result[0]))
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithZeroOutputs_WithAllErrors(t *testing.T) {
	// Arrange
	in := []any{1, 2, 3, 4, 5}
	expectedErr := errors.New("test error")
	handleFunc := func(_ context.Context, i []int) (*BatchHandleResult[int], error) {
		failures := make([]*BatchFailure[int], len(i))
		for i, v := range i {
			failures[i] = NewBatchFailure(v, expectedErr)
		}
		return NewBatchHandleResult(failures), nil
	}
	handler := NewBatchHandler_0(handleFunc)

	// Act
	result, err := handler.Handle(t.Context(), in)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 5, len(result[0]))

	for i, v := range in {
		assert.Equal(t, v, result[0][i].input)
		assert.Equal(t, expectedErr, result[0][i].err)
	}
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithZeroOutputs_WithPartialErrors(t *testing.T) {
	// Arrange
	in := []any{1, 2, 3, 4}
	expectedErr := errors.New("test error")
	handleFunc := func(_ context.Context, i []int) (*BatchHandleResult[int], error) {
		failures := make([]*BatchFailure[int], 0)
		for _, v := range i {
			if v <= 2 {
				failures = append(failures, NewBatchFailure(v, expectedErr))
			}
		}
		return NewBatchHandleResult(failures), nil
	}
	handler := NewBatchHandler_0(handleFunc)

	// Act
	result, err := handler.Handle(t.Context(), in)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 2, len(result[0]))

	for i := range 2 {
		assert.Equal(t, in[i], result[0][i].input)
		assert.Equal(t, expectedErr, result[0][i].err)
	}
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithSingleOutput_WithNoError(t *testing.T) {
	// Arrange
	in := []any{1}
	expectedOut := "1"
	handleFunc := func(_ context.Context, i int) (*ValueHandleResult[string], error) {
		return NewValueHandleResult(strconv.Itoa(i), true), nil
	}
	handler := NewHandler_1(handleFunc)

	// Act
	result, err := handler.Handle(t.Context(), in)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 1, len(result[0]))
	assert.Equal(t, true, result[0][0].ok)
	assert.Equal(t, 1, result[0][0].input)
	assert.Equal(t, expectedOut, result[0][0].output)
	assert.Nil(t, result[0][0].err)
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithSingleOutput_WithError(t *testing.T) {
	// Arrange
	in := []any{1}
	expectedErr := errors.New("test error")
	handleFunc := func(_ context.Context, i int) (*ValueHandleResult[string], error) {
		return nil, expectedErr
	}
	handler := NewHandler_1(handleFunc)

	// Act
	result, err := handler.Handle(t.Context(), in)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 1, len(result[0]))
	assert.Equal(t, in[0], result[0][0].input)
	assert.Equal(t, expectedErr, result[0][0].err)
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithSingleOutput_WithNoErrors(t *testing.T) {
	// Arrange
	in := []any{1, 2, 3, 4, 5}
	expectedOut := []string{"1", "2", "3", "4", "5"}
	handleFunc := func(_ context.Context, i []int) (*ValueBatchHandleResult[int, string], error) {
		result := make([]string, len(i))
		for i, v := range i {
			result[i] = strconv.Itoa(v)
		}
		return NewValueBatchHandleResult[int](result, nil), nil
	}
	handler := NewBatchHandler_1(handleFunc)

	// Act
	result, err := handler.Handle(t.Context(), in)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 5, len(result[0]))
	for i, v := range result[0] {
		assert.Equal(t, expectedOut[i], v.output)
		assert.Equal(t, true, v.ok)
		assert.Nil(t, v.err)
		assert.Nil(t, v.input)
	}
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithSingleOutput_WithAllErrors(t *testing.T) {
	// Arrange
	in := []any{1, 2, 3, 4, 5}
	expectedErr := errors.New("test error")
	handleFunc := func(_ context.Context, i []int) (*ValueBatchHandleResult[int, string], error) {
		failures := make([]*BatchFailure[int], len(i))
		for i, v := range i {
			failures[i] = NewBatchFailure(v, expectedErr)
		}
		return NewValueBatchHandleResult[int, string](nil, failures), nil
	}
	handler := NewBatchHandler_1(handleFunc)

	// Act
	result, err := handler.Handle(t.Context(), in)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 5, len(result[0]))
	for i, v := range result[0] {
		assert.Equal(t, expectedErr, v.err)
		assert.Equal(t, in[i], v.input)
	}
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithSingleOutput_WithPartialErrors(t *testing.T) {
	// Arrange
	in := []any{1, 2, 3, 4}
	expectedErr := errors.New("test error")
	expectedOut := []string{"3", "4"}
	handleFunc := func(_ context.Context, i []int) (*ValueBatchHandleResult[int, string], error) {
		failures := make([]*BatchFailure[int], 0)
		output := make([]string, 0)
		for _, v := range i {
			if v <= 2 {
				failures = append(failures, NewBatchFailure(v, expectedErr))
			} else {
				output = append(output, strconv.Itoa(v))
			}
		}
		return NewValueBatchHandleResult[int, string](output, failures), nil
	}
	handler := NewBatchHandler_1(handleFunc)

	// Act
	result, err := handler.Handle(t.Context(), in)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 4, len(result[0]))
	// Successes are added first, errors are added second
	for i := range 2 {
		assert.Nil(t, result[0][i].err)
		assert.Equal(t, true, result[0][i].ok)
		assert.Equal(t, expectedOut[i], result[0][i].output)
	}
	for i := range 2 {
		assert.Equal(t, expectedErr, result[0][i+2].err)
		assert.Equal(t, in[i], result[0][i+2].input)
	}
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithMultipleOutputs_WithNoError(t *testing.T) {
	// Arrange
	in1 := []any{1}
	in2 := []any{2}
	expectedOut1 := 1
	expectedOut2 := "2"
	handleFunc := func(_ context.Context, i int) (*ValueHandleResult[int], *ValueHandleResult[string], error) {
		if i == 1 {
			return NewValueHandleResult(i, true), NewValueHandleResult("", false), nil
		} else {
			return NewValueHandleResult(0, false), NewValueHandleResult(strconv.Itoa(i), true), nil
		}
	}
	handler := NewHandler_2(handleFunc)

	// Act
	result1, err1 := handler.Handle(t.Context(), in1)
	result2, err2 := handler.Handle(t.Context(), in2)

	// Assert
	assert.NoError(t, err1)
	assert.Equal(t, 2, len(result1))
	assert.Equal(t, 1, len(result1[0]))
	assert.Equal(t, 1, len(result1[1]))
	assert.Equal(t, true, result1[0][0].ok)
	assert.Equal(t, false, result1[1][0].ok)
	assert.Equal(t, in1[0], result1[0][0].input)
	assert.Equal(t, in1[0], result1[1][0].input)
	assert.Equal(t, expectedOut1, result1[0][0].output)
	assert.Nil(t, result1[0][0].err)
	assert.Nil(t, result1[1][0].err)

	assert.NoError(t, err2)
	assert.Equal(t, 2, len(result2))
	assert.Equal(t, 1, len(result2[0]))
	assert.Equal(t, 1, len(result2[1]))
	assert.Equal(t, false, result2[0][0].ok)
	assert.Equal(t, true, result2[1][0].ok)
	assert.Equal(t, in2[0], result2[0][0].input)
	assert.Equal(t, in2[0], result2[1][0].input)
	assert.Equal(t, expectedOut2, result2[1][0].output)
	assert.Nil(t, result2[0][0].err)
	assert.Nil(t, result2[1][0].err)
}

func TestHandle_ReturnsCorrectResponse_ForHandlerWithMultipleOutputs_WithError(t *testing.T) {
	// Arrange
	in1 := []any{1}
	in2 := []any{2}
	expectedErr1 := errors.New("test error 1")
	expectedErr2 := errors.New("test error 2")
	handleFunc := func(_ context.Context, i int) (*ValueHandleResult[int], *ValueHandleResult[string], error) {
		if i == 1 {
			return nil, nil, expectedErr1
		} else {
			return nil, nil, expectedErr2
		}
	}
	handler := NewHandler_2(handleFunc)

	// Act
	result1, err1 := handler.Handle(t.Context(), in1)
	result2, err2 := handler.Handle(t.Context(), in2)

	// Assert
	assert.Error(t, err1)
	assert.Equal(t, 2, len(result1))
	assert.Equal(t, 1, len(result1[0]))
	assert.Equal(t, expectedErr1, result1[0][0].err)
	assert.Equal(t, expectedErr1, result1[1][0].err)
	assert.Equal(t, in1[0], result1[0][0].input)
	assert.Equal(t, in1[0], result1[1][0].input)

	assert.Error(t, err2)
	assert.Equal(t, 2, len(result2))
	assert.Equal(t, 1, len(result2[0]))
	assert.Equal(t, expectedErr2, result2[0][0].err)
	assert.Equal(t, expectedErr2, result2[1][0].err)
	assert.Equal(t, in2[0], result2[0][0].input)
	assert.Equal(t, in2[0], result2[1][0].input)
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithMultipleOutputs_WithNoError(t *testing.T) {
	// Arrange
	in1 := []any{1, 2, 3, 4, 5}
	in2 := []any{6, 7, 8, 9, 10}
	expectedOut1 := []int{1, 2, 3, 4, 5}
	expectedOut2 := []string{"6", "7", "8", "9", "10"}
	handleFunc := func(_ context.Context, i []int) (*ValueBatchHandleResult[int, int], *ValueBatchHandleResult[int, string], error) {
		result1 := make([]int, 0, len(i))
		result2 := make([]string, 0, len(i))
		for _, v := range i {
			if v <= 5 {
				result1 = append(result1, v)
			} else {
				result2 = append(result2, strconv.Itoa(v))
			}
		}

		return NewValueBatchHandleResult[int](result1, nil), NewValueBatchHandleResult[int](result2, nil), nil
	}
	handler := NewBatchHandler_2(handleFunc)

	// Act
	result1, err1 := handler.Handle(t.Context(), in1)
	result2, err2 := handler.Handle(t.Context(), in2)

	// Assert
	assert.NoError(t, err1)
	assert.Equal(t, 2, len(result1))
	assert.Equal(t, 5, len(result1[0]))
	assert.Equal(t, 0, len(result1[1]))
	for i := range in1 {
		assert.Equal(t, true, result1[0][i].ok)
		assert.Equal(t, expectedOut1[i], result1[0][i].output)
		assert.Nil(t, result1[0][i].err)
	}

	assert.NoError(t, err2)
	assert.Equal(t, 2, len(result2))
	assert.Equal(t, 0, len(result2[0]))
	assert.Equal(t, 5, len(result2[1]))
	for i := range in2 {
		assert.Equal(t, true, result2[1][i].ok)
		assert.Equal(t, expectedOut2[i], result2[1][i].output)
		assert.Nil(t, result2[1][i].err)
	}
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithMultipleOutputs_WithAllErrors(t *testing.T) {
	// Arrange
	in1 := []any{1, 2, 3, 4, 5}
	in2 := []any{6, 7, 8, 9, 10}
	expectedErr1 := errors.New("test error 1")
	expectedErr2 := errors.New("test error 2")
	handleFunc := func(_ context.Context, i []int) (*ValueBatchHandleResult[int, int], *ValueBatchHandleResult[int, string], error) {
		failures1 := make([]*BatchFailure[int], 0)
		failures2 := make([]*BatchFailure[int], 0)
		for _, v := range i {
			if v <= 5 {
				failures1 = append(failures1, NewBatchFailure(v, expectedErr1))
			} else {
				failures2 = append(failures2, NewBatchFailure(v, expectedErr2))
			}
		}

		return NewValueBatchHandleResult[int, int](nil, failures1), NewValueBatchHandleResult[int, string](nil, failures2), nil
	}
	handler := NewBatchHandler_2(handleFunc)

	// Act
	result1, err1 := handler.Handle(t.Context(), in1)
	result2, err2 := handler.Handle(t.Context(), in2)

	// Assert
	assert.NoError(t, err1)
	assert.Equal(t, 2, len(result1))
	assert.Equal(t, 5, len(result1[0]))
	assert.Equal(t, 0, len(result1[1]))
	for i := range in1 {
		assert.Equal(t, expectedErr1, result1[0][i].err)
		assert.Equal(t, in1[i], result1[0][i].input)
	}

	assert.NoError(t, err2)
	assert.Equal(t, 2, len(result2))
	assert.Equal(t, 0, len(result2[0]))
	assert.Equal(t, 5, len(result2[1]))
	for i := range in2 {
		assert.Equal(t, expectedErr2, result2[1][i].err)
		assert.Equal(t, in2[i], result2[1][i].input)
	}
}

func TestHandle_ReturnsCorrectResponse_ForBatchHandlerWithMultipleOutputs_WithPartialErrors(t *testing.T) {
	// Arrange
	in1 := []any{1, 2, 3, 4}
	in2 := []any{6, 7, 8, 9}
	expectedErr1 := errors.New("test error 1")
	expectedOut1 := []int{3, 4}
	expectedErr2 := errors.New("test error 2")
	expectedOut2 := []string{"8", "9"}
	handleFunc := func(_ context.Context, i []int) (*ValueBatchHandleResult[int, int], *ValueBatchHandleResult[int, string], error) {
		failures1 := make([]*BatchFailure[int], 0)
		success1 := make([]int, 0)
		failures2 := make([]*BatchFailure[int], 0)
		success2 := make([]string, 0)
		for _, v := range i {
			if v <= 4 {
				if v <= 2 {
					failures1 = append(failures1, NewBatchFailure(v, expectedErr1))
				} else {
					success1 = append(success1, v)
				}
			} else {
				if v <= 7 {
					failures2 = append(failures2, NewBatchFailure(v, expectedErr2))
				} else {
					success2 = append(success2, strconv.Itoa(v))
				}
			}
		}

		return NewValueBatchHandleResult(success1, failures1), NewValueBatchHandleResult(success2, failures2), nil
	}
	handler := NewBatchHandler_2(handleFunc)

	// Act
	result1, err1 := handler.Handle(t.Context(), in1)
	result2, err2 := handler.Handle(t.Context(), in2)

	// Assert
	assert.NoError(t, err1)
	assert.Equal(t, 2, len(result1))
	assert.Equal(t, 4, len(result1[0]))
	assert.Equal(t, 0, len(result1[1]))
	for i := range 2 {
		assert.Equal(t, true, result1[0][i].ok)
		assert.Equal(t, expectedOut1[i], result1[0][i].output)
		assert.Nil(t, result1[0][i].err)
	}
	for i := range 2 {
		assert.Equal(t, in1[i], result1[0][i+2].input)
		assert.Equal(t, expectedErr1, result1[0][i+2].err)
	}

	assert.NoError(t, err2)
	assert.Equal(t, 2, len(result2))
	assert.Equal(t, 0, len(result2[0]))
	assert.Equal(t, 4, len(result2[1]))
	for i := range 2 {
		assert.Equal(t, true, result2[1][i].ok)
		assert.Equal(t, expectedOut2[i], result2[1][i].output)
		assert.Nil(t, result2[1][i].err)
	}
	for i := range 2 {
		assert.Equal(t, in2[i], result2[1][i+2].input)
		assert.Equal(t, expectedErr2, result2[1][i+2].err)
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
	handler := NewHandler_1[Tin, Tout](h.handleTestInput)

	// Act
	output, _ := handler.Handle(t.Context(), []any{input})

	// Assert
	oConv, ok := output[0][0].output.(Tout)
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

func (h *testHandler[Tin, Tout]) handleTestInput(_ context.Context, in Tin) (*ValueHandleResult[Tout], error) {
	assert.Equal(h.t, reflect.TypeOf(h.expectedInput), reflect.TypeOf(in))
	if h.assertInputValue {
		assert.Equal(h.t, h.expectedInput, in)
	}

	return NewValueHandleResult(h.values[reflect.TypeFor[Tout]()].(Tout), true), nil
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
