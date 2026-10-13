package flow

import (
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
	handleFunc := func(i []int) ([]string, error) {
		out := make([]string, len(i))
		for i, v := range i {
			out[i] = strconv.Itoa(v)
		}
		return out, nil
	}
	handler := NewHandler(handleFunc)

	// Act
	result, _ := handler.Handle(in)

	// Assert
	for i, v := range result {
		conv, ok := v.(string)
		assert.True(t, ok)
		assert.Equal(t, expectedOut[i], conv)
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
	h.expectedInput = []Tin{input}
	if reflect.TypeFor[Tin]().Kind() == reflect.Func {
		// Don't assert function equality - it can't be done
		h.assertInputValue = false
	}
	handler := NewHandler[Tin, Tout](h.handleTestInput)

	// Act
	output, _ := handler.Handle([]any{input})

	// Assert
	oConv, ok := output[0].(Tout)
	assert.True(t, ok)
	if reflect.TypeFor[Tout]().Kind() != reflect.Func {
		assert.True(t, reflect.DeepEqual(expectedOutput, oConv))
	}
}

// ------------------------------------------------------------------------------------------------------------------ //

type testHandler[Tin any, Tout any] struct {
	t                *testing.T
	values           map[reflect.Type]any
	expectedInput    []Tin
	assertInputValue bool
}

func newTestHandler[Tin any, Tout any](t *testing.T, values map[reflect.Type]any) *testHandler[Tin, Tout] {
	return &testHandler[Tin, Tout]{
		t:                t,
		values:           values,
		assertInputValue: true,
	}
}

func (h *testHandler[Tin, Tout]) handleTestInput(in []Tin) ([]Tout, error) {
	for i, v := range in {
		assert.Equal(h.t, reflect.TypeOf(h.expectedInput[i]), reflect.TypeOf(v))
		if h.assertInputValue {
			assert.Equal(h.t, h.expectedInput[i], v)
		}
	}

	return []Tout{h.values[reflect.TypeFor[Tout]()].(Tout)}, nil
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
