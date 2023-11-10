// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package array_test

import (
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/internal/testing/tools"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/internal/types"
	"github.com/stretchr/testify/assert"
)

type testDataType struct {
	id arrow.Type
}

func (d *testDataType) ID() arrow.Type            { return d.id }
func (d *testDataType) Name() string              { panic("implement me") }
func (d *testDataType) BitWidth() int             { return 8 }
func (d *testDataType) Bytes() int                { return 1 }
func (d *testDataType) Fingerprint() string       { return "" }
func (testDataType) Layout() arrow.DataTypeLayout { return arrow.DataTypeLayout{} }
func (testDataType) String() string               { return "" }

func TestMakeFromData(t *testing.T) {
	tests := []struct {
		name     string
		d        arrow.DataType
		size     int
		child    []arrow.ArrayData
		dict     *array.Data
		expPanic bool
		expError string
	}{
		// supported types
		{name: "null", d: &testDataType{arrow.NULL}},
		{name: "bool", d: &testDataType{arrow.BOOL}},
		{name: "uint8", d: &testDataType{arrow.UINT8}},
		{name: "uint16", d: &testDataType{arrow.UINT16}},
		{name: "uint32", d: &testDataType{arrow.UINT32}},
		{name: "uint64", d: &testDataType{arrow.UINT64}},
		{name: "int8", d: &testDataType{arrow.INT8}},
		{name: "int16", d: &testDataType{arrow.INT16}},
		{name: "int32", d: &testDataType{arrow.INT32}},
		{name: "int64", d: &testDataType{arrow.INT64}},
		{name: "float16", d: &testDataType{arrow.FLOAT16}},
		{name: "float32", d: &testDataType{arrow.FLOAT32}},
		{name: "float64", d: &testDataType{arrow.FLOAT64}},
		{name: "string", d: &testDataType{arrow.STRING}, size: 3},
		{name: "binary", d: &testDataType{arrow.BINARY}, size: 3},
		{name: "large_string", d: &testDataType{arrow.LARGE_STRING}, size: 3},
		{name: "large_binary", d: &testDataType{arrow.LARGE_BINARY}, size: 3},
		{name: "fixed_size_binary", d: &testDataType{arrow.FIXED_SIZE_BINARY}},
		{name: "date32", d: &testDataType{arrow.DATE32}},
		{name: "date64", d: &testDataType{arrow.DATE64}},
		{name: "timestamp", d: &testDataType{arrow.TIMESTAMP}},
		{name: "time32", d: &testDataType{arrow.TIME32}},
		{name: "time64", d: &testDataType{arrow.TIME64}},
		{name: "month_interval", d: arrow.FixedWidthTypes.MonthInterval},
		{name: "day_time_interval", d: arrow.FixedWidthTypes.DayTimeInterval},
		{name: "decimal128", d: &testDataType{arrow.DECIMAL128}},
		{name: "decimal256", d: &testDataType{arrow.DECIMAL256}},
		{name: "month_day_nano_interval", d: arrow.FixedWidthTypes.MonthDayNanoInterval},

		{name: "list", d: &testDataType{arrow.LIST}, child: []arrow.ArrayData{
			array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
			array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
		}},

		{name: "large list", d: &testDataType{arrow.LARGE_LIST}, child: []arrow.ArrayData{
			array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
			array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
		}},

		{name: "struct", d: &testDataType{arrow.STRUCT}},
		{name: "struct", d: &testDataType{arrow.STRUCT}, child: []arrow.ArrayData{
			array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
			array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
		}},

		{name: "fixed_size_list", d: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Int64), child: []arrow.ArrayData{
			array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
			array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
		}},
		{name: "duration", d: &testDataType{arrow.DURATION}},

		{name: "map", d: &testDataType{arrow.MAP}, child: []arrow.ArrayData{
			array.NewData(&testDataType{arrow.STRUCT}, 0 /* length */, make([]*memory.Buffer, 3 /*null bitmap, values, offsets*/), []arrow.ArrayData{
				array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
				array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
			}, 0 /* nulls */, 0 /* offset */)},
		},

		{name: "sparse union", d: arrow.SparseUnionOf(nil, nil), child: []arrow.ArrayData{}, size: 2},
		{name: "dense union", d: arrow.DenseUnionOf(nil, nil), child: []arrow.ArrayData{}, size: 3},

		// various dictionary index types and value types
		{name: "dictionary", d: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: &testDataType{arrow.INT64}}, dict: array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */)},
		{name: "dictionary", d: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: &testDataType{arrow.INT32}}, dict: array.NewData(&testDataType{arrow.INT32}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */)},
		{name: "dictionary", d: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: &testDataType{arrow.UINT16}}, dict: array.NewData(&testDataType{arrow.UINT16}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */)},
		{name: "dictionary", d: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: &testDataType{arrow.INT64}}, dict: array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */)},
		{name: "dictionary", d: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: &testDataType{arrow.UINT32}}, dict: array.NewData(&testDataType{arrow.UINT32}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */)},
		{name: "dictionary", d: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint32, ValueType: &testDataType{arrow.TIMESTAMP}}, dict: array.NewData(&testDataType{arrow.TIMESTAMP}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */)},
		{name: "dictionary", d: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int64, ValueType: &testDataType{arrow.UINT32}}, dict: array.NewData(&testDataType{arrow.UINT32}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */)},
		{name: "dictionary", d: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint64, ValueType: &testDataType{arrow.TIMESTAMP}}, dict: array.NewData(&testDataType{arrow.TIMESTAMP}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */)},

		{name: "extension", d: &testDataType{arrow.EXTENSION}, expPanic: true, expError: "arrow/array: DataType for ExtensionArray must implement arrow.ExtensionType"},
		{name: "extension", d: types.NewUUIDType()},

		{name: "run end encoded", d: arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64), child: []arrow.ArrayData{
			array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
			array.NewData(&testDataType{arrow.INT64}, 0 /* length */, make([]*memory.Buffer, 2 /*null bitmap, values*/), nil /* childData */, 0 /* nulls */, 0 /* offset */),
		}},

		// invalid types
		{name: "invalid(-1)", d: &testDataType{arrow.Type(-1)}, expPanic: true, expError: "invalid data type: Type(-1)"},
		{name: "invalid(63)", d: &testDataType{arrow.Type(63)}, expPanic: true, expError: "invalid data type: Type(63)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				b    [4]*memory.Buffer
				n    = 4
				data arrow.ArrayData
			)
			if test.size != 0 {
				n = test.size
			}
			if test.dict != nil {
				data = array.NewDataWithDictionary(test.d, 0, b[:n], 0, 0, test.dict)
			} else {
				data = array.NewData(test.d, 0, b[:n], test.child, 0, 0)
			}

			if test.expPanic {
				assert.PanicsWithValue(t, test.expError, func() {
					array.MakeFromData(data)
				})
			} else {
				assert.NotNil(t, array.MakeFromData(data))
			}
		})
	}
}

func bbits(v ...int32) []byte {
	return tools.IntsToBitsLSB(v...)
}

func TestArray_NullN(t *testing.T) {
	tests := []struct {
		name string
		l    int
		bm   []byte
		n    int
		exp  int
	}{
		{name: "unknown,l16", l: 16, bm: bbits(0x11001010, 0x00110011), n: array.UnknownNullCount, exp: 8},
		{name: "unknown,l12,ignores last nibble", l: 12, bm: bbits(0x11001010, 0x00111111), n: array.UnknownNullCount, exp: 6},
		{name: "unknown,l12,12 nulls", l: 12, bm: bbits(0x00000000, 0x00000000), n: array.UnknownNullCount, exp: 12},
		{name: "unknown,l12,00 nulls", l: 12, bm: bbits(0x11111111, 0x11111111), n: array.UnknownNullCount, exp: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := memory.NewBufferBytes(test.bm)
			data := array.NewData(arrow.FixedWidthTypes.Boolean, test.l, []*memory.Buffer{buf, nil}, nil, test.n, 0)
			buf.Release()
			ar := array.MakeFromData(data)
			data.Release()
			got := ar.NullN()
			ar.Release()
			assert.Equal(t, test.exp, got)
		})
	}
}

func TestArraySlice(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		valids = []bool{true, true, true, false, true, true}
		vs     = []float64{1, 2, 3, 0, 4, 5}
	)

	b := array.NewFloat64Builder(pool)
	defer b.Release()

	for _, tc := range []struct {
		i, j   int
		panics bool
		len    int
	}{
		{i: 0, j: len(valids), panics: false, len: len(valids)},
		{i: len(valids), j: len(valids), panics: false, len: 0},
		{i: 0, j: 1, panics: false, len: 1},
		{i: 1, j: 1, panics: false, len: 0},
		{i: 0, j: len(valids) + 1, panics: true},
		{i: 2, j: 1, panics: true},
		{i: len(valids) + 1, j: len(valids) + 1, panics: true},
	} {
		t.Run("", func(t *testing.T) {
			b.AppendValues(vs, valids)

			arr := b.NewFloat64Array()
			defer arr.Release()

			if got, want := arr.Len(), len(valids); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			if tc.panics {
				defer func() {
					e := recover()
					if e == nil {
						t.Fatalf("this should have panicked, but did not")
					}
				}()
			}

			slice := array.NewSlice(arr, int64(tc.i), int64(tc.j)).(*array.Float64)
			defer slice.Release()

			if got, want := slice.Len(), tc.len; got != want {
				t.Fatalf("invalid slice length: got=%d, want=%d", got, want)
			}
		})
	}
}

func TestArraySliceTypes(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	valids := []bool{true, true, true, false, true, true}

	for _, tc := range []struct {
		values  interface{}
		builder array.Builder
		append  func(b array.Builder, vs interface{})
	}{
		{
			values:  []bool{true, false, true, false, true, false},
			builder: array.NewBooleanBuilder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.BooleanBuilder).AppendValues(vs.([]bool), valids) },
		},
		{
			values:  []uint8{1, 2, 3, 0, 4, 5},
			builder: array.NewUint8Builder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.Uint8Builder).AppendValues(vs.([]uint8), valids) },
		},
		{
			values:  []uint16{1, 2, 3, 0, 4, 5},
			builder: array.NewUint16Builder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.Uint16Builder).AppendValues(vs.([]uint16), valids) },
		},
		{
			values:  []uint32{1, 2, 3, 0, 4, 5},
			builder: array.NewUint32Builder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.Uint32Builder).AppendValues(vs.([]uint32), valids) },
		},
		{
			values:  []uint64{1, 2, 3, 0, 4, 5},
			builder: array.NewUint64Builder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.Uint64Builder).AppendValues(vs.([]uint64), valids) },
		},
		{
			values:  []int8{1, 2, 3, 0, 4, 5},
			builder: array.NewInt8Builder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.Int8Builder).AppendValues(vs.([]int8), valids) },
		},
		{
			values:  []int16{1, 2, 3, 0, 4, 5},
			builder: array.NewInt16Builder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.Int16Builder).AppendValues(vs.([]int16), valids) },
		},
		{
			values:  []int32{1, 2, 3, 0, 4, 5},
			builder: array.NewInt32Builder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.Int32Builder).AppendValues(vs.([]int32), valids) },
		},
		{
			values:  []int64{1, 2, 3, 0, 4, 5},
			builder: array.NewInt64Builder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.Int64Builder).AppendValues(vs.([]int64), valids) },
		},
		{
			values:  []float32{1, 2, 3, 0, 4, 5},
			builder: array.NewFloat32Builder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.Float32Builder).AppendValues(vs.([]float32), valids) },
		},
		{
			values:  []float64{1, 2, 3, 0, 4, 5},
			builder: array.NewFloat64Builder(pool),
			append:  func(b array.Builder, vs interface{}) { b.(*array.Float64Builder).AppendValues(vs.([]float64), valids) },
		},
	} {
		t.Run("", func(t *testing.T) {
			defer tc.builder.Release()

			b := tc.builder
			tc.append(b, tc.values)

			arr := b.NewArray()
			defer arr.Release()

			if got, want := arr.Len(), len(valids); got != want {
				t.Fatalf("invalid length: got=%d, want=%d", got, want)
			}

			slice := array.NewSlice(arr, 2, 5)
			defer slice.Release()

			if got, want := slice.Len(), 3; got != want {
				t.Fatalf("invalid slice length: got=%d, want=%d", got, want)
			}

			shortSlice := array.NewSlice(arr, 2, 3)
			defer shortSlice.Release()

			sliceOfShortSlice := array.NewSlice(shortSlice, 0, 1)
			defer sliceOfShortSlice.Release()

			if got, want := sliceOfShortSlice.Len(), 1; got != want {
				t.Fatalf("invalid short slice length: got=%d, want=%d", got, want)
			}
		})
	}
}
