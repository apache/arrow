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
	"math"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/float16"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/internal/json"
	"github.com/stretchr/testify/assert"
)

func TestNewFloat64Data(t *testing.T) {
	exp := []float64{1.0, 2.0, 4.0, 8.0, 16.0}

	ad := array.NewData(
		arrow.PrimitiveTypes.Float64, len(exp),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Float64Traits.CastToBytes(exp))},
		nil, 0, 0,
	)
	fa := array.NewFloat64Data(ad)

	assert.Equal(t, len(exp), fa.Len(), "unexpected Len()")
	assert.Equal(t, exp, fa.Float64Values(), "unexpected Float64Values()")
}

func TestFloat64SliceData(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const (
		beg = 2
		end = 4
	)

	var (
		vs  = []float64{1, 2, 3, 4, 5}
		sub = vs[beg:end]
	)

	b := array.NewFloat64Builder(pool)
	defer b.Release()

	for _, v := range vs {
		b.Append(v)
	}

	arr := b.NewArray().(*array.Float64)
	defer arr.Release()

	if got, want := arr.Len(), len(vs); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.Float64Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	slice := array.NewSlice(arr, beg, end).(*array.Float64)
	defer slice.Release()

	if got, want := slice.Len(), len(sub); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Float64Values(), sub; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestFloat64SliceDataWithNull(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const (
		beg = 2
		end = 5
	)

	var (
		valids = []bool{true, true, true, false, true, true}
		vs     = []float64{1, 2, 3, 0, 4, 5}
		sub    = vs[beg:end]
	)

	b := array.NewFloat64Builder(pool)
	defer b.Release()

	b.AppendValues(vs, valids)

	arr := b.NewArray().(*array.Float64)
	defer arr.Release()

	if got, want := arr.Len(), len(valids); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 1; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.Float64Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	slice := array.NewSlice(arr, beg, end).(*array.Float64)
	defer slice.Release()

	if got, want := slice.NullN(), 1; got != want {
		t.Errorf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Len(), len(sub); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Float64Values(), sub; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestFloat16MarshalJSON(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	bldr := array.NewFloat16Builder(pool)
	defer bldr.Release()
	
	jsonstr := `[0, 1, 2, 3, "NaN", "NaN", 4, 5, "+Inf", "-Inf"]`

	bldr.Append(float16.New(0))
	bldr.Append(float16.New(1))
	bldr.Append(float16.New(2))
	bldr.Append(float16.New(3))
	bldr.Append(float16.NaN())
	bldr.Append(float16.NaN())
	bldr.Append(float16.New(4))
	bldr.Append(float16.New(5))
	bldr.Append(float16.Inf())
	bldr.Append(float16.Inf().Negate())


	expected := bldr.NewFloat16Array()
	defer expected.Release()
	expected_json, err := expected.MarshalJSON()
	assert.NoError(t, err)
	assert.JSONEq(t, jsonstr, string(expected_json))
}

func TestFloat32MarshalJSON(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	bldr := array.NewFloat32Builder(pool)
	defer bldr.Release()
	
	jsonstr := `[0, 1, "+Inf", 2, 3, "NaN", "NaN", 4, 5, "-Inf"]`

	bldr.Append(0)
	bldr.Append(1)
	bldr.Append(float32(math.Inf(1)))
	bldr.Append(2)
	bldr.Append(3)
	bldr.Append(float32(math.NaN()))
	bldr.Append(float32(math.NaN()))
	bldr.Append(4)
	bldr.Append(5)
	bldr.Append(float32(math.Inf(-1)))


	expected := bldr.NewFloat32Array()
	defer expected.Release()
	
	expected_json, err := expected.MarshalJSON()
	assert.NoError(t, err)

	assert.JSONEq(t, jsonstr, string(expected_json))
}

func TestFloat64MarshalJSON(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	bldr := array.NewFloat64Builder(pool)
	defer bldr.Release()

	jsonstr := `[0, 1, "+Inf", 2, 3, "NaN", "NaN", 4, 5, "-Inf"]`

	bldr.Append(0)
	bldr.Append(1)
	bldr.Append(math.Inf(1))
	bldr.Append(2)
	bldr.Append(3)
	bldr.Append(math.NaN())
	bldr.Append(math.NaN())
	bldr.Append(4)
	bldr.Append(5)
	bldr.Append(math.Inf(-1))

	expected := bldr.NewFloat64Array()
	defer expected.Release()

	expected_json, err := expected.MarshalJSON()
	assert.NoError(t, err)

	assert.JSONEq(t, jsonstr, string(expected_json))
	
}

func TestUnmarshalSpecialFloat(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	bldr := array.NewFloat32Builder(pool)
	defer bldr.Release()

	assert.NoError(t, json.Unmarshal([]byte(`[3.4, "Inf", "-Inf"]`), bldr))
	arr := bldr.NewFloat32Array()
	defer arr.Release()

	assert.False(t, math.IsInf(float64(arr.Value(0)), 0), arr.Value(0))
	assert.True(t, math.IsInf(float64(arr.Value(1)), 1), arr.Value(1))
	assert.True(t, math.IsInf(float64(arr.Value(2)), -1), arr.Value(2))
}

func TestNewTime32Data(t *testing.T) {
	data := []arrow.Time32{
		arrow.Time32(1),
		arrow.Time32(2),
		arrow.Time32(4),
		arrow.Time32(8),
		arrow.Time32(16),
	}

	dtype := arrow.FixedWidthTypes.Time32s
	ad := array.NewData(dtype, len(data),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Time32Traits.CastToBytes(data))},
		nil, 0, 0,
	)
	t32a := array.NewTime32Data(ad)

	assert.Equal(t, len(data), t32a.Len(), "unexpected Len()")
	assert.Equal(t, data, t32a.Time32Values(), "unexpected Float64Values()")
}

func TestTime32SliceData(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const (
		beg = 2
		end = 4
	)

	var (
		vs = []arrow.Time32{
			arrow.Time32(1),
			arrow.Time32(2),
			arrow.Time32(4),
			arrow.Time32(8),
			arrow.Time32(16),
		}
		sub = vs[beg:end]
	)

	dtype := arrow.FixedWidthTypes.Time32s
	b := array.NewTime32Builder(pool, dtype.(*arrow.Time32Type))
	defer b.Release()

	for _, v := range vs {
		b.Append(v)
	}

	arr := b.NewArray().(*array.Time32)
	defer arr.Release()

	if got, want := arr.Len(), len(vs); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.Time32Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	slice := array.NewSlice(arr, beg, end).(*array.Time32)
	defer slice.Release()

	if got, want := slice.Len(), len(sub); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Time32Values(), sub; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestTime32SliceDataWithNull(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const (
		beg = 2
		end = 5
	)

	var (
		valids = []bool{true, true, true, false, true, true}
		vs     = []arrow.Time32{
			arrow.Time32(1),
			arrow.Time32(2),
			arrow.Time32(3),
			arrow.Time32(0),
			arrow.Time32(4),
			arrow.Time32(5),
		}
		sub = vs[beg:end]
	)

	dtype := arrow.FixedWidthTypes.Time32s
	b := array.NewTime32Builder(pool, dtype.(*arrow.Time32Type))
	defer b.Release()

	b.AppendValues(vs, valids)

	arr := b.NewArray().(*array.Time32)
	defer arr.Release()

	if got, want := arr.Len(), len(valids); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 1; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.Time32Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	slice := array.NewSlice(arr, beg, end).(*array.Time32)
	defer slice.Release()

	if got, want := slice.NullN(), 1; got != want {
		t.Errorf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Len(), len(sub); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Time32Values(), sub; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestNewTime64Data(t *testing.T) {
	data := []arrow.Time64{
		arrow.Time64(1),
		arrow.Time64(2),
		arrow.Time64(4),
		arrow.Time64(8),
		arrow.Time64(16),
	}

	dtype := arrow.FixedWidthTypes.Time64us
	ad := array.NewData(dtype, len(data),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Time64Traits.CastToBytes(data))},
		nil, 0, 0,
	)
	t64a := array.NewTime64Data(ad)

	assert.Equal(t, len(data), t64a.Len(), "unexpected Len()")
	assert.Equal(t, data, t64a.Time64Values(), "unexpected Float64Values()")
}

func TestTime64SliceData(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const (
		beg = 2
		end = 4
	)

	var (
		vs = []arrow.Time64{
			arrow.Time64(1),
			arrow.Time64(2),
			arrow.Time64(4),
			arrow.Time64(8),
			arrow.Time64(16),
		}
		sub = vs[beg:end]
	)

	dtype := arrow.FixedWidthTypes.Time64us
	b := array.NewTime64Builder(pool, dtype.(*arrow.Time64Type))
	defer b.Release()

	for _, v := range vs {
		b.Append(v)
	}

	arr := b.NewArray().(*array.Time64)
	defer arr.Release()

	if got, want := arr.Len(), len(vs); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.Time64Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	slice := array.NewSlice(arr, beg, end).(*array.Time64)
	defer slice.Release()

	if got, want := slice.Len(), len(sub); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Time64Values(), sub; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestTime64SliceDataWithNull(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const (
		beg = 2
		end = 5
	)

	var (
		valids = []bool{true, true, true, false, true, true}
		vs     = []arrow.Time64{
			arrow.Time64(1),
			arrow.Time64(2),
			arrow.Time64(3),
			arrow.Time64(0),
			arrow.Time64(4),
			arrow.Time64(5),
		}
		sub = vs[beg:end]
	)

	dtype := arrow.FixedWidthTypes.Time64us
	b := array.NewTime64Builder(pool, dtype.(*arrow.Time64Type))
	defer b.Release()

	b.AppendValues(vs, valids)

	arr := b.NewArray().(*array.Time64)
	defer arr.Release()

	if got, want := arr.Len(), len(valids); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 1; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.Time64Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	slice := array.NewSlice(arr, beg, end).(*array.Time64)
	defer slice.Release()

	if got, want := slice.NullN(), 1; got != want {
		t.Errorf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Len(), len(sub); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Time64Values(), sub; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestNewDate32Data(t *testing.T) {
	exp := []arrow.Date32{1, 2, 4, 8, 16}

	dtype := &arrow.Date32Type{}
	ad := array.NewData(
		dtype, len(exp),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Date32Traits.CastToBytes(exp))},
		nil, 0, 0,
	)
	fa := array.NewDate32Data(ad)

	assert.Equal(t, len(exp), fa.Len(), "unexpected Len()")
	assert.Equal(t, exp, fa.Date32Values(), "unexpected Date32Values()")
}

func TestDate32SliceData(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const (
		beg = 2
		end = 4
	)

	var (
		vs  = []arrow.Date32{1, 2, 3, 4, 5}
		sub = vs[beg:end]
	)

	b := array.NewDate32Builder(pool)
	defer b.Release()

	for _, v := range vs {
		b.Append(v)
	}

	arr := b.NewArray().(*array.Date32)
	defer arr.Release()

	if got, want := arr.Len(), len(vs); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.Date32Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	slice := array.NewSlice(arr, beg, end).(*array.Date32)
	defer slice.Release()

	if got, want := slice.Len(), len(sub); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Date32Values(), sub; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestDate32SliceDataWithNull(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const (
		beg = 2
		end = 5
	)

	var (
		valids = []bool{true, true, true, false, true, true}
		vs     = []arrow.Date32{1, 2, 3, 0, 4, 5}
		sub    = vs[beg:end]
	)

	b := array.NewDate32Builder(pool)
	defer b.Release()

	b.AppendValues(vs, valids)

	arr := b.NewArray().(*array.Date32)
	defer arr.Release()

	if got, want := arr.Len(), len(valids); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 1; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.Date32Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	slice := array.NewSlice(arr, beg, end).(*array.Date32)
	defer slice.Release()

	if got, want := slice.NullN(), 1; got != want {
		t.Errorf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Len(), len(sub); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Date32Values(), sub; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestNewDate64Data(t *testing.T) {
	exp := []arrow.Date64{1, 2, 4, 8, 16}

	dtype := &arrow.Date64Type{}
	ad := array.NewData(
		dtype, len(exp),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Date64Traits.CastToBytes(exp))},
		nil, 0, 0,
	)
	fa := array.NewDate64Data(ad)

	assert.Equal(t, len(exp), fa.Len(), "unexpected Len()")
	assert.Equal(t, exp, fa.Date64Values(), "unexpected Date64Values()")
}

func TestDate64SliceData(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const (
		beg = 2
		end = 4
	)

	var (
		vs  = []arrow.Date64{1, 2, 3, 4, 5}
		sub = vs[beg:end]
	)

	b := array.NewDate64Builder(pool)
	defer b.Release()

	for _, v := range vs {
		b.Append(v)
	}

	arr := b.NewArray().(*array.Date64)
	defer arr.Release()

	if got, want := arr.Len(), len(vs); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.Date64Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	slice := array.NewSlice(arr, beg, end).(*array.Date64)
	defer slice.Release()

	if got, want := slice.Len(), len(sub); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Date64Values(), sub; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestDate64SliceDataWithNull(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	const (
		beg = 2
		end = 5
	)

	var (
		valids = []bool{true, true, true, false, true, true}
		vs     = []arrow.Date64{1, 2, 3, 0, 4, 5}
		sub    = vs[beg:end]
	)

	b := array.NewDate64Builder(pool)
	defer b.Release()

	b.AppendValues(vs, valids)

	arr := b.NewArray().(*array.Date64)
	defer arr.Release()

	if got, want := arr.Len(), len(valids); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 1; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.Date64Values(), vs; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	slice := array.NewSlice(arr, beg, end).(*array.Date64)
	defer slice.Release()

	if got, want := slice.NullN(), 1; got != want {
		t.Errorf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Len(), len(sub); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := slice.Date64Values(), sub; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}
}

func TestInt64MarshalJSON(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		vs = []int64{-5474557666971701248}
	)

	b := array.NewInt64Builder(pool)
	defer b.Release()

	for _, v := range vs {
		b.Append(v)
	}

	arr := b.NewArray().(*array.Int64)
	defer arr.Release()

	jsonBytes, err := json.Marshal(arr)
	if err != nil {
		t.Fatal(err)
	}
	got := string(jsonBytes)
	want := `[-5474557666971701248]`
	if got != want {
		t.Fatalf("got=%s, want=%s", got, want)
	}
}

func TestUInt64MarshalJSON(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		vs = []uint64{14697929703826477056}
	)

	b := array.NewUint64Builder(pool)
	defer b.Release()

	for _, v := range vs {
		b.Append(v)
	}

	arr := b.NewArray().(*array.Uint64)
	defer arr.Release()

	jsonBytes, err := json.Marshal(arr)
	if err != nil {
		t.Fatal(err)
	}
	got := string(jsonBytes)
	want := `[14697929703826477056]`
	if got != want {
		t.Fatalf("got=%s, want=%s", got, want)
	}
}
