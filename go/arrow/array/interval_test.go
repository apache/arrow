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
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestMonthIntervalArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		want   = []arrow.MonthInterval{1, 2, 3, 4}
		valids = []bool{true, true, false, true}
	)

	b := array.NewMonthIntervalBuilder(mem)
	defer b.Release()

	b.Retain()
	b.Release()

	b.AppendValues(want[:2], nil)
	b.AppendNull()
	b.Append(want[3])

	if got, want := b.Len(), len(want); got != want {
		t.Fatalf("invalid len: got=%d, want=%d", got, want)
	}

	if got, want := b.NullN(), 1; got != want {
		t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
	}

	arr := b.NewMonthIntervalArray()
	defer arr.Release()

	arr.Retain()
	arr.Release()

	if got, want := arr.Len(), len(want); got != want {
		t.Fatalf("invalid len: got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 1; got != want {
		t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
	}

	for i := range want {
		if arr.IsNull(i) != !valids[i] {
			t.Fatalf("arr[%d]-validity: got=%v want=%v", i, !arr.IsNull(i), valids[i])
		}
		switch {
		case arr.IsNull(i):
		default:
			got := arr.Value(i)
			if got != want[i] {
				t.Fatalf("arr[%d]: got=%q, want=%q", i, got, want[i])
			}
		}
	}

	sub := array.MakeFromData(arr.Data())
	defer sub.Release()

	if sub.DataType().ID() != arrow.INTERVAL_MONTHS {
		t.Fatalf("invalid type: got=%q, want=interval_months", sub.DataType().Name())
	}

	if _, ok := sub.(*array.MonthInterval); !ok {
		t.Fatalf("could not type-assert to array.MonthInterval")
	}

	if got, want := arr.String(), `[1 2 (null) 4]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
	slice := array.NewSliceData(arr.Data(), 2, 4)
	defer slice.Release()

	sub1 := array.MakeFromData(slice)
	defer sub1.Release()

	v, ok := sub1.(*array.MonthInterval)
	if !ok {
		t.Fatalf("could not type-assert to array.MonthInterval")
	}

	if got, want := v.String(), `[(null) 4]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
}

func TestMonthIntervalBuilder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	want := []arrow.MonthInterval{1, 2, 3, 4}

	b := array.NewMonthIntervalBuilder(mem)
	defer b.Release()

	miValues := func(a *array.MonthInterval) []arrow.MonthInterval {
		vs := make([]arrow.MonthInterval, a.Len())
		for i := range vs {
			vs[i] = a.Value(i)
		}
		return vs
	}

	b.AppendValues([]arrow.MonthInterval{}, nil)
	arr := b.NewMonthIntervalArray()
	assert.Zero(t, arr.Len())
	arr.Release()

	b.AppendValues(nil, nil)
	arr = b.NewMonthIntervalArray()
	assert.Zero(t, arr.Len())
	arr.Release()

	b.AppendValues([]arrow.MonthInterval{}, nil)
	b.AppendValues(want, nil)
	arr = b.NewMonthIntervalArray()
	assert.Equal(t, want, miValues(arr))
	arr.Release()

	b.AppendValues(want, nil)
	b.AppendValues([]arrow.MonthInterval{}, nil)
	arr = b.NewMonthIntervalArray()
	assert.Equal(t, want, miValues(arr))
	arr.Release()
}

func TestMonthIntervalStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		values = []arrow.MonthInterval{1, 2, 3, 4}
		valid  = []bool{true, true, false, true}
	)

	b := array.NewMonthIntervalBuilder(mem)
	defer b.Release()

	b.AppendValues(values, valid)

	arr := b.NewArray().(*array.MonthInterval)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewMonthIntervalBuilder(mem)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.MonthInterval)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestDayTimeArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		want = []arrow.DayTimeInterval{
			{Days: 1, Milliseconds: 1}, {Days: 2, Milliseconds: 2},
			{Days: 3, Milliseconds: 3}, {Days: 4, Milliseconds: 4}}
		valids = []bool{true, true, false, true}
	)

	b := array.NewDayTimeIntervalBuilder(mem)
	defer b.Release()

	b.Retain()
	b.Release()

	b.AppendValues(want[:2], nil)
	b.AppendNull()
	b.Append(want[3])

	if got, want := b.Len(), len(want); got != want {
		t.Fatalf("invalid len: got=%d, want=%d", got, want)
	}

	if got, want := b.NullN(), 1; got != want {
		t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
	}

	arr := b.NewDayTimeIntervalArray()
	defer arr.Release()

	arr.Retain()
	arr.Release()

	if got, want := arr.Len(), len(want); got != want {
		t.Fatalf("invalid len: got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 1; got != want {
		t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
	}

	for i := range want {
		if arr.IsNull(i) != !valids[i] {
			t.Fatalf("arr[%d]-validity: got=%v want=%v", i, !arr.IsNull(i), valids[i])
		}
		switch {
		case arr.IsNull(i):
		default:
			got := arr.Value(i)
			if got != want[i] {
				t.Fatalf("arr[%d]: got=%q, want=%q", i, got, want[i])
			}
		}
	}

	sub := array.MakeFromData(arr.Data())
	defer sub.Release()

	if sub.DataType().ID() != arrow.INTERVAL_DAY_TIME {
		t.Fatalf("invalid type: got=%q, want=interval_day_time", sub.DataType().Name())
	}

	if _, ok := sub.(*array.DayTimeInterval); !ok {
		t.Fatalf("could not type-assert to array.DayTimeInterval")
	}

	if got, want := arr.String(), `[{1 1} {2 2} (null) {4 4}]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
	slice := array.NewSliceData(arr.Data(), 2, 4)
	defer slice.Release()

	sub1 := array.MakeFromData(slice)
	defer sub1.Release()

	v, ok := sub1.(*array.DayTimeInterval)
	if !ok {
		t.Fatalf("could not type-assert to array.DayInterval")
	}

	if got, want := v.String(), `[(null) {4 4}]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
}

func TestDayTimeIntervalBuilder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	want := []arrow.DayTimeInterval{
		{Days: 1, Milliseconds: 1}, {Days: 2, Milliseconds: 2},
		{Days: 3, Milliseconds: 3}, {Days: 4, Milliseconds: 4}}

	b := array.NewDayTimeIntervalBuilder(mem)
	defer b.Release()

	dtValues := func(a *array.DayTimeInterval) []arrow.DayTimeInterval {
		vs := make([]arrow.DayTimeInterval, a.Len())
		for i := range vs {
			vs[i] = a.Value(i)
		}
		return vs
	}

	b.AppendValues([]arrow.DayTimeInterval{}, nil)
	arr := b.NewDayTimeIntervalArray()
	assert.Zero(t, arr.Len())
	arr.Release()

	b.AppendValues(nil, nil)
	arr = b.NewDayTimeIntervalArray()
	assert.Zero(t, arr.Len())
	arr.Release()

	b.AppendValues([]arrow.DayTimeInterval{}, nil)
	b.AppendValues(want, nil)
	arr = b.NewDayTimeIntervalArray()
	assert.Equal(t, want, dtValues(arr))
	arr.Release()

	b.AppendValues(want, nil)
	b.AppendValues([]arrow.DayTimeInterval{}, nil)
	arr = b.NewDayTimeIntervalArray()
	assert.Equal(t, want, dtValues(arr))
	arr.Release()
}

func TestDayTimeIntervalStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		values = []arrow.DayTimeInterval{
			{Days: 1, Milliseconds: 1},
			{Days: 2, Milliseconds: 2},
			{Days: 3, Milliseconds: 3},
			{Days: 4, Milliseconds: 4},
		}
		valid = []bool{true, true, false, true}
	)

	b := array.NewDayTimeIntervalBuilder(mem)
	defer b.Release()

	b.AppendValues(values, valid)

	arr := b.NewArray().(*array.DayTimeInterval)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewDayTimeIntervalBuilder(mem)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.DayTimeInterval)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestMonthDayNanoArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		want = []arrow.MonthDayNanoInterval{
			{Months: 1, Days: 1, Nanoseconds: 1000}, {Months: 2, Days: 2, Nanoseconds: 2000},
			{Months: 3, Days: 3, Nanoseconds: 3000}, {Months: 4, Days: 4, Nanoseconds: 4000},
			{Months: 0, Days: 0, Nanoseconds: 0}, {Months: -1, Days: -2, Nanoseconds: -300},
			{Months: math.MaxInt32, Days: math.MinInt32, Nanoseconds: math.MaxInt64},
			{Months: math.MinInt32, Days: math.MaxInt32, Nanoseconds: math.MinInt64},
		}
		valids = []bool{true, true, false, true, true, true, false, true}
	)

	b := array.NewMonthDayNanoIntervalBuilder(mem)
	defer b.Release()

	b.Retain()
	b.Release()

	b.AppendValues(want[:2], nil)
	b.AppendNull()
	b.Append(want[3])
	b.AppendValues(want[4:], valids[4:])

	if got, want := b.Len(), len(want); got != want {
		t.Fatalf("invalid len: got=%d, want=%d", got, want)
	}

	if got, want := b.NullN(), 2; got != want {
		t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
	}

	arr := b.NewMonthDayNanoIntervalArray()
	defer arr.Release()

	arr.Retain()
	arr.Release()

	if got, want := arr.Len(), len(want); got != want {
		t.Fatalf("invalid len: got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 2; got != want {
		t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
	}

	for i := range want {
		if arr.IsNull(i) != !valids[i] {
			t.Fatalf("arr[%d]-validity: got=%v want=%v", i, !arr.IsNull(i), valids[i])
		}
		switch {
		case arr.IsNull(i):
		default:
			got := arr.Value(i)
			if got != want[i] {
				t.Fatalf("arr[%d]: got=%q, want=%q", i, got, want[i])
			}
		}
	}

	sub := array.MakeFromData(arr.Data())
	defer sub.Release()

	if sub.DataType().ID() != arrow.INTERVAL_MONTH_DAY_NANO {
		t.Fatalf("invalid type: got=%q, want=interval", sub.DataType().Name())
	}

	if _, ok := sub.(*array.MonthDayNanoInterval); !ok {
		t.Fatalf("could not type-assert to array.MonthDayNanoInterval")
	}

	if got, want := arr.String(), `[{1 1 1000} {2 2 2000} (null) {4 4 4000} {0 0 0} {-1 -2 -300} (null) {-2147483648 2147483647 -9223372036854775808}]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
	slice := array.NewSliceData(arr.Data(), 2, 4)
	defer slice.Release()

	sub1 := array.MakeFromData(slice)
	defer sub1.Release()

	v, ok := sub1.(*array.MonthDayNanoInterval)
	if !ok {
		t.Fatalf("could not type-assert to array.MonthDayNanoInterval")
	}

	if got, want := v.String(), `[(null) {4 4 4000}]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}
}

func TestMonthDayNanoIntervalBuilder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	want := []arrow.MonthDayNanoInterval{
		{Months: 1, Days: 1, Nanoseconds: 1000},
		{Months: 2, Days: 2, Nanoseconds: 2000},
		{Months: 3, Days: 3, Nanoseconds: 3000},
		{Months: 4, Days: 4, Nanoseconds: 4000}}

	b := array.NewMonthDayNanoIntervalBuilder(mem)
	defer b.Release()

	dtValues := func(a *array.MonthDayNanoInterval) []arrow.MonthDayNanoInterval {
		vs := make([]arrow.MonthDayNanoInterval, a.Len())
		for i := range vs {
			vs[i] = a.Value(i)
		}
		return vs
	}

	b.AppendValues([]arrow.MonthDayNanoInterval{}, nil)
	arr := b.NewMonthDayNanoIntervalArray()
	assert.Zero(t, arr.Len())
	arr.Release()

	b.AppendValues(nil, nil)
	arr = b.NewMonthDayNanoIntervalArray()
	assert.Zero(t, arr.Len())
	arr.Release()

	b.AppendValues([]arrow.MonthDayNanoInterval{}, nil)
	b.AppendValues(want, nil)
	arr = b.NewMonthDayNanoIntervalArray()
	assert.Equal(t, want, dtValues(arr))
	arr.Release()

	b.AppendValues(want, nil)
	b.AppendValues([]arrow.MonthDayNanoInterval{}, nil)
	arr = b.NewMonthDayNanoIntervalArray()
	assert.Equal(t, want, dtValues(arr))
	arr.Release()
}

func TestMonthDayNanoIntervalStringRoundTrip(t *testing.T) {
	// 1. create array
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		values = []arrow.MonthDayNanoInterval{
			{Months: 1, Days: 1, Nanoseconds: 1000}, {Months: 2, Days: 2, Nanoseconds: 2000},
			{Months: 3, Days: 3, Nanoseconds: 3000}, {Months: 4, Days: 4, Nanoseconds: 4000},
			{Months: 0, Days: 0, Nanoseconds: 0}, {Months: -1, Days: -2, Nanoseconds: -300},
			{Months: math.MaxInt32, Days: math.MinInt32, Nanoseconds: math.MaxInt64},
			{Months: math.MinInt32, Days: math.MaxInt32, Nanoseconds: math.MinInt64},
		}
		valid = []bool{true, true, false, true, true, true, false, true}
	)

	b := array.NewMonthDayNanoIntervalBuilder(mem)
	defer b.Release()

	b.AppendValues(values, valid)

	arr := b.NewArray().(*array.MonthDayNanoInterval)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewMonthDayNanoIntervalBuilder(mem)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.MonthDayNanoInterval)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}
