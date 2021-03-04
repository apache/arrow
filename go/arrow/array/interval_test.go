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

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
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

	if sub.DataType().ID() != arrow.INTERVAL {
		t.Fatalf("invalid type: got=%q, want=interval", sub.DataType().Name())
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

func TestDayTimeArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		want   = []arrow.DayTimeInterval{{1, 1}, {2, 2}, {3, 3}, {4, 4}}
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

	if sub.DataType().ID() != arrow.INTERVAL {
		t.Fatalf("invalid type: got=%q, want=interval", sub.DataType().Name())
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

	want := []arrow.DayTimeInterval{{1, 1}, {2, 2}, {3, 3}, {4, 4}}

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
