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
	"reflect"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestBooleanSliceData(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	values := []bool{true, false, true, true, true, true, true, false, true, false}

	b := array.NewBooleanBuilder(pool)
	defer b.Release()

	for _, v := range values {
		b.Append(v)
	}

	arr := b.NewArray().(*array.Boolean)
	defer arr.Release()

	if got, want := arr.Len(), len(values); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	vs := make([]bool, arr.Len())

	for i := range vs {
		vs[i] = arr.Value(i)
	}

	if got, want := vs, values; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	tests := []struct {
		interval [2]int64
		want     []bool
	}{
		{
			interval: [2]int64{0, 0},
			want:     []bool{},
		},
		{
			interval: [2]int64{10, 10},
			want:     []bool{},
		},
		{
			interval: [2]int64{0, 5},
			want:     []bool{true, false, true, true, true},
		},
		{
			interval: [2]int64{5, 10},
			want:     []bool{true, true, false, true, false},
		},
		{
			interval: [2]int64{2, 7},
			want:     []bool{true, true, true, true, true},
		},
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {

			slice := array.NewSlice(arr, tc.interval[0], tc.interval[1]).(*array.Boolean)
			defer slice.Release()

			if got, want := slice.Len(), len(tc.want); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			vs := make([]bool, slice.Len())

			for i := range vs {
				vs[i] = slice.Value(i)
			}

			if got, want := vs, tc.want; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}
}

func TestBooleanSliceDataWithNull(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	values := []bool{true, false, true, false, false, false, true, false, true, false}
	valids := []bool{true, false, true, true, true, true, true, false, true, true}

	b := array.NewBooleanBuilder(pool)
	defer b.Release()

	b.AppendValues(values, valids)

	arr := b.NewArray().(*array.Boolean)
	defer arr.Release()

	if got, want := arr.Len(), len(valids); got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	if got, want := arr.NullN(), 2; got != want {
		t.Fatalf("got=%d, want=%d", got, want)
	}

	vs := make([]bool, arr.Len())

	for i := range vs {
		vs[i] = arr.Value(i)
	}

	if got, want := vs, values; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v, want=%v", got, want)
	}

	tests := []struct {
		interval [2]int64
		nulls    int
		want     []bool
	}{
		{
			interval: [2]int64{2, 9},
			nulls:    1,
			want:     []bool{true, false, false, false, true, false, true},
		},
		{
			interval: [2]int64{0, 7},
			nulls:    1,
			want:     []bool{true, false, true, false, false, false, true},
		},
		{
			interval: [2]int64{1, 8},
			nulls:    2,
			want:     []bool{false, true, false, false, false, true, false},
		},
		{
			interval: [2]int64{2, 7},
			nulls:    0,
			want:     []bool{true, false, false, false, true},
		},
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {

			slice := array.NewSlice(arr, tc.interval[0], tc.interval[1]).(*array.Boolean)
			defer slice.Release()

			if got, want := slice.NullN(), tc.nulls; got != want {
				t.Errorf("got=%d, want=%d", got, want)
			}

			if got, want := slice.Len(), len(tc.want); got != want {
				t.Fatalf("got=%d, want=%d", got, want)
			}

			vs := make([]bool, slice.Len())

			for i := range vs {
				vs[i] = slice.Value(i)
			}

			if got, want := vs, tc.want; !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%v, want=%v", got, want)
			}
		})
	}
}

func TestBooleanSliceOutOfBounds(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	values := []bool{true, false, true, false, true, false, true, false, true, false}

	b := array.NewBooleanBuilder(pool)
	defer b.Release()

	for _, v := range values {
		b.Append(v)
	}

	arr := b.NewArray().(*array.Boolean)
	defer arr.Release()

	slice := array.NewSlice(arr, 3, 8).(*array.Boolean)
	defer slice.Release()

	tests := []struct {
		index int
		panic bool
	}{
		{
			index: -1,
			panic: true,
		},
		{
			index: 5,
			panic: true,
		},
		{
			index: 0,
			panic: false,
		},
		{
			index: 4,
			panic: false,
		},
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {

			var val bool

			if tc.panic {
				defer func() {
					e := recover()
					if e == nil {
						t.Fatalf("this should have panicked, but did not; slice value %v", val)
					}
					if got, want := e.(string), "arrow/array: index out of range"; got != want {
						t.Fatalf("invalid error. got=%q, want=%q", got, want)
					}
				}()
			} else {
				defer func() {
					if e := recover(); e != nil {
						t.Fatalf("unexpected panic: %v", e)
					}
				}()
			}

			val = slice.Value(tc.index)
		})
	}
}
