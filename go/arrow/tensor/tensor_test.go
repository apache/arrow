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

package tensor_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/tensor"
)

func TestTensor(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewFloat64Builder(mem)
	defer bld.Release()

	raw := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bld.AppendValues(raw, nil)

	arr := bld.NewFloat64Array()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
	)

	f64 := tensor.New(arr.Data(), shape, nil, names).(*tensor.Float64)
	defer f64.Release()

	f64.Retain()
	f64.Release()

	if got, want := f64.Len(), 10; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	if got, want := f64.Shape(), shape; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid shape: got=%v, want=%v", got, want)
	}

	if got, want := f64.Strides(), []int64{40, 8}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid strides: got=%v, want=%v", got, want)
	}

	if got, want := f64.NumDims(), 2; got != want {
		t.Fatalf("invalid dims: got=%d, want=%d", got, want)
	}

	if got, want := f64.DimNames(), names; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid dim-names: got=%v, want=%v", got, want)
	}

	for i, name := range names {
		if got, want := f64.DimName(i), name; got != want {
			t.Fatalf("invalid dim-name[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if got, want := f64.DataType(), arr.DataType(); got != want {
		t.Fatalf("invalid data-type: got=%q, want=%q", got.Name(), want.Name())
	}

	if got, want := f64.Data(), arr.Data(); got != want {
		t.Fatalf("invalid data: got=%v, want=%v", got, want)
	}

	if f64.IsMutable() {
		t.Fatalf("should not be mutable")
	}

	if !f64.IsContiguous() {
		t.Fatalf("should be contiguous")
	}

	if !f64.IsRowMajor() || f64.IsColMajor() {
		t.Fatalf("should be row-major")
	}

	if got, want := f64.Float64Values(), raw; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid backing array: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i []int64
		v float64
	}{
		{i: []int64{0, 0}, v: 1},
		{i: []int64{0, 1}, v: 2},
		{i: []int64{0, 2}, v: 3},
		{i: []int64{0, 3}, v: 4},
		{i: []int64{0, 4}, v: 5},
		{i: []int64{1, 0}, v: 6},
		{i: []int64{1, 1}, v: 7},
		{i: []int64{1, 2}, v: 8},
		{i: []int64{1, 3}, v: 9},
		{i: []int64{1, 4}, v: 10},
	} {
		t.Run(fmt.Sprintf("%v", tc.i), func(t *testing.T) {
			got := f64.Value(tc.i)
			if got != tc.v {
				t.Fatalf("arr[%v]: got=%v, want=%v", tc.i, got, tc.v)
			}
		})
	}
}

func TestInvalidTensor(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bld := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer bld.Release()

	raw := [][]byte{{1}, {2, 2}, {3, 3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}}
	bld.AppendValues(raw, nil)

	arr := bld.NewBinaryArray()
	defer arr.Release()

	var (
		shape = []int64{2, 5}
		names = []string{"x", "y"}
	)

	t.Run("invalid-binary", func(t *testing.T) {
		want := fmt.Errorf("arrow/tensor: invalid data type binary")
		defer func() {
			e := recover()
			if e == nil {
				t.Fatalf("expected an error: %v", want)
			}
			switch err := e.(type) {
			case error:
				if !reflect.DeepEqual(err, want) {
					t.Fatalf("invalid error: got=%v (%T), want=%v", err, err, want)
				}
			default:
				t.Fatalf("invalid error: got=%v (%T), want=%v", err, err, want)
			}
		}()
		tsr := tensor.New(arr.Data(), shape, nil, names)
		defer tsr.Release()
	})

}
