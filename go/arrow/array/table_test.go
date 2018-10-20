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
)

func TestChunked(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	c1 := array.NewChunked(arrow.PrimitiveTypes.Int32, nil)
	c1.Retain()
	c1.Release()
	if got, want := c1.Len(), 0; got != want {
		t.Fatalf("len differ. got=%d, want=%d", got, want)
	}
	if got, want := c1.NullN(), 0; got != want {
		t.Fatalf("nulls: got=%d, want=%d", got, want)
	}
	if got, want := c1.DataType(), arrow.PrimitiveTypes.Int32; got != want {
		t.Fatalf("dtype: got=%v, want=%v", got, want)
	}
	c1.Release()

	fb := array.NewFloat64Builder(mem)
	defer fb.Release()

	fb.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	f1 := fb.NewFloat64Array()
	defer f1.Release()

	fb.AppendValues([]float64{6, 7}, nil)
	f2 := fb.NewFloat64Array()
	defer f2.Release()

	fb.AppendValues([]float64{8, 9, 10}, nil)
	f3 := fb.NewFloat64Array()
	defer f3.Release()

	c2 := array.NewChunked(
		arrow.PrimitiveTypes.Float64,
		[]array.Interface{f1, f2, f3},
	)
	defer c2.Release()

	if got, want := c2.Len(), 10; got != want {
		t.Fatalf("len: got=%d, want=%d", got, want)
	}
	if got, want := c2.NullN(), 0; got != want {
		t.Fatalf("nulls: got=%d, want=%d", got, want)
	}
	if got, want := c2.DataType(), arrow.PrimitiveTypes.Float64; got != want {
		t.Fatalf("dtype: got=%v, want=%v", got, want)
	}

	for _, tc := range []struct {
		i, j   int64
		len    int
		nulls  int
		chunks int
	}{
		{i: 0, j: 10, len: 10, nulls: 0, chunks: 3},
		{i: 2, j: 3, len: 1, nulls: 0, chunks: 1},
		{i: 9, j: 10, len: 1, nulls: 0, chunks: 1},
		{i: 0, j: 5, len: 5, nulls: 0, chunks: 1},
		{i: 5, j: 7, len: 2, nulls: 0, chunks: 1},
		{i: 7, j: 10, len: 3, nulls: 0, chunks: 1},
		{i: 10, j: 10, len: 0, nulls: 0, chunks: 0},
	} {
		t.Run("", func(t *testing.T) {
			sub := c2.NewSlice(tc.i, tc.j)
			defer sub.Release()

			if got, want := sub.Len(), tc.len; got != want {
				t.Fatalf("len: got=%d, want=%d", got, want)
			}
			if got, want := sub.NullN(), tc.nulls; got != want {
				t.Fatalf("nulls: got=%d, want=%d", got, want)
			}
			if got, want := sub.DataType(), arrow.PrimitiveTypes.Float64; got != want {
				t.Fatalf("dtype: got=%v, want=%v", got, want)
			}
			if got, want := len(sub.Chunks()), tc.chunks; got != want {
				t.Fatalf("chunks: got=%d, want=%d", got, want)
			}
		})
	}
}

func TestChunkedInvalid(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	fb := array.NewFloat64Builder(mem)
	defer fb.Release()

	fb.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	f1 := fb.NewFloat64Array()
	defer f1.Release()

	ib := array.NewInt32Builder(mem)
	defer ib.Release()

	ib.AppendValues([]int32{6, 7}, nil)
	f2 := ib.NewInt32Array()
	defer f2.Release()

	defer func() {
		e := recover()
		if e == nil {
			t.Fatalf("expected a panic")
		}
		if got, want := e.(string), "arrow/array: mismatch data type"; got != want {
			t.Fatalf("invalid error. got=%q, want=%q", got, want)
		}
	}()

	c1 := array.NewChunked(arrow.PrimitiveTypes.Int32, []array.Interface{
		f1, f2,
	})
	defer c1.Release()
}

func TestChunkedSliceInvalid(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	fb := array.NewFloat64Builder(mem)
	defer fb.Release()

	fb.AppendValues([]float64{1, 2, 3, 4, 5}, nil)
	f1 := fb.NewFloat64Array()
	defer f1.Release()

	fb.AppendValues([]float64{6, 7}, nil)
	f2 := fb.NewFloat64Array()
	defer f2.Release()

	fb.AppendValues([]float64{8, 9, 10}, nil)
	f3 := fb.NewFloat64Array()
	defer f3.Release()

	c := array.NewChunked(
		arrow.PrimitiveTypes.Float64,
		[]array.Interface{f1, f2, f3},
	)
	defer c.Release()

	for _, tc := range []struct {
		i, j int64
	}{
		{i: 2, j: 1},
		{i: 10, j: 11},
		{i: 11, j: 11},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				e := recover()
				if e == nil {
					t.Fatalf("expected a panic")
				}
				if got, want := e.(string), "arrow/array: index out of range"; got != want {
					t.Fatalf("invalid error. got=%q, want=%q", got, want)
				}
			}()
			sub := c.NewSlice(tc.i, tc.j)
			defer sub.Release()
		})
	}
}
