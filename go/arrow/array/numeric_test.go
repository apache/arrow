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

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
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
