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
	"bytes"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestStringArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		want    = []string{"hello", "世界", "", "bye"}
		valids  = []bool{true, true, false, true}
		offsets = []int32{0, 5, 11, 11, 14}
	)

	sb := array.NewStringBuilder(mem)
	defer sb.Release()

	sb.Retain()
	sb.Release()

	sb.AppendValues(want[:2], nil)

	sb.AppendNull()
	sb.Append(want[3])

	if got, want := sb.Len(), len(want); got != want {
		t.Fatalf("invalid len: got=%d, want=%d", got, want)
	}

	if got, want := sb.NullN(), 1; got != want {
		t.Fatalf("invalid nulls: got=%d, want=%d", got, want)
	}

	arr := sb.NewStringArray()
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

		if got, want := arr.ValueOffset(i), int(offsets[i]); got != want {
			t.Fatalf("arr-offset-beg[%d]: got=%d, want=%d", i, got, want)
		}
		if got, want := arr.ValueOffset(i+1), int(offsets[i+1]); got != want {
			t.Fatalf("arr-offset-end[%d]: got=%d, want=%d", i+1, got, want)
		}
	}

	if !reflect.DeepEqual(offsets, arr.ValueOffsets()) {
		t.Fatalf("ValueOffsets got=%v, want=%v", arr.ValueOffsets(), offsets)
	}

	sub := array.MakeFromData(arr.Data())
	defer sub.Release()

	if sub.DataType().ID() != arrow.STRING {
		t.Fatalf("invalid type: got=%q, want=string", sub.DataType().Name())
	}

	if _, ok := sub.(*array.String); !ok {
		t.Fatalf("could not type-assert to array.String")
	}

	if got, want := arr.String(), `["hello" "世界" (null) "bye"]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	if !bytes.Equal([]byte(`hello世界bye`), arr.ValueBytes()) {
		t.Fatalf("got=%q, want=%q", string(arr.ValueBytes()), `hello世界bye`)
	}

	slice := array.NewSliceData(arr.Data(), 2, 4)
	defer slice.Release()

	sub1 := array.MakeFromData(slice)
	defer sub1.Release()

	v, ok := sub1.(*array.String)
	if !ok {
		t.Fatalf("could not type-assert to array.String")
	}

	if got, want := v.String(), `[(null) "bye"]`; got != want {
		t.Fatalf("got=%q, want=%q", got, want)
	}

	if !bytes.Equal(v.ValueBytes(), []byte("bye")) {
		t.Fatalf("got=%q, want=%q", string(v.ValueBytes()), "bye")
	}

	for i := 0; i < v.Len(); i++ {
		if got, want := v.ValueOffset(0), int(offsets[i+slice.Offset()]); got != want {
			t.Fatalf("val-offset-with-offset[%d]: got=%q, want=%q", i, got, want)
		}
	}

	if !reflect.DeepEqual(offsets[2:5], v.ValueOffsets()) {
		t.Fatalf("ValueOffsets got=%v, want=%v", v.ValueOffsets(), offsets[2:5])
	}
}

func TestStringBuilder_Empty(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	want := []string{"hello", "世界", "", "bye"}

	ab := array.NewStringBuilder(mem)
	defer ab.Release()

	stringValues := func(a *array.String) []string {
		vs := make([]string, a.Len())
		for i := range vs {
			vs[i] = a.Value(i)
		}
		return vs
	}

	ab.AppendValues([]string{}, nil)
	a := ab.NewStringArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues(nil, nil)
	a = ab.NewStringArray()
	assert.Zero(t, a.Len())
	a.Release()

	ab.AppendValues([]string{}, nil)
	ab.AppendValues(want, nil)
	a = ab.NewStringArray()
	assert.Equal(t, want, stringValues(a))
	a.Release()

	ab.AppendValues(want, nil)
	ab.AppendValues([]string{}, nil)
	a = ab.NewStringArray()
	assert.Equal(t, want, stringValues(a))
	a.Release()
}

// TestStringReset tests the Reset() method on the String type by creating two different Strings and then
// reseting the contents of string2 with the values from string1.
func TestStringReset(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	sb1 := array.NewStringBuilder(mem)
	sb2 := array.NewStringBuilder(mem)
	defer sb1.Release()
	defer sb2.Release()

	sb1.Append("string1")
	sb1.AppendNull()

	var (
		string1 = sb1.NewStringArray()
		string2 = sb2.NewStringArray()

		string1Data = string1.Data()
	)
	string2.Reset(string1Data)

	assert.Equal(t, "string1", string2.Value(0))
}
