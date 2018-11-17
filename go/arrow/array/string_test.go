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

func TestStringArray(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	var (
		want   = []string{"hello", "世界", "", "bye"}
		valids = []bool{true, true, false, true}
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
}
