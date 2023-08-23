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

package dictutils_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/internal/dictutils"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

func TestDictMemo(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldr := array.NewFloat64Builder(mem)
	defer bldr.Release()

	bldr.AppendValues([]float64{1.0, 1.1, 1.2, 1.3}, nil)
	f0 := bldr.NewFloat64Array()
	defer f0.Release()

	bldr.AppendValues([]float64{11.0, 11.1, 11.2, 11.3}, nil)
	f1 := bldr.NewFloat64Array()
	defer f1.Release()

	bldr.AppendValues([]float64{11.0, 11.1, 11.2, 11.3}, nil)
	f2 := bldr.NewFloat64Array()
	defer f2.Release()

	memo := dictutils.NewMemo()
	defer memo.Clear()

	if got, want := memo.Len(), 0; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	memo.Add(0, f0.Data())
	memo.Add(1, f1.Data())

	if !memo.HasID(0) {
		t.Fatalf("could not find id=0")
	}

	if !memo.HasID(1) {
		t.Fatalf("could not find id=1")
	}

	if got, want := memo.Len(), 2; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	var ff arrow.Array

	ff = f0
	if !memo.HasDict(ff.Data()) {
		t.Fatalf("failed to find f0 through interface")
	}

	ff = f1
	if !memo.HasDict(ff.Data()) {
		t.Fatalf("failed to find f1 through interface")
	}

	ff = f2
	if memo.HasDict(ff.Data()) {
		t.Fatalf("should not have found f2")
	}

	fct := func(v arrow.Array) arrow.Array {
		return v
	}

	if !memo.HasDict(fct(f1).Data()) {
		t.Fatalf("failed to find dict through func through interface")
	}

	if memo.HasDict(f2.Data()) {
		t.Fatalf("should not have found f2")
	}

	ff = f0
	for i, f := range []arrow.Array{f0, f1, ff, fct(f0), fct(f1)} {
		if !memo.HasDict(f.Data()) {
			t.Fatalf("failed to find dict %d", i)
		}
	}

	v, err := memo.Dict(0, mem)
	if err != nil {
		t.Fatalf("expected to find id=0")
	}
	if v != f0.Data() {
		t.Fatalf("expected fo find id=0 array")
	}

	_, err = memo.Dict(2, mem)
	if err == nil {
		t.Fatalf("should not have found id=2")
	}
	_, err = memo.Dict(-2, mem)
	if err == nil {
		t.Fatalf("should not have found id=-2")
	}

	// test we don't leak nor "double-delete" when adding an array multiple times.
	memo.Add(42, f2.Data())
	memo.Add(43, f2.Data())
	if got, want := memo.Len(), 4; got != want {
		t.Fatalf("invalid length. got=%d, want=%d", got, want)
	}
}

func TestDictMemoPanics(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldr := array.NewFloat64Builder(mem)
	defer bldr.Release()

	bldr.AppendValues([]float64{1.0, 1.1, 1.2, 1.3}, nil)
	f0 := bldr.NewFloat64Array()
	defer f0.Release()

	bldr.AppendValues([]float64{11.0, 11.1, 11.2, 11.3}, nil)
	f1 := bldr.NewFloat64Array()
	defer f1.Release()

	for _, tc := range []struct {
		vs  []arrow.Array
		ids []int64
	}{
		{
			vs:  []arrow.Array{f0, f1},
			ids: []int64{0, 0},
		},
		{
			vs:  []arrow.Array{f0, f0},
			ids: []int64{0, 0},
		},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				e := recover()
				if e == nil {
					t.Fatalf("should have panicked!")
				}
				if got, want := e.(error), fmt.Errorf("arrow/ipc: duplicate id=%d", 0); got.Error() != want.Error() {
					t.Fatalf("invalid panic message.\ngot= %q\nwant=%q", got, want)
				}
			}()

			memo := dictutils.NewMemo()
			defer memo.Clear()

			if got, want := memo.Len(), 0; got != want {
				t.Fatalf("invalid length: got=%d, want=%d", got, want)
			}

			memo.Add(tc.ids[0], tc.vs[0].Data())
			memo.Add(tc.ids[1], tc.vs[1].Data())
		})
	}
}
