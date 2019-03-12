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

package ipc // import "github.com/apache/arrow/go/arrow/ipc"

import (
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestDictMemo(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	bldr := array.NewFloat64Builder(mem)
	defer bldr.Release()

	bldr.AppendValues([]float64{1.0, 1.1, 1.2, 1.3}, nil)
	f1 := bldr.NewFloat64Array()
	defer f1.Release()

	bldr.AppendValues([]float64{11.0, 11.1, 11.2, 11.3}, nil)
	f2 := bldr.NewFloat64Array()
	defer f2.Release()

	bldr.AppendValues([]float64{11.0, 11.1, 11.2, 11.3}, nil)
	f3 := bldr.NewFloat64Array()
	defer f3.Release()

	memo := newMemo()
	defer memo.delete()

	if got, want := memo.Len(), 0; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	memo.Add(1, f1)
	memo.Add(2, f2)

	if !memo.HasID(1) {
		t.Fatalf("could not find id=1")
	}

	if !memo.HasID(2) {
		t.Fatalf("could not find id=1")
	}

	if got, want := memo.Len(), 2; got != want {
		t.Fatalf("invalid length: got=%d, want=%d", got, want)
	}

	var ff array.Interface

	ff = f1
	if !memo.HasDict(ff) {
		t.Fatalf("failed to find f1 through interface")
	}

	ff = f2
	if !memo.HasDict(ff) {
		t.Fatalf("failed to find f2 through interface")
	}

	ff = f3
	if memo.HasDict(ff) {
		t.Fatalf("should not have found f3")
	}

	fct := func(v array.Interface) array.Interface {
		return v
	}

	if !memo.HasDict(fct(f2)) {
		t.Fatalf("failed to find dict through func through interface")
	}

	if memo.HasDict(f3) {
		t.Fatalf("should not have found f3")
	}

	ff = f1
	for i, f := range []array.Interface{f1, f2, ff, fct(f1), fct(f2)} {
		if !memo.HasDict(f) {
			t.Fatalf("failed to find dict %d", i)
		}
	}
}
