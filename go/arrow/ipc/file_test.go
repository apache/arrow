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

package ipc_test

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestFile(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	f, err := ioutil.TempFile("", "arrow-ipc-")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	defer os.Remove(f.Name())

	const nrecs = 2
	rec := genRecord(mem)
	defer rec.Release()

	func(rec array.Record) {
		w, err := ipc.NewFileWriter(f, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()

		for i := 0; i < nrecs; i++ {
			err = w.Write(rec)
			if err != nil {
				t.Fatalf("could not write record: %v", err)
			}
		}

		err = w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(rec)

	err = f.Sync()
	if err != nil {
		t.Fatalf("could not sync data to disk: %v", err)
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("could not seek to start: %v", err)
	}

	func(rec array.Record) {
		r, err := ipc.NewFileReader(f, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if got, want := r.NumRecords(), nrecs; got != want {
			t.Fatalf("invalid number of records. got=%d, want=%d", got, want)
		}

		for i := 0; i < r.NumRecords(); i++ {
			rr, err := r.Record(i)
			if err != nil {
				t.Fatalf("could not read record %d: %v", i, err)
			}
			if !cmpRecs(rr, rec) {
				t.Fatalf("records[%d] differ", i)
			}
		}

		err = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(rec)

}
