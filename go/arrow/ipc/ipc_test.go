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
	"os"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

func checkArrowFile(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []array.Record) {
	t.Helper()

	err := f.Sync()
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	r, err := ipc.NewFileReader(f, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i := 0; i < r.NumRecords(); i++ {
		rec, err := r.Record(i)
		if err != nil {
			t.Fatalf("could not read record %d: %v", i, err)
		}
		if !array.RecordEqual(rec, recs[i]) {
			t.Fatalf("records[%d] differ", i)
		}
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func checkArrowStream(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []array.Record) {
	t.Helper()

	err := f.Sync()
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	r, err := ipc.NewReader(f, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()

	n := 0
	for r.Next() {
		rec := r.Record()
		if !array.RecordEqual(rec, recs[n]) {
			t.Fatalf("records[%d] differ", n)
		}
		n++
	}

	if len(recs) != n {
		t.Fatalf("invalid number of records. got=%d, want=%d", n, len(recs))

	}
}

func writeFile(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []array.Record) {
	t.Helper()

	w, err := ipc.NewFileWriter(f, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i, rec := range recs {
		err = w.Write(rec)
		if err != nil {
			t.Fatalf("could not write record[%d]: %v", i, err)
		}
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = f.Sync()
	if err != nil {
		t.Fatalf("could not sync data to disk: %v", err)
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("could not seek to start: %v", err)
	}
}

func writeStream(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []array.Record) {
	t.Helper()

	w := ipc.NewWriter(f, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	defer w.Close()

	for i, rec := range recs {
		err := w.Write(rec)
		if err != nil {
			t.Fatalf("could not write record[%d]: %v", i, err)
		}
	}

	err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
}
