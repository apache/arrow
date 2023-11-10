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

package arrdata

import (
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/internal/flatbuf"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

// CheckArrowFile checks whether a given ARROW file contains the expected list of records.
func CheckArrowFile(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []arrow.Record) {
	t.Helper()

	_, err := f.Seek(0, io.SeekStart)
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

func CheckArrowConcurrentFile(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []arrow.Record) {
	t.Helper()

	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	r, err := ipc.NewFileReader(f, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var g sync.WaitGroup
	errs := make(chan error, r.NumRecords())
	checkRecord := func(i int) {
		defer g.Done()
		rec, err := r.RecordAt(i)
		if err != nil {
			errs <- fmt.Errorf("could not read record %d: %v", i, err)
			return
		}
		defer rec.Release()
		if !array.RecordEqual(rec, recs[i]) {
			errs <- fmt.Errorf("records[%d] differ", i)
		}
	}

	for i := 0; i < r.NumRecords(); i++ {
		g.Add(1)
		go checkRecord(i)
	}

	g.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// CheckArrowStream checks whether a given ARROW stream contains the expected list of records.
func CheckArrowStream(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []arrow.Record) {
	t.Helper()

	_, err := f.Seek(0, io.SeekStart)
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
			t.Fatalf("records[%d] differ, got: %s, expected %s", n, rec, recs[n])
		}
		n++
	}

	if len(recs) != n {
		t.Fatalf("invalid number of records. got=%d, want=%d", n, len(recs))

	}
}

// WriteFile writes a list of records to the given file descriptor, as an ARROW file.
func WriteFile(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []arrow.Record) {
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

	// put the cursor back at the start of the file before returning rather than
	// leaving it at the end so the reader can just start reading from the handle
	// immediately for the test.
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("could not seek to start: %v", err)
	}
}

// WriteFile writes a list of records to the given file descriptor, as an ARROW file.
func WriteFileCompressed(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []arrow.Record, codec flatbuf.CompressionType, concurrency int) {
	t.Helper()

	opts := []ipc.Option{ipc.WithSchema(schema), ipc.WithAllocator(mem), ipc.WithCompressConcurrency(concurrency)}
	switch codec {
	case flatbuf.CompressionTypeLZ4_FRAME:
		opts = append(opts, ipc.WithLZ4())
	case flatbuf.CompressionTypeZSTD:
		opts = append(opts, ipc.WithZstd())
	default:
		t.Fatalf("invalid compression codec %v, only LZ4_FRAME or ZSTD is allowed", codec)
	}

	w, err := ipc.NewFileWriter(f, opts...)
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

	// put the cursor back at the start of the file before returning rather than
	// leaving it at the end so the reader can just start reading from the handle
	// immediately for the test.
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("could not seek to start: %v", err)
	}
}

// WriteStream writes a list of records to the given file descriptor, as an ARROW stream.
func WriteStream(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []arrow.Record) {
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

// WriteStreamCompressed writes a list of records to the given file descriptor as an ARROW stream
// using the provided compression type.
func WriteStreamCompressed(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []arrow.Record, codec flatbuf.CompressionType, np int) {
	t.Helper()

	opts := []ipc.Option{ipc.WithSchema(schema), ipc.WithAllocator(mem), ipc.WithCompressConcurrency(np)}
	switch codec {
	case flatbuf.CompressionTypeLZ4_FRAME:
		opts = append(opts, ipc.WithLZ4())
	case flatbuf.CompressionTypeZSTD:
		opts = append(opts, ipc.WithZstd())
	default:
		t.Fatalf("invalid compression codec %v, only LZ4_FRAME or ZSTD is allowed", codec)
	}

	w := ipc.NewWriter(f, opts...)
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
