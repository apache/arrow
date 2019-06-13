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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/internal/arrdata"
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

type copyKind int

const (
	fileKind copyKind = iota
	streamKind
)

func (k copyKind) write(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []array.Record) {
	t.Helper()

	switch k {
	case fileKind:
		writeFile(t, f, mem, schema, recs)
	case streamKind:
		writeStream(t, f, mem, schema, recs)
	default:
		panic("invalid copyKind")
	}
}

func (k copyKind) check(t *testing.T, f *os.File, mem memory.Allocator, schema *arrow.Schema, recs []array.Record) {
	t.Helper()

	switch k {
	case fileKind:
		checkArrowFile(t, f, mem, schema, recs)
	case streamKind:
		checkArrowStream(t, f, mem, schema, recs)
	default:
		panic("invalid copyKind")
	}
}

func TestCopy(t *testing.T) {
	type kind int

	for _, tc := range []struct {
		name     string
		src, dst copyKind
	}{
		{name: "file2file", src: fileKind, dst: fileKind},
		{name: "file2stream", src: fileKind, dst: streamKind},
		{name: "stream2file", src: streamKind, dst: fileKind},
		{name: "stream2stream", src: streamKind, dst: streamKind},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for name, recs := range arrdata.Records {
				t.Run(name, func(t *testing.T) {
					for _, tcopy := range []struct {
						n    int
						want int
						err  error
					}{
						{-1, len(recs), nil},
						{1, 1, nil},
						{0, 0, nil},
						{len(recs), len(recs), nil},
						{len(recs) + 1, len(recs), io.EOF},
					} {
						t.Run(fmt.Sprintf("-copy-n=%d", tcopy.n), func(t *testing.T) {
							mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
							defer mem.AssertSize(t, 0)

							f, err := ioutil.TempFile("", "arrow-ipc-")
							if err != nil {
								t.Fatal(err)
							}
							defer f.Close()
							defer os.Remove(f.Name())

							o, err := ioutil.TempFile("", "arrow-ipc-")
							if err != nil {
								t.Fatal(err)
							}
							defer o.Close()
							defer os.Remove(o.Name())

							tc.src.write(t, f, mem, recs[0].Schema(), recs)
							tc.src.check(t, f, mem, recs[0].Schema(), recs)

							_, err = f.Seek(0, io.SeekStart)
							if err != nil {
								t.Fatal(err)
							}

							var r ipc.RecordReader
							switch tc.src {
							case fileKind:
								rr, err := ipc.NewFileReader(f, ipc.WithSchema(recs[0].Schema()), ipc.WithAllocator(mem))
								if err != nil {
									t.Fatal(err)
								}
								defer rr.Close()
								r = rr
							case streamKind:
								rr, err := ipc.NewReader(f, ipc.WithSchema(recs[0].Schema()), ipc.WithAllocator(mem))
								if err != nil {
									t.Fatal(err)
								}
								defer rr.Release()
								r = rr
							default:
								t.Fatalf("invalid src type %v", tc.src)
							}

							var w interface {
								ipc.RecordWriter
								io.Closer
							}

							switch tc.dst {
							case fileKind:
								w, err = ipc.NewFileWriter(o, ipc.WithSchema(recs[0].Schema()), ipc.WithAllocator(mem))
								if err != nil {
									t.Fatal(err)
								}
							case streamKind:
								w = ipc.NewWriter(o, ipc.WithSchema(recs[0].Schema()), ipc.WithAllocator(mem))
							default:
								t.Fatalf("invalid dst type %v", tc.dst)
							}
							defer w.Close()

							var (
								n int64
							)
							switch tcopy.n {
							case -1:
								n, err = ipc.Copy(w, r)
							case len(recs) + 1:
								n, err = ipc.CopyN(w, r, int64(tcopy.n))
							default:
								n, err = ipc.CopyN(w, r, int64(tcopy.n))
							}

							switch err {
							case nil:
								if tcopy.err != nil {
									t.Fatalf("got a nil error, want=%v", tcopy.err)
								}
							default:
								switch tcopy.err {
								case nil:
									t.Fatalf("invalid error: got=%v, want=%v", err, tcopy.err)
								default:
									if tcopy.err.Error() != err.Error() {
										t.Fatalf("invalid error: got=%v, want=%v", err, tcopy.err)
									}
								}
							}

							if got, want := n, int64(tcopy.want); got != want {
								t.Fatalf("invalid number of records copied: got=%d, want=%d", got, want)
							}

							err = w.Close()
							if err != nil {
								t.Fatal(err)
							}

							tc.dst.check(t, o, mem, recs[0].Schema(), recs[:tcopy.want])
						})
					}
				})
			}
		})
	}
}
