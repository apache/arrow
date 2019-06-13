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

package arrjson // import "github.com/apache/arrow/go/arrow/internal/arrjson"

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/internal/arrdata"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestReadWrite(t *testing.T) {
	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			f, err := ioutil.TempFile("", "arrjson-")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			defer os.RemoveAll(f.Name())

			w, err := NewWriter(f, recs[0].Schema())
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()

			for i, rec := range recs {
				err = w.Write(rec)
				if err != nil {
					t.Fatalf("could not write record[%d] to JSON: %v", i, err)
				}
			}

			err = w.Close()
			if err != nil {
				t.Fatalf("could not close JSON writer: %v", err)
			}

			err = f.Sync()
			if err != nil {
				t.Fatalf("could not sync data to disk: %v", err)
			}

			_, err = f.Seek(0, io.SeekStart)
			if err != nil {
				t.Fatalf("could not rewind file: %v", err)
			}

			r, err := NewReader(f, WithAllocator(mem), WithSchema(recs[0].Schema()))
			if err != nil {
				raw, _ := ioutil.ReadFile(f.Name())
				t.Fatalf("could not read JSON file: %v\n%v\n", err, string(raw))
			}
			defer r.Release()

			r.Retain()
			r.Release()

			if got, want := r.Schema(), recs[0].Schema(); !got.Equal(want) {
				t.Fatalf("invalid schema\ngot:\n%v\nwant:\n%v\n", got, want)
			}

			if got, want := r.NumRecords(), len(recs); got != want {
				t.Fatalf("invalid number of records: got=%d, want=%d", got, want)
			}

			nrecs := 0
			for {
				rec, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("could not read record[%d]: %v", nrecs, err)
				}

				if !array.RecordEqual(rec, recs[nrecs]) {
					t.Fatalf("records[%d] differ", nrecs)
				}
				nrecs++
			}

			if got, want := nrecs, len(recs); got != want {
				t.Fatalf("invalid number of records: got=%d, want=%d", got, want)
			}
		})
	}
}
