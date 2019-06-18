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

	"github.com/apache/arrow/go/arrow/internal/arrdata"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestStream(t *testing.T) {
	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			f, err := ioutil.TempFile("", "arrow-ipc-")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			defer os.Remove(f.Name())

			{
				w := ipc.NewWriter(f, ipc.WithSchema(recs[0].Schema()), ipc.WithAllocator(mem))
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
			}

			err = f.Sync()
			if err != nil {
				t.Fatalf("could not sync data to disk: %v", err)
			}

			_, err = f.Seek(0, io.SeekStart)
			if err != nil {
				t.Fatalf("could not seek to start: %v", err)
			}

			{
				r, err := ipc.NewReader(f, ipc.WithSchema(recs[0].Schema()), ipc.WithAllocator(mem))
				if err != nil {
					t.Fatal(err)
				}
				defer r.Release()

				n := 0
				for r.Next() {
					rec := r.Record()
					if !cmpRecs(rec, recs[n]) {
						t.Fatalf("records[%d] differ", n)
					}
					n++
				}

				if len(recs) != n {
					t.Fatalf("invalid number of records. got=%d, want=%d", n, len(recs))
				}
			}
		})
	}
}
