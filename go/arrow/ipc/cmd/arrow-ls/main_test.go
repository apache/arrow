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

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/internal/arrdata"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

func TestLsStream(t *testing.T) {
	tempDir := t.TempDir()

	for _, tc := range []struct {
		name string
		want string
	}{
		{
			name: "primitives",
			want: `schema:
  fields: 11
    - bools: type=bool, nullable
    - int8s: type=int8, nullable
    - int16s: type=int16, nullable
    - int32s: type=int32, nullable
    - int64s: type=int64, nullable
    - uint8s: type=uint8, nullable
    - uint16s: type=uint16, nullable
    - uint32s: type=uint32, nullable
    - uint64s: type=uint64, nullable
    - float32s: type=float32, nullable
    - float64s: type=float64, nullable
  metadata: ["k1": "v1", "k2": "v2", "k3": "v3"]
records: 3
`,
		},
		{
			name: "structs",
			want: `schema:
  fields: 1
    - struct_nullable: type=struct<f1: int32, f2: utf8>, nullable
records: 2
`,
		},
		{
			name: "lists",
			want: `schema:
  fields: 1
    - list_nullable: type=list<item: int32, nullable>, nullable
records: 4
`,
		},
		{
			name: "strings",
			want: `schema:
  fields: 2
    - strings: type=utf8
    - bytes: type=binary
records: 3
`,
		},
		{
			name: "fixed_size_lists",
			want: `schema:
  fields: 1
    - fixed_size_list_nullable: type=fixed_size_list<item: int32, nullable>[3], nullable
records: 3
`,
		},
		{
			name: "fixed_width_types",
			want: `schema:
  fields: 11
    - float16s: type=float16, nullable
    - time32ms: type=time32[ms], nullable
    - time32s: type=time32[s], nullable
    - time64ns: type=time64[ns], nullable
    - time64us: type=time64[us], nullable
    - timestamp_s: type=timestamp[s, tz=UTC], nullable
    - timestamp_ms: type=timestamp[ms, tz=UTC], nullable
    - timestamp_us: type=timestamp[us, tz=UTC], nullable
    - timestamp_ns: type=timestamp[ns, tz=UTC], nullable
    - date32s: type=date32, nullable
    - date64s: type=date64, nullable
records: 3
`,
		},
		{
			name: "fixed_size_binaries",
			want: `schema:
  fields: 1
    - fixed_size_binary_3: type=fixed_size_binary[3], nullable
records: 3
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			fname := func() string {
				f, err := os.CreateTemp(tempDir, "go-arrow-ls-stream-")
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()

				w := ipc.NewWriter(f, ipc.WithSchema(arrdata.Records[tc.name][0].Schema()), ipc.WithAllocator(mem))
				defer w.Close()

				for _, rec := range arrdata.Records[tc.name] {
					err = w.Write(rec)
					if err != nil {
						t.Fatal(err)
					}
				}

				err = w.Close()
				if err != nil {
					t.Fatal(err)
				}

				err = f.Close()
				if err != nil {
					t.Fatal(err)
				}

				return f.Name()
			}()

			f, err := os.Open(fname)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			w := new(bytes.Buffer)
			err = processStream(w, f)
			if err != nil {
				t.Fatal(err)
			}

			if got, want := w.String(), tc.want; got != want {
				t.Fatalf("invalid output:\ngot:\n%s\nwant:\n%s\n", got, want)
			}
		})
	}
}

func TestLsFile(t *testing.T) {
	tempDir := t.TempDir()

	for _, tc := range []struct {
		stream bool
		name   string
		want   string
	}{
		{
			stream: true,
			name:   "primitives",
			want: `schema:
  fields: 11
    - bools: type=bool, nullable
    - int8s: type=int8, nullable
    - int16s: type=int16, nullable
    - int32s: type=int32, nullable
    - int64s: type=int64, nullable
    - uint8s: type=uint8, nullable
    - uint16s: type=uint16, nullable
    - uint32s: type=uint32, nullable
    - uint64s: type=uint64, nullable
    - float32s: type=float32, nullable
    - float64s: type=float64, nullable
  metadata: ["k1": "v1", "k2": "v2", "k3": "v3"]
records: 3
`,
		},
		{
			name: "primitives",
			want: `version: V5
schema:
  fields: 11
    - bools: type=bool, nullable
    - int8s: type=int8, nullable
    - int16s: type=int16, nullable
    - int32s: type=int32, nullable
    - int64s: type=int64, nullable
    - uint8s: type=uint8, nullable
    - uint16s: type=uint16, nullable
    - uint32s: type=uint32, nullable
    - uint64s: type=uint64, nullable
    - float32s: type=float32, nullable
    - float64s: type=float64, nullable
  metadata: ["k1": "v1", "k2": "v2", "k3": "v3"]
records: 3
`,
		},
		{
			stream: true,
			name:   "structs",
			want: `schema:
  fields: 1
    - struct_nullable: type=struct<f1: int32, f2: utf8>, nullable
records: 2
`,
		},
		{
			name: "structs",
			want: `version: V5
schema:
  fields: 1
    - struct_nullable: type=struct<f1: int32, f2: utf8>, nullable
records: 2
`,
		},
		{
			stream: true,
			name:   "lists",
			want: `schema:
  fields: 1
    - list_nullable: type=list<item: int32, nullable>, nullable
records: 4
`,
		},
		{
			name: "lists",
			want: `version: V5
schema:
  fields: 1
    - list_nullable: type=list<item: int32, nullable>, nullable
records: 4
`,
		},
		{
			stream: true,
			name:   "fixed_size_binaries",
			want: `schema:
  fields: 1
    - fixed_size_binary_3: type=fixed_size_binary[3], nullable
records: 3
`,
		},
		{
			name: "fixed_size_binaries",
			want: `version: V5
schema:
  fields: 1
    - fixed_size_binary_3: type=fixed_size_binary[3], nullable
records: 3
`,
		},
	} {
		t.Run(fmt.Sprintf("%s-stream=%v", tc.name, tc.stream), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			fname := func() string {
				f, err := os.CreateTemp(tempDir, "go-arrow-ls-file-")
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()

				var w interface {
					io.Closer
					Write(arrow.Record) error
				}

				switch {
				case tc.stream:
					w = ipc.NewWriter(f, ipc.WithSchema(arrdata.Records[tc.name][0].Schema()), ipc.WithAllocator(mem))
				default:
					w, err = ipc.NewFileWriter(f, ipc.WithSchema(arrdata.Records[tc.name][0].Schema()), ipc.WithAllocator(mem))
					if err != nil {
						t.Fatal(err)
					}
				}
				defer w.Close()

				for _, rec := range arrdata.Records[tc.name] {
					err = w.Write(rec)
					if err != nil {
						t.Fatal(err)
					}
				}

				err = w.Close()
				if err != nil {
					t.Fatal(err)
				}

				err = f.Close()
				if err != nil {
					t.Fatal(err)
				}

				return f.Name()
			}()

			w := new(bytes.Buffer)
			err := processFile(w, fname)
			if err != nil {
				t.Fatal(err)
			}

			if got, want := w.String(), tc.want; got != want {
				t.Fatalf("invalid output:\ngot:\n%s\nwant:\n%s\n", got, want)
			}
		})
	}
}
