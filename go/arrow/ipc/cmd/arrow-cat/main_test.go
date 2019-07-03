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
	"io/ioutil"
	"os"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/internal/arrdata"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestCatStream(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
	}{
		{
			name: "primitives",
			want: `record 1...
  col[0] "bools": [true (null) (null) false true]
  col[1] "int8s": [-1 (null) (null) -4 -5]
  col[2] "int16s": [-1 (null) (null) -4 -5]
  col[3] "int32s": [-1 (null) (null) -4 -5]
  col[4] "int64s": [-1 (null) (null) -4 -5]
  col[5] "uint8s": [1 (null) (null) 4 5]
  col[6] "uint16s": [1 (null) (null) 4 5]
  col[7] "uint32s": [1 (null) (null) 4 5]
  col[8] "uint64s": [1 (null) (null) 4 5]
  col[9] "float32s": [1 (null) (null) 4 5]
  col[10] "float64s": [1 (null) (null) 4 5]
record 2...
  col[0] "bools": [true (null) (null) false true]
  col[1] "int8s": [-11 (null) (null) -14 -15]
  col[2] "int16s": [-11 (null) (null) -14 -15]
  col[3] "int32s": [-11 (null) (null) -14 -15]
  col[4] "int64s": [-11 (null) (null) -14 -15]
  col[5] "uint8s": [11 (null) (null) 14 15]
  col[6] "uint16s": [11 (null) (null) 14 15]
  col[7] "uint32s": [11 (null) (null) 14 15]
  col[8] "uint64s": [11 (null) (null) 14 15]
  col[9] "float32s": [11 (null) (null) 14 15]
  col[10] "float64s": [11 (null) (null) 14 15]
record 3...
  col[0] "bools": [true (null) (null) false true]
  col[1] "int8s": [-21 (null) (null) -24 -25]
  col[2] "int16s": [-21 (null) (null) -24 -25]
  col[3] "int32s": [-21 (null) (null) -24 -25]
  col[4] "int64s": [-21 (null) (null) -24 -25]
  col[5] "uint8s": [21 (null) (null) 24 25]
  col[6] "uint16s": [21 (null) (null) 24 25]
  col[7] "uint32s": [21 (null) (null) 24 25]
  col[8] "uint64s": [21 (null) (null) 24 25]
  col[9] "float32s": [21 (null) (null) 24 25]
  col[10] "float64s": [21 (null) (null) 24 25]
`,
		},
		{
			name: "structs",
			want: `record 1...
  col[0] "struct_nullable": {[-1 (null) (null) -4 -5 (null) -11 (null) (null) -14 -15 -21 (null) (null) -24 -25 -31 (null) (null) -34 -35 -41 (null) (null) -44 -45] ["111" (null) (null) "444" "555" (null) "1111" (null) (null) "1444" "1555" "2111" (null) (null) "2444" "2555" "3111" (null) (null) "3444" "3555" "4111" (null) (null) "4444" "4555"]}
record 2...
  col[0] "struct_nullable": {[1 (null) (null) 4 5 (null) 11 (null) (null) 14 15 (null) 21 (null) (null) 24 25 31 (null) (null) 34 35 41 (null) (null) 44 45] ["-111" (null) (null) "-444" "-555" (null) "-1111" (null) (null) "-1444" "-1555" (null) "-2111" (null) (null) "-2444" "-2555" "-3111" (null) (null) "-3444" "-3555" "-4111" (null) (null) "-4444" "-4555"]}
`,
		},
		{
			name: "lists",
			want: `record 1...
  col[0] "list_nullable": [[1 (null) (null) 4 5] [11 (null) (null) 14 15] [21 (null) (null) 24 25]]
record 2...
  col[0] "list_nullable": [[-1 (null) (null) -4 -5] [-11 (null) (null) -14 -15] [-21 (null) (null) -24 -25]]
record 3...
  col[0] "list_nullable": [[-1 (null) (null) -4 -5] (null) [-21 (null) (null) -24 -25]]
record 4...
  col[0] "list_nullable": []
`,
		},
		{
			name: "strings",
			want: `record 1...
  col[0] "strings": ["1é" (null) (null) "4" "5"]
  col[1] "bytes": ["1é" (null) (null) "4" "5"]
record 2...
  col[0] "strings": ["11" (null) (null) "44" "55"]
  col[1] "bytes": ["11" (null) (null) "44" "55"]
record 3...
  col[0] "strings": ["111" (null) (null) "444" "555"]
  col[1] "bytes": ["111" (null) (null) "444" "555"]
`,
		},
		{
			name: "fixed_size_lists",
			want: `record 1...
  col[0] "fixed_size_list_nullable": [[1 (null) 3] [11 (null) 13] [21 (null) 23]]
record 2...
  col[0] "fixed_size_list_nullable": [[-1 (null) -3] [-11 (null) -13] [-21 (null) -23]]
record 3...
  col[0] "fixed_size_list_nullable": [[-1 (null) -3] (null) [-21 (null) -23]]
`,
		},
		{
			name: "fixed_width_types",
			want: `record 1...
  col[0] "float16s": [1 (null) (null) 4 5]
  col[1] "time32ms": [-2 (null) (null) 1 2]
  col[2] "time32s": [-2 (null) (null) 1 2]
  col[3] "time64ns": [-2 (null) (null) 1 2]
  col[4] "time64us": [-2 (null) (null) 1 2]
  col[5] "timestamp_s": [0 (null) (null) 3 4]
  col[6] "timestamp_ms": [0 (null) (null) 3 4]
  col[7] "timestamp_us": [0 (null) (null) 3 4]
  col[8] "timestamp_ns": [0 (null) (null) 3 4]
  col[9] "date32s": [-2 (null) (null) 1 2]
  col[10] "date64s": [-2 (null) (null) 1 2]
record 2...
  col[0] "float16s": [11 (null) (null) 14 15]
  col[1] "time32ms": [-12 (null) (null) 11 12]
  col[2] "time32s": [-12 (null) (null) 11 12]
  col[3] "time64ns": [-12 (null) (null) 11 12]
  col[4] "time64us": [-12 (null) (null) 11 12]
  col[5] "timestamp_s": [10 (null) (null) 13 14]
  col[6] "timestamp_ms": [10 (null) (null) 13 14]
  col[7] "timestamp_us": [10 (null) (null) 13 14]
  col[8] "timestamp_ns": [10 (null) (null) 13 14]
  col[9] "date32s": [-12 (null) (null) 11 12]
  col[10] "date64s": [-12 (null) (null) 11 12]
record 3...
  col[0] "float16s": [21 (null) (null) 24 25]
  col[1] "time32ms": [-22 (null) (null) 21 22]
  col[2] "time32s": [-22 (null) (null) 21 22]
  col[3] "time64ns": [-22 (null) (null) 21 22]
  col[4] "time64us": [-22 (null) (null) 21 22]
  col[5] "timestamp_s": [20 (null) (null) 23 24]
  col[6] "timestamp_ms": [20 (null) (null) 23 24]
  col[7] "timestamp_us": [20 (null) (null) 23 24]
  col[8] "timestamp_ns": [20 (null) (null) 23 24]
  col[9] "date32s": [-22 (null) (null) 21 22]
  col[10] "date64s": [-22 (null) (null) 21 22]
`,
		},
		{
			name: "fixed_size_binaries",
			want: `record 1...
  col[0] "fixed_size_binary_3": ["001" (null) (null) "004" "005"]
record 2...
  col[0] "fixed_size_binary_3": ["011" (null) (null) "014" "015"]
record 3...
  col[0] "fixed_size_binary_3": ["021" (null) (null) "024" "025"]
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			fname := func() string {
				f, err := ioutil.TempFile("", "go-arrow-")
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
			defer os.Remove(fname)

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

func TestCatFile(t *testing.T) {
	for _, tc := range []struct {
		name   string
		want   string
		stream bool
	}{
		{
			stream: true,
			name:   "primitives",
			want: `record 1...
  col[0] "bools": [true (null) (null) false true]
  col[1] "int8s": [-1 (null) (null) -4 -5]
  col[2] "int16s": [-1 (null) (null) -4 -5]
  col[3] "int32s": [-1 (null) (null) -4 -5]
  col[4] "int64s": [-1 (null) (null) -4 -5]
  col[5] "uint8s": [1 (null) (null) 4 5]
  col[6] "uint16s": [1 (null) (null) 4 5]
  col[7] "uint32s": [1 (null) (null) 4 5]
  col[8] "uint64s": [1 (null) (null) 4 5]
  col[9] "float32s": [1 (null) (null) 4 5]
  col[10] "float64s": [1 (null) (null) 4 5]
record 2...
  col[0] "bools": [true (null) (null) false true]
  col[1] "int8s": [-11 (null) (null) -14 -15]
  col[2] "int16s": [-11 (null) (null) -14 -15]
  col[3] "int32s": [-11 (null) (null) -14 -15]
  col[4] "int64s": [-11 (null) (null) -14 -15]
  col[5] "uint8s": [11 (null) (null) 14 15]
  col[6] "uint16s": [11 (null) (null) 14 15]
  col[7] "uint32s": [11 (null) (null) 14 15]
  col[8] "uint64s": [11 (null) (null) 14 15]
  col[9] "float32s": [11 (null) (null) 14 15]
  col[10] "float64s": [11 (null) (null) 14 15]
record 3...
  col[0] "bools": [true (null) (null) false true]
  col[1] "int8s": [-21 (null) (null) -24 -25]
  col[2] "int16s": [-21 (null) (null) -24 -25]
  col[3] "int32s": [-21 (null) (null) -24 -25]
  col[4] "int64s": [-21 (null) (null) -24 -25]
  col[5] "uint8s": [21 (null) (null) 24 25]
  col[6] "uint16s": [21 (null) (null) 24 25]
  col[7] "uint32s": [21 (null) (null) 24 25]
  col[8] "uint64s": [21 (null) (null) 24 25]
  col[9] "float32s": [21 (null) (null) 24 25]
  col[10] "float64s": [21 (null) (null) 24 25]
`,
		},
		{
			name: "primitives",
			want: `version: V4
record 1/3...
  col[0] "bools": [true (null) (null) false true]
  col[1] "int8s": [-1 (null) (null) -4 -5]
  col[2] "int16s": [-1 (null) (null) -4 -5]
  col[3] "int32s": [-1 (null) (null) -4 -5]
  col[4] "int64s": [-1 (null) (null) -4 -5]
  col[5] "uint8s": [1 (null) (null) 4 5]
  col[6] "uint16s": [1 (null) (null) 4 5]
  col[7] "uint32s": [1 (null) (null) 4 5]
  col[8] "uint64s": [1 (null) (null) 4 5]
  col[9] "float32s": [1 (null) (null) 4 5]
  col[10] "float64s": [1 (null) (null) 4 5]
record 2/3...
  col[0] "bools": [true (null) (null) false true]
  col[1] "int8s": [-11 (null) (null) -14 -15]
  col[2] "int16s": [-11 (null) (null) -14 -15]
  col[3] "int32s": [-11 (null) (null) -14 -15]
  col[4] "int64s": [-11 (null) (null) -14 -15]
  col[5] "uint8s": [11 (null) (null) 14 15]
  col[6] "uint16s": [11 (null) (null) 14 15]
  col[7] "uint32s": [11 (null) (null) 14 15]
  col[8] "uint64s": [11 (null) (null) 14 15]
  col[9] "float32s": [11 (null) (null) 14 15]
  col[10] "float64s": [11 (null) (null) 14 15]
record 3/3...
  col[0] "bools": [true (null) (null) false true]
  col[1] "int8s": [-21 (null) (null) -24 -25]
  col[2] "int16s": [-21 (null) (null) -24 -25]
  col[3] "int32s": [-21 (null) (null) -24 -25]
  col[4] "int64s": [-21 (null) (null) -24 -25]
  col[5] "uint8s": [21 (null) (null) 24 25]
  col[6] "uint16s": [21 (null) (null) 24 25]
  col[7] "uint32s": [21 (null) (null) 24 25]
  col[8] "uint64s": [21 (null) (null) 24 25]
  col[9] "float32s": [21 (null) (null) 24 25]
  col[10] "float64s": [21 (null) (null) 24 25]
`,
		},
		{
			stream: true,
			name:   "structs",
			want: `record 1...
  col[0] "struct_nullable": {[-1 (null) (null) -4 -5 (null) -11 (null) (null) -14 -15 -21 (null) (null) -24 -25 -31 (null) (null) -34 -35 -41 (null) (null) -44 -45] ["111" (null) (null) "444" "555" (null) "1111" (null) (null) "1444" "1555" "2111" (null) (null) "2444" "2555" "3111" (null) (null) "3444" "3555" "4111" (null) (null) "4444" "4555"]}
record 2...
  col[0] "struct_nullable": {[1 (null) (null) 4 5 (null) 11 (null) (null) 14 15 (null) 21 (null) (null) 24 25 31 (null) (null) 34 35 41 (null) (null) 44 45] ["-111" (null) (null) "-444" "-555" (null) "-1111" (null) (null) "-1444" "-1555" (null) "-2111" (null) (null) "-2444" "-2555" "-3111" (null) (null) "-3444" "-3555" "-4111" (null) (null) "-4444" "-4555"]}
`,
		},
		{
			name: "structs",
			want: `version: V4
record 1/2...
  col[0] "struct_nullable": {[-1 (null) (null) -4 -5 (null) -11 (null) (null) -14 -15 -21 (null) (null) -24 -25 -31 (null) (null) -34 -35 -41 (null) (null) -44 -45] ["111" (null) (null) "444" "555" (null) "1111" (null) (null) "1444" "1555" "2111" (null) (null) "2444" "2555" "3111" (null) (null) "3444" "3555" "4111" (null) (null) "4444" "4555"]}
record 2/2...
  col[0] "struct_nullable": {[1 (null) (null) 4 5 (null) 11 (null) (null) 14 15 (null) 21 (null) (null) 24 25 31 (null) (null) 34 35 41 (null) (null) 44 45] ["-111" (null) (null) "-444" "-555" (null) "-1111" (null) (null) "-1444" "-1555" (null) "-2111" (null) (null) "-2444" "-2555" "-3111" (null) (null) "-3444" "-3555" "-4111" (null) (null) "-4444" "-4555"]}
`,
		},
		{
			stream: true,
			name:   "lists",
			want: `record 1...
  col[0] "list_nullable": [[1 (null) (null) 4 5] [11 (null) (null) 14 15] [21 (null) (null) 24 25]]
record 2...
  col[0] "list_nullable": [[-1 (null) (null) -4 -5] [-11 (null) (null) -14 -15] [-21 (null) (null) -24 -25]]
record 3...
  col[0] "list_nullable": [[-1 (null) (null) -4 -5] (null) [-21 (null) (null) -24 -25]]
record 4...
  col[0] "list_nullable": []
`,
		},
		{
			name: "lists",
			want: `version: V4
record 1/4...
  col[0] "list_nullable": [[1 (null) (null) 4 5] [11 (null) (null) 14 15] [21 (null) (null) 24 25]]
record 2/4...
  col[0] "list_nullable": [[-1 (null) (null) -4 -5] [-11 (null) (null) -14 -15] [-21 (null) (null) -24 -25]]
record 3/4...
  col[0] "list_nullable": [[-1 (null) (null) -4 -5] (null) [-21 (null) (null) -24 -25]]
record 4/4...
  col[0] "list_nullable": []
`,
		},
		{
			stream: true,
			name:   "strings",
			want: `record 1...
  col[0] "strings": ["1é" (null) (null) "4" "5"]
  col[1] "bytes": ["1é" (null) (null) "4" "5"]
record 2...
  col[0] "strings": ["11" (null) (null) "44" "55"]
  col[1] "bytes": ["11" (null) (null) "44" "55"]
record 3...
  col[0] "strings": ["111" (null) (null) "444" "555"]
  col[1] "bytes": ["111" (null) (null) "444" "555"]
`,
		},
		{
			name: "strings",
			want: `version: V4
record 1/3...
  col[0] "strings": ["1é" (null) (null) "4" "5"]
  col[1] "bytes": ["1é" (null) (null) "4" "5"]
record 2/3...
  col[0] "strings": ["11" (null) (null) "44" "55"]
  col[1] "bytes": ["11" (null) (null) "44" "55"]
record 3/3...
  col[0] "strings": ["111" (null) (null) "444" "555"]
  col[1] "bytes": ["111" (null) (null) "444" "555"]
`,
		},
		{
			stream: true,
			name:   "fixed_size_lists",
			want: `record 1...
  col[0] "fixed_size_list_nullable": [[1 (null) 3] [11 (null) 13] [21 (null) 23]]
record 2...
  col[0] "fixed_size_list_nullable": [[-1 (null) -3] [-11 (null) -13] [-21 (null) -23]]
record 3...
  col[0] "fixed_size_list_nullable": [[-1 (null) -3] (null) [-21 (null) -23]]
`,
		},
		{
			name: "fixed_size_lists",
			want: `version: V4
record 1/3...
  col[0] "fixed_size_list_nullable": [[1 (null) 3] [11 (null) 13] [21 (null) 23]]
record 2/3...
  col[0] "fixed_size_list_nullable": [[-1 (null) -3] [-11 (null) -13] [-21 (null) -23]]
record 3/3...
  col[0] "fixed_size_list_nullable": [[-1 (null) -3] (null) [-21 (null) -23]]
`,
		},
		{
			stream: true,
			name:   "fixed_width_types",
			want: `record 1...
  col[0] "float16s": [1 (null) (null) 4 5]
  col[1] "time32ms": [-2 (null) (null) 1 2]
  col[2] "time32s": [-2 (null) (null) 1 2]
  col[3] "time64ns": [-2 (null) (null) 1 2]
  col[4] "time64us": [-2 (null) (null) 1 2]
  col[5] "timestamp_s": [0 (null) (null) 3 4]
  col[6] "timestamp_ms": [0 (null) (null) 3 4]
  col[7] "timestamp_us": [0 (null) (null) 3 4]
  col[8] "timestamp_ns": [0 (null) (null) 3 4]
  col[9] "date32s": [-2 (null) (null) 1 2]
  col[10] "date64s": [-2 (null) (null) 1 2]
record 2...
  col[0] "float16s": [11 (null) (null) 14 15]
  col[1] "time32ms": [-12 (null) (null) 11 12]
  col[2] "time32s": [-12 (null) (null) 11 12]
  col[3] "time64ns": [-12 (null) (null) 11 12]
  col[4] "time64us": [-12 (null) (null) 11 12]
  col[5] "timestamp_s": [10 (null) (null) 13 14]
  col[6] "timestamp_ms": [10 (null) (null) 13 14]
  col[7] "timestamp_us": [10 (null) (null) 13 14]
  col[8] "timestamp_ns": [10 (null) (null) 13 14]
  col[9] "date32s": [-12 (null) (null) 11 12]
  col[10] "date64s": [-12 (null) (null) 11 12]
record 3...
  col[0] "float16s": [21 (null) (null) 24 25]
  col[1] "time32ms": [-22 (null) (null) 21 22]
  col[2] "time32s": [-22 (null) (null) 21 22]
  col[3] "time64ns": [-22 (null) (null) 21 22]
  col[4] "time64us": [-22 (null) (null) 21 22]
  col[5] "timestamp_s": [20 (null) (null) 23 24]
  col[6] "timestamp_ms": [20 (null) (null) 23 24]
  col[7] "timestamp_us": [20 (null) (null) 23 24]
  col[8] "timestamp_ns": [20 (null) (null) 23 24]
  col[9] "date32s": [-22 (null) (null) 21 22]
  col[10] "date64s": [-22 (null) (null) 21 22]
`,
		},
		{
			name: "fixed_width_types",
			want: `version: V4
record 1/3...
  col[0] "float16s": [1 (null) (null) 4 5]
  col[1] "time32ms": [-2 (null) (null) 1 2]
  col[2] "time32s": [-2 (null) (null) 1 2]
  col[3] "time64ns": [-2 (null) (null) 1 2]
  col[4] "time64us": [-2 (null) (null) 1 2]
  col[5] "timestamp_s": [0 (null) (null) 3 4]
  col[6] "timestamp_ms": [0 (null) (null) 3 4]
  col[7] "timestamp_us": [0 (null) (null) 3 4]
  col[8] "timestamp_ns": [0 (null) (null) 3 4]
  col[9] "date32s": [-2 (null) (null) 1 2]
  col[10] "date64s": [-2 (null) (null) 1 2]
record 2/3...
  col[0] "float16s": [11 (null) (null) 14 15]
  col[1] "time32ms": [-12 (null) (null) 11 12]
  col[2] "time32s": [-12 (null) (null) 11 12]
  col[3] "time64ns": [-12 (null) (null) 11 12]
  col[4] "time64us": [-12 (null) (null) 11 12]
  col[5] "timestamp_s": [10 (null) (null) 13 14]
  col[6] "timestamp_ms": [10 (null) (null) 13 14]
  col[7] "timestamp_us": [10 (null) (null) 13 14]
  col[8] "timestamp_ns": [10 (null) (null) 13 14]
  col[9] "date32s": [-12 (null) (null) 11 12]
  col[10] "date64s": [-12 (null) (null) 11 12]
record 3/3...
  col[0] "float16s": [21 (null) (null) 24 25]
  col[1] "time32ms": [-22 (null) (null) 21 22]
  col[2] "time32s": [-22 (null) (null) 21 22]
  col[3] "time64ns": [-22 (null) (null) 21 22]
  col[4] "time64us": [-22 (null) (null) 21 22]
  col[5] "timestamp_s": [20 (null) (null) 23 24]
  col[6] "timestamp_ms": [20 (null) (null) 23 24]
  col[7] "timestamp_us": [20 (null) (null) 23 24]
  col[8] "timestamp_ns": [20 (null) (null) 23 24]
  col[9] "date32s": [-22 (null) (null) 21 22]
  col[10] "date64s": [-22 (null) (null) 21 22]
`,
		},
		{
			stream: true,
			name:   "fixed_size_binaries",
			want: `record 1...
  col[0] "fixed_size_binary_3": ["001" (null) (null) "004" "005"]
record 2...
  col[0] "fixed_size_binary_3": ["011" (null) (null) "014" "015"]
record 3...
  col[0] "fixed_size_binary_3": ["021" (null) (null) "024" "025"]
`,
		},
		{
			name: "fixed_size_binaries",
			want: `version: V4
record 1/3...
  col[0] "fixed_size_binary_3": ["001" (null) (null) "004" "005"]
record 2/3...
  col[0] "fixed_size_binary_3": ["011" (null) (null) "014" "015"]
record 3/3...
  col[0] "fixed_size_binary_3": ["021" (null) (null) "024" "025"]
`,
		},
	} {
		t.Run(fmt.Sprintf("%s-stream=%v", tc.name, tc.stream), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			fname := func() string {
				f, err := ioutil.TempFile("", "go-arrow-")
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()

				var w interface {
					io.Closer
					Write(array.Record) error
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
			defer os.Remove(fname)

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
