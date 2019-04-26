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
	"os"
	"testing"
)

func TestLsStream(t *testing.T) {
	for _, tc := range []struct {
		fname string
		want  string
	}{
		{
			fname: "../../testdata/primitives.stream.data",
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
records: 3
`,
		},
		{
			fname: "../../testdata/structs.stream.data",
			want: `schema:
  fields: 1
    - struct_nullable: type=struct<f1: int32, f2: utf8>, nullable
records: 2
`,
		},
		{
			fname: "../../testdata/lists.stream.data",
			want: `schema:
  fields: 2
    - list_nullable: type=list<item: int32>, nullable
    - struct_nullable: type=struct<f1: int32, f2: utf8>, nullable
records: 2
`,
		},
	} {
		t.Run(tc.fname, func(t *testing.T) {
			f, err := os.Open(tc.fname)
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
	for _, tc := range []struct {
		fname string
		want  string
	}{
		{
			fname: "../../testdata/primitives.stream.data",
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
records: 3
`,
		},
		{
			fname: "../../testdata/primitives.file.data",
			want: `version: V4
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
records: 3
`,
		},
		{
			fname: "../../testdata/structs.stream.data",
			want: `schema:
  fields: 1
    - struct_nullable: type=struct<f1: int32, f2: utf8>, nullable
records: 2
`,
		},
		{
			fname: "../../testdata/structs.file.data",
			want: `version: V4
schema:
  fields: 1
    - struct_nullable: type=struct<f1: int32, f2: utf8>, nullable
records: 2
`,
		},
		{
			fname: "../../testdata/lists.file.data",
			want: `version: V4
schema:
  fields: 2
    - list_nullable: type=list<item: int32>, nullable
    - struct_nullable: type=struct<f1: int32, f2: utf8>, nullable
records: 2
`,
		},
		{
			fname: "../../testdata/lists.stream.data",
			want: `schema:
  fields: 2
    - list_nullable: type=list<item: int32>, nullable
    - struct_nullable: type=struct<f1: int32, f2: utf8>, nullable
records: 2
`,
		},
	} {
		t.Run(tc.fname, func(t *testing.T) {
			w := new(bytes.Buffer)
			err := processFile(w, tc.fname)
			if err != nil {
				t.Fatal(err)
			}

			if got, want := w.String(), tc.want; got != want {
				t.Fatalf("invalid output:\ngot:\n%s\nwant:\n%s\n", got, want)
			}
		})
	}
}
