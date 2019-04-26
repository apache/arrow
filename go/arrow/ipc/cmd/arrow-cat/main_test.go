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

func TestCatStream(t *testing.T) {
	for _, tc := range []struct {
		fname string
		want  string
	}{
		{
			fname: "../../testdata/primitives.stream.data",
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
			fname: "../../testdata/structs.stream.data",
			want: `record 1...
  col[0] "struct_nullable": {[1402032511 (null) 137773603 410361374 1959836418 (null) (null)] [(null) "MhRNxD4" "3F9HBxK" "aVd88fp" (null) "3loZrRf" (null)]}
record 2...
  col[0] "struct_nullable": {[(null) (null) (null) (null) (null) (null) 413888857 (null) (null) (null)] ["AS5oARE" (null) (null) "JGdagcX" "78SLiRw" "vbGf7OY" "5uh5fTs" "0ilsf82" "LjS9MbU" (null)]}
`,
		},
		{
			fname: "../../testdata/lists.stream.data",
			want: `record 1...
  col[0] "list_nullable": [[(null) -340485928 -259730121 -1430069283] [900706715 -630883814 (null) (null)] (null) [-1410464938 -854246234 -818241763 893338527] (null) (null) (null)]
  col[1] "struct_nullable": {[(null) (null) (null) -724187464 (null) (null) -1169064100] ["85r6rN5" "jrDJR09" "YfZLF07" (null) (null) "8jDK2tj" "146gCTu"]}
record 2...
  col[0] "list_nullable": [(null) [(null) (null) 2968015 (null)] [(null) 1556114914 (null) 139341080] (null) [-1938030331] [(null) (null)] [] (null) (null) []]
  col[1] "struct_nullable": {[1689993413 -379972520 441054176 -906001202 -968548682 -450386762 916507802 (null) (null) 2072407432] [(null) (null) (null) "DhKdnsR" "20Zs1ez" "ky5eL4K" "UkTkWDX" (null) (null) "OR0xSBn"]}
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

func TestCatFile(t *testing.T) {
	for _, tc := range []struct {
		fname string
		want  string
	}{
		{
			fname: "../../testdata/primitives.stream.data",
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
			fname: "../../testdata/primitives.file.data",
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
			fname: "../../testdata/structs.stream.data",
			want: `record 1...
  col[0] "struct_nullable": {[1402032511 (null) 137773603 410361374 1959836418 (null) (null)] [(null) "MhRNxD4" "3F9HBxK" "aVd88fp" (null) "3loZrRf" (null)]}
record 2...
  col[0] "struct_nullable": {[(null) (null) (null) (null) (null) (null) 413888857 (null) (null) (null)] ["AS5oARE" (null) (null) "JGdagcX" "78SLiRw" "vbGf7OY" "5uh5fTs" "0ilsf82" "LjS9MbU" (null)]}
`,
		},
		{
			fname: "../../testdata/structs.file.data",
			want: `version: V4
record 1/2...
  col[0] "struct_nullable": {[1402032511 (null) 137773603 410361374 1959836418 (null) (null)] [(null) "MhRNxD4" "3F9HBxK" "aVd88fp" (null) "3loZrRf" (null)]}
record 2/2...
  col[0] "struct_nullable": {[(null) (null) (null) (null) (null) (null) 413888857 (null) (null) (null)] ["AS5oARE" (null) (null) "JGdagcX" "78SLiRw" "vbGf7OY" "5uh5fTs" "0ilsf82" "LjS9MbU" (null)]}
`,
		},
		{
			fname: "../../testdata/lists.stream.data",
			want: `record 1...
  col[0] "list_nullable": [[(null) -340485928 -259730121 -1430069283] [900706715 -630883814 (null) (null)] (null) [-1410464938 -854246234 -818241763 893338527] (null) (null) (null)]
  col[1] "struct_nullable": {[(null) (null) (null) -724187464 (null) (null) -1169064100] ["85r6rN5" "jrDJR09" "YfZLF07" (null) (null) "8jDK2tj" "146gCTu"]}
record 2...
  col[0] "list_nullable": [(null) [(null) (null) 2968015 (null)] [(null) 1556114914 (null) 139341080] (null) [-1938030331] [(null) (null)] [] (null) (null) []]
  col[1] "struct_nullable": {[1689993413 -379972520 441054176 -906001202 -968548682 -450386762 916507802 (null) (null) 2072407432] [(null) (null) (null) "DhKdnsR" "20Zs1ez" "ky5eL4K" "UkTkWDX" (null) (null) "OR0xSBn"]}
`,
		},
		{
			fname: "../../testdata/lists.file.data",
			want: `version: V4
record 1/2...
  col[0] "list_nullable": [[(null) -340485928 -259730121 -1430069283] [900706715 -630883814 (null) (null)] (null) [-1410464938 -854246234 -818241763 893338527] (null) (null) (null)]
  col[1] "struct_nullable": {[(null) (null) (null) -724187464 (null) (null) -1169064100] ["85r6rN5" "jrDJR09" "YfZLF07" (null) (null) "8jDK2tj" "146gCTu"]}
record 2/2...
  col[0] "list_nullable": [(null) [(null) (null) 2968015 (null)] [(null) 1556114914 (null) 139341080] (null) [-1938030331] [(null) (null)] [] (null) (null) []]
  col[1] "struct_nullable": {[1689993413 -379972520 441054176 -906001202 -968548682 -450386762 916507802 (null) (null) 2072407432] [(null) (null) (null) "DhKdnsR" "20Zs1ez" "ky5eL4K" "UkTkWDX" (null) (null) "OR0xSBn"]}
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
