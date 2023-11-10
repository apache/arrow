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
	"os"
	"testing"

	"github.com/apache/arrow/go/v14/arrow/internal/arrdata"
	"github.com/apache/arrow/go/v14/arrow/internal/flatbuf"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

func TestFile(t *testing.T) {
	tempDir := t.TempDir()

	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			f, err := os.CreateTemp(tempDir, "go-arrow-file-")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			arrdata.WriteFile(t, f, mem, recs[0].Schema(), recs)
			arrdata.CheckArrowFile(t, f, mem, recs[0].Schema(), recs)
			arrdata.CheckArrowConcurrentFile(t, f, mem, recs[0].Schema(), recs)
		})
	}
}

func TestFileCompressed(t *testing.T) {
	tempDir := t.TempDir()

	compressTypes := []flatbuf.CompressionType{
		flatbuf.CompressionTypeLZ4_FRAME, flatbuf.CompressionTypeZSTD,
	}

	for _, codec := range compressTypes {
		for name, recs := range arrdata.Records {
			for _, n := range []int{0, 1, 2, 3} {
				t.Run(fmt.Sprintf("%s compress concurrency %d", name, n), func(t *testing.T) {
					mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
					defer mem.AssertSize(t, 0)

					f, err := os.CreateTemp(tempDir, "go-arrow-file-")
					if err != nil {
						t.Fatal(err)
					}
					defer f.Close()

					arrdata.WriteFileCompressed(t, f, mem, recs[0].Schema(), recs, codec, n)
					arrdata.CheckArrowFile(t, f, mem, recs[0].Schema(), recs)
					arrdata.CheckArrowConcurrentFile(t, f, mem, recs[0].Schema(), recs)
				})
			}
		}
	}
}
