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
	"io"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/internal/arrdata"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func TestFileToStream(t *testing.T) {
	tempDir := t.TempDir()

	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			f, err := os.CreateTemp(tempDir, "go-arrow-file-to-stream-")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			arrdata.WriteFile(t, f, mem, recs[0].Schema(), recs)

			o, err := os.CreateTemp(tempDir, "go-arrow-file-to-stream-")
			if err != nil {
				t.Fatal(err)
			}
			defer o.Close()

			err = processFile(o, f.Name())
			if err != nil {
				t.Fatal(err)
			}

			err = o.Sync()
			if err != nil {
				t.Fatal(err)
			}

			_, err = o.Seek(0, io.SeekStart)
			if err != nil {
				t.Fatal(err)
			}

			arrdata.CheckArrowStream(t, o, mem, recs[0].Schema(), recs)
		})
	}
}
