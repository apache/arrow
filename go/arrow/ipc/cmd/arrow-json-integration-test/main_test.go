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
	"io/ioutil"
	"os"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/internal/arrdata"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

func TestIntegration(t *testing.T) {
	tempDir := t.TempDir()

	const verbose = true
	for name, recs := range arrdata.Records {
		t.Run(name, func(t *testing.T) {
			if name == "decimal128" {
				t.Skip() // FIXME(sbinet): implement full decimal128 support
			}
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			af1, err := ioutil.TempFile(tempDir, "go-arrow-integration-")
			if err != nil {
				t.Fatal(err)
			}
			defer af1.Close()

			arrdata.WriteFile(t, af1, mem, recs[0].Schema(), recs)
			arrdata.CheckArrowFile(t, af1, mem, recs[0].Schema(), recs)

			aj, err := ioutil.TempFile(tempDir, "arrow-json-integration-")
			if err != nil {
				t.Fatal(err)
			}
			defer aj.Close()

			err = cnvToJSON(af1.Name(), aj.Name(), verbose)
			if err != nil {
				t.Fatal(err)
			}

			err = validate(af1.Name(), aj.Name(), verbose)
			if err != nil {
				t.Fatal(err)
			}

			af2, err := ioutil.TempFile(tempDir, "go-arrow-integration-")
			if err != nil {
				t.Fatal(err)
			}
			defer af2.Close()

			err = cnvToARROW(af2.Name(), aj.Name(), verbose)
			if err != nil {
				t.Fatal(err)
			}

			err = validate(af2.Name(), aj.Name(), verbose)
			if err != nil {
				t.Fatal(err)
			}

			af2, err = os.Open(af2.Name())
			if err != nil {
				t.Fatal(err)
			}
			defer af2.Close()

			arrdata.CheckArrowFile(t, af2, mem, recs[0].Schema(), recs)
		})
	}
}
