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

// Command arrow-ls displays the listing of an Arrow file.
//
// Examples:
//
//  $> arrow-ls ./testdata/primitives.data
//  version: V4
//  schema:
//    fields: 11
//      - bools: type=bool, nullable
//      - int8s: type=int8, nullable
//      - int16s: type=int16, nullable
//      - int32s: type=int32, nullable
//      - int64s: type=int64, nullable
//      - uint8s: type=uint8, nullable
//      - uint16s: type=uint16, nullable
//      - uint32s: type=uint32, nullable
//      - uint64s: type=uint64, nullable
//      - float32s: type=float32, nullable
//      - float64s: type=float64, nullable
//  records: 3
//
//  $> gen-arrow-stream | arrow-ls
//  schema:
//    fields: 11
//      - bools: type=bool, nullable
//      - int8s: type=int8, nullable
//      - int16s: type=int16, nullable
//      - int32s: type=int32, nullable
//      - int64s: type=int64, nullable
//      - uint8s: type=uint8, nullable
//      - uint16s: type=uint16, nullable
//      - uint32s: type=uint32, nullable
//      - uint64s: type=uint64, nullable
//      - float32s: type=float32, nullable
//      - float64s: type=float64, nullable
//  records: 3
package main // import "github.com/apache/arrow/go/arrow/ipc/cmd/arrow-ls"

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

func main() {
	log.SetPrefix("arrow-ls: ")
	log.SetFlags(0)

	flag.Parse()

	switch flag.NArg() {
	case 0:
		processStream(os.Stdin)
	default:
		processFiles(flag.Args())
	}
}

func processStream(rin io.Reader) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(nil, 0)

	r, err := ipc.NewReader(rin, ipc.WithAllocator(mem))
	if err != nil {
		log.Fatal(err)
	}
	defer r.Release()

	fmt.Printf("schema:\n%v", displaySchema(r.Schema()))

	nrecs := 0
	for r.Next() {
		nrecs++
	}
	fmt.Printf("records: %d\n", nrecs)
}

func processFiles(names []string) {
	for _, name := range names {
		processFile(name)
	}
}

func processFile(fname string) {

	f, err := os.Open(fname)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(nil, 0)

	r, err := ipc.NewFileReader(f, ipc.WithAllocator(mem))
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	fmt.Printf("version: %v\n", r.Version())
	fmt.Printf("schema:\n%v", displaySchema(r.Schema()))
	fmt.Printf("records: %d\n", r.NumRecords())
}

func displaySchema(s *arrow.Schema) string {
	o := new(strings.Builder)
	fmt.Fprintf(o, "%*.sfields: %d\n", 2, "", len(s.Fields()))
	for _, f := range s.Fields() {
		displayField(o, f, 4)
	}
	if meta := s.Metadata(); meta.Len() > 0 {
		fmt.Fprintf(o, "metadata: %v\n", meta)
	}
	return o.String()
}

func displayField(o io.Writer, field arrow.Field, inc int) {
	nullable := ""
	if field.Nullable {
		nullable = ", nullable"
	}
	fmt.Fprintf(o, "%*.s- %s: type=%v%v\n", inc, "", field.Name, field.Type.Name(), nullable)
	if field.HasMetadata() {
		fmt.Fprintf(o, "%*.smetadata: %v\n", inc, "", field.Metadata)
	}
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Command arrow-ls displays the listing of an Arrow file.

Usage: arrow-ls [OPTIONS] [FILE1 [FILE2 [...]]]

Examples:

 $> arrow-ls ./testdata/primitives.data
 version: V4
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

 $> gen-arrow-stream | arrow-ls
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
`)
		os.Exit(0)
	}
}
