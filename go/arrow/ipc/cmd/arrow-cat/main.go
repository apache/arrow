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

// Command arrow-cat displays the content of an Arrow stream or file.
//
// Examples:
//
//  $> arrow-cat ./testdata/primitives.data
//  version: V4
//  record 1/3...
//    col[0] "bools": [true (null) (null) false true]
//    col[1] "int8s": [-1 (null) (null) -4 -5]
//    col[2] "int16s": [-1 (null) (null) -4 -5]
//    col[3] "int32s": [-1 (null) (null) -4 -5]
//    col[4] "int64s": [-1 (null) (null) -4 -5]
//    col[5] "uint8s": [1 (null) (null) 4 5]
//    col[6] "uint16s": [1 (null) (null) 4 5]
//    col[7] "uint32s": [1 (null) (null) 4 5]
//    col[8] "uint64s": [1 (null) (null) 4 5]
//    col[9] "float32s": [1 (null) (null) 4 5]
//    col[10] "float64s": [1 (null) (null) 4 5]
//  record 2/3...
//    col[0] "bools": [true (null) (null) false true]
//  [...]
//
//  $> gen-arrow-stream | arrow-cat
//  record 1...
//    col[0] "bools": [true (null) (null) false true]
//    col[1] "int8s": [-1 (null) (null) -4 -5]
//    col[2] "int16s": [-1 (null) (null) -4 -5]
//    col[3] "int32s": [-1 (null) (null) -4 -5]
//    col[4] "int64s": [-1 (null) (null) -4 -5]
//    col[5] "uint8s": [1 (null) (null) 4 5]
//    col[6] "uint16s": [1 (null) (null) 4 5]
//    col[7] "uint32s": [1 (null) (null) 4 5]
//    col[8] "uint64s": [1 (null) (null) 4 5]
//    col[9] "float32s": [1 (null) (null) 4 5]
//    col[10] "float64s": [1 (null) (null) 4 5]
//  record 2...
//    col[0] "bools": [true (null) (null) false true]
//  [...]
package main // import "github.com/apache/arrow/go/arrow/ipc/cmd/arrow-cat"

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

func main() {
	log.SetPrefix("arrow-cat: ")
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

	n := 0
	for r.Next() {
		n++
		fmt.Printf("record %d...\n", n)
		rec := r.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("  col[%d] %q: %v\n", i, rec.ColumnName(i), col)
		}
	}
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

	hdr := make([]byte, len(ipc.Magic))
	_, err = io.ReadFull(f, hdr)
	if err != nil {
		log.Fatalf("could not read file header: %v", err)
	}
	f.Seek(0, io.SeekStart)

	if !bytes.Equal(hdr, ipc.Magic) {
		// try as a stream.
		processStream(f)
		return
	}

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(nil, 0)

	r, err := ipc.NewFileReader(f, ipc.WithAllocator(mem))
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	fmt.Printf("version: %v\n", r.Version())
	for i := 0; i < r.NumRecords(); i++ {
		fmt.Printf("record %d/%d...\n", i+1, r.NumRecords())
		rec, err := r.Record(i)
		if err != nil {
			log.Fatal(err)
		}
		defer rec.Release()

		for i, col := range rec.Columns() {
			fmt.Printf("  col[%d] %q: %v\n", i, rec.ColumnName(i), col)
		}
	}
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Command arrow-cat displays the content of an Arrow stream or file.

Usage: arrow-cat [OPTIONS] [FILE1 [FILE2 [...]]]

Examples:

 $> arrow-cat ./testdata/primitives.data
 version: V4
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
 [...]

 $> gen-arrow-stream | arrow-cat
 record 1...
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
 [...]
`)
		os.Exit(0)
	}
}
