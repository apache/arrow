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
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

func main() {
	log.SetPrefix("arrow-cat: ")
	log.SetFlags(0)

	flag.Parse()

	var err error
	switch flag.NArg() {
	case 0:
		err = processStream(os.Stdout, os.Stdin)
	default:
		err = processFiles(os.Stdout, flag.Args())
	}
	if err != nil {
		log.Fatal(err)
	}
}

func processStream(w io.Writer, rin io.Reader) error {
	mem := memory.NewGoAllocator()
	for {
		r, err := ipc.NewReader(rin, ipc.WithAllocator(mem))
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		n := 0
		for r.Next() {
			n++
			fmt.Fprintf(w, "record %d...\n", n)
			rec := r.Record()
			for i, col := range rec.Columns() {
				fmt.Fprintf(w, "  col[%d] %q: %v\n", i, rec.ColumnName(i), col)
			}
		}
		r.Release()
	}
	return nil
}

func processFiles(w io.Writer, names []string) error {
	for _, name := range names {
		err := processFile(w, name)
		if err != nil {
			return err
		}
	}
	return nil
}

func processFile(w io.Writer, fname string) error {

	f, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer f.Close()

	hdr := make([]byte, len(ipc.Magic))
	_, err = io.ReadFull(f, hdr)
	if err != nil {
		return fmt.Errorf("could not read file header: %w", err)
	}
	f.Seek(0, io.SeekStart)

	if !bytes.Equal(hdr, ipc.Magic) {
		// try as a stream.
		return processStream(w, f)
	}

	mem := memory.NewGoAllocator()

	r, err := ipc.NewFileReader(f, ipc.WithAllocator(mem))
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	defer r.Close()

	fmt.Fprintf(w, "version: %v\n", r.Version())
	for i := 0; i < r.NumRecords(); i++ {
		fmt.Fprintf(w, "record %d/%d...\n", i+1, r.NumRecords())
		rec, err := r.Record(i)
		if err != nil {
			return err
		}

		for i, col := range rec.Columns() {
			fmt.Fprintf(w, "  col[%d] %q: %v\n", i, rec.ColumnName(i), col)
		}
	}

	return nil
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
