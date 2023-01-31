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
	log.SetPrefix("arrow-ls: ")
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
				return nil
			}
			return err
		}

		fmt.Fprintf(w, "%v\n", r.Schema())

		nrecs := 0
		for r.Next() {
			nrecs++
		}
		fmt.Fprintf(w, "records: %d\n", nrecs)
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
	fmt.Fprintf(w, "%v\n", r.Schema())
	fmt.Fprintf(w, "records: %d\n", r.NumRecords())

	return nil
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
