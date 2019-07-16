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

package main // import "github.com/apache/arrow/go/arrow/ipc/cmd/arrow-file-to-stream"

import (
	"flag"
	"io"
	"log"
	"os"

	"github.com/apache/arrow/go/arrow/arrio"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/pkg/errors"
)

func main() {
	log.SetPrefix("arrow-file-to-stream: ")
	log.SetFlags(0)

	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		log.Fatalf("missing path to input ARROW file")
	}

	err := processFile(os.Stdout, flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
}

func processFile(w io.Writer, fname string) error {
	r, err := os.Open(fname)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	mem := memory.NewGoAllocator()

	rr, err := ipc.NewFileReader(r, ipc.WithAllocator(mem))
	if err != nil {
		if errors.Cause(err) == io.EOF {
			return nil
		}
		return err
	}
	defer rr.Close()

	ww := ipc.NewWriter(w, ipc.WithAllocator(mem), ipc.WithSchema(rr.Schema()))
	defer ww.Close()

	n, err := arrio.Copy(ww, rr)
	if err != nil {
		return errors.Wrap(err, "could not copy ARROW stream")
	}
	if got, want := n, int64(rr.NumRecords()); got != want {
		return errors.Errorf("invalid number of records written (got=%d, want=%d)", got, want)
	}

	err = ww.Close()
	if err != nil {
		return errors.Wrap(err, "could not close output ARROW stream")
	}

	return nil
}
