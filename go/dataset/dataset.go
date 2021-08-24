// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package dataset

// #include <stdlib.h>
// #include "arrow/dataset/c/api.h"
// #include "arrow/dataset/c/helpers.h"
// #include "arrow/c/helpers.h"
// typedef struct Dataset Dataset;
// typedef struct ArrowSchema ArrowSchema;
// typedef struct Scanner Scanner;
// typedef struct ArrowArrayStream ArrowArrayStream;
//
// const char* ds_type_name(struct Dataset* ds) { return ds->get_dataset_type_name(ds); }
// int ds_get_schema(struct Dataset* ds, struct ArrowSchema* out) { return ds->get_schema(ds, out); }
// const char* ds_last_error(struct Dataset* ds) { return ds->last_error(ds); }
// int ds_new_scan(struct Dataset* ds, const char** columns, const int n_cols, uint64_t batch_size, struct Scanner* out) {
//	return ds->new_scan(ds, columns, n_cols, batch_size, out);
// }
//
// int scanner_to_stream(struct Scanner* scanner, struct ArrowArrayStream* out) { return scanner->to_stream(scanner, out); }
// const char* scanner_last_error(struct Scanner* scanner) { return scanner->last_error(scanner); }
//
import "C"
import (
	"runtime"
	"syscall"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/arrio"
	"golang.org/x/xerrors"
)

type Dataset struct {
	ds     C.Dataset
	schema *arrow.Schema

	dstype string
}

func (d *Dataset) Close() {
	C.ArrowDatasetRelease(&d.ds)
}

func (d *Dataset) Schema() (*arrow.Schema, error) {
	if d.schema != nil {
		return d.schema, nil
	}

	var sc C.ArrowSchema
	errno := C.ds_get_schema(&d.ds, &sc)
	if errno != 0 {
		return nil, xerrors.Errorf("%w: %s", syscall.Errno(errno), C.GoString(C.ds_last_error(&d.ds)))
	}
	defer C.ArrowSchemaRelease(&sc)

	s, err := arrowSchemaToSchema(&sc)
	if err != nil {
		return nil, err
	}

	d.schema = s
	return d.schema, nil
}

func (d *Dataset) Type() string {
	if d.dstype == "" {
		d.dstype = C.GoString(C.ds_type_name(&d.ds))
	}
	return d.dstype
}

func (d *Dataset) NewScan(columns []string, batchSize uint64) (*Scanner, error) {
	var (
		ccols   = make([]*C.char, len(columns))
		scanner C.Scanner
	)

	for i, c := range columns {
		ccols[i] = C.CString(c)
		defer C.free(unsafe.Pointer(ccols[i]))
	}

	errno := C.ds_new_scan(&d.ds, (**C.char)(unsafe.Pointer(&ccols[0])), C.int(len(columns)), C.uint64_t(batchSize), &scanner)
	if errno != 0 {
		return nil, xerrors.Errorf("%w: %s", syscall.Errno(errno), C.GoString(C.ds_last_error(&d.ds)))
	}

	ret := &Scanner{scanner: scanner}
	runtime.SetFinalizer(ret, (*Scanner).Close)
	return ret, nil
}

type Scanner struct {
	scanner C.Scanner
}

func (s *Scanner) Close() {
	C.ArrowScannerRelease(&s.scanner)
}

func (s *Scanner) GetReader() (arrio.Reader, error) {
	var stream C.ArrowArrayStream
	errno := C.scanner_to_stream(&s.scanner, &stream)
	if errno != 0 {
		return nil, xerrors.Errorf("%w: %s", syscall.Errno(errno), C.GoString(C.scanner_last_error(&s.scanner)))
	}

	ret := &nativeCRecordBatchReader{stream: &stream}
	runtime.SetFinalizer(ret, func(n *nativeCRecordBatchReader) { C.ArrowArrayStreamRelease(n.stream) })
	return ret, nil
}
