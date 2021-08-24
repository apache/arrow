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

// include the necessary C headers and provide wrappers for the callback functions
// as CGO currently is not able to directly call C function pointers from Go yet.

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
// int ds_new_scan(struct Dataset* ds, const char** columns, const int n_cols, int64_t batch_size, struct Scanner* out) {
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

// Dataset is an active Dataset which has a given physical schema.
type Dataset struct {
	// Corresponding C object that holds pointers to the memory.
	ds C.Dataset
	// cache the schema after the first time retrieving it.
	schema *arrow.Schema
	// cache the dataset type after the first time retrieving it
	dstype string
}

// Close releases the corresponding allocated C memory for this dataset.
func (d *Dataset) Close() {
	C.ArrowDatasetRelease(&d.ds)
}

// Schema returns the schema for this dataset, or an error if there is an
// issue in determining it.
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

// Type returns a string name of the Dataset Type, currently only "filesystem"
// is implemented.
func (d *Dataset) Type() string {
	if d.dstype == "" {
		d.dstype = C.GoString(C.ds_type_name(&d.ds))
	}
	return d.dstype
}

// NewScan creates a new scanner for this dataset projecting the columns named by
// the columns slice, reading batches of the requested size from the dataset fragments.
//
// If batchSize <= 0, then a default batch size will be used.
//
// runtime.SetFinalizer is used on the returned Scanner so that when it gets
// garbage collected, it will clean up the C memory automatically. If you wish
// to have more control over it, you can manually call Close on the returned Scanner
// when done with it.
func (d *Dataset) NewScan(columns []string, batchSize int64) (*Scanner, error) {
	var (
		ccols   = make([]*C.char, len(columns))
		scanner C.Scanner
	)

	for i, c := range columns {
		ccols[i] = C.CString(c)
		defer C.free(unsafe.Pointer(ccols[i]))
	}

	errno := C.ds_new_scan(&d.ds, (**C.char)(unsafe.Pointer(&ccols[0])), C.int(len(columns)), C.int64_t(batchSize), &scanner)
	if errno != 0 {
		return nil, xerrors.Errorf("%w: %s", syscall.Errno(errno), C.GoString(C.ds_last_error(&d.ds)))
	}

	ret := &Scanner{scanner: scanner, ds: d}
	runtime.SetFinalizer(ret, (*Scanner).Close)
	return ret, nil
}

// Scanner represents a specific, configured scanner for a dataset.
type Scanner struct {
	scanner C.Scanner
	ds      *Dataset
}

// Close allows manual control of cleaning up the memory associated with this scanner,
// otherwise when creating a scanner via a dataset, it will have called SetFinalizer
// in order to prevent any leaks.
func (s *Scanner) Close() {
	C.ArrowScannerRelease(&s.scanner)
}

// GetReader uses the Arrow Array Stream interface to produce a stream of
// record batches that can then be read out using the arrio.Reader interface.
// The reader will return io.EOF when there are no more batches.
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
