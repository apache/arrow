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

// #cgo pkg-config: arrow-dataset
// #include <stdlib.h>
// #include "arrow/dataset/c/api.h"
// #include "arrow/dataset/c/helpers.h"
// #include "arrow/c/helpers.h"
// typedef struct ArrowSchema ArrowSchema;
// typedef struct DatasetFactory DatasetFactory;
// typedef struct Dataset Dataset;
//
// int df_inspect_schema(struct DatasetFactory* factory, const int num_fragments, struct ArrowSchema* out) {
//	 return factory->inspect_schema(factory, num_fragments, out);
// }
//
// const char* df_last_error(struct DatasetFactory* factory) { return factory->last_error(factory); }
//
// int df_create_dataset(struct DatasetFactory* factory, struct Dataset* out) { return factory->create_dataset(factory, out); }
//
import "C"

import (
	"runtime"
	"syscall"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	"golang.org/x/xerrors"
)

var (
	InspectAllFragments     = C.kInspectAllFragments
	DisableInspectFragments = C.DISABLE_INSPECT_FRAGMENTS
	DefaultInspectFragments = C.DEFAULT_NUM_FRAGMENTS
)

func arrowSchemaToSchema(out *C.ArrowSchema) (*arrow.Schema, error) {
	ret, err := importSchema(out)
	if err != nil {
		return nil, err
	}

	return arrow.NewSchema(ret.Type.(*arrow.StructType).Fields(), &ret.Metadata), nil
}

type FileFormat int

const (
	PARQUET FileFormat = C.DS_PARQUET_FORMAT
	CSV     FileFormat = C.DS_CSV_FORMAT
	IPC     FileFormat = C.DS_IPC_FORMAT
)

type DatasetFactory struct {
	ds C.DatasetFactory
}

// func (d DatasetFactory) Close() {
// 	C.release_dataset_factory(C.uintptr_t(d))
// }

func (d DatasetFactory) Inspect(numFragments int) (*arrow.Schema, error) {
	var sc C.ArrowSchema
	errno := C.df_inspect_schema(&d.ds, C.int(numFragments), &sc)
	if errno != 0 {
		return nil, xerrors.Errorf("%w: %s", syscall.Errno(errno), C.GoString(C.df_last_error(&d.ds)))
	}
	defer C.ArrowSchemaRelease(&sc)

	return arrowSchemaToSchema(&sc)
}

func (d DatasetFactory) CreateDataset() (*Dataset, error) {
	var ds C.Dataset
	errno := C.df_create_dataset(&d.ds, &ds)
	if errno != 0 {
		return nil, xerrors.Errorf("%w: %s", syscall.Errno(errno), C.GoString(C.df_last_error(&d.ds)))
	}

	ret := &Dataset{ds: ds}
	runtime.SetFinalizer(ret, (*Dataset).Close)
	return ret, nil
}

func CreateDatasetFactory(uri string) (*DatasetFactory, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))

	var ds C.DatasetFactory

	errno := C.dataset_factory_from_path(curi, C.DS_PARQUET_FORMAT, &ds)
	if errno != 0 {
		return nil, syscall.Errno(errno)
	}

	ret := &DatasetFactory{ds}
	runtime.SetFinalizer(ret, func(d *DatasetFactory) { C.ArrowDatasetFactoryRelease(&d.ds) })

	return ret, nil
}
