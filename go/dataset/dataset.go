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

// #include "arrow/dataset/c/api.h"
// #include "arrow/dataset/c/helpers.h"
// #include "arrow/c/helpers.h"
// typedef struct Dataset Dataset;
// typedef struct ArrowSchema ArrowSchema;
//
// const char* ds_type_name(struct Dataset* ds) { return ds->get_dataset_type_name(ds); }
// int ds_get_schema(struct Dataset* ds, struct ArrowSchema* out) { return ds->get_schema(ds, out); }
// const char* ds_last_error(struct Dataset* ds) { return ds->last_error(ds); }
//
import "C"
import (
	"syscall"

	"github.com/apache/arrow/go/arrow"
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
