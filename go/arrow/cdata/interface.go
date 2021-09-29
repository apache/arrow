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

// +build cgo

package cdata

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/arrio"
	"golang.org/x/xerrors"
)

// ImportCArrowField takes in an ArrowSchema from the C Data interface, it
// will copy the metadata and type definitions rather than keep direct references
// to them. It is safe to call C.ArrowSchemaRelease after receiving the field
// from this function.
func ImportCArrowField(out *CArrowSchema) (arrow.Field, error) {
	return importSchema(out)
}

// ImportCArrowSchema takes in the ArrowSchema from the C Data Interface, it
// will copy the metadata and schema definitions over from the C object rather
// than keep direct references to them. This function will call ArrowSchemaRelease
// on the passed in schema regardless of whether or not there is an error returned.
//
// This version is intended to take in a schema for a record batch, which means
// that the top level of the schema should be a struct of the schema fields. If
// importing a single array's schema, then use ImportCArrowField instead.
func ImportCArrowSchema(out *CArrowSchema) (*arrow.Schema, error) {
	ret, err := importSchema(out)
	if err != nil {
		return nil, err
	}

	return arrow.NewSchema(ret.Type.(*arrow.StructType).Fields(), &ret.Metadata), nil
}

// ImportCArrayWithType takes a pointer to a C Data ArrowArray and interprets the values
// as an array with the given datatype. If err is not nil, then ArrowArrayRelease must still
// be called on arr to release the memory.
//
// The underlying buffers will not be copied, but will instead be referenced directly
// by the resulting array interface object. The passed in ArrowArray will have it's ownership
// transferred to the resulting array.Interface via ArrowArrayMove. The underlying array.Data
// object that is owned by the Array will now be the owner of the memory pointer and
// will call ArrowArrayRelease when it is released and garbage collected via runtime.SetFinalizer.
//
// NOTE: The array takes ownership of the underlying memory buffers via ArrowArrayMove,
// it does not take ownership of the actual arr object itself.
func ImportCArrayWithType(arr *CArrowArray, dt arrow.DataType) (array.Interface, error) {
	imp, err := importCArrayAsType(arr, dt)
	if err != nil {
		return nil, err
	}
	defer imp.data.Release()
	return array.MakeFromData(imp.data), nil
}

// ImportCArray takes a pointer to both a C Data ArrowArray and C Data ArrowSchema in order
// to import them into usable Go Objects. If err is not nil, then ArrowArrayRelease must still
// be called on arr to release the memory. The ArrowSchemaRelease will be called on the passed in
// schema regardless of whether there is an error or not.
//
// The Schema will be copied with the information used to populate the returned Field, complete
// with metadata. The array will reference the same memory that is referred to by the ArrowArray
// object and take ownership of it as per ImportCArrayWithType. The returned array.Interface will
// own the C memory and call ArrowArrayRelease when the array.Data object is cleaned up.
//
// NOTE: The array takes ownership of the underlying memory buffers via ArrowArrayMove,
// it does not take ownership of the actual arr object itself.
func ImportCArray(arr *CArrowArray, schema *CArrowSchema) (arrow.Field, array.Interface, error) {
	field, err := importSchema(schema)
	if err != nil {
		return field, nil, err
	}

	ret, err := ImportCArrayWithType(arr, field.Type)
	return field, ret, err
}

// ImportCRecordBatchWithSchema is used for importing a Record Batch array when the schema
// is already known such as when receiving record batches through a stream.
//
// All of the semantics regarding memory ownership are the same as when calling
// ImportCRecordBatch directly with a schema.
//
// NOTE: The array takes ownership of the underlying memory buffers via ArrowArrayMove,
// it does not take ownership of the actual arr object itself.
func ImportCRecordBatchWithSchema(arr *CArrowArray, sc *arrow.Schema) (array.Record, error) {
	imp, err := importCArrayAsType(arr, arrow.StructOf(sc.Fields()...))
	if err != nil {
		return nil, err
	}

	st := array.NewStructData(imp.data)
	defer st.Release()

	// now that we have our fields, we can split them out into the slice of arrays
	// and construct a record batch from them to return.
	cols := make([]array.Interface, st.NumField())
	for i := 0; i < st.NumField(); i++ {
		cols[i] = st.Field(i)
	}

	return array.NewRecord(sc, cols, int64(st.Len())), nil
}

// ImportCRecordBatch imports an ArrowArray from C as a record batch. If err is not nil,
// then ArrowArrayRelease must still be called to release the memory.
//
// A record batch is represented in the C Data Interface as a Struct Array whose fields
// are the columns of the record batch. Thus after importing the schema passed in here,
// if it is not a Struct type, this will return an error. As with ImportCArray, the
// columns in the record batch will take ownership of the CArrowArray memory if successful.
// Since ArrowArrayMove is used, it's still safe to call ArrowArrayRelease on the source
// regardless. But if there is an error, it *MUST* be called to ensure there is no memory leak.
//
// NOTE: The array takes ownership of the underlying memory buffers via ArrowArrayMove,
// it does not take ownership of the actual arr object itself.
func ImportCRecordBatch(arr *CArrowArray, sc *CArrowSchema) (array.Record, error) {
	field, err := importSchema(sc)
	if err != nil {
		return nil, err
	}

	if field.Type.ID() != arrow.STRUCT {
		return nil, xerrors.New("recordbatch array import must be of struct type")
	}

	return ImportCRecordBatchWithSchema(arr, arrow.NewSchema(field.Type.(*arrow.StructType).Fields(), &field.Metadata))
}

// ImportCArrayStream creates an arrio.Reader from an ArrowArrayStream taking ownership
// of the underlying stream object via ArrowArrayStreamMove.
//
// The records returned by this reader must be released manually after they are returned.
// The reader itself will release the stream via SetFinalizer when it is garbage collected.
// It will return (nil, io.EOF) from the Read function when there are no more records to return.
//
// NOTE: The reader takes ownership of the underlying memory buffers via ArrowArrayStreamMove,
// it does not take ownership of the actual stream object itself.
func ImportCArrayStream(stream *CArrowArrayStream, schema *arrow.Schema) arrio.Reader {
	out := &nativeCRecordBatchReader{schema: schema}
	initReader(out, stream)
	return out
}
