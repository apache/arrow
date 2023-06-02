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

//go:build cgo
// +build cgo

package cdata

import (
	"unsafe"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/arrio"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"golang.org/x/xerrors"
)

// SchemaFromPtr is a simple helper function to cast a uintptr to a *CArrowSchema
func SchemaFromPtr(ptr uintptr) *CArrowSchema { return (*CArrowSchema)(unsafe.Pointer(ptr)) }

// ArrayFromPtr is a simple helper function to cast a uintptr to a *CArrowArray
func ArrayFromPtr(ptr uintptr) *CArrowArray { return (*CArrowArray)(unsafe.Pointer(ptr)) }

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
// transferred to the resulting arrow.Array via ArrowArrayMove. The underlying array.Data
// object that is owned by the Array will now be the owner of the memory pointer and
// will call ArrowArrayRelease when it is released and garbage collected via runtime.SetFinalizer.
//
// NOTE: The array takes ownership of the underlying memory buffers via ArrowArrayMove,
// it does not take ownership of the actual arr object itself.
func ImportCArrayWithType(arr *CArrowArray, dt arrow.DataType) (arrow.Array, error) {
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
// object and take ownership of it as per ImportCArrayWithType. The returned arrow.Array will
// own the C memory and call ArrowArrayRelease when the array.Data object is cleaned up.
//
// NOTE: The array takes ownership of the underlying memory buffers via ArrowArrayMove,
// it does not take ownership of the actual arr object itself.
func ImportCArray(arr *CArrowArray, schema *CArrowSchema) (arrow.Field, arrow.Array, error) {
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
func ImportCRecordBatchWithSchema(arr *CArrowArray, sc *arrow.Schema) (arrow.Record, error) {
	imp, err := importCArrayAsType(arr, arrow.StructOf(sc.Fields()...))
	if err != nil {
		return nil, err
	}

	st := array.NewStructData(imp.data)
	defer st.Release()

	// now that we have our fields, we can split them out into the slice of arrays
	// and construct a record batch from them to return.
	cols := make([]arrow.Array, st.NumField())
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
func ImportCRecordBatch(arr *CArrowArray, sc *CArrowSchema) (arrow.Record, error) {
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

// ExportArrowSchema populates the passed in CArrowSchema with the schema passed in so
// that it can be passed to some consumer of the C Data Interface. The `release` function
// is tied to a callback in order to properly release any memory that was allocated during
// the populating of the struct. Any memory allocated will be allocated using malloc
// which means that it is invisible to the Go Garbage Collector and must be freed manually
// using the callback on the CArrowSchema object.
func ExportArrowSchema(schema *arrow.Schema, out *CArrowSchema) {
	dummy := arrow.Field{Type: arrow.StructOf(schema.Fields()...), Metadata: schema.Metadata()}
	exportField(dummy, out)
}

// ExportArrowRecordBatch populates the passed in CArrowArray (and optionally the schema too)
// by sharing the memory used for the buffers of each column's arrays. It does not
// copy the data, and will internally increment the reference counters so that releasing
// the record will not free the memory prematurely.
//
// When using CGO, memory passed to C is pinned so that the Go garbage collector won't
// move where it is allocated out from under the C pointer locations, ensuring the C pointers
// stay valid. This is only true until the CGO call returns, at which point the garbage collector
// is free to move things around again. As a result, if the function you're calling is going to
// hold onto the pointers or otherwise continue to reference the memory *after* the call returns,
// you should use the CgoArrowAllocator rather than the GoAllocator (or DefaultAllocator) so that
// the memory which is allocated for the record batch in the first place is allocated in C,
// not by the Go runtime and is therefore not subject to the Garbage collection.
//
// The release function on the populated CArrowArray will properly decrease the reference counts,
// and release the memory if the record has already been released. But since this must be explicitly
// done, make sure it is released so that you do not create a memory leak.
func ExportArrowRecordBatch(rb arrow.Record, out *CArrowArray, outSchema *CArrowSchema) {
	children := make([]arrow.ArrayData, rb.NumCols())
	for i := range rb.Columns() {
		children[i] = rb.Column(i).Data()
	}

	data := array.NewData(arrow.StructOf(rb.Schema().Fields()...), int(rb.NumRows()), []*memory.Buffer{nil},
		children, 0, 0)
	defer data.Release()
	arr := array.NewStructData(data)
	defer arr.Release()

	if outSchema != nil {
		ExportArrowSchema(rb.Schema(), outSchema)
	}

	exportArray(arr, out, nil)
}

// ExportArrowArray populates the CArrowArray that is passed in with the pointers to the memory
// being used by the arrow.Array passed in, in order to share with zero-copy across the C
// Data Interface. See the documentation for ExportArrowRecordBatch for details on how to ensure
// you do not leak memory and prevent unwanted, undefined or strange behaviors.
func ExportArrowArray(arr arrow.Array, out *CArrowArray, outSchema *CArrowSchema) {
	exportArray(arr, out, outSchema)
}

// ExportRecordReader populates the CArrowArrayStream that is passed in with the appropriate
// callbacks to be a working ArrowArrayStream utilizing the passed in RecordReader. The
// CArrowArrayStream takes ownership of the RecordReader until the consumer calls the release
// callback, as such it is unnecesary to call Release on the passed in reader unless it has
// previously been retained.
func ExportRecordReader(reader array.RecordReader, out *CArrowArrayStream) {
	exportStream(reader, out)
}

// ReleaseCArrowArray calls ArrowArrayRelease on the passed in cdata array
func ReleaseCArrowArray(arr *CArrowArray) { releaseArr(arr) }

// ReleaseCArrowSchema calls ArrowSchemaRelease on the passed in cdata schema
func ReleaseCArrowSchema(schema *CArrowSchema) { releaseSchema(schema) }
