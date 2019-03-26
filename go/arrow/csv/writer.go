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

package csv

import (
	"encoding/csv"
	"io"
	"strconv"
	"sync"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

// Writer wraps encoding/csv.Writer and writes array.Record based on a schema.
type Writer struct {
	w      *csv.Writer
	schema *arrow.Schema
	header bool
	once   sync.Once
}

// NewWriter returns a writer that writes array.Records to the CSV file
// with the given schema.
//
// NewWriter panics if the given schema contains fields that have types that are not
// primitive types.
func NewWriter(w io.Writer, schema *arrow.Schema, opts ...Option) *Writer {
	validate(schema)

	ww := &Writer{w: csv.NewWriter(w), schema: schema}
	for _, opt := range opts {
		opt(ww)
	}

	return ww
}

func (w *Writer) Schema() *arrow.Schema { return w.schema }

// Write writes a single Record as one row to the CSV file
func (w *Writer) Write(record array.Record) error {
	if !record.Schema().Equal(w.schema) {
		return ErrMismatchFields
	}

	var err error
	if w.header {
		w.once.Do(func() {
			err = w.writeHeader()
		})
		if err != nil {
			return err
		}
	}

	recs := make([][]string, record.NumRows())
	for i := range recs {
		recs[i] = make([]string, record.NumCols())
	}

	for j, col := range record.Columns() {
		switch w.schema.Field(j).Type.(type) {
		case *arrow.BooleanType:
			arr := col.(*array.Boolean)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = strconv.FormatBool(arr.Value(i))
			}
		case *arrow.Int8Type:
			arr := col.(*array.Int8)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = strconv.FormatInt(int64(arr.Value(i)), 10)
			}
		case *arrow.Int16Type:
			arr := col.(*array.Int16)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = strconv.FormatInt(int64(arr.Value(i)), 10)
			}
		case *arrow.Int32Type:
			arr := col.(*array.Int32)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = strconv.FormatInt(int64(arr.Value(i)), 10)
			}
		case *arrow.Int64Type:
			arr := col.(*array.Int64)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = strconv.FormatInt(int64(arr.Value(i)), 10)
			}
		case *arrow.Uint8Type:
			arr := col.(*array.Uint8)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = strconv.FormatUint(uint64(arr.Value(i)), 10)
			}
		case *arrow.Uint16Type:
			arr := col.(*array.Uint16)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = strconv.FormatUint(uint64(arr.Value(i)), 10)
			}
		case *arrow.Uint32Type:
			arr := col.(*array.Uint32)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = strconv.FormatUint(uint64(arr.Value(i)), 10)
			}
		case *arrow.Uint64Type:
			arr := col.(*array.Uint64)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = strconv.FormatUint(uint64(arr.Value(i)), 10)
			}
		case *arrow.Float32Type:
			arr := col.(*array.Float32)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = strconv.FormatFloat(float64(arr.Value(i)), 'g', -1, 32)
			}
		case *arrow.Float64Type:
			arr := col.(*array.Float64)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = strconv.FormatFloat(float64(arr.Value(i)), 'g', -1, 64)
			}
		case *arrow.StringType:
			arr := col.(*array.String)
			for i := 0; i < arr.Len(); i++ {
				recs[i][j] = arr.Value(i)
			}
		}
	}

	return w.w.WriteAll(recs)
}

// Flush writes any buffered data to the underlying csv Writer.
// If an error occurred during the Flush, return it
func (w *Writer) Flush() error {
	w.w.Flush()
	return w.w.Error()
}

// Error reports any error that has occurred during a previous Write or Flush.
func (w *Writer) Error() error {
	return w.w.Error()
}

func (w *Writer) writeHeader() error {
	headers := make([]string, len(w.schema.Fields()))
	for i := range headers {
		headers[i] = w.schema.Field(i).Name
	}
	if err := w.w.Write(headers); err != nil {
		return err
	}
	return nil
}
