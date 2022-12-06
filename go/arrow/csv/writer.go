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
	"math"
	"math/big"
	"strconv"
	"sync"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
)

// Writer wraps encoding/csv.Writer and writes arrow.Record based on a schema.
type Writer struct {
	boolFormatter func(bool) string
	header        bool
	nullValue     string
	once          sync.Once
	schema        *arrow.Schema
	w             *csv.Writer
}

// NewWriter returns a writer that writes arrow.Records to the CSV file
// with the given schema.
//
// NewWriter panics if the given schema contains fields that have types that are not
// primitive types.
func NewWriter(w io.Writer, schema *arrow.Schema, opts ...Option) *Writer {
	validate(schema)

	ww := &Writer{
		boolFormatter: strconv.FormatBool, // override by passing WithBoolWriter() as an option
		nullValue:     "NULL",             // override by passing WithNullWriter() as an option
		schema:        schema,
		w:             csv.NewWriter(w),
	}
	for _, opt := range opts {
		opt(ww)
	}

	return ww
}

func (w *Writer) Schema() *arrow.Schema { return w.schema }

// Write writes a single Record as one row to the CSV file
func (w *Writer) Write(record arrow.Record) error {
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
				if arr.IsValid(i) {
					recs[i][j] = w.boolFormatter(arr.Value(i))
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Int8Type:
			arr := col.(*array.Int8)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = strconv.FormatInt(int64(arr.Value(i)), 10)
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Int16Type:
			arr := col.(*array.Int16)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = strconv.FormatInt(int64(arr.Value(i)), 10)
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Int32Type:
			arr := col.(*array.Int32)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = strconv.FormatInt(int64(arr.Value(i)), 10)
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Int64Type:
			arr := col.(*array.Int64)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = strconv.FormatInt(int64(arr.Value(i)), 10)
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Uint8Type:
			arr := col.(*array.Uint8)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = strconv.FormatUint(uint64(arr.Value(i)), 10)
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Uint16Type:
			arr := col.(*array.Uint16)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = strconv.FormatUint(uint64(arr.Value(i)), 10)
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Uint32Type:
			arr := col.(*array.Uint32)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = strconv.FormatUint(uint64(arr.Value(i)), 10)
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Uint64Type:
			arr := col.(*array.Uint64)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = strconv.FormatUint(uint64(arr.Value(i)), 10)
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Float32Type:
			arr := col.(*array.Float32)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = strconv.FormatFloat(float64(arr.Value(i)), 'g', -1, 32)
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Float64Type:
			arr := col.(*array.Float64)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = strconv.FormatFloat(float64(arr.Value(i)), 'g', -1, 64)
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.StringType:
			arr := col.(*array.String)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = arr.Value(i)
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Date32Type:
			arr := col.(*array.Date32)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = arr.Value(i).FormattedString()
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Date64Type:
			arr := col.(*array.Date64)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = arr.Value(i).FormattedString()
				} else {
					recs[i][j] = w.nullValue
				}
			}

		case *arrow.TimestampType:
			arr := col.(*array.Timestamp)
			t := w.schema.Field(j).Type.(*arrow.TimestampType)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					recs[i][j] = arr.Value(i).ToTime(t.Unit).Format("2006-01-02 15:04:05.999999999")
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Decimal128Type:
			fieldType := w.schema.Field(j).Type.(*arrow.Decimal128Type)
			scale := fieldType.Scale
			precision := fieldType.Precision
			arr := col.(*array.Decimal128)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					f := (&big.Float{}).SetInt(arr.Value(i).BigInt())
					f.Quo(f, big.NewFloat(math.Pow10(int(scale))))
					recs[i][j] = f.Text('g', int(precision))
				} else {
					recs[i][j] = w.nullValue
				}
			}
		case *arrow.Decimal256Type:
			fieldType := w.schema.Field(j).Type.(*arrow.Decimal256Type)
			scale := fieldType.Scale
			precision := fieldType.Precision
			arr := col.(*array.Decimal256)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					f := (&big.Float{}).SetInt(arr.Value(i).BigInt())
					f.Quo(f, big.NewFloat(math.Pow10(int(scale))))
					recs[i][j] = f.Text('g', int(precision))
				} else {
					recs[i][j] = w.nullValue
				}
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
