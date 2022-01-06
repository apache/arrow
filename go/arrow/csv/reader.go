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
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/internal/debug"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"golang.org/x/xerrors"
)

// Reader wraps encoding/csv.Reader and creates array.Records from a schema.
type Reader struct {
	r      *csv.Reader
	schema *arrow.Schema

	refs int64
	bld  *array.RecordBuilder
	cur  array.Record
	err  error

	chunk int
	done  bool
	next  func() bool

	mem memory.Allocator

	header bool
	once   sync.Once

	fieldConverter []func(field array.Builder, val string)

	stringsCanBeNull bool
	nulls            []string
}

// NewReader returns a reader that reads from the CSV file and creates
// array.Records from the given schema.
//
// NewReader panics if the given schema contains fields that have types that are not
// primitive types.
func NewReader(r io.Reader, schema *arrow.Schema, opts ...Option) *Reader {
	validate(schema)

	rr := &Reader{
		r:                csv.NewReader(r),
		schema:           schema,
		refs:             1,
		chunk:            1,
		stringsCanBeNull: false,
	}
	rr.r.ReuseRecord = true
	for _, opt := range opts {
		opt(rr)
	}

	if rr.mem == nil {
		rr.mem = memory.DefaultAllocator
	}

	rr.bld = array.NewRecordBuilder(rr.mem, rr.schema)

	switch {
	case rr.chunk < 0:
		rr.next = rr.nextall
	case rr.chunk > 1:
		rr.next = rr.nextn
	default:
		rr.next = rr.next1
	}

	// Create a table of functions that will parse columns. This optimization
	// allows us to specialize the implementation of each column's decoding
	// and hoist type-based branches outside the inner loop.
	rr.fieldConverter = make([]func(array.Builder, string), len(schema.Fields()))
	for idx, field := range schema.Fields() {
		rr.fieldConverter[idx] = rr.initFieldConverter(&field)
	}

	return rr
}

func (r *Reader) readHeader() error {
	records, err := r.r.Read()
	if err != nil {
		return xerrors.Errorf("arrow/csv: could not read header from file: %w", err)
	}

	if len(records) != len(r.schema.Fields()) {
		return ErrMismatchFields
	}

	fields := make([]arrow.Field, len(records))
	for idx, name := range records {
		fields[idx] = r.schema.Field(idx)
		fields[idx].Name = name
	}

	meta := r.schema.Metadata()
	r.schema = arrow.NewSchema(fields, &meta)
	r.bld = array.NewRecordBuilder(r.mem, r.schema)
	return nil
}

// Err returns the last error encountered during the iteration over the
// underlying CSV file.
func (r *Reader) Err() error { return r.err }

func (r *Reader) Schema() *arrow.Schema { return r.schema }

// Record returns the current record that has been extracted from the
// underlying CSV file.
// It is valid until the next call to Next.
func (r *Reader) Record() array.Record { return r.cur }

// Next returns whether a Record could be extracted from the underlying CSV file.
//
// Next panics if the number of records extracted from a CSV row does not match
// the number of fields of the associated schema.
func (r *Reader) Next() bool {
	if r.header {
		r.once.Do(func() {
			r.err = r.readHeader()
		})
	}

	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}

	if r.err != nil || r.done {
		return false
	}

	return r.next()
}

// next1 reads one row from the CSV file and creates a single Record
// from that row.
func (r *Reader) next1() bool {
	var recs []string
	recs, r.err = r.r.Read()
	if r.err != nil {
		r.done = true
		if r.err == io.EOF {
			r.err = nil
		}
		return false
	}

	r.validate(recs)
	r.read(recs)
	r.cur = r.bld.NewRecord()

	return true
}

// nextall reads the whole CSV file into memory and creates one single
// Record from all the CSV rows.
func (r *Reader) nextall() bool {
	defer func() {
		r.done = true
	}()

	var (
		recs [][]string
	)

	recs, r.err = r.r.ReadAll()
	if r.err != nil {
		return false
	}

	for _, rec := range recs {
		r.validate(rec)
		r.read(rec)
	}
	r.cur = r.bld.NewRecord()

	return true
}

// nextn reads n rows from the CSV file, where n is the chunk size, and creates
// a Record from these rows.
func (r *Reader) nextn() bool {
	var (
		recs []string
		n    = 0
	)

	for i := 0; i < r.chunk && !r.done; i++ {
		recs, r.err = r.r.Read()
		if r.err != nil {
			r.done = true
			break
		}

		r.validate(recs)
		r.read(recs)
		n++
	}

	if r.err != nil {
		r.done = true
		if r.err == io.EOF {
			r.err = nil
		}
	}

	r.cur = r.bld.NewRecord()
	return n > 0
}

func (r *Reader) validate(recs []string) {
	if r.err != nil {
		return
	}

	if len(recs) != len(r.schema.Fields()) {
		r.err = ErrMismatchFields
		return
	}
}

func (r *Reader) isNull(val string) bool {
	for _, v := range r.nulls {
		if v == val {
			return true
		}
	}
	return false
}

func (r *Reader) read(recs []string) {
	for i, str := range recs {
		r.fieldConverter[i](r.bld.Field(i), str)
	}
}

func (r *Reader) initFieldConverter(field *arrow.Field) func(array.Builder, string) {
	switch field.Type.(type) {
	case *arrow.BooleanType:
		return func(field array.Builder, str string) {
			r.parseBool(field, str)
		}
	case *arrow.Int8Type:
		return func(field array.Builder, str string) {
			r.parseInt8(field, str)
		}
	case *arrow.Int16Type:
		return func(field array.Builder, str string) {
			r.parseInt16(field, str)
		}
	case *arrow.Int32Type:
		return func(field array.Builder, str string) {
			r.parseInt32(field, str)
		}
	case *arrow.Int64Type:
		return func(field array.Builder, str string) {
			r.parseInt64(field, str)
		}
	case *arrow.Uint8Type:
		return func(field array.Builder, str string) {
			r.parseUint8(field, str)
		}
	case *arrow.Uint16Type:
		return func(field array.Builder, str string) {
			r.parseUint16(field, str)
		}
	case *arrow.Uint32Type:
		return func(field array.Builder, str string) {
			r.parseUint32(field, str)
		}
	case *arrow.Uint64Type:
		return func(field array.Builder, str string) {
			r.parseUint64(field, str)
		}
	case *arrow.Float32Type:
		return func(field array.Builder, str string) {
			r.parseFloat32(field, str)
		}
	case *arrow.Float64Type:
		return func(field array.Builder, str string) {
			r.parseFloat64(field, str)
		}
	case *arrow.StringType:
		// specialize the implementation when we know we cannot have nulls
		if r.stringsCanBeNull {
			return func(field array.Builder, str string) {
				if r.isNull(str) {
					field.AppendNull()
				} else {
					field.(*array.StringBuilder).Append(str)
				}
			}
		} else {
			return func(field array.Builder, str string) {
				field.(*array.StringBuilder).Append(str)
			}
		}

	default:
		panic(fmt.Errorf("arrow/csv: unhandled field type %T", field.Type))
	}
}

func (r *Reader) parseBool(field array.Builder, str string) {
	if r.isNull(str) {
		field.AppendNull()
		return
	}

	var v bool
	switch str {
	case "false", "False", "0":
		v = false
	case "true", "True", "1":
		v = true
	default:
		r.err = fmt.Errorf("unrecognized boolean: %s", str)
		field.AppendNull()
		return
	}

	field.(*array.BooleanBuilder).Append(v)
}

func (r *Reader) parseInt8(field array.Builder, str string) {
	if r.isNull(str) {
		field.AppendNull()
		return
	}

	v, err := strconv.ParseInt(str, 10, 8)
	if err != nil && r.err == nil {
		r.err = err
		field.AppendNull()
		return
	}

	field.(*array.Int8Builder).Append(int8(v))
}

func (r *Reader) parseInt16(field array.Builder, str string) {
	if r.isNull(str) {
		field.AppendNull()
		return
	}

	v, err := strconv.ParseInt(str, 10, 16)
	if err != nil && r.err == nil {
		r.err = err
		field.AppendNull()
		return
	}

	field.(*array.Int16Builder).Append(int16(v))
}

func (r *Reader) parseInt32(field array.Builder, str string) {
	if r.isNull(str) {
		field.AppendNull()
		return
	}

	v, err := strconv.ParseInt(str, 10, 32)
	if err != nil && r.err == nil {
		r.err = err
		field.AppendNull()
		return
	}

	field.(*array.Int32Builder).Append(int32(v))
}

func (r *Reader) parseInt64(field array.Builder, str string) {
	if r.isNull(str) {
		field.AppendNull()
		return
	}

	v, err := strconv.ParseInt(str, 10, 64)
	if err != nil && r.err == nil {
		r.err = err
		field.AppendNull()
		return
	}

	field.(*array.Int64Builder).Append(v)
}

func (r *Reader) parseUint8(field array.Builder, str string) {
	if r.isNull(str) {
		field.AppendNull()
		return
	}

	v, err := strconv.ParseUint(str, 10, 8)
	if err != nil && r.err == nil {
		r.err = err
		field.AppendNull()
		return
	}

	field.(*array.Uint8Builder).Append(uint8(v))
}

func (r *Reader) parseUint16(field array.Builder, str string) {
	if r.isNull(str) {
		field.AppendNull()
		return
	}

	v, err := strconv.ParseUint(str, 10, 16)
	if err != nil && r.err == nil {
		r.err = err
		field.AppendNull()
		return
	}

	field.(*array.Uint16Builder).Append(uint16(v))
}

func (r *Reader) parseUint32(field array.Builder, str string) {
	if r.isNull(str) {
		field.AppendNull()
		return
	}

	v, err := strconv.ParseUint(str, 10, 32)
	if err != nil && r.err == nil {
		r.err = err
		field.AppendNull()
		return
	}

	field.(*array.Uint32Builder).Append(uint32(v))
}

func (r *Reader) parseUint64(field array.Builder, str string) {
	if r.isNull(str) {
		field.AppendNull()
		return
	}

	v, err := strconv.ParseUint(str, 10, 64)
	if err != nil && r.err == nil {
		r.err = err
		field.AppendNull()
		return
	}

	field.(*array.Uint64Builder).Append(v)
}

func (r *Reader) parseFloat32(field array.Builder, str string) {
	if r.isNull(str) {
		field.AppendNull()
		return
	}

	v, err := strconv.ParseFloat(str, 32)
	if err != nil && r.err == nil {
		r.err = err
		field.AppendNull()
		return
	}
	field.(*array.Float32Builder).Append(float32(v))

}

func (r *Reader) parseFloat64(field array.Builder, str string) {
	if r.isNull(str) {
		field.AppendNull()
		return
	}

	v, err := strconv.ParseFloat(str, 64)
	if err != nil && r.err == nil {
		r.err = err
		field.AppendNull()
		return
	}
	field.(*array.Float64Builder).Append(v)
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (r *Reader) Retain() {
	atomic.AddInt64(&r.refs, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (r *Reader) Release() {
	debug.Assert(atomic.LoadInt64(&r.refs) > 0, "too many releases")

	if atomic.AddInt64(&r.refs, -1) == 0 {
		if r.cur != nil {
			r.cur.Release()
		}
	}
}

var (
	_ array.RecordReader = (*Reader)(nil)
)
