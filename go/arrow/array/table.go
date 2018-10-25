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

package array

import (
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/internal/debug"
)

// Table represents a logical sequence of chunked arrays.
type Table interface {
	Schema() *arrow.Schema
	NumRows() int
	NumCols() int
	Column(i int) *Column

	Retain()
	Release()
}

// Column is an immutable column data structure consisting of
// a field (type metadata) and a chunked data array.
type Column struct {
	field arrow.Field
	data  *Chunked
}

// NewColumn returns a column from a field and a chunked data array.
//
// NewColumn panics if the field's data type is inconsistent with the data type
// of the chunked data array.
func NewColumn(field arrow.Field, chunks *Chunked) *Column {
	col := Column{
		field: field,
		data:  chunks,
	}
	col.data.Retain()

	if col.data.DataType() != col.field.Type {
		col.data.Release()
		panic("arrow/array: inconsistent data type")
	}

	return &col
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (col *Column) Retain() {
	col.data.Retain()
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (col *Column) Release() {
	col.data.Release()
}

func (col *Column) Len() int                 { return col.data.Len() }
func (col *Column) NullN() int               { return col.data.NullN() }
func (col *Column) Data() *Chunked           { return col.data }
func (col *Column) Field() arrow.Field       { return col.field }
func (col *Column) Name() string             { return col.field.Name }
func (col *Column) DataType() arrow.DataType { return col.field.Type }

// NewSlice returns a new zero-copy slice of the column with the indicated
// indices i and j, corresponding to the column's array[i:j].
// The returned column must be Release()'d after use.
//
// NewSlice panics if the slice is outside the valid range of the column's array.
// NewSlice panics if j < i.
func (col *Column) NewSlice(i, j int64) *Column {
	return &Column{
		field: col.field,
		data:  col.data.NewSlice(i, j),
	}
}

// Chunked manages a collection of primitives arrays as one logical large array.
type Chunked struct {
	chunks []Interface

	refCount int64

	length int
	nulls  int
	dtype  arrow.DataType
}

// NewChunked returns a new chunked array from the slice of arrays.
//
// NewChunked panics if the chunks do not have the same data type.
func NewChunked(dtype arrow.DataType, chunks []Interface) *Chunked {
	arr := &Chunked{
		chunks:   make([]Interface, len(chunks)),
		refCount: 1,
		dtype:    dtype,
	}
	for i, chunk := range chunks {
		if chunk.DataType() != dtype {
			panic("arrow/array: mismatch data type")
		}
		chunk.Retain()
		arr.chunks[i] = chunk
		arr.length += chunk.Len()
		arr.nulls += chunk.NullN()
	}
	return arr
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (a *Chunked) Retain() {
	atomic.AddInt64(&a.refCount, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (a *Chunked) Release() {
	debug.Assert(atomic.LoadInt64(&a.refCount) > 0, "too many releases")

	if atomic.AddInt64(&a.refCount, -1) == 0 {
		for _, arr := range a.chunks {
			arr.Release()
		}
		a.chunks = nil
		a.length = 0
		a.nulls = 0
	}
}

func (a *Chunked) Len() int                 { return a.length }
func (a *Chunked) NullN() int               { return a.nulls }
func (a *Chunked) DataType() arrow.DataType { return a.dtype }
func (a *Chunked) Chunks() []Interface      { return a.chunks }
func (a *Chunked) Chunk(i int) Interface    { return a.chunks[i] }

// NewSlice constructs a zero-copy slice of the chunked array with the indicated
// indices i and j, corresponding to array[i:j].
// The returned chunked array must be Release()'d after use.
//
// NewSlice panics if the slice is outside the valid range of the input array.
// NewSlice panics if j < i.
func (a *Chunked) NewSlice(i, j int64) *Chunked {
	if j > int64(a.length) || i > j || i > int64(a.length) {
		panic("arrow/array: index out of range")
	}

	var (
		cur    = 0
		beg    = i
		sz     = j - i
		chunks = make([]Interface, 0, len(a.chunks))
	)

	for cur < len(a.chunks) && beg >= int64(a.chunks[cur].Len()) {
		beg -= int64(a.chunks[cur].Len())
		cur++
	}

	for cur < len(a.chunks) && sz > 0 {
		arr := a.chunks[cur]
		end := beg + sz
		if end > int64(arr.Len()) {
			end = int64(arr.Len())
		}
		chunks = append(chunks, NewSlice(arr, beg, end))
		sz -= int64(arr.Len()) - beg
		beg = 0
		cur++
	}
	chunks = chunks[:len(chunks):len(chunks)]
	defer func() {
		for _, chunk := range chunks {
			chunk.Release()
		}
	}()

	return NewChunked(a.dtype, chunks)
}

// simpleTable is a basic, non-lazy in-memory table.
type simpleTable struct {
	refCount int64

	rows int
	cols []Column

	schema *arrow.Schema
}

// NewTable returns a new basic, non-lazy in-memory table.
// If rows is negative, the number of rows will be inferred from the height
// of the columns.
//
// NewTable panics if the columns and schema are inconsistent.
// NewTable panics if rows is larger than the height of the columns.
func NewTable(schema *arrow.Schema, cols []Column, rows int) *simpleTable {
	tbl := simpleTable{
		refCount: 1,
		rows:     rows,
		cols:     cols,
		schema:   schema,
	}

	if tbl.rows < 0 {
		switch len(tbl.cols) {
		case 0:
			tbl.rows = 0
		default:
			tbl.rows = tbl.cols[0].Len()
		}
	}

	// validate the table and its constituents.
	// note we retain the columns after having validated the table
	// in case the validation fails and panics (and would otherwise leak
	// a ref-count on the columns.)
	tbl.validate()

	for i := range tbl.cols {
		tbl.cols[i].Retain()
	}

	return &tbl
}

func (tbl *simpleTable) Schema() *arrow.Schema { return tbl.schema }
func (tbl *simpleTable) NumRows() int          { return tbl.rows }
func (tbl *simpleTable) NumCols() int          { return len(tbl.cols) }
func (tbl *simpleTable) Column(i int) *Column  { return &tbl.cols[i] }

func (tbl *simpleTable) validate() {
	if len(tbl.cols) != len(tbl.schema.Fields()) {
		panic("arrow/array: table schema mismatch")
	}
	for i, col := range tbl.cols {
		if !reflect.DeepEqual(col.field, tbl.schema.Field(i)) { // FIXME(sbinet): impl+use arrow.Field.Equal()
			panic(fmt.Errorf("arrow/array: column field %q is inconsistent with schema", col.Name()))
		}

		if col.Len() < tbl.rows {
			panic(fmt.Errorf("arrow/array: column %q expected length >= %d but got length %d", col.Name(), tbl.rows, col.Len()))
		}
	}
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (tbl *simpleTable) Retain() {
	atomic.AddInt64(&tbl.refCount, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (tbl *simpleTable) Release() {
	debug.Assert(atomic.LoadInt64(&tbl.refCount) > 0, "too many releases")

	if atomic.AddInt64(&tbl.refCount, -1) == 0 {
		for i := range tbl.cols {
			tbl.cols[i].Release()
		}
		tbl.cols = nil
	}
}

var (
	_ Table = (*simpleTable)(nil)
)
