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

package compute

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/scalar"
)

//go:generate go run golang.org/x/tools/cmd/stringer -type=ValueShape -linecomment
//go:generate go run golang.org/x/tools/cmd/stringer -type=DatumKind -linecomment

type ValueShape int8

const (
	ShapeAny    ValueShape = iota // any
	ShapeArray                    // array
	ShapeScalar                   // scalar
)

type ValueDescr struct {
	Shape ValueShape
	Type  arrow.DataType
}

func (v *ValueDescr) String() string {
	return fmt.Sprintf("%s [%s]", v.Shape, v.Type)
}

type DatumKind int

const (
	KindNone       DatumKind = iota // none
	KindScalar                      // scalar
	KindArray                       // array
	KindChunked                     // chunked_array
	KindRecord                      // record_batch
	KindTable                       // table
	KindCollection                  // collection
)

const UnknownLength int64 = -1

type Datum interface {
	fmt.Stringer
	Kind() DatumKind
	Len() int64
	Equals(Datum) bool
	Release()
}

type ArrayLikeDatum interface {
	Datum
	Shape() ValueShape
	Descr() ValueDescr
	NullN() int64
	Type() arrow.DataType
	Chunks() []array.Interface
}

type TableLikeDatum interface {
	Datum
	Schema() *arrow.Schema
}

type EmptyDatum struct{}

func (EmptyDatum) String() string  { return "nullptr" }
func (EmptyDatum) Kind() DatumKind { return KindNone }
func (EmptyDatum) Len() int64      { return UnknownLength }
func (EmptyDatum) Release()        {}
func (EmptyDatum) Equals(other Datum) bool {
	_, ok := other.(EmptyDatum)
	return ok
}

type ScalarDatum struct {
	Value scalar.Scalar
}

func (ScalarDatum) Kind() DatumKind           { return KindScalar }
func (ScalarDatum) Shape() ValueShape         { return ShapeScalar }
func (ScalarDatum) Len() int64                { return 1 }
func (ScalarDatum) Chunks() []array.Interface { return nil }
func (d *ScalarDatum) Type() arrow.DataType   { return d.Value.DataType() }
func (d *ScalarDatum) String() string         { return d.Value.String() }
func (d *ScalarDatum) Descr() ValueDescr      { return ValueDescr{ShapeScalar, d.Value.DataType()} }

func (d *ScalarDatum) NullN() int64 {
	if d.Value.IsValid() {
		return 0
	}
	return 1
}

type releasable interface {
	Release()
}

func (d *ScalarDatum) Release() {
	if v, ok := d.Value.(releasable); ok {
		v.Release()
	}
}

func (d *ScalarDatum) Equals(other Datum) bool {
	if rhs, ok := other.(*ScalarDatum); ok {
		return scalar.Equals(d.Value, rhs.Value)
	}
	return false
}

type ArrayDatum struct {
	Value *array.Data
}

func (ArrayDatum) Kind() DatumKind               { return KindArray }
func (ArrayDatum) Shape() ValueShape             { return ShapeArray }
func (d *ArrayDatum) Type() arrow.DataType       { return d.Value.DataType() }
func (d *ArrayDatum) Len() int64                 { return int64(d.Value.Len()) }
func (d *ArrayDatum) NullN() int64               { return int64(d.Value.NullN()) }
func (d *ArrayDatum) Descr() ValueDescr          { return ValueDescr{ShapeArray, d.Value.DataType()} }
func (d *ArrayDatum) String() string             { return fmt.Sprintf("Array:{%s}", d.Value.DataType()) }
func (d *ArrayDatum) MakeArray() array.Interface { return array.MakeFromData(d.Value) }
func (d *ArrayDatum) Chunks() []array.Interface  { return []array.Interface{d.MakeArray()} }

func (d *ArrayDatum) Release() {
	d.Value.Release()
	d.Value = nil
}

func (d *ArrayDatum) Equals(other Datum) bool {
	rhs, ok := other.(*ArrayDatum)
	if !ok {
		return false
	}

	left := d.MakeArray()
	defer left.Release()
	right := rhs.MakeArray()
	defer right.Release()

	return array.ArrayEqual(left, right)
}

type ChunkedDatum struct {
	Value *array.Chunked
}

func (ChunkedDatum) Kind() DatumKind              { return KindChunked }
func (ChunkedDatum) Shape() ValueShape            { return ShapeArray }
func (d *ChunkedDatum) Type() arrow.DataType      { return d.Value.DataType() }
func (d *ChunkedDatum) Len() int64                { return int64(d.Value.Len()) }
func (d *ChunkedDatum) NullN() int64              { return int64(d.Value.NullN()) }
func (d *ChunkedDatum) Descr() ValueDescr         { return ValueDescr{ShapeArray, d.Value.DataType()} }
func (d *ChunkedDatum) String() string            { return fmt.Sprintf("Array:{%s}", d.Value.DataType()) }
func (d *ChunkedDatum) Chunks() []array.Interface { return d.Value.Chunks() }

func (d *ChunkedDatum) Release() {
	d.Value.Release()
	d.Value = nil
}

func (d *ChunkedDatum) Equals(other Datum) bool {
	if rhs, ok := other.(*ChunkedDatum); ok {
		return array.ChunkedEqual(d.Value, rhs.Value)
	}
	return false
}

type RecordDatum struct {
	Value array.Record
}

func (RecordDatum) Kind() DatumKind          { return KindRecord }
func (RecordDatum) String() string           { return "RecordBatch" }
func (r *RecordDatum) Len() int64            { return r.Value.NumRows() }
func (r *RecordDatum) Schema() *arrow.Schema { return r.Value.Schema() }

func (r *RecordDatum) Release() {
	r.Value.Release()
	r.Value = nil
}

func (r *RecordDatum) Equals(other Datum) bool {
	if rhs, ok := other.(*RecordDatum); ok {
		return array.RecordEqual(r.Value, rhs.Value)
	}
	return false
}

type TableDatum struct {
	Value array.Table
}

func (TableDatum) Kind() DatumKind          { return KindTable }
func (TableDatum) String() string           { return "Table" }
func (d *TableDatum) Len() int64            { return d.Value.NumRows() }
func (d *TableDatum) Schema() *arrow.Schema { return d.Value.Schema() }

func (d *TableDatum) Release() {
	d.Value.Release()
	d.Value = nil
}

func (d *TableDatum) Equals(other Datum) bool {
	if rhs, ok := other.(*TableDatum); ok {
		return array.TableEqual(d.Value, rhs.Value)
	}
	return false
}

type CollectionDatum []Datum

func (CollectionDatum) Kind() DatumKind { return KindCollection }
func (c CollectionDatum) Len() int64    { return int64(len(c)) }
func (c CollectionDatum) String() string {
	var b strings.Builder
	b.WriteString("Collection(")
	for i, d := range c {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(d.String())
	}
	b.WriteByte(')')
	return b.String()
}

func (c CollectionDatum) Release() {
	for _, v := range c {
		v.Release()
	}
}

func (c CollectionDatum) Equals(other Datum) bool {
	rhs, ok := other.(CollectionDatum)
	if !ok {
		return false
	}

	if len(c) != len(rhs) {
		return false
	}

	for i := range c {
		if !c[i].Equals(rhs[i]) {
			return false
		}
	}
	return true
}

func NewDatum(value interface{}) Datum {
	switch v := value.(type) {
	case array.Interface:
		v.Data().Retain()
		return &ArrayDatum{v.Data()}
	case *array.Chunked:
		v.Retain()
		return &ChunkedDatum{v}
	case array.Record:
		v.Retain()
		return &RecordDatum{v}
	case array.Table:
		v.Retain()
		return &TableDatum{v}
	case []Datum:
		return CollectionDatum(v)
	case scalar.Scalar:
		return &ScalarDatum{v}
	default:
		return &ScalarDatum{scalar.MakeScalar(value)}
	}
}

var (
	_ ArrayLikeDatum = (*ScalarDatum)(nil)
	_ ArrayLikeDatum = (*ArrayDatum)(nil)
	_ ArrayLikeDatum = (*ChunkedDatum)(nil)
	_ TableLikeDatum = (*RecordDatum)(nil)
	_ TableLikeDatum = (*TableDatum)(nil)
	_ Datum          = (CollectionDatum)(nil)
)
