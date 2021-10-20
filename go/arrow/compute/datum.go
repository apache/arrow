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
