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

package scalar

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"golang.org/x/xerrors"
)

type ListScalar interface {
	Scalar
	GetList() arrow.Array
	Release()
	Retain()
}

type List struct {
	scalar
	Value arrow.Array
}

func (l *List) Release()             { l.Value.Release() }
func (l *List) Retain()              { l.Value.Retain() }
func (l *List) value() interface{}   { return l.Value }
func (l *List) GetList() arrow.Array { return l.Value }
func (l *List) equals(rhs Scalar) bool {
	return array.Equal(l.Value, rhs.(ListScalar).GetList())
}
func (l *List) Validate() (err error) {
	if err = l.scalar.Validate(); err != nil {
		return
	}
	if err = validateOptional(&l.scalar, l.Value, "value"); err != nil {
		return
	}

	if !l.Valid {
		return
	}

	var (
		valueType arrow.DataType
	)

	switch dt := l.Type.(type) {
	case *arrow.ListType:
		valueType = dt.Elem()
	case *arrow.LargeListType:
		valueType = dt.Elem()
	case *arrow.FixedSizeListType:
		valueType = dt.Elem()
	case *arrow.MapType:
		valueType = dt.ValueType()
	}
	listType := l.Type

	if !arrow.TypeEqual(l.Value.DataType(), valueType) {
		err = fmt.Errorf("%s scalar should have a value of type %s, got %s",
			listType, valueType, l.Value.DataType())
	}
	return
}

func (l *List) ValidateFull() error { return l.Validate() }
func (l *List) CastTo(to arrow.DataType) (Scalar, error) {
	if !l.Valid {
		return MakeNullScalar(to), nil
	}

	if arrow.TypeEqual(l.Type, to) {
		return l, nil
	}

	if to.ID() == arrow.STRING {
		var bld bytes.Buffer
		fmt.Fprint(&bld, l.Value)
		buf := memory.NewBufferBytes(bld.Bytes())
		defer buf.Release()
		return NewStringScalarFromBuffer(buf), nil
	}

	return nil, fmt.Errorf("cannot convert non-nil list scalar to type %s", to)
}

func (l *List) String() string {
	if !l.Valid {
		return "null"
	}
	val, err := l.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func NewListScalar(val arrow.Array) *List {
	return &List{scalar{arrow.ListOf(val.DataType()), true}, array.MakeFromData(val.Data())}
}

func NewListScalarData(val arrow.ArrayData) *List {
	return &List{scalar{arrow.ListOf(val.DataType()), true}, array.MakeFromData(val)}
}

type LargeList struct {
	*List
}

func NewLargeListScalar(val arrow.Array) *LargeList {
	return &LargeList{&List{scalar{arrow.LargeListOf(val.DataType()), true}, array.MakeFromData(val.Data())}}
}

func NewLargeListScalarData(val arrow.ArrayData) *LargeList {
	return &LargeList{&List{scalar{arrow.LargeListOf(val.DataType()), true}, array.MakeFromData(val)}}
}

func makeMapType(typ *arrow.StructType) *arrow.MapType {
	debug.Assert(len(typ.Fields()) == 2, "must pass struct with only 2 fields for MapScalar")
	return arrow.MapOf(typ.Field(0).Type, typ.Field(1).Type)
}

type Map struct {
	*List
}

func NewMapScalar(val arrow.Array) *Map {
	return &Map{&List{scalar{makeMapType(val.DataType().(*arrow.StructType)), true}, array.MakeFromData(val.Data())}}
}

type FixedSizeList struct {
	*List
}

func (f *FixedSizeList) Validate() (err error) {
	if err = f.List.Validate(); err != nil {
		return
	}

	if f.Valid {
		listType := f.Type.(*arrow.FixedSizeListType)
		if f.Value.Len() != int(listType.Len()) {
			return fmt.Errorf("%s scalar should have a child value of length %d, got %d",
				f.Type, listType.Len(), f.Value.Len())
		}
	}
	return
}

func (f *FixedSizeList) ValidateFull() error { return f.Validate() }

func NewFixedSizeListScalar(val arrow.Array) *FixedSizeList {
	return NewFixedSizeListScalarWithType(val, arrow.FixedSizeListOf(int32(val.Len()), val.DataType()))
}

func NewFixedSizeListScalarWithType(val arrow.Array, typ arrow.DataType) *FixedSizeList {
	debug.Assert(val.Len() == int(typ.(*arrow.FixedSizeListType).Len()), "length of value for fixed size list scalar must match type")
	return &FixedSizeList{&List{scalar{typ, true}, array.MakeFromData(val.Data())}}
}

type Vector []Scalar

type Struct struct {
	scalar
	Value Vector
}

func (s *Struct) Release() {
	for _, v := range s.Value {
		if v, ok := v.(Releasable); ok {
			v.Release()
		}
	}
}

func (s *Struct) Field(name string) (Scalar, error) {
	idx, ok := s.Type.(*arrow.StructType).FieldIdx(name)
	if !ok {
		return nil, fmt.Errorf("no field named %s found in struct scalar %s", name, s.Type)
	}

	return s.Value[idx], nil
}

func (s *Struct) value() interface{} { return s.Value }

func (s *Struct) String() string {
	if !s.Valid {
		return "null"
	}
	val, err := s.CastTo(arrow.BinaryTypes.String)
	if err != nil {
		return "..."
	}
	return string(val.(*String).Value.Bytes())
}

func (s *Struct) CastTo(to arrow.DataType) (Scalar, error) {
	if !s.Valid {
		return MakeNullScalar(to), nil
	}

	if to.ID() != arrow.STRING {
		return nil, fmt.Errorf("cannot cast non-null struct scalar to type %s", to)
	}

	var bld bytes.Buffer
	st := s.Type.(*arrow.StructType)
	bld.WriteByte('{')
	for i, v := range s.Value {
		if i > 0 {
			bld.WriteString(", ")
		}
		bld.WriteString(fmt.Sprintf("%s:%s = %s", st.Field(i).Name, st.Field(i).Type, v.String()))
	}
	bld.WriteByte('}')
	buf := memory.NewBufferBytes(bld.Bytes())
	defer buf.Release()
	return NewStringScalarFromBuffer(buf), nil
}

func (s *Struct) equals(rhs Scalar) bool {
	right := rhs.(*Struct)
	if len(s.Value) != len(right.Value) {
		return false
	}

	for i := range s.Value {
		if !Equals(s.Value[i], right.Value[i]) {
			return false
		}
	}
	return true
}

func (s *Struct) Validate() (err error) {
	if err = s.scalar.Validate(); err != nil {
		return
	}

	if !s.Valid {
		if len(s.Value) != 0 {
			err = fmt.Errorf("%s scalar is marked null but has child values", s.Type)
		}
		return
	}

	st := s.Type.(*arrow.StructType)
	num := len(st.Fields())
	if len(s.Value) != num {
		return fmt.Errorf("non-null %s scalar should have %d child values, got %d", s.Type, num, len(s.Value))
	}

	for i, f := range st.Fields() {
		if s.Value[i] == nil {
			return fmt.Errorf("non-null %s scalar has missing child value at index %d", s.Type, i)
		}

		err = s.Value[i].Validate()
		if err != nil {
			return fmt.Errorf("%s scalar fails validation for child at index %d: %w", s.Type, i, err)
		}

		if !arrow.TypeEqual(s.Value[i].DataType(), f.Type) {
			return fmt.Errorf("%s scalar should have a child value of type %s at index %d, got %s", s.Type, f.Type, i, s.Value[i].DataType())
		}
	}
	return
}

func (s *Struct) ValidateFull() (err error) {
	if err = s.scalar.ValidateFull(); err != nil {
		return
	}

	if !s.Valid {
		if len(s.Value) != 0 {
			err = fmt.Errorf("%s scalar is marked null but has child values", s.Type)
		}
		return
	}

	st := s.Type.(*arrow.StructType)
	num := len(st.Fields())
	if len(s.Value) != num {
		return fmt.Errorf("non-null %s scalar should have %d child values, got %d", s.Type, num, len(s.Value))
	}

	for i, f := range st.Fields() {
		if s.Value[i] == nil {
			return fmt.Errorf("non-null %s scalar has missing child value at index %d", s.Type, i)
		}

		err = s.Value[i].ValidateFull()
		if err != nil {
			return fmt.Errorf("%s scalar fails validation for child at index %d: %w", s.Type, i, err)
		}

		if !arrow.TypeEqual(s.Value[i].DataType(), f.Type) {
			return fmt.Errorf("%s scalar should have a child value of type %s at index %d, got %s", s.Type, f.Type, i, s.Value[i].DataType())
		}
	}
	return
}

func NewStructScalar(val []Scalar, typ arrow.DataType) *Struct {
	return &Struct{scalar{typ, true}, val}
}

func NewStructScalarWithNames(val []Scalar, names []string) (*Struct, error) {
	if len(val) != len(names) {
		return nil, xerrors.New("mismatching number of field names and child scalars")
	}

	fields := make([]arrow.Field, len(names))
	for i, n := range names {
		fields[i] = arrow.Field{Name: n, Type: val[i].DataType(), Nullable: true}
	}
	return NewStructScalar(val, arrow.StructOf(fields...)), nil
}

type Dictionary struct {
	scalar

	Value struct {
		Index Scalar
		Dict  arrow.Array
	}
}

func NewNullDictScalar(dt arrow.DataType) *Dictionary {
	ret := &Dictionary{scalar: scalar{dt, false}}
	ret.Value.Index = MakeNullScalar(dt.(*arrow.DictionaryType).IndexType)
	ret.Value.Dict = nil
	return ret
}

func NewDictScalar(index Scalar, dict arrow.Array) *Dictionary {
	ret := &Dictionary{scalar: scalar{&arrow.DictionaryType{IndexType: index.DataType(), ValueType: dict.DataType()}, index.IsValid()}}
	ret.Value.Index = index
	ret.Value.Dict = dict
	ret.Retain()
	return ret
}

func (s *Dictionary) Retain() {
	if r, ok := s.Value.Index.(Releasable); ok {
		r.Retain()
	}
	if s.Value.Dict != (arrow.Array)(nil) {
		s.Value.Dict.Retain()
	}
}

func (s *Dictionary) Release() {
	if r, ok := s.Value.Index.(Releasable); ok {
		r.Release()
	}
	if s.Value.Dict != (arrow.Array)(nil) {
		s.Value.Dict.Release()
	}
}

func (s *Dictionary) Validate() (err error) {
	dt, ok := s.Type.(*arrow.DictionaryType)
	if !ok {
		return errors.New("arrow/scalar: dictionary scalar should have type Dictionary")
	}

	if s.Value.Index == (Scalar)(nil) {
		return fmt.Errorf("%s scalar doesn't have an index value", dt)
	}

	if err = s.Value.Index.Validate(); err != nil {
		return fmt.Errorf("%s scalar fails validation for index value: %w", dt, err)
	}

	if !arrow.TypeEqual(s.Value.Index.DataType(), dt.IndexType) {
		return fmt.Errorf("%s scalar should have an index value of type %s, got %s",
			dt, dt.IndexType, s.Value.Index.DataType())
	}

	if s.IsValid() && !s.Value.Index.IsValid() {
		return fmt.Errorf("non-null %s scalar has null index value", dt)
	}

	if !s.IsValid() && s.Value.Index.IsValid() {
		return fmt.Errorf("null %s scalar has non-null index value", dt)
	}

	if !s.IsValid() {
		return
	}

	if s.Value.Dict == (arrow.Array)(nil) {
		return fmt.Errorf("%s scalar doesn't have a dictionary value", dt)
	}

	if !arrow.TypeEqual(s.Value.Dict.DataType(), dt.ValueType) {
		return fmt.Errorf("%s scalar's value type doesn't match dict type: got %s", dt, s.Value.Dict.DataType())
	}

	return
}

func (s *Dictionary) ValidateFull() (err error) {
	if err = s.Validate(); err != nil {
		return
	}

	if !s.Value.Index.IsValid() {
		return nil
	}

	max := s.Value.Dict.Len() - 1
	switch idx := s.Value.Index.value().(type) {
	case int8:
		if idx < 0 || int(idx) > max {
			err = fmt.Errorf("%s scalar index value out of bounds: %d", s.DataType(), idx)
		}
	case uint8:
		if int(idx) > max {
			err = fmt.Errorf("%s scalar index value out of bounds: %d", s.DataType(), idx)
		}
	case int16:
		if idx < 0 || int(idx) > max {
			err = fmt.Errorf("%s scalar index value out of bounds: %d", s.DataType(), idx)
		}
	case uint16:
		if int(idx) > max {
			err = fmt.Errorf("%s scalar index value out of bounds: %d", s.DataType(), idx)
		}
	case int32:
		if idx < 0 || int(idx) > max {
			err = fmt.Errorf("%s scalar index value out of bounds: %d", s.DataType(), idx)
		}
	case uint32:
		if int(idx) > max {
			err = fmt.Errorf("%s scalar index value out of bounds: %d", s.DataType(), idx)
		}
	case int64:
		if idx < 0 || int(idx) > max {
			err = fmt.Errorf("%s scalar index value out of bounds: %d", s.DataType(), idx)
		}
	case uint64:
		if int(idx) > max {
			err = fmt.Errorf("%s scalar index value out of bounds: %d", s.DataType(), idx)
		}
	}

	return
}

func (s *Dictionary) String() string {
	if !s.Valid {
		return "null"
	}

	return s.Value.Dict.String() + "[" + s.Value.Index.String() + "]"
}

func (s *Dictionary) equals(rhs Scalar) bool {
	return s.Value.Index.equals(rhs.(*Dictionary).Value.Index) &&
		array.Equal(s.Value.Dict, rhs.(*Dictionary).Value.Dict)
}

func (s *Dictionary) CastTo(arrow.DataType) (Scalar, error) {
	return nil, fmt.Errorf("cast from scalar %s not implemented", s.DataType())
}

func (s *Dictionary) GetEncodedValue() (Scalar, error) {
	dt := s.Type.(*arrow.DictionaryType)
	if !s.IsValid() {
		return MakeNullScalar(dt.ValueType), nil
	}

	var idxValue int
	switch dt.IndexType.ID() {
	case arrow.INT8:
		idxValue = int(s.Value.Index.value().(int8))
	case arrow.UINT8:
		idxValue = int(s.Value.Index.value().(uint8))
	case arrow.INT16:
		idxValue = int(s.Value.Index.value().(int16))
	case arrow.UINT16:
		idxValue = int(s.Value.Index.value().(uint16))
	case arrow.INT32:
		idxValue = int(s.Value.Index.value().(int32))
	case arrow.UINT32:
		idxValue = int(s.Value.Index.value().(uint32))
	case arrow.INT64:
		idxValue = int(s.Value.Index.value().(int64))
	case arrow.UINT64:
		idxValue = int(s.Value.Index.value().(uint64))
	default:
		return nil, fmt.Errorf("unimplemented dictionary type %s", dt.IndexType)
	}
	return GetScalar(s.Value.Dict, idxValue)
}

func (s *Dictionary) value() interface{} {
	return s.Value.Index.value()
}
