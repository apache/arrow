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
	"fmt"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/internal/debug"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"golang.org/x/xerrors"
)

type ListScalar interface {
	Scalar
	GetList() array.Interface
	Release()
	Retain()
}

type List struct {
	scalar
	Value array.Interface
}

func (l *List) Release()                 { l.Value.Release() }
func (l *List) Retain()                  { l.Value.Retain() }
func (l *List) value() interface{}       { return l.Value }
func (l *List) GetList() array.Interface { return l.Value }
func (l *List) equals(rhs Scalar) bool {
	return array.ArrayEqual(l.Value, rhs.(ListScalar).GetList())
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
	case *arrow.FixedSizeListType:
		valueType = dt.Elem()
	case *arrow.MapType:
		valueType = dt.ValueType()
	}
	listType := l.Type

	if !arrow.TypeEqual(l.Value.DataType(), valueType) {
		err = xerrors.Errorf("%s scalar should have a value of type %s, got %s",
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

	return nil, xerrors.Errorf("cannot convert non-nil list scalar to type %s", to)
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

func NewListScalar(val array.Interface) *List {
	return &List{scalar{arrow.ListOf(val.DataType()), true}, array.MakeFromData(val.Data())}
}

func makeMapType(typ *arrow.StructType) *arrow.MapType {
	debug.Assert(len(typ.Fields()) == 2, "must pass struct with only 2 fields for MapScalar")
	return arrow.MapOf(typ.Field(0).Type, typ.Field(1).Type)
}

type Map struct {
	*List
}

func NewMapScalar(val array.Interface) *Map {
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
			return xerrors.Errorf("%s scalar should have a child value of length %d, got %d",
				f.Type, listType.Len(), f.Value.Len())
		}
	}
	return
}

func (f *FixedSizeList) ValidateFull() error { return f.Validate() }

func NewFixedSizeListScalar(val array.Interface) *FixedSizeList {
	return NewFixedSizeListScalarWithType(val, arrow.FixedSizeListOf(int32(val.Len()), val.DataType()))
}

func NewFixedSizeListScalarWithType(val array.Interface, typ arrow.DataType) *FixedSizeList {
	debug.Assert(val.Len() == int(typ.(*arrow.FixedSizeListType).Len()), "length of value for fixed size list scalar must match type")
	return &FixedSizeList{&List{scalar{typ, true}, array.MakeFromData(val.Data())}}
}

type Vector []Scalar

type Struct struct {
	scalar
	Value Vector
}

func (s *Struct) Field(name string) (Scalar, error) {
	idx, ok := s.Type.(*arrow.StructType).FieldIdx(name)
	if !ok {
		return nil, xerrors.Errorf("no field named %s found in struct scalar %s", name, s.Type)
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
		return nil, xerrors.Errorf("cannot cast non-null struct scalar to type %s", to)
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
			err = xerrors.Errorf("%s scalar is marked null but has child values", s.Type)
		}
		return
	}

	st := s.Type.(*arrow.StructType)
	num := len(st.Fields())
	if len(s.Value) != num {
		return xerrors.Errorf("non-null %s scalar should have %d child values, got %d", s.Type, num, len(s.Value))
	}

	for i, f := range st.Fields() {
		if s.Value[i] == nil {
			return xerrors.Errorf("non-null %s scalar has missing child value at index %d", s.Type, i)
		}

		err = s.Value[i].Validate()
		if err != nil {
			return xerrors.Errorf("%s scalar fails validation for child at index %d: %w", s.Type, i, err)
		}

		if !arrow.TypeEqual(s.Value[i].DataType(), f.Type) {
			return xerrors.Errorf("%s scalar should have a child value of type %s at index %d, got %s", s.Type, f.Type, i, s.Value[i].DataType())
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
			err = xerrors.Errorf("%s scalar is marked null but has child values", s.Type)
		}
		return
	}

	st := s.Type.(*arrow.StructType)
	num := len(st.Fields())
	if len(s.Value) != num {
		return xerrors.Errorf("non-null %s scalar should have %d child values, got %d", s.Type, num, len(s.Value))
	}

	for i, f := range st.Fields() {
		if s.Value[i] == nil {
			return xerrors.Errorf("non-null %s scalar has missing child value at index %d", s.Type, i)
		}

		err = s.Value[i].ValidateFull()
		if err != nil {
			return xerrors.Errorf("%s scalar fails validation for child at index %d: %w", s.Type, i, err)
		}

		if !arrow.TypeEqual(s.Value[i].DataType(), f.Type) {
			return xerrors.Errorf("%s scalar should have a child value of type %s at index %d, got %s", s.Type, f.Type, i, s.Value[i].DataType())
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
