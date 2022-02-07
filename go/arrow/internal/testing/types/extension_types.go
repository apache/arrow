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

// Package types contains user-defined types for use in the tests for the arrow package
package types

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"golang.org/x/xerrors"
)

// UUIDArray is a simple array which is a FixedSizeBinary(16)
type UUIDArray struct {
	array.ExtensionArrayBase
}

// UUIDType is a simple extension type that represents a FixedSizeBinary(16)
// to be used for representing UUIDs
type UUIDType struct {
	arrow.ExtensionBase
}

// NewUUIDType is a convenience function to create an instance of UuidType
// with the correct storage type
func NewUUIDType() *UUIDType {
	return &UUIDType{
		ExtensionBase: arrow.ExtensionBase{
			Storage: &arrow.FixedSizeBinaryType{ByteWidth: 16}}}
}

// ArrayType returns TypeOf(UuidArray) for constructing uuid arrays
func (UUIDType) ArrayType() reflect.Type { return reflect.TypeOf(UUIDArray{}) }

func (UUIDType) ExtensionName() string { return "uuid" }

// Serialize returns "uuid-serialized" for testing proper metadata passing
func (UUIDType) Serialize() string { return "uuid-serialized" }

// Deserialize expects storageType to be FixedSizeBinaryType{ByteWidth: 16} and the data to be
// "uuid-serialized" in order to correctly create a UuidType for testing deserialize.
func (UUIDType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	if string(data) != "uuid-serialized" {
		return nil, xerrors.Errorf("type identifier did not match: '%s'", string(data))
	}
	if !arrow.TypeEqual(storageType, &arrow.FixedSizeBinaryType{ByteWidth: 16}) {
		return nil, xerrors.Errorf("invalid storage type for UuidType: %s", storageType.Name())
	}
	return NewUUIDType(), nil
}

// UuidTypes are equal if both are named "uuid"
func (u UUIDType) ExtensionEquals(other arrow.ExtensionType) bool {
	return u.ExtensionName() == other.ExtensionName()
}

// Parametric1Array is a simple int32 array for use with the Parametric1Type
// in testing a parameterized user-defined extension type.
type Parametric1Array struct {
	array.ExtensionArrayBase
}

// Parametric2Array is another simple int32 array for use with the Parametric2Type
// also for testing a parameterized user-defined extension type that utilizes
// the parameter for defining different types based on the param.
type Parametric2Array struct {
	array.ExtensionArrayBase
}

// A type where ExtensionName is always the same
type Parametric1Type struct {
	arrow.ExtensionBase

	param int32
}

func NewParametric1Type(p int32) *Parametric1Type {
	ret := &Parametric1Type{param: p}
	ret.ExtensionBase.Storage = arrow.PrimitiveTypes.Int32
	return ret
}

func (p *Parametric1Type) String() string { return "extension<" + p.ExtensionName() + ">" }

// ExtensionEquals returns true if other is a *Parametric1Type and has the same param
func (p *Parametric1Type) ExtensionEquals(other arrow.ExtensionType) bool {
	o, ok := other.(*Parametric1Type)
	if !ok {
		return false
	}
	return p.param == o.param
}

// ExtensionName is always "parametric-type-1"
func (Parametric1Type) ExtensionName() string { return "parametric-type-1" }

// ArrayType returns the TypeOf(Parametric1Array{})
func (Parametric1Type) ArrayType() reflect.Type { return reflect.TypeOf(Parametric1Array{}) }

// Serialize returns the param as 4 little endian bytes
func (p *Parametric1Type) Serialize() string {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(p.param))
	return string(buf[:])
}

// Deserialize requires storage to be an int32 type and data should be a 4 byte little endian int32 value
func (Parametric1Type) Deserialize(storage arrow.DataType, data string) (arrow.ExtensionType, error) {
	if len(data) != 4 {
		return nil, xerrors.Errorf("parametric1type: invalid serialized data size: %d", len(data))
	}

	if storage.ID() != arrow.INT32 {
		return nil, xerrors.New("parametric1type: must have int32 as underlying storage type")
	}

	return &Parametric1Type{arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32}, int32(binary.LittleEndian.Uint32([]byte(data)))}, nil
}

// a parametric type where the extension name is different for each
// parameter, and must be registered separately
type Parametric2Type struct {
	arrow.ExtensionBase

	param int32
}

func NewParametric2Type(p int32) *Parametric2Type {
	ret := &Parametric2Type{param: p}
	ret.ExtensionBase.Storage = arrow.PrimitiveTypes.Int32
	return ret
}

func (p *Parametric2Type) String() string { return "extension<" + p.ExtensionName() + ">" }

// ExtensionEquals returns true if other is a *Parametric2Type and has the same param
func (p *Parametric2Type) ExtensionEquals(other arrow.ExtensionType) bool {
	o, ok := other.(*Parametric2Type)
	if !ok {
		return false
	}
	return p.param == o.param
}

// ExtensionName incorporates the param in the name requiring different instances of
// Parametric2Type to be registered separately if they have different params. this is
// used for testing registration of different types with the same struct type.
func (p *Parametric2Type) ExtensionName() string {
	return fmt.Sprintf("parametric-type-2<param=%d>", p.param)
}

// ArrayType returns TypeOf(Parametric2Array{})
func (Parametric2Type) ArrayType() reflect.Type { return reflect.TypeOf(Parametric2Array{}) }

// Serialize returns the param as a 4 byte little endian slice
func (p *Parametric2Type) Serialize() string {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(p.param))
	return string(buf[:])
}

// Deserialize expects storage to be int32 type and data must be a 4 byte little endian slice.
func (Parametric2Type) Deserialize(storage arrow.DataType, data string) (arrow.ExtensionType, error) {
	if len(data) != 4 {
		return nil, xerrors.Errorf("parametric1type: invalid serialized data size: %d", len(data))
	}

	if storage.ID() != arrow.INT32 {
		return nil, xerrors.New("parametric1type: must have int32 as underlying storage type")
	}

	return &Parametric2Type{arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int32}, int32(binary.LittleEndian.Uint32([]byte(data)))}, nil
}

// ExtStructArray is a struct array type for testing an extension type with non-primitive storage
type ExtStructArray struct {
	array.ExtensionArrayBase
}

// ExtStructType is an extension type with a non-primitive storage type containing a struct
// with fields {a: int64, b: float64}
type ExtStructType struct {
	arrow.ExtensionBase
}

func NewExtStructType() *ExtStructType {
	return &ExtStructType{
		ExtensionBase: arrow.ExtensionBase{Storage: arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float64},
		)},
	}
}

func (p *ExtStructType) String() string { return "extension<" + p.ExtensionName() + ">" }

// ExtensionName is always "ext-struct-type"
func (ExtStructType) ExtensionName() string { return "ext-struct-type" }

// ExtensionEquals returns true if other is a *ExtStructType
func (ExtStructType) ExtensionEquals(other arrow.ExtensionType) bool {
	_, ok := other.(*ExtStructType)
	return ok
}

// ArrayType returns TypeOf(ExtStructType{})
func (ExtStructType) ArrayType() reflect.Type { return reflect.TypeOf(ExtStructArray{}) }

// Serialize just returns "ext-struct-type-unique-code" to test metadata passing in IPC
func (ExtStructType) Serialize() string { return "ext-struct-type-unique-code" }

// Deserialize ignores the passed in storage datatype and only checks the serialized data byte slice
// returning the correct type if it matches "ext-struct-type-unique-code".
func (ExtStructType) Deserialize(_ arrow.DataType, serialized string) (arrow.ExtensionType, error) {
	if string(serialized) != "ext-struct-type-unique-code" {
		return nil, xerrors.New("type identifier did not match")
	}
	return NewExtStructType(), nil
}

var (
	_ arrow.ExtensionType  = (*UUIDType)(nil)
	_ arrow.ExtensionType  = (*Parametric1Type)(nil)
	_ arrow.ExtensionType  = (*Parametric2Type)(nil)
	_ arrow.ExtensionType  = (*ExtStructType)(nil)
	_ array.ExtensionArray = (*UUIDArray)(nil)
	_ array.ExtensionArray = (*Parametric1Array)(nil)
	_ array.ExtensionArray = (*Parametric2Array)(nil)
	_ array.ExtensionArray = (*ExtStructArray)(nil)
)
