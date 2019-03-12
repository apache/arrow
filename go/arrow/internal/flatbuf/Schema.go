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

// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

/// ----------------------------------------------------------------------
/// A Schema describes the columns in a row batch
type Schema struct {
	_tab flatbuffers.Table
}

func GetRootAsSchema(buf []byte, offset flatbuffers.UOffsetT) *Schema {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Schema{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Schema) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Schema) Table() flatbuffers.Table {
	return rcv._tab
}

/// endianness of the buffer
/// it is Little Endian by default
/// if endianness doesn't match the underlying system then the vectors need to be converted
func (rcv *Schema) Endianness() int16 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt16(o + rcv._tab.Pos)
	}
	return 0
}

/// endianness of the buffer
/// it is Little Endian by default
/// if endianness doesn't match the underlying system then the vectors need to be converted
func (rcv *Schema) MutateEndianness(n int16) bool {
	return rcv._tab.MutateInt16Slot(4, n)
}

func (rcv *Schema) Fields(obj *Field, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Schema) FieldsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *Schema) CustomMetadata(obj *KeyValue, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Schema) CustomMetadataLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func SchemaStart(builder *flatbuffers.Builder) {
	builder.StartObject(3)
}
func SchemaAddEndianness(builder *flatbuffers.Builder, endianness int16) {
	builder.PrependInt16Slot(0, endianness, 0)
}
func SchemaAddFields(builder *flatbuffers.Builder, fields flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(fields), 0)
}
func SchemaStartFieldsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func SchemaAddCustomMetadata(builder *flatbuffers.Builder, customMetadata flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(customMetadata), 0)
}
func SchemaStartCustomMetadataVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func SchemaEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
