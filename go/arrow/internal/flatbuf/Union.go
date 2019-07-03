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

/// A union is a complex type with children in Field
/// By default ids in the type vector refer to the offsets in the children
/// optionally typeIds provides an indirection between the child offset and the type id
/// for each child typeIds[offset] is the id used in the type vector
type Union struct {
	_tab flatbuffers.Table
}

func GetRootAsUnion(buf []byte, offset flatbuffers.UOffsetT) *Union {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Union{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Union) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Union) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Union) Mode() UnionMode {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt16(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Union) MutateMode(n UnionMode) bool {
	return rcv._tab.MutateInt16Slot(4, n)
}

func (rcv *Union) TypeIds(j int) int32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetInt32(a + flatbuffers.UOffsetT(j*4))
	}
	return 0
}

func (rcv *Union) TypeIdsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *Union) MutateTypeIds(j int, n int32) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateInt32(a+flatbuffers.UOffsetT(j*4), n)
	}
	return false
}

func UnionStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func UnionAddMode(builder *flatbuffers.Builder, mode int16) {
	builder.PrependInt16Slot(0, mode, 0)
}
func UnionAddTypeIds(builder *flatbuffers.Builder, typeIds flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(typeIds), 0)
}
func UnionStartTypeIdsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func UnionEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
