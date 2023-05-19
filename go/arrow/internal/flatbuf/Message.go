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

type Message struct {
	_tab flatbuffers.Table
}

func GetRootAsMessage(buf []byte, offset flatbuffers.UOffsetT) *Message {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Message{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsMessage(buf []byte, offset flatbuffers.UOffsetT) *Message {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Message{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *Message) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Message) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Message) Version() MetadataVersion {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return MetadataVersion(rcv._tab.GetInt16(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *Message) MutateVersion(n MetadataVersion) bool {
	return rcv._tab.MutateInt16Slot(4, int16(n))
}

func (rcv *Message) HeaderType() MessageHeader {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return MessageHeader(rcv._tab.GetByte(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *Message) MutateHeaderType(n MessageHeader) bool {
	return rcv._tab.MutateByteSlot(6, byte(n))
}

func (rcv *Message) Header(obj *flatbuffers.Table) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		rcv._tab.Union(obj, o)
		return true
	}
	return false
}

func (rcv *Message) BodyLength() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Message) MutateBodyLength(n int64) bool {
	return rcv._tab.MutateInt64Slot(10, n)
}

func (rcv *Message) CustomMetadata(obj *KeyValue, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Message) CustomMetadataLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func MessageStart(builder *flatbuffers.Builder) {
	builder.StartObject(5)
}
func MessageAddVersion(builder *flatbuffers.Builder, version MetadataVersion) {
	builder.PrependInt16Slot(0, int16(version), 0)
}
func MessageAddHeaderType(builder *flatbuffers.Builder, headerType MessageHeader) {
	builder.PrependByteSlot(1, byte(headerType), 0)
}
func MessageAddHeader(builder *flatbuffers.Builder, header flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(header), 0)
}
func MessageAddBodyLength(builder *flatbuffers.Builder, bodyLength int64) {
	builder.PrependInt64Slot(3, bodyLength, 0)
}
func MessageAddCustomMetadata(builder *flatbuffers.Builder, customMetadata flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(customMetadata), 0)
}
func MessageStartCustomMetadataVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func MessageEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
