// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

/// Same as Utf8, but with 64-bit offsets, allowing to represent
/// extremely large data values.
type LargeUtf8 struct {
	_tab flatbuffers.Table
}

func GetRootAsLargeUtf8(buf []byte, offset flatbuffers.UOffsetT) *LargeUtf8 {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &LargeUtf8{}
	x.Init(buf, n+offset)
	return x
}

func FinishLargeUtf8Buffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsLargeUtf8(buf []byte, offset flatbuffers.UOffsetT) *LargeUtf8 {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &LargeUtf8{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedLargeUtf8Buffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *LargeUtf8) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *LargeUtf8) Table() flatbuffers.Table {
	return rcv._tab
}

func LargeUtf8Start(builder *flatbuffers.Builder) {
	builder.StartObject(0)
}
func LargeUtf8End(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
