// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Block struct {
	_tab flatbuffers.Struct
}

func (rcv *Block) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Block) Table() flatbuffers.Table {
	return rcv._tab.Table
}

/// Index to the start of the RecordBlock (note this is past the Message header)
func (rcv *Block) Offset() int64 {
	return rcv._tab.GetInt64(rcv._tab.Pos + flatbuffers.UOffsetT(0))
}
/// Index to the start of the RecordBlock (note this is past the Message header)
func (rcv *Block) MutateOffset(n int64) bool {
	return rcv._tab.MutateInt64(rcv._tab.Pos+flatbuffers.UOffsetT(0), n)
}

/// Length of the metadata
func (rcv *Block) MetaDataLength() int32 {
	return rcv._tab.GetInt32(rcv._tab.Pos + flatbuffers.UOffsetT(8))
}
/// Length of the metadata
func (rcv *Block) MutateMetaDataLength(n int32) bool {
	return rcv._tab.MutateInt32(rcv._tab.Pos+flatbuffers.UOffsetT(8), n)
}

/// Length of the data (this is aligned so there can be a gap between this and
/// the metadata).
func (rcv *Block) BodyLength() int64 {
	return rcv._tab.GetInt64(rcv._tab.Pos + flatbuffers.UOffsetT(16))
}
/// Length of the data (this is aligned so there can be a gap between this and
/// the metadata).
func (rcv *Block) MutateBodyLength(n int64) bool {
	return rcv._tab.MutateInt64(rcv._tab.Pos+flatbuffers.UOffsetT(16), n)
}

func CreateBlock(builder *flatbuffers.Builder, offset int64, metaDataLength int32, bodyLength int64) flatbuffers.UOffsetT {
	builder.Prep(8, 24)
	builder.PrependInt64(bodyLength)
	builder.Pad(4)
	builder.PrependInt32(metaDataLength)
	builder.PrependInt64(offset)
	return builder.Offset()
}
