// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Duration struct {
	_tab flatbuffers.Table
}

func GetRootAsDuration(buf []byte, offset flatbuffers.UOffsetT) *Duration {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Duration{}
	x.Init(buf, n+offset)
	return x
}

func FinishDurationBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsDuration(buf []byte, offset flatbuffers.UOffsetT) *Duration {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Duration{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedDurationBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *Duration) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Duration) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Duration) Unit() TimeUnit {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return TimeUnit(rcv._tab.GetInt16(o + rcv._tab.Pos))
	}
	return 1
}

func (rcv *Duration) MutateUnit(n TimeUnit) bool {
	return rcv._tab.MutateInt16Slot(4, int16(n))
}

func DurationStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func DurationAddUnit(builder *flatbuffers.Builder, unit TimeUnit) {
	builder.PrependInt16Slot(0, int16(unit), 1)
}
func DurationEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
