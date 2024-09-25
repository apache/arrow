// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

/// ----------------------------------------------------------------------
/// A field represents a named column in a record / row batch or child of a
/// nested type.
type Field struct {
	_tab flatbuffers.Table
}

func GetRootAsField(buf []byte, offset flatbuffers.UOffsetT) *Field {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Field{}
	x.Init(buf, n+offset)
	return x
}

func FinishFieldBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsField(buf []byte, offset flatbuffers.UOffsetT) *Field {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Field{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedFieldBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *Field) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Field) Table() flatbuffers.Table {
	return rcv._tab
}

/// Name is not required, in i.e. a List
func (rcv *Field) Name() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

/// Name is not required, in i.e. a List
/// Whether or not this field can contain nulls. Should be true in general.
func (rcv *Field) Nullable() bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetBool(o + rcv._tab.Pos)
	}
	return false
}

/// Whether or not this field can contain nulls. Should be true in general.
func (rcv *Field) MutateNullable(n bool) bool {
	return rcv._tab.MutateBoolSlot(6, n)
}

func (rcv *Field) TypeType() Type {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return Type(rcv._tab.GetByte(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *Field) MutateTypeType(n Type) bool {
	return rcv._tab.MutateByteSlot(8, byte(n))
}

/// This is the type of the decoded value if the field is dictionary encoded.
func (rcv *Field) Type(obj *flatbuffers.Table) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		rcv._tab.Union(obj, o)
		return true
	}
	return false
}

/// This is the type of the decoded value if the field is dictionary encoded.
/// Present only if the field is dictionary encoded.
func (rcv *Field) Dictionary(obj *DictionaryEncoding) *DictionaryEncoding {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(DictionaryEncoding)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

/// Present only if the field is dictionary encoded.
/// children apply only to nested data types like Struct, List and Union. For
/// primitive types children will have length 0.
func (rcv *Field) Children(obj *Field, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Field) ChildrenLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

/// children apply only to nested data types like Struct, List and Union. For
/// primitive types children will have length 0.
/// User-defined metadata
func (rcv *Field) CustomMetadata(obj *KeyValue, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Field) CustomMetadataLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

/// User-defined metadata
func FieldStart(builder *flatbuffers.Builder) {
	builder.StartObject(7)
}
func FieldAddName(builder *flatbuffers.Builder, name flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(name), 0)
}
func FieldAddNullable(builder *flatbuffers.Builder, nullable bool) {
	builder.PrependBoolSlot(1, nullable, false)
}
func FieldAddTypeType(builder *flatbuffers.Builder, typeType Type) {
	builder.PrependByteSlot(2, byte(typeType), 0)
}
func FieldAddType(builder *flatbuffers.Builder, type_ flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(type_), 0)
}
func FieldAddDictionary(builder *flatbuffers.Builder, dictionary flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(dictionary), 0)
}
func FieldAddChildren(builder *flatbuffers.Builder, children flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(5, flatbuffers.UOffsetT(children), 0)
}
func FieldStartChildrenVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func FieldAddCustomMetadata(builder *flatbuffers.Builder, customMetadata flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(6, flatbuffers.UOffsetT(customMetadata), 0)
}
func FieldStartCustomMetadataVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func FieldEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
