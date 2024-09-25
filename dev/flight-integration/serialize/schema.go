package serialize

import (
	"encoding/binary"

	"github.com/apache/arrow/dev/flight-integration/protocol/message/org/apache/arrow/flatbuf"
	flatbuffers "github.com/google/flatbuffers/go"
)

type TypeTableFn func(*flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT)

type Field struct {
	Name         string
	Type         flatbuf.Type
	Nullable     bool
	Metadata     map[string]string
	GetTypeTable TypeTableFn
}

func WriteFlatbufferPayload(fields []Field) []byte {
	schema := BuildFlatbufferSchema(fields)
	size := uint32(len(schema))

	res := make([]byte, 8+size)
	res[0] = 255
	res[1] = 255
	res[2] = 255
	res[3] = 255
	binary.LittleEndian.PutUint32(res[4:], size)
	copy(res[8:], schema)

	return res
}

func BuildFlatbufferSchema(fields []Field) []byte {
	b := flatbuffers.NewBuilder(1024)

	fieldOffsets := make([]flatbuffers.UOffsetT, len(fields))
	for i, f := range fields {
		fieldOffsets[len(fields)-i-1] = BuildFlatbufferField(b, f)
	}

	flatbuf.SchemaStartFieldsVector(b, len(fields))

	for _, f := range fieldOffsets {
		b.PrependUOffsetT(f)
	}

	fieldsFB := b.EndVector(len(fields))

	flatbuf.SchemaStart(b)
	flatbuf.SchemaAddFields(b, fieldsFB)
	headerOffset := flatbuf.SchemaEnd(b)

	flatbuf.MessageStart(b)
	flatbuf.MessageAddVersion(b, flatbuf.MetadataVersionV5)
	flatbuf.MessageAddHeaderType(b, flatbuf.MessageHeaderSchema)
	flatbuf.MessageAddHeader(b, headerOffset)
	msg := flatbuf.MessageEnd(b)

	b.Finish(msg)

	return b.FinishedBytes()
}

func BuildFlatbufferField(b *flatbuffers.Builder, f Field) flatbuffers.UOffsetT {
	nameOffset := b.CreateString(f.Name)
	typOffset, childrenOffset := f.GetTypeTable(b)

	var kvOffsets []flatbuffers.UOffsetT
	for k, v := range f.Metadata {
		kk := b.CreateString(k)
		vv := b.CreateString(v)
		flatbuf.KeyValueStart(b)
		flatbuf.KeyValueAddKey(b, kk)
		flatbuf.KeyValueAddValue(b, vv)
		kvOffsets = append(kvOffsets, flatbuf.KeyValueEnd(b))
	}

	var metadataOffset flatbuffers.UOffsetT
	if len(kvOffsets) > 0 {
		flatbuf.FieldStartCustomMetadataVector(b, len(kvOffsets))
		for i := len(kvOffsets) - 1; i >= 0; i-- {
			b.PrependUOffsetT(kvOffsets[i])
		}
		metadataOffset = b.EndVector(len(kvOffsets))
	}

	flatbuf.FieldStart(b)
	flatbuf.FieldAddName(b, nameOffset)
	flatbuf.FieldAddTypeType(b, f.Type)
	flatbuf.FieldAddType(b, typOffset)
	flatbuf.FieldAddChildren(b, childrenOffset)
	flatbuf.FieldAddCustomMetadata(b, metadataOffset)
	flatbuf.FieldAddNullable(b, f.Nullable)
	return flatbuf.FieldEnd(b)
}

func ParseFlatbufferSchemaFields(msg *flatbuf.Message) ([]flatbuf.Field, bool) {
	var schema flatbuf.Schema
	table := schema.Table()
	if ok := msg.Header(&table); !ok {
		return nil, false
	}
	schema.Init(table.Bytes, table.Pos)

	fields := make([]flatbuf.Field, schema.FieldsLength())
	for i := range fields {
		var field flatbuf.Field
		if ok := schema.Fields(&field, i); !ok {
			return nil, false
		}
		fields[i] = field
	}
	return fields, true
}

func ExtractFlatbufferPayload(b []byte) []byte {
	b = consumeContinuationIndicator(b)
	b, size := consumeMetadataSize(b)
	return b[:size]
}

func Utf8TypeTable() TypeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		flatbuf.Utf8Start(b)
		return flatbuf.Utf8End(b), 0
	}
}

func BinaryTypeTable() TypeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		flatbuf.BinaryStart(b)
		return flatbuf.BinaryEnd(b), 0
	}
}

func IntTypeTable(bitWidth int32, isSigned bool) TypeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		flatbuf.IntStart(b)
		flatbuf.IntAddBitWidth(b, bitWidth)
		flatbuf.IntAddIsSigned(b, isSigned)
		return flatbuf.IntEnd(b), 0
	}
}

func BoolTypeTable() TypeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		flatbuf.BoolStart(b)
		return flatbuf.BoolEnd(b), 0
	}
}

func ListTypeTable(child Field) TypeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		childOffset := BuildFlatbufferField(b, child)

		flatbuf.ListStart(b)
		listOffset := flatbuf.ListEnd(b)

		flatbuf.FieldStartChildrenVector(b, 1)
		b.PrependUOffsetT(childOffset)
		childVecOffset := b.EndVector(1)

		return listOffset, childVecOffset
	}
}

func StructTypeTable(children []Field) TypeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		nChildren := len(children)
		childOffsets := make([]flatbuffers.UOffsetT, nChildren)
		for i, child := range children {
			childOffsets[i] = BuildFlatbufferField(b, child)
		}

		flatbuf.Struct_Start(b)
		for i := nChildren - 1; i >= 0; i-- {
			b.PrependUOffsetT(childOffsets[i])
		}
		structOffset := flatbuf.Struct_End(b)

		flatbuf.FieldStartChildrenVector(b, nChildren)
		for i := nChildren - 1; i >= 0; i-- {
			b.PrependUOffsetT(childOffsets[i])
		}
		childVecOffset := b.EndVector(nChildren)

		return structOffset, childVecOffset
	}
}

func MapTypeTable(key, val Field) TypeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		childOffset := BuildFlatbufferField(
			b,
			Field{
				Name:         "entries",
				Type:         flatbuf.TypeStruct_,
				GetTypeTable: StructTypeTable([]Field{key, val}),
			},
		)

		flatbuf.MapStart(b)
		mapOffset := flatbuf.MapEnd(b)

		flatbuf.FieldStartChildrenVector(b, 1)
		b.PrependUOffsetT(childOffset)
		childVecOffset := b.EndVector(1)

		return mapOffset, childVecOffset
	}
}

func UnionTypeTable(mode flatbuf.UnionMode, children []Field) TypeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		childOffsets := make([]flatbuffers.UOffsetT, len(children))
		for i, child := range children {
			childOffsets[i] = BuildFlatbufferField(b, child)
		}

		flatbuf.UnionStartTypeIdsVector(b, len(children))
		for i := len(children) - 1; i >= 0; i-- {
			b.PlaceInt32(int32(i))
		}
		typeIDVecOffset := b.EndVector(len(children))

		flatbuf.UnionStart(b)
		flatbuf.UnionAddMode(b, mode)
		flatbuf.UnionAddTypeIds(b, typeIDVecOffset)
		unionOffset := flatbuf.UnionEnd(b)

		flatbuf.FieldStartChildrenVector(b, len(children))
		for i := len(children) - 1; i >= 0; i-- {
			b.PrependUOffsetT(childOffsets[i])
		}
		childVecOffset := b.EndVector(len(children))

		return unionOffset, childVecOffset
	}
}

func consumeMetadataSize(b []byte) ([]byte, int32) {
	size := int32(binary.LittleEndian.Uint32(b[:4]))
	return b[4:], size
}

func consumeContinuationIndicator(b []byte) []byte {
	indicator := []byte{255, 255, 255, 255}
	for i, v := range indicator {
		if b[i] != v {
			// indicator not found
			return b
		}
	}
	// indicator found, truncate leading bytes
	return b[4:]
}
