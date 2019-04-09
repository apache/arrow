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

package ipc // import "github.com/apache/arrow/go/arrow/ipc"

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/internal/flatbuf"
	"github.com/apache/arrow/go/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
)

// Magic string identifying an Apache Arrow file.
var Magic = []byte("ARROW1")

const (
	currentMetadataVersion = MetadataV4
	minMetadataVersion     = MetadataV4

	kExtensionTypeKeyName = "arrow_extension_name"
	kExtensionDataKeyName = "arrow_extension_data"

	// ARROW-109: We set this number arbitrarily to help catch user mistakes. For
	// deeply nested schemas, it is expected the user will indicate explicitly the
	// maximum allowed recursion depth
	kMaxNestingDepth = 64
)

type fieldMetadata struct {
	Len    int64
	Nulls  int64
	Offset int64
}

type bufferMetadata struct {
	Offset int64 // relative offset into the memory page to the starting byte of the buffer
	Len    int64 // absolute length in bytes of the buffer
}

type fileBlock struct {
	Offset int64
	Meta   int32
	Body   int64

	r io.ReaderAt
}

func (blk fileBlock) NewMessage() (*Message, error) {
	var (
		err error
		buf []byte
		r   = blk.section()
	)

	buf = make([]byte, blk.Meta)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, errors.Wrap(err, "arrow/ipc: could not read message metadata")
	}
	meta := memory.NewBufferBytes(buf[4:]) // drop buf-size already known from blk.Meta

	buf = make([]byte, blk.Body)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, errors.Wrap(err, "arrow/ipc: could not read message body")
	}
	body := memory.NewBufferBytes(buf)

	return NewMessage(meta, body), nil
}

func (blk fileBlock) section() io.Reader {
	return io.NewSectionReader(blk.r, blk.Offset, int64(blk.Meta)+blk.Body)
}

func nullableFromFB(v byte) bool {
	return v != 0
}

// initFB is a helper function to handle flatbuffers' polymorphism.
func initFB(t interface {
	Table() flatbuffers.Table
	Init([]byte, flatbuffers.UOffsetT)
}, f func(tbl *flatbuffers.Table) bool) {
	tbl := t.Table()
	if !f(&tbl) {
		panic(errors.Errorf("arrow/ipc: could not initialize %T from flatbuffer", t))
	}
	t.Init(tbl.Bytes, tbl.Pos)
}

func fieldFromFB(field *flatbuf.Field, memo *dictMemo) (arrow.Field, error) {
	var (
		err error
		o   arrow.Field
	)

	o.Name = string(field.Name())
	o.Nullable = nullableFromFB(field.Nullable())
	o.Metadata, err = metadataFrom(field)
	if err != nil {
		return o, err
	}

	encoding := field.Dictionary(nil)
	switch encoding {
	case nil:
		n := field.ChildrenLength()
		children := make([]arrow.Field, n)
		for i := range children {
			var childFB flatbuf.Field
			if !field.Children(&childFB, i) {
				return o, errors.Errorf("arrow/ipc: could not load field child %d", i)
			}
			child, err := fieldFromFB(&childFB, memo)
			if err != nil {
				return o, errors.Wrapf(err, "arrow/ipc: could not convert field child %d", i)
			}
			children[i] = child
		}

		o.Type, err = typeFromFB(field, children, o.Metadata)
		if err != nil {
			return o, errors.Wrapf(err, "arrow/ipc: could not convert field type")
		}
	default:
		//		log.Printf("encoding: %v", encoding.Id())
		//		n := field.ChildrenLength()
		//		log.Printf("children: %v", n)
		panic("not implemented") // FIXME(sbinet)
	}

	return o, nil
}

func fieldFromFBDict(field *flatbuf.Field) (arrow.Field, error) {
	var (
		o = arrow.Field{
			Name:     string(field.Name()),
			Nullable: nullableFromFB(field.Nullable()),
		}
		err  error
		memo = newMemo()
	)

	// any DictionaryEncoding set is ignored here.

	kids := make([]arrow.Field, field.ChildrenLength())
	for i := range kids {
		var kid flatbuf.Field
		if !field.Children(&kid, i) {
			return o, errors.Errorf("arrow/ipc: could not load field child %d", i)
		}
		kids[i], err = fieldFromFB(&kid, &memo)
		if err != nil {
			return o, errors.Wrap(err, "arrow/ipc: field from dict")
		}
	}

	meta, err := metadataFrom(field)
	if err != nil {
		return o, errors.Wrap(err, "arrow/ipc: metadata for field from dict")
	}

	o.Type, err = typeFromFB(field, kids, meta)
	if err != nil {
		return o, errors.Wrap(err, "arrow/ipc: type for field from dict")
	}

	return o, nil
}

func typeFromFB(field *flatbuf.Field, children []arrow.Field, md arrow.Metadata) (arrow.DataType, error) {
	var data flatbuffers.Table
	if !field.Type(&data) {
		return nil, errors.Errorf("arrow/ipc: could not load field type data")
	}

	dt, err := concreteTypeFromFB(field.TypeType(), data, children)
	if err != nil {
		return dt, err
	}

	// look for extension metadata in custom metadata field.
	if md.Len() > 0 {
		i := md.FindKey(kExtensionTypeKeyName)
		if i < 0 {
			return dt, err
		}

		panic("not implemented") // FIXME(sbinet)
	}

	return dt, err
}

func concreteTypeFromFB(typ flatbuf.Type, data flatbuffers.Table, children []arrow.Field) (arrow.DataType, error) {
	var (
		dt  arrow.DataType
		err error
	)

	switch typ {
	case flatbuf.TypeNONE:
		return nil, errors.Errorf("arrow/ipc: Type metadata cannot be none")

	case flatbuf.TypeNull:
		return arrow.Null, nil

	case flatbuf.TypeInt:
		var dt flatbuf.Int
		dt.Init(data.Bytes, data.Pos)
		return intFromFB(dt)

	case flatbuf.TypeFloatingPoint:
		var dt flatbuf.FloatingPoint
		dt.Init(data.Bytes, data.Pos)
		return floatFromFB(dt)

	case flatbuf.TypeBinary:
		return arrow.BinaryTypes.Binary, nil

	case flatbuf.TypeFixedSizeBinary:
		var dt flatbuf.FixedSizeBinary
		dt.Init(data.Bytes, data.Pos)
		return &arrow.FixedSizeBinaryType{ByteWidth: int(dt.ByteWidth())}, nil

	case flatbuf.TypeUtf8:
		return arrow.BinaryTypes.String, nil

	case flatbuf.TypeBool:
		return arrow.FixedWidthTypes.Boolean, nil

	case flatbuf.TypeList:
		if len(children) != 1 {
			return nil, errors.Errorf("arrow/ipc: List must have exactly 1 child field (got=%d)", len(children))
		}
		return arrow.ListOf(children[0].Type), nil

	case flatbuf.TypeStruct_:
		return arrow.StructOf(children...), nil

	default:
		// FIXME(sbinet): implement all the other types.
		panic(fmt.Errorf("arrow/ipc: type %v not implemented", flatbuf.EnumNamesType[typ]))
	}

	return dt, err
}

func intFromFB(data flatbuf.Int) (arrow.DataType, error) {
	bw := data.BitWidth()
	if bw > 64 {
		return nil, errors.Errorf("arrow/ipc: integers with more than 64 bits not implemented (bits=%d)", bw)
	}
	if bw < 8 {
		return nil, errors.Errorf("arrow/ipc: integers with less than 8 bits not implemented (bits=%d)", bw)
	}

	switch bw {
	case 8:
		switch data.IsSigned() {
		case 0:
			return arrow.PrimitiveTypes.Uint8, nil
		default:
			return arrow.PrimitiveTypes.Int8, nil
		}

	case 16:
		switch data.IsSigned() {
		case 0:
			return arrow.PrimitiveTypes.Uint16, nil
		default:
			return arrow.PrimitiveTypes.Int16, nil
		}

	case 32:
		switch data.IsSigned() {
		case 0:
			return arrow.PrimitiveTypes.Uint32, nil
		default:
			return arrow.PrimitiveTypes.Int32, nil
		}

	case 64:
		switch data.IsSigned() {
		case 0:
			return arrow.PrimitiveTypes.Uint64, nil
		default:
			return arrow.PrimitiveTypes.Int64, nil
		}
	default:
		return nil, errors.Errorf("arrow/ipc: integers not in cstdint are not implemented")
	}
}

func floatFromFB(data flatbuf.FloatingPoint) (arrow.DataType, error) {
	switch p := data.Precision(); p {
	case flatbuf.PrecisionHALF:
		return nil, errors.Errorf("arrow/ipc: float16 not implemented")
	case flatbuf.PrecisionSINGLE:
		return arrow.PrimitiveTypes.Float32, nil
	case flatbuf.PrecisionDOUBLE:
		return arrow.PrimitiveTypes.Float64, nil
	default:
		return nil, errors.Errorf("arrow/ipc: floating point type with %d precision not implemented", p)
	}
}

type customMetadataer interface {
	CustomMetadataLength() int
	CustomMetadata(*flatbuf.KeyValue, int) bool
}

func metadataFrom(md customMetadataer) (arrow.Metadata, error) {
	var (
		keys = make([]string, md.CustomMetadataLength())
		vals = make([]string, md.CustomMetadataLength())
	)

	for i := range keys {
		var kv flatbuf.KeyValue
		if !md.CustomMetadata(&kv, i) {
			return arrow.Metadata{}, errors.Errorf("arrow/ipc: could not read key-value %d from flatbuffer", i)
		}
		keys[i] = string(kv.Key())
		vals[i] = string(kv.Value())
	}

	return arrow.NewMetadata(keys, vals), nil
}

func schemaFromFB(schema *flatbuf.Schema, memo *dictMemo) (*arrow.Schema, error) {
	var (
		err    error
		fields = make([]arrow.Field, schema.FieldsLength())
	)

	for i := range fields {
		var field flatbuf.Field
		if !schema.Fields(&field, i) {
			return nil, errors.Errorf("arrow/ipc: could not read field %d from schema", i)
		}

		fields[i], err = fieldFromFB(&field, memo)
		if err != nil {
			return nil, errors.Wrapf(err, "arrow/ipc: could not convert field %d from flatbuf", i)
		}
	}

	md, err := metadataFrom(schema)
	if err != nil {
		return nil, errors.Wrapf(err, "arrow/ipc: could not convert schema metadata from flatbuf")
	}

	return arrow.NewSchema(fields, &md), nil
}

func dictTypesFromFB(schema *flatbuf.Schema) (dictTypeMap, error) {
	var (
		err    error
		fields = make(dictTypeMap, schema.FieldsLength())
	)
	for i := 0; i < schema.FieldsLength(); i++ {
		var field flatbuf.Field
		if !schema.Fields(&field, i) {
			return nil, errors.Errorf("arrow/ipc: could not load field %d from schema", i)
		}
		fields, err = visitField(&field, fields)
		if err != nil {
			return nil, errors.Wrapf(err, "arrow/ipc: could not visit field %d from schema", i)
		}
	}
	return fields, err
}

func visitField(field *flatbuf.Field, dict dictTypeMap) (dictTypeMap, error) {
	var err error
	meta := field.Dictionary(nil)
	switch meta {
	case nil:
		// field is not dictionary encoded.
		// => visit children.
		for i := 0; i < field.ChildrenLength(); i++ {
			var child flatbuf.Field
			if !field.Children(&child, i) {
				return nil, errors.Errorf("arrow/ipc: could not visit child %d from field", i)
			}
			dict, err = visitField(&child, dict)
			if err != nil {
				return nil, err
			}
		}
	default:
		// field is dictionary encoded.
		// construct the data type for the dictionary: no descendants can be dict-encoded.
		dfield, err := fieldFromFBDict(field)
		if err != nil {
			return nil, errors.Wrap(err, "arrow/ipc: could not create data type for dictionary")
		}
		dict[meta.Id()] = dfield
	}
	return dict, err
}
