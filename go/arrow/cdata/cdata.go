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

//go:build cgo
// +build cgo

package cdata

// implement handling of the Arrow C Data Interface. At least from a consuming side.

// #include "arrow/c/abi.h"
// #include "arrow/c/helpers.h"
// #include <stdlib.h>
// int stream_get_schema(struct ArrowArrayStream* st, struct ArrowSchema* out) { return st->get_schema(st, out); }
// int stream_get_next(struct ArrowArrayStream* st, struct ArrowArray* out) { return st->get_next(st, out); }
// const char* stream_get_last_error(struct ArrowArrayStream* st) { return st->get_last_error(st); }
// struct ArrowArray* get_arr() {
//	struct ArrowArray* out = (struct ArrowArray*)(malloc(sizeof(struct ArrowArray)));
//	memset(out, 0, sizeof(struct ArrowArray));
//	return out;
// }
// struct ArrowArrayStream* get_stream() { return (struct ArrowArrayStream*)malloc(sizeof(struct ArrowArrayStream)); }
//
import "C"

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/bitutil"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"golang.org/x/xerrors"
)

type (
	// CArrowSchema is the C Data Interface for ArrowSchemas defined in abi.h
	CArrowSchema = C.struct_ArrowSchema
	// CArrowArray is the C Data Interface object for Arrow Arrays as defined in abi.h
	CArrowArray = C.struct_ArrowArray
	// CArrowArrayStream is the C Stream Interface object for handling streams of record batches.
	CArrowArrayStream = C.struct_ArrowArrayStream
)

// Map from the defined strings to their corresponding arrow.DataType interface
// object instances, for types that don't require params.
var formatToSimpleType = map[string]arrow.DataType{
	"n":   arrow.Null,
	"b":   arrow.FixedWidthTypes.Boolean,
	"c":   arrow.PrimitiveTypes.Int8,
	"C":   arrow.PrimitiveTypes.Uint8,
	"s":   arrow.PrimitiveTypes.Int16,
	"S":   arrow.PrimitiveTypes.Uint16,
	"i":   arrow.PrimitiveTypes.Int32,
	"I":   arrow.PrimitiveTypes.Uint32,
	"l":   arrow.PrimitiveTypes.Int64,
	"L":   arrow.PrimitiveTypes.Uint64,
	"e":   arrow.FixedWidthTypes.Float16,
	"f":   arrow.PrimitiveTypes.Float32,
	"g":   arrow.PrimitiveTypes.Float64,
	"z":   arrow.BinaryTypes.Binary,
	"Z":   arrow.BinaryTypes.LargeBinary,
	"u":   arrow.BinaryTypes.String,
	"U":   arrow.BinaryTypes.LargeString,
	"tdD": arrow.FixedWidthTypes.Date32,
	"tdm": arrow.FixedWidthTypes.Date64,
	"tts": arrow.FixedWidthTypes.Time32s,
	"ttm": arrow.FixedWidthTypes.Time32ms,
	"ttu": arrow.FixedWidthTypes.Time64us,
	"ttn": arrow.FixedWidthTypes.Time64ns,
	"tDs": arrow.FixedWidthTypes.Duration_s,
	"tDm": arrow.FixedWidthTypes.Duration_ms,
	"tDu": arrow.FixedWidthTypes.Duration_us,
	"tDn": arrow.FixedWidthTypes.Duration_ns,
	"tiM": arrow.FixedWidthTypes.MonthInterval,
	"tiD": arrow.FixedWidthTypes.DayTimeInterval,
	"tin": arrow.FixedWidthTypes.MonthDayNanoInterval,
}

// decode metadata from C which is encoded as
//
//	 [int32] -> number of metadata pairs
//		for 0..n
//			[int32] -> number of bytes in key
//			[n bytes] -> key value
//			[int32] -> number of bytes in value
//			[n bytes] -> value
func decodeCMetadata(md *C.char) arrow.Metadata {
	if md == nil {
		return arrow.Metadata{}
	}

	// don't copy the bytes, just reference them directly
	const maxlen = 0x7fffffff
	data := (*[maxlen]byte)(unsafe.Pointer(md))[:]

	readint32 := func() int32 {
		v := *(*int32)(unsafe.Pointer(&data[0]))
		data = data[arrow.Int32SizeBytes:]
		return v
	}

	readstr := func() string {
		l := readint32()
		s := string(data[:l])
		data = data[l:]
		return s
	}

	npairs := readint32()
	if npairs == 0 {
		return arrow.Metadata{}
	}

	keys := make([]string, npairs)
	vals := make([]string, npairs)

	for i := int32(0); i < npairs; i++ {
		keys[i] = readstr()
		vals[i] = readstr()
	}

	return arrow.NewMetadata(keys, vals)
}

// convert a C.ArrowSchema to an arrow.Field to maintain metadata with the schema
func importSchema(schema *CArrowSchema) (ret arrow.Field, err error) {
	// always release, even on error
	defer C.ArrowSchemaRelease(schema)

	var childFields []arrow.Field
	if schema.n_children > 0 {
		// call ourselves recursively if there are children.
		var schemaChildren []*CArrowSchema
		// set up a slice to reference safely
		s := (*reflect.SliceHeader)(unsafe.Pointer(&schemaChildren))
		s.Data = uintptr(unsafe.Pointer(schema.children))
		s.Len = int(schema.n_children)
		s.Cap = int(schema.n_children)

		childFields = make([]arrow.Field, schema.n_children)
		for i, c := range schemaChildren {
			childFields[i], err = importSchema((*CArrowSchema)(c))
			if err != nil {
				return
			}
		}
	}

	// copy the schema name from the c-string
	ret.Name = C.GoString(schema.name)
	ret.Nullable = (schema.flags & C.ARROW_FLAG_NULLABLE) != 0
	ret.Metadata = decodeCMetadata(schema.metadata)

	// copies the c-string here, but it's very small
	f := C.GoString(schema.format)
	// handle our non-parameterized simple types.
	dt, ok := formatToSimpleType[f]
	if ok {
		ret.Type = dt

		if schema.dictionary != nil {
			valueField, err := importSchema(schema.dictionary)
			if err != nil {
				return ret, err
			}

			ret.Type = &arrow.DictionaryType{
				IndexType: ret.Type,
				ValueType: valueField.Type,
				Ordered:   schema.dictionary.flags&C.ARROW_FLAG_DICTIONARY_ORDERED != 0}
		}

		return
	}

	// handle types with params via colon
	typs := strings.Split(f, ":")
	defaulttz := ""
	switch typs[0] {
	case "tss":
		tz := typs[1]
		if len(typs[1]) == 0 {
			tz = defaulttz
		}
		dt = &arrow.TimestampType{Unit: arrow.Second, TimeZone: tz}
	case "tsm":
		tz := typs[1]
		if len(typs[1]) == 0 {
			tz = defaulttz
		}
		dt = &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: tz}
	case "tsu":
		tz := typs[1]
		if len(typs[1]) == 0 {
			tz = defaulttz
		}
		dt = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: tz}
	case "tsn":
		tz := typs[1]
		if len(typs[1]) == 0 {
			tz = defaulttz
		}
		dt = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: tz}
	case "w": // fixed size binary is "w:##" where ## is the byteWidth
		byteWidth, err := strconv.Atoi(typs[1])
		if err != nil {
			return ret, err
		}
		dt = &arrow.FixedSizeBinaryType{ByteWidth: byteWidth}
	case "d": // decimal types are d:<precision>,<scale>[,<bitsize>] size is assumed 128 if left out
		props := typs[1]
		propList := strings.Split(props, ",")
		bitwidth := 128
		var precision, scale int

		if len(propList) < 2 || len(propList) > 3 {
			return ret, xerrors.Errorf("invalid decimal spec '%s': wrong number of properties", f)
		} else if len(propList) == 3 {
			bitwidth, err = strconv.Atoi(propList[2])
			if err != nil {
				return ret, xerrors.Errorf("could not parse decimal bitwidth in '%s': %s", f, err.Error())
			}
		}

		precision, err = strconv.Atoi(propList[0])
		if err != nil {
			return ret, xerrors.Errorf("could not parse decimal precision in '%s': %s", f, err.Error())
		}

		scale, err = strconv.Atoi(propList[1])
		if err != nil {
			return ret, xerrors.Errorf("could not parse decimal scale in '%s': %s", f, err.Error())
		}

		if bitwidth == 128 {
			dt = &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
		} else if bitwidth == 256 {
			dt = &arrow.Decimal256Type{Precision: int32(precision), Scale: int32(scale)}
		} else {
			return ret, xerrors.Errorf("only decimal128 and decimal256 are supported, got '%s'", f)
		}
	}

	if f[0] == '+' { // types with children
		switch f[1] {
		case 'l': // list
			dt = arrow.ListOfField(childFields[0])
		case 'L': // large list
			dt = arrow.LargeListOfField(childFields[0])
		case 'w': // fixed size list is w:# where # is the list size.
			listSize, err := strconv.Atoi(strings.Split(f, ":")[1])
			if err != nil {
				return ret, err
			}

			dt = arrow.FixedSizeListOfField(int32(listSize), childFields[0])
		case 's': // struct
			dt = arrow.StructOf(childFields...)
		case 'r': // run-end encoded
			if len(childFields) != 2 {
				return ret, fmt.Errorf("%w: run-end encoded arrays must have 2 children", arrow.ErrInvalid)
			}
			dt = arrow.RunEndEncodedOf(childFields[0].Type, childFields[1].Type)
		case 'm': // map type is basically a list of structs.
			st := childFields[0].Type.(*arrow.StructType)
			dt = arrow.MapOf(st.Field(0).Type, st.Field(1).Type)
			dt.(*arrow.MapType).KeysSorted = (schema.flags & C.ARROW_FLAG_MAP_KEYS_SORTED) != 0
		case 'u': // union
			var mode arrow.UnionMode
			switch f[2] {
			case 'd':
				mode = arrow.DenseMode
			case 's':
				mode = arrow.SparseMode
			default:
				err = fmt.Errorf("%w: invalid union type", arrow.ErrInvalid)
				return
			}

			codes := strings.Split(strings.Split(f, ":")[1], ",")
			typeCodes := make([]arrow.UnionTypeCode, 0, len(codes))
			for _, i := range codes {
				v, e := strconv.ParseInt(i, 10, 8)
				if e != nil {
					err = fmt.Errorf("%w: invalid type code: %s", arrow.ErrInvalid, e)
					return
				}
				if v < 0 {
					err = fmt.Errorf("%w: negative type code in union: format string %s", arrow.ErrInvalid, f)
					return
				}
				typeCodes = append(typeCodes, arrow.UnionTypeCode(v))
			}

			if len(childFields) != len(typeCodes) {
				err = fmt.Errorf("%w: ArrowArray struct number of children incompatible with format string", arrow.ErrInvalid)
				return
			}

			dt = arrow.UnionOf(mode, childFields, typeCodes)
		}
	}

	if dt == nil {
		// if we didn't find a type, then it's something we haven't implemented.
		err = xerrors.New("unimplemented type")
	} else {
		ret.Type = dt
	}

	return
}

// importer to keep track when importing C ArrowArray objects.
type cimporter struct {
	dt       arrow.DataType
	arr      *CArrowArray
	data     arrow.ArrayData
	parent   *cimporter
	children []cimporter
	cbuffers []*C.void
}

func (imp *cimporter) importChild(parent *cimporter, src *CArrowArray) error {
	imp.parent = parent
	return imp.doImport(src)
}

// import any child arrays for lists, structs, and so on.
func (imp *cimporter) doImportChildren() error {
	var children []*CArrowArray
	// create a proper slice for our children
	s := (*reflect.SliceHeader)(unsafe.Pointer(&children))
	s.Data = uintptr(unsafe.Pointer(imp.arr.children))
	s.Len = int(imp.arr.n_children)
	s.Cap = int(imp.arr.n_children)

	if len(children) > 0 {
		imp.children = make([]cimporter, len(children))
	}

	// handle the cases
	switch imp.dt.ID() {
	case arrow.LIST: // only one child to import
		imp.children[0].dt = imp.dt.(*arrow.ListType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.LARGE_LIST: // only one child to import
		imp.children[0].dt = imp.dt.(*arrow.LargeListType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.FIXED_SIZE_LIST: // only one child to import
		imp.children[0].dt = imp.dt.(*arrow.FixedSizeListType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.STRUCT: // import all the children
		st := imp.dt.(*arrow.StructType)
		for i, c := range children {
			imp.children[i].dt = st.Field(i).Type
			imp.children[i].importChild(imp, c)
		}
	case arrow.RUN_END_ENCODED: // import run-ends and values
		st := imp.dt.(*arrow.RunEndEncodedType)
		imp.children[0].dt = st.RunEnds()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
		imp.children[1].dt = st.Encoded()
		if err := imp.children[1].importChild(imp, children[1]); err != nil {
			return err
		}
	case arrow.MAP: // only one child to import, it's a struct array
		imp.children[0].dt = imp.dt.(*arrow.MapType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.DENSE_UNION:
		dt := imp.dt.(*arrow.DenseUnionType)
		for i, c := range children {
			imp.children[i].dt = dt.Fields()[i].Type
			imp.children[i].importChild(imp, c)
		}
	case arrow.SPARSE_UNION:
		dt := imp.dt.(*arrow.SparseUnionType)
		for i, c := range children {
			imp.children[i].dt = dt.Fields()[i].Type
			imp.children[i].importChild(imp, c)
		}
	}

	return nil
}

func (imp *cimporter) initarr() {
	imp.arr = C.get_arr()
}

// import is called recursively as needed for importing an array and its children
// in order to generate array.Data objects
func (imp *cimporter) doImport(src *CArrowArray) error {
	imp.initarr()
	// move the array from the src object passed in to the one referenced by
	// this importer. That way we can set up a finalizer on the created
	// arrow.ArrayData object so we clean up our Array's memory when garbage collected.
	C.ArrowArrayMove(src, imp.arr)
	defer func(arr *CArrowArray) {
		if imp.data != nil {
			runtime.SetFinalizer(imp.data, func(arrow.ArrayData) {
				defer C.free(unsafe.Pointer(arr))
				C.ArrowArrayRelease(arr)
				if C.ArrowArrayIsReleased(arr) != 1 {
					panic("did not release C mem")
				}
			})
		} else {
			C.free(unsafe.Pointer(arr))
		}
	}(imp.arr)

	// import any children
	if err := imp.doImportChildren(); err != nil {
		return err
	}

	if imp.arr.n_buffers > 0 {
		// get a view of the buffers, zero-copy. we're just looking at the pointers
		imp.cbuffers = unsafe.Slice((**C.void)(unsafe.Pointer(imp.arr.buffers)), imp.arr.n_buffers)
	}

	// handle each of our type cases
	switch dt := imp.dt.(type) {
	case *arrow.NullType:
		if err := imp.checkNoChildren(); err != nil {
			return err
		}
		imp.data = array.NewData(dt, int(imp.arr.length), nil, nil, int(imp.arr.null_count), int(imp.arr.offset))
	case arrow.FixedWidthDataType:
		return imp.importFixedSizePrimitive()
	case *arrow.StringType:
		return imp.importStringLike(int64(arrow.Int32SizeBytes))
	case *arrow.BinaryType:
		return imp.importStringLike(int64(arrow.Int32SizeBytes))
	case *arrow.LargeStringType:
		return imp.importStringLike(int64(arrow.Int64SizeBytes))
	case *arrow.LargeBinaryType:
		return imp.importStringLike(int64(arrow.Int64SizeBytes))
	case *arrow.ListType:
		return imp.importListLike()
	case *arrow.LargeListType:
		return imp.importListLike()
	case *arrow.MapType:
		return imp.importListLike()
	case *arrow.FixedSizeListType:
		if err := imp.checkNumChildren(1); err != nil {
			return err
		}

		if err := imp.checkNumBuffers(1); err != nil {
			return err
		}

		nulls, err := imp.importNullBitmap(0)
		if err != nil {
			return err
		}

		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{nulls}, []arrow.ArrayData{imp.children[0].data}, int(imp.arr.null_count), int(imp.arr.offset))
	case *arrow.StructType:
		if err := imp.checkNumBuffers(1); err != nil {
			return err
		}

		nulls, err := imp.importNullBitmap(0)
		if err != nil {
			return err
		}

		children := make([]arrow.ArrayData, len(imp.children))
		for i := range imp.children {
			children[i] = imp.children[i].data
		}

		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{nulls}, children, int(imp.arr.null_count), int(imp.arr.offset))
	case *arrow.RunEndEncodedType:
		if err := imp.checkNumBuffers(0); err != nil {
			return err
		}

		if len(imp.children) != 2 {
			return fmt.Errorf("%w: run-end encoded array should have 2 children", arrow.ErrInvalid)
		}

		children := []arrow.ArrayData{imp.children[0].data, imp.children[1].data}
		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{}, children, int(imp.arr.null_count), int(imp.arr.offset))
	case *arrow.DenseUnionType:
		if err := imp.checkNoNulls(); err != nil {
			return err
		}

		bufs := []*memory.Buffer{nil, nil, nil}
		var err error
		if imp.arr.n_buffers == 3 {
			// legacy format exported by older arrow c++ versions
			if bufs[1], err = imp.importFixedSizeBuffer(1, 1); err != nil {
				return err
			}
			if bufs[2], err = imp.importFixedSizeBuffer(2, int64(arrow.Int32SizeBytes)); err != nil {
				return err
			}
		} else {
			if err := imp.checkNumBuffers(2); err != nil {
				return err
			}

			if bufs[1], err = imp.importFixedSizeBuffer(0, 1); err != nil {
				return err
			}
			if bufs[2], err = imp.importFixedSizeBuffer(1, int64(arrow.Int32SizeBytes)); err != nil {
				return err
			}
		}

		children := make([]arrow.ArrayData, len(imp.children))
		for i := range imp.children {
			children[i] = imp.children[i].data
		}
		imp.data = array.NewData(dt, int(imp.arr.length), bufs, children, 0, int(imp.arr.offset))
	case *arrow.SparseUnionType:
		if err := imp.checkNoNulls(); err != nil {
			return err
		}

		var buf *memory.Buffer
		var err error
		if imp.arr.n_buffers == 2 {
			// legacy format exported by older Arrow C++ versions
			if buf, err = imp.importFixedSizeBuffer(1, 1); err != nil {
				return err
			}
		} else {
			if err := imp.checkNumBuffers(1); err != nil {
				return err
			}

			if buf, err = imp.importFixedSizeBuffer(0, 1); err != nil {
				return err
			}
		}

		children := make([]arrow.ArrayData, len(imp.children))
		for i := range imp.children {
			children[i] = imp.children[i].data
		}
		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{nil, buf}, children, 0, int(imp.arr.offset))
	default:
		return fmt.Errorf("unimplemented type %s", dt)
	}

	return nil
}

func (imp *cimporter) importStringLike(offsetByteWidth int64) (err error) {
	if err = imp.checkNoChildren(); err != nil {
		return
	}

	if err = imp.checkNumBuffers(3); err != nil {
		return
	}

	var (
		nulls, offsets, values *memory.Buffer
	)

	if nulls, err = imp.importNullBitmap(0); err != nil {
		return
	}

	if offsets, err = imp.importOffsetsBuffer(1, offsetByteWidth); err != nil {
		return
	}

	var nvals int64
	switch offsetByteWidth {
	case 4:
		typedOffsets := arrow.Int32Traits.CastFromBytes(offsets.Bytes())
		nvals = int64(typedOffsets[imp.arr.offset+imp.arr.length])
	case 8:
		typedOffsets := arrow.Int64Traits.CastFromBytes(offsets.Bytes())
		nvals = typedOffsets[imp.arr.offset+imp.arr.length]
	}
	if values, err = imp.importVariableValuesBuffer(2, 1, nvals); err != nil {
		return
	}
	imp.data = array.NewData(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, offsets, values}, nil, int(imp.arr.null_count), int(imp.arr.offset))
	return
}

func (imp *cimporter) importListLike() (err error) {
	if err = imp.checkNumChildren(1); err != nil {
		return err
	}

	if err = imp.checkNumBuffers(2); err != nil {
		return err
	}

	var nulls, offsets *memory.Buffer
	if nulls, err = imp.importNullBitmap(0); err != nil {
		return
	}

	offsetSize := imp.dt.Layout().Buffers[1].ByteWidth
	if offsets, err = imp.importOffsetsBuffer(1, int64(offsetSize)); err != nil {
		return
	}

	imp.data = array.NewData(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, offsets}, []arrow.ArrayData{imp.children[0].data}, int(imp.arr.null_count), int(imp.arr.offset))
	return
}

func (imp *cimporter) importFixedSizePrimitive() error {
	if err := imp.checkNoChildren(); err != nil {
		return err
	}

	if err := imp.checkNumBuffers(2); err != nil {
		return err
	}

	nulls, err := imp.importNullBitmap(0)
	if err != nil {
		return err
	}

	var values *memory.Buffer

	fw := imp.dt.(arrow.FixedWidthDataType)
	if bitutil.IsMultipleOf8(int64(fw.BitWidth())) {
		values, err = imp.importFixedSizeBuffer(1, bitutil.BytesForBits(int64(fw.BitWidth())))
	} else {
		if fw.BitWidth() != 1 {
			return xerrors.New("invalid bitwidth")
		}
		values, err = imp.importBitsBuffer(1)
	}

	if err != nil {
		return err
	}

	var dict *array.Data
	if dt, ok := imp.dt.(*arrow.DictionaryType); ok {
		dictImp := &cimporter{dt: dt.ValueType}
		if err := dictImp.doImport(imp.arr.dictionary); err != nil {
			return err
		}
		defer dictImp.data.Release()

		dict = dictImp.data.(*array.Data)
	}

	imp.data = array.NewDataWithDictionary(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, values}, int(imp.arr.null_count), int(imp.arr.offset), dict)
	return nil
}

func (imp *cimporter) checkNoChildren() error { return imp.checkNumChildren(0) }

func (imp *cimporter) checkNoNulls() error {
	if imp.arr.null_count != 0 {
		return fmt.Errorf("%w: unexpected non-zero null count for imported type %s", arrow.ErrInvalid, imp.dt)
	}
	return nil
}

func (imp *cimporter) checkNumChildren(n int64) error {
	if int64(imp.arr.n_children) != n {
		return fmt.Errorf("expected %d children, for imported type %s, ArrowArray has %d", n, imp.dt, imp.arr.n_children)
	}
	return nil
}

func (imp *cimporter) checkNumBuffers(n int64) error {
	if int64(imp.arr.n_buffers) != n {
		return fmt.Errorf("expected %d buffers for imported type %s, ArrowArray has %d", n, imp.dt, imp.arr.n_buffers)
	}
	return nil
}

func (imp *cimporter) importBuffer(bufferID int, sz int64) (*memory.Buffer, error) {
	// this is not a copy, we're just having a slice which points at the data
	// it's still owned by the C.ArrowArray object and its backing C++ object.
	if imp.cbuffers[bufferID] == nil {
		if sz != 0 {
			return nil, errors.New("invalid buffer")
		}
		return memory.NewBufferBytes([]byte{}), nil
	}
	const maxLen = 0x7fffffff
	data := (*[maxLen]byte)(unsafe.Pointer(imp.cbuffers[bufferID]))[:sz:sz]
	return memory.NewBufferBytes(data), nil
}

func (imp *cimporter) importBitsBuffer(bufferID int) (*memory.Buffer, error) {
	bufsize := bitutil.BytesForBits(int64(imp.arr.length) + int64(imp.arr.offset))
	return imp.importBuffer(bufferID, bufsize)
}

func (imp *cimporter) importNullBitmap(bufferID int) (*memory.Buffer, error) {
	if imp.arr.null_count > 0 && imp.cbuffers[bufferID] == nil {
		return nil, fmt.Errorf("arrowarray struct has null bitmap buffer, but non-zero null_count %d", imp.arr.null_count)
	}

	if imp.arr.null_count == 0 && imp.cbuffers[bufferID] == nil {
		return nil, nil
	}

	return imp.importBitsBuffer(bufferID)
}

func (imp *cimporter) importFixedSizeBuffer(bufferID int, byteWidth int64) (*memory.Buffer, error) {
	bufsize := byteWidth * int64(imp.arr.length+imp.arr.offset)
	return imp.importBuffer(bufferID, bufsize)
}

func (imp *cimporter) importOffsetsBuffer(bufferID int, offsetsize int64) (*memory.Buffer, error) {
	bufsize := offsetsize * int64((imp.arr.length + imp.arr.offset + 1))
	return imp.importBuffer(bufferID, bufsize)
}

func (imp *cimporter) importVariableValuesBuffer(bufferID int, byteWidth, nvals int64) (*memory.Buffer, error) {
	bufsize := byteWidth * nvals
	return imp.importBuffer(bufferID, int64(bufsize))
}

func importCArrayAsType(arr *CArrowArray, dt arrow.DataType) (imp *cimporter, err error) {
	imp = &cimporter{dt: dt}
	err = imp.doImport(arr)
	return
}

func initReader(rdr *nativeCRecordBatchReader, stream *CArrowArrayStream) error {
	rdr.stream = C.get_stream()
	C.ArrowArrayStreamMove(stream, rdr.stream)
	rdr.arr = C.get_arr()
	runtime.SetFinalizer(rdr, func(r *nativeCRecordBatchReader) {
		if r.cur != nil {
			r.cur.Release()
		}
		C.ArrowArrayStreamRelease(r.stream)
		C.ArrowArrayRelease(r.arr)
		C.free(unsafe.Pointer(r.stream))
		C.free(unsafe.Pointer(r.arr))
	})

	var sc CArrowSchema
	errno := C.stream_get_schema(rdr.stream, &sc)
	if errno != 0 {
		return rdr.getError(int(errno))
	}
	defer C.ArrowSchemaRelease(&sc)
	s, err := ImportCArrowSchema((*CArrowSchema)(&sc))
	if err != nil {
		return err
	}
	rdr.schema = s

	return nil
}

// Record Batch reader that conforms to arrio.Reader for the ArrowArrayStream interface
type nativeCRecordBatchReader struct {
	stream *CArrowArrayStream
	arr    *CArrowArray
	schema *arrow.Schema

	cur arrow.Record
	err error
}

// No need to implement retain and release here as we used runtime.SetFinalizer when constructing
// the reader to free up the ArrowArrayStream memory when the garbage collector cleans it up.
func (n *nativeCRecordBatchReader) Retain()  {}
func (n *nativeCRecordBatchReader) Release() {}

func (n *nativeCRecordBatchReader) Err() error           { return n.err }
func (n *nativeCRecordBatchReader) Record() arrow.Record { return n.cur }

func (n *nativeCRecordBatchReader) Next() bool {
	err := n.next()
	switch {
	case err == nil:
		return true
	case err == io.EOF:
		return false
	}
	n.err = err
	return false
}

func (n *nativeCRecordBatchReader) next() error {
	if n.schema == nil {
		var sc CArrowSchema
		errno := C.stream_get_schema(n.stream, &sc)
		if errno != 0 {
			return n.getError(int(errno))
		}
		defer C.ArrowSchemaRelease(&sc)
		s, err := ImportCArrowSchema((*CArrowSchema)(&sc))
		if err != nil {
			return err
		}

		n.schema = s
	}

	if n.cur != nil {
		n.cur.Release()
		n.cur = nil
	}

	errno := C.stream_get_next(n.stream, n.arr)
	if errno != 0 {
		return n.getError(int(errno))
	}

	if C.ArrowArrayIsReleased(n.arr) == 1 {
		return io.EOF
	}

	rec, err := ImportCRecordBatchWithSchema(n.arr, n.schema)
	if err != nil {
		return err
	}

	n.cur = rec
	return nil
}

func (n *nativeCRecordBatchReader) Schema() *arrow.Schema {
	return n.schema
}

func (n *nativeCRecordBatchReader) getError(errno int) error {
	return fmt.Errorf("%w: %s", syscall.Errno(errno), C.GoString(C.stream_get_last_error(n.stream)))
}

func (n *nativeCRecordBatchReader) Read() (arrow.Record, error) {
	if err := n.next(); err != nil {
		n.err = err
		return nil, err
	}
	return n.cur, nil
}

func releaseArr(arr *CArrowArray) {
	C.ArrowArrayRelease(arr)
}

func releaseSchema(schema *CArrowSchema) {
	C.ArrowSchemaRelease(schema)
}
