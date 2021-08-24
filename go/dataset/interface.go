// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package dataset

// #include "arrow/c/abi.h"
// #include "arrow/c/helpers.h"
// typedef struct ArrowSchema ArrowSchema;
// typedef struct ArrowArray ArrowArray;
// typedef struct ArrowArrayStream ArrowArrayStream;
//
// int stream_get_schema(struct ArrowArrayStream* st, struct ArrowSchema* out) { return st->get_schema(st, out); }
// int stream_get_next(struct ArrowArrayStream* st, struct ArrowArray* out) { return st->get_next(st, out); }
// const char* stream_get_last_error(struct ArrowArrayStream* st) { return st->get_last_error(st); }
//
import "C"

import (
	"io"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/arrow/memory"
	"golang.org/x/xerrors"
)

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
	"u":   arrow.BinaryTypes.String,
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
}

func decodeCMetadata(md *C.char) arrow.Metadata {
	if md == nil {
		return arrow.Metadata{}
	}

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

func importSchema(schema *C.ArrowSchema) (ret arrow.Field, err error) {
	var childFields []arrow.Field
	if schema.n_children > 0 {
		var schemaChildren []*C.ArrowSchema
		s := (*reflect.SliceHeader)(unsafe.Pointer(&schemaChildren))
		s.Data = uintptr(unsafe.Pointer(schema.children))
		s.Len = int(schema.n_children)
		s.Cap = int(schema.n_children)

		childFields = make([]arrow.Field, schema.n_children)
		for i, c := range schemaChildren {
			childFields[i], err = importSchema(c)
			if err != nil {
				return
			}
		}
	}

	ret.Name = C.GoString(schema.name)
	ret.Nullable = (schema.flags & C.ARROW_FLAG_NULLABLE) != 0
	ret.Metadata = decodeCMetadata(schema.metadata)

	f := C.GoString(schema.format)
	dt, ok := formatToSimpleType[f]
	if ok {
		ret.Type = dt
		return
	}

	switch f[0] {
	case 'w':
		byteWidth, err := strconv.Atoi(strings.Split(f, ":")[1])
		if err != nil {
			return ret, err
		}
		dt = &arrow.FixedSizeBinaryType{ByteWidth: byteWidth}
	case 'd':
		props := strings.Split(f, ":")[1]
		propList := strings.Split(props, ",")
		if len(propList) == 3 {
			err = xerrors.New("only decimal128 is supported")
			return
		}

		precision, _ := strconv.Atoi(propList[0])
		scale, _ := strconv.Atoi(propList[1])
		dt = &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
	case '+':
		switch f[1] {
		case 'l':
			dt = arrow.ListOf(childFields[0].Type)
		case 'w':
			listSize, err := strconv.Atoi(strings.Split(f, ":")[1])
			if err != nil {
				return ret, err
			}

			dt = arrow.FixedSizeListOf(int32(listSize), childFields[0].Type)
		case 's':
			dt = arrow.StructOf(childFields...)
		case 'm':
			st := childFields[0].Type.(*arrow.StructType)
			dt = arrow.MapOf(st.Field(0).Type, st.Field(1).Type)
			dt.(*arrow.MapType).KeysSorted = (schema.flags & C.ARROW_FLAG_MAP_KEYS_SORTED) != 0
		}
	}

	if dt == nil {
		err = xerrors.New("unimplemented type")
	} else {
		ret.Type = dt
	}
	return
}

type cimporter struct {
	dt       arrow.DataType
	arr      C.ArrowArray
	data     *array.Data
	parent   *cimporter
	children []cimporter
	cbuffers []*C.void
}

func (imp *cimporter) importChild(parent *cimporter, src *C.ArrowArray) error {
	imp.parent = parent
	return imp.doImport(src)
}

func (imp *cimporter) doImportChildren() error {
	var children []*C.ArrowArray
	s := (*reflect.SliceHeader)(unsafe.Pointer(&children))
	s.Data = uintptr(unsafe.Pointer(imp.arr.children))
	s.Len = int(imp.arr.n_children)
	s.Cap = int(imp.arr.n_children)

	if len(children) > 0 {
		imp.children = make([]cimporter, len(children))
	}

	switch imp.dt.ID() {
	case arrow.LIST:
		imp.children[0].dt = imp.dt.(*arrow.ListType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.FIXED_SIZE_LIST:
		imp.children[0].dt = imp.dt.(*arrow.FixedSizeListType).Elem()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	case arrow.STRUCT:
		st := imp.dt.(*arrow.StructType)
		for i, c := range children {
			imp.children[i].dt = st.Field(i).Type
			imp.children[i].importChild(imp, c)
		}
	case arrow.MAP:
		imp.children[0].dt = imp.dt.(*arrow.MapType).ValueType()
		if err := imp.children[0].importChild(imp, children[0]); err != nil {
			return err
		}
	}

	return nil
}

func (imp *cimporter) doImport(src *C.ArrowArray) error {
	C.ArrowArrayMove(src, &imp.arr)
	defer func(arr *C.ArrowArray) {
		if imp.data != nil {
			runtime.SetFinalizer(imp.data, func(*array.Data) {
				C.ArrowArrayRelease(arr)
			})
		}
	}(&imp.arr)

	if err := imp.doImportChildren(); err != nil {
		return err
	}
	const maxlen = 0x7fffffff
	imp.cbuffers = (*[maxlen]*C.void)(unsafe.Pointer(imp.arr.buffers))[:imp.arr.n_buffers:imp.arr.n_buffers]

	switch dt := imp.dt.(type) {
	case *arrow.NullType:
		if err := imp.checkNoChildren(); err != nil {
			return err
		}
		imp.data = array.NewData(dt, int(imp.arr.length), nil, nil, int(imp.arr.null_count), int(imp.arr.offset))
	case arrow.FixedWidthDataType:
		return imp.importFixedSizePrimitive()
	case *arrow.StringType:
		return imp.importStringLike()
	case *arrow.BinaryType:
		return imp.importStringLike()
	case *arrow.ListType:
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

		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{nulls}, []*array.Data{imp.children[0].data}, int(imp.arr.null_count), int(imp.arr.offset))
	case *arrow.StructType:
		if err := imp.checkNumBuffers(1); err != nil {
			return err
		}

		nulls, err := imp.importNullBitmap(0)
		if err != nil {
			return err
		}

		children := make([]*array.Data, len(imp.children))
		for i := range imp.children {
			children[i] = imp.children[i].data
		}

		imp.data = array.NewData(dt, int(imp.arr.length), []*memory.Buffer{nulls}, children, int(imp.arr.null_count), int(imp.arr.offset))
	default:
		return xerrors.Errorf("unimplemented type %s", dt)
	}

	return nil
}

func (imp *cimporter) importStringLike() error {
	if err := imp.checkNoChildren(); err != nil {
		return err
	}

	if err := imp.checkNumBuffers(3); err != nil {
		return err
	}

	nulls, err := imp.importNullBitmap(0)
	if err != nil {
		return err
	}

	offsets := imp.importOffsetsBuffer(1)
	values := imp.importVariableValuesBuffer(2, 1, arrow.Int32Traits.CastFromBytes(offsets.Bytes()))
	imp.data = array.NewData(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, offsets, values}, nil, int(imp.arr.null_count), int(imp.arr.offset))
	return nil
}

func (imp *cimporter) importListLike() error {
	if err := imp.checkNumChildren(1); err != nil {
		return err
	}

	if err := imp.checkNumBuffers(2); err != nil {
		return err
	}

	nulls, err := imp.importNullBitmap(0)
	if err != nil {
		return err
	}

	offsets := imp.importOffsetsBuffer(1)
	imp.data = array.NewData(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, offsets}, []*array.Data{imp.children[0].data}, int(imp.arr.null_count), int(imp.arr.offset))
	return nil
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
		values = imp.importFixedSizeBuffer(1, bitutil.BytesForBits(int64(fw.BitWidth())))
	} else {
		if fw.BitWidth() != 1 {
			panic("invalid bitwidth")
		}
		values = imp.importBitsBuffer(1)
	}
	imp.data = array.NewData(imp.dt, int(imp.arr.length), []*memory.Buffer{nulls, values}, nil, int(imp.arr.null_count), int(imp.arr.offset))
	return nil
}

func (imp *cimporter) checkNoChildren() error { return imp.checkNumChildren(0) }

func (imp *cimporter) checkNumChildren(n int64) error {
	if int64(imp.arr.n_children) != n {
		return xerrors.Errorf("expected %d children, for imported type %s, ArrowArray has %d", n, imp.dt, imp.arr.n_children)
	}
	return nil
}

func (imp *cimporter) checkNumBuffers(n int64) error {
	if int64(imp.arr.n_buffers) != n {
		return xerrors.Errorf("expected %d buffers for imported type %s, ArrowArray has %d", n, imp.dt, imp.arr.n_buffers)
	}
	return nil
}

func (imp *cimporter) importBuffer(bufferID int, sz int64) *memory.Buffer {
	const maxLen = 0x7fffffff
	data := (*[maxLen]byte)(unsafe.Pointer(imp.cbuffers[bufferID]))[:sz:sz]
	return memory.NewBufferBytes(data)
}

func (imp *cimporter) importBitsBuffer(bufferID int) *memory.Buffer {
	bufsize := bitutil.BytesForBits(int64(imp.arr.length) + int64(imp.arr.offset))
	return imp.importBuffer(bufferID, bufsize)
}

func (imp *cimporter) importNullBitmap(bufferID int) (*memory.Buffer, error) {
	if imp.arr.null_count > 0 && imp.cbuffers[bufferID] == nil {
		return nil, xerrors.Errorf("arrowarray struct has null bitmap buffer, but non-zero null_count %d", imp.arr.null_count)
	}

	if imp.arr.null_count == 0 && imp.cbuffers[bufferID] == nil {
		return nil, nil
	}

	return imp.importBitsBuffer(bufferID), nil
}

func (imp *cimporter) importFixedSizeBuffer(bufferID int, byteWidth int64) *memory.Buffer {
	bufsize := byteWidth * int64(imp.arr.length+imp.arr.offset)
	return imp.importBuffer(bufferID, bufsize)
}

func (imp *cimporter) importOffsetsBuffer(bufferID int) *memory.Buffer {
	const offsetsize = int64(arrow.Int32SizeBytes) // go doesn't implement int64 offsets yet
	bufsize := offsetsize * int64((imp.arr.length + imp.arr.offset + 1))
	return imp.importBuffer(bufferID, bufsize)
}

func (imp *cimporter) importVariableValuesBuffer(bufferID int, byteWidth int, offsets []int32) *memory.Buffer {
	bufsize := byteWidth * int(offsets[imp.arr.length])
	return imp.importBuffer(bufferID, int64(bufsize))
}

func importCRecordBatchWithSchema(arr *C.ArrowArray, sc *arrow.Schema) (array.Record, error) {
	imp, err := importCArrayAsType(arr, arrow.StructOf(sc.Fields()...))
	if err != nil {
		return nil, err
	}

	// var ch []*C.ArrowArray
	// s := (*reflect.SliceHeader)(unsafe.Pointer(&ch))
	// s.Data = uintptr(unsafe.Pointer(imp.arr.children))
	// s.Len = int(imp.arr.n_children)
	// s.Cap = int(imp.arr.n_children)

	st := array.NewStructData(imp.data)
	defer st.Release()

	cols := make([]array.Interface, st.NumField())
	for i := 0; i < st.NumField(); i++ {
		// n := &nativeInterface{Interface: st.Field(i), refCount: 1}
		// n.Interface.Retain()
		// C.ArrowArrayMove(ch[i], &n.native)
		cols[i] = st.Field(i)
	}

	return array.NewRecord(sc, cols, int64(st.Len())), nil
}

func importCRecordBatch(arr *C.ArrowArray, sc *C.ArrowSchema) (array.Record, error) {
	field, err := importSchema(sc)
	if err != nil {
		return nil, err
	}

	if field.Type.ID() != arrow.STRUCT {
		return nil, xerrors.New("recordbatch array import must be of struct type")
	}

	return importCRecordBatchWithSchema(arr, arrow.NewSchema(field.Type.(*arrow.StructType).Fields(), &field.Metadata))
}

func importCArray(arr *C.ArrowArray, sc *C.ArrowSchema) (field arrow.Field, imp *cimporter, err error) {
	field, err = importSchema(sc)
	if err != nil {
		return
	}

	imp, err = importCArrayAsType(arr, field.Type)
	return
}

func importCArrayAsType(arr *C.ArrowArray, dt arrow.DataType) (imp *cimporter, err error) {
	imp = &cimporter{dt: dt}
	err = imp.doImport(arr)
	return
}

type nativeCRecordBatchReader struct {
	stream *C.ArrowArrayStream
	schema *arrow.Schema
}

func (n *nativeCRecordBatchReader) getError(errno int) error {
	return xerrors.Errorf("%w: %s", syscall.Errno(errno), C.GoString(C.stream_get_last_error(n.stream)))
}

func (n *nativeCRecordBatchReader) Read() (array.Record, error) {
	if n.schema == nil {
		var sc C.ArrowSchema
		errno := C.stream_get_schema(n.stream, &sc)
		if errno != 0 {
			return nil, n.getError(int(errno))
		}
		defer C.ArrowSchemaRelease(&sc)
		s, err := arrowSchemaToSchema(&sc)
		if err != nil {
			return nil, err
		}

		n.schema = s
	}

	var arr C.ArrowArray
	errno := C.stream_get_next(n.stream, &arr)
	if errno != 0 {
		return nil, n.getError(int(errno))
	}

	if C.ArrowArrayIsReleased(&arr) == 1 {
		return nil, io.EOF
	}

	return importCRecordBatchWithSchema(&arr, n.schema)
}
