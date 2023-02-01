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

package cdata

// #include <errno.h>
// #include <stdint.h>
// #include <stdlib.h>
// #include "arrow/c/abi.h"
// #include "arrow/c/helpers.h"
//
// extern void releaseExportedSchema(struct ArrowSchema* schema);
// extern void releaseExportedArray(struct ArrowArray* array);
//
// const uint8_t kGoCdataZeroRegion[8] = {0};
//
// void goReleaseArray(struct ArrowArray* array) {
//	releaseExportedArray(array);
// }
// void goReleaseSchema(struct ArrowSchema* schema) {
//	 releaseExportedSchema(schema);
// }
import "C"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"runtime/cgo"
	"strconv"
	"strings"
	"unsafe"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/endian"
	"github.com/apache/arrow/go/v12/arrow/internal"
	"github.com/apache/arrow/go/v12/arrow/ipc"
)

func encodeCMetadata(keys, values []string) []byte {
	if len(keys) != len(values) {
		panic("unequal metadata key/values length")
	}
	npairs := int32(len(keys))

	var b bytes.Buffer
	totalSize := 4
	for i := range keys {
		totalSize += 8 + len(keys[i]) + len(values[i])
	}
	b.Grow(totalSize)

	b.Write((*[4]byte)(unsafe.Pointer(&npairs))[:])
	for i := range keys {
		binary.Write(&b, endian.Native, int32(len(keys[i])))
		b.WriteString(keys[i])
		binary.Write(&b, endian.Native, int32(len(values[i])))
		b.WriteString(values[i])
	}
	return b.Bytes()
}

type schemaExporter struct {
	format, name string

	extraMeta arrow.Metadata
	metadata  []byte
	flags     int64
	children  []schemaExporter
	dict      *schemaExporter
}

func (exp *schemaExporter) handleExtension(dt arrow.DataType) arrow.DataType {
	if dt.ID() != arrow.EXTENSION {
		return dt
	}

	ext := dt.(arrow.ExtensionType)
	exp.extraMeta = arrow.NewMetadata([]string{ipc.ExtensionTypeKeyName, ipc.ExtensionMetadataKeyName}, []string{ext.ExtensionName(), ext.Serialize()})
	return ext.StorageType()
}

func (exp *schemaExporter) exportMeta(m *arrow.Metadata) {
	var (
		finalKeys   []string
		finalValues []string
	)

	if m == nil {
		if exp.extraMeta.Len() > 0 {
			finalKeys = exp.extraMeta.Keys()
			finalValues = exp.extraMeta.Values()
		}
		exp.metadata = encodeCMetadata(finalKeys, finalValues)
		return
	}

	finalKeys = m.Keys()
	finalValues = m.Values()

	if exp.extraMeta.Len() > 0 {
		for i, k := range exp.extraMeta.Keys() {
			if m.FindKey(k) != -1 {
				continue
			}
			finalKeys = append(finalKeys, k)
			finalValues = append(finalValues, exp.extraMeta.Values()[i])
		}
	}
	exp.metadata = encodeCMetadata(finalKeys, finalValues)
}

func (exp *schemaExporter) exportFormat(dt arrow.DataType) string {
	switch dt := dt.(type) {
	case *arrow.NullType:
		return "n"
	case *arrow.BooleanType:
		return "b"
	case *arrow.Int8Type:
		return "c"
	case *arrow.Uint8Type:
		return "C"
	case *arrow.Int16Type:
		return "s"
	case *arrow.Uint16Type:
		return "S"
	case *arrow.Int32Type:
		return "i"
	case *arrow.Uint32Type:
		return "I"
	case *arrow.Int64Type:
		return "l"
	case *arrow.Uint64Type:
		return "L"
	case *arrow.Float16Type:
		return "e"
	case *arrow.Float32Type:
		return "f"
	case *arrow.Float64Type:
		return "g"
	case *arrow.FixedSizeBinaryType:
		return fmt.Sprintf("w:%d", dt.ByteWidth)
	case *arrow.Decimal128Type:
		return fmt.Sprintf("d:%d,%d", dt.Precision, dt.Scale)
	case *arrow.Decimal256Type:
		return fmt.Sprintf("d:%d,%d,256", dt.Precision, dt.Scale)
	case *arrow.BinaryType:
		return "z"
	case *arrow.LargeBinaryType:
		return "Z"
	case *arrow.StringType:
		return "u"
	case *arrow.LargeStringType:
		return "U"
	case *arrow.Date32Type:
		return "tdD"
	case *arrow.Date64Type:
		return "tdm"
	case *arrow.Time32Type:
		switch dt.Unit {
		case arrow.Second:
			return "tts"
		case arrow.Millisecond:
			return "ttm"
		default:
			panic(fmt.Sprintf("invalid time unit for time32: %s", dt.Unit))
		}
	case *arrow.Time64Type:
		switch dt.Unit {
		case arrow.Microsecond:
			return "ttu"
		case arrow.Nanosecond:
			return "ttn"
		default:
			panic(fmt.Sprintf("invalid time unit for time64: %s", dt.Unit))
		}
	case *arrow.TimestampType:
		var b strings.Builder
		switch dt.Unit {
		case arrow.Second:
			b.WriteString("tss:")
		case arrow.Millisecond:
			b.WriteString("tsm:")
		case arrow.Microsecond:
			b.WriteString("tsu:")
		case arrow.Nanosecond:
			b.WriteString("tsn:")
		default:
			panic(fmt.Sprintf("invalid time unit for timestamp: %s", dt.Unit))
		}
		b.WriteString(dt.TimeZone)
		return b.String()
	case *arrow.DurationType:
		switch dt.Unit {
		case arrow.Second:
			return "tDs"
		case arrow.Millisecond:
			return "tDm"
		case arrow.Microsecond:
			return "tDu"
		case arrow.Nanosecond:
			return "tDn"
		default:
			panic(fmt.Sprintf("invalid time unit for duration: %s", dt.Unit))
		}
	case *arrow.MonthIntervalType:
		return "tiM"
	case *arrow.DayTimeIntervalType:
		return "tiD"
	case *arrow.MonthDayNanoIntervalType:
		return "tin"
	case *arrow.ListType:
		return "+l"
	case *arrow.LargeListType:
		return "+L"
	case *arrow.FixedSizeListType:
		return fmt.Sprintf("+w:%d", dt.Len())
	case *arrow.StructType:
		return "+s"
	case *arrow.MapType:
		if dt.KeysSorted {
			exp.flags |= C.ARROW_FLAG_MAP_KEYS_SORTED
		}
		return "+m"
	case *arrow.DictionaryType:
		if dt.Ordered {
			exp.flags |= C.ARROW_FLAG_DICTIONARY_ORDERED
		}
		return exp.exportFormat(dt.IndexType)
	case arrow.UnionType:
		var b strings.Builder
		if dt.Mode() == arrow.SparseMode {
			b.WriteString("+us:")
		} else {
			b.WriteString("+ud:")
		}
		for i, c := range dt.TypeCodes() {
			if i != 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Itoa(int(c)))
		}
		return b.String()
	}
	panic("unsupported data type for export")
}

func (exp *schemaExporter) export(field arrow.Field) {
	exp.name = field.Name
	exp.format = exp.exportFormat(exp.handleExtension(field.Type))
	if field.Nullable {
		exp.flags |= C.ARROW_FLAG_NULLABLE
	}

	switch dt := field.Type.(type) {
	case *arrow.DictionaryType:
		exp.dict = new(schemaExporter)
		exp.dict.export(arrow.Field{Type: dt.ValueType})
	case arrow.NestedType:
		exp.children = make([]schemaExporter, len(dt.Fields()))
		for i, f := range dt.Fields() {
			exp.children[i].export(f)
		}
	}

	exp.exportMeta(&field.Metadata)
}

func allocateArrowSchemaArr(n int) (out []CArrowSchema) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	s.Data = uintptr(C.malloc(C.sizeof_struct_ArrowSchema * C.size_t(n)))
	s.Len = n
	s.Cap = n

	return
}

func allocateArrowSchemaPtrArr(n int) (out []*CArrowSchema) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	s.Data = uintptr(C.malloc(C.size_t(unsafe.Sizeof((*CArrowSchema)(nil))) * C.size_t(n)))
	s.Len = n
	s.Cap = n

	return
}

func allocateArrowArrayArr(n int) (out []CArrowArray) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	s.Data = uintptr(C.malloc(C.sizeof_struct_ArrowArray * C.size_t(n)))
	s.Len = n
	s.Cap = n

	return
}

func allocateArrowArrayPtrArr(n int) (out []*CArrowArray) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	s.Data = uintptr(C.malloc(C.size_t(unsafe.Sizeof((*CArrowArray)(nil))) * C.size_t(n)))
	s.Len = n
	s.Cap = n

	return
}

func allocateBufferPtrArr(n int) (out []*C.void) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	s.Data = uintptr(C.malloc(C.size_t(unsafe.Sizeof((*C.void)(nil))) * C.size_t(n)))
	s.Len = n
	s.Cap = n

	return
}

func (exp *schemaExporter) finish(out *CArrowSchema) {
	out.dictionary = nil
	if exp.dict != nil {
		out.dictionary = (*CArrowSchema)(C.malloc(C.sizeof_struct_ArrowSchema))
		exp.dict.finish(out.dictionary)
	}
	out.name = C.CString(exp.name)
	out.format = C.CString(exp.format)
	out.metadata = (*C.char)(C.CBytes(exp.metadata))
	out.flags = C.int64_t(exp.flags)
	out.n_children = C.int64_t(len(exp.children))

	if len(exp.children) > 0 {
		children := allocateArrowSchemaArr(len(exp.children))
		childPtrs := allocateArrowSchemaPtrArr(len(exp.children))

		for i, c := range exp.children {
			c.finish(&children[i])
			childPtrs[i] = &children[i]
		}

		out.children = (**CArrowSchema)(unsafe.Pointer(&childPtrs[0]))
	} else {
		out.children = nil
	}

	out.release = (*[0]byte)(C.goReleaseSchema)
}

func exportField(field arrow.Field, out *CArrowSchema) {
	var exp schemaExporter
	exp.export(field)
	exp.finish(out)
}

func exportArray(arr arrow.Array, out *CArrowArray, outSchema *CArrowSchema) {
	if outSchema != nil {
		exportField(arrow.Field{Type: arr.DataType()}, outSchema)
	}

	out.dictionary = nil
	out.null_count = C.int64_t(arr.NullN())
	out.length = C.int64_t(arr.Len())
	out.offset = C.int64_t(arr.Data().Offset())
	out.n_buffers = C.int64_t(len(arr.Data().Buffers()))

	if out.n_buffers > 0 {
		var (
			nbuffers = len(arr.Data().Buffers())
			bufs     = arr.Data().Buffers()
		)
		// unions don't have validity bitmaps, but we keep them shifted
		// to make processing easier in other contexts. This means that
		// we have to adjust for union arrays
		if !internal.DefaultHasValidityBitmap(arr.DataType().ID()) {
			out.n_buffers--
			nbuffers--
			bufs = bufs[1:]
		}
		buffers := allocateBufferPtrArr(nbuffers)
		for i := range bufs {
			buf := bufs[i]
			if buf == nil || buf.Len() == 0 {
				if i > 0 || !internal.DefaultHasValidityBitmap(arr.DataType().ID()) {
					// apache/arrow#33936: export a dummy buffer to be friendly to
					// implementations that don't import NULL properly
					buffers[i] = (*C.void)(unsafe.Pointer(&C.kGoCdataZeroRegion))
				} else {
					buffers[i] = nil
				}
				continue
			}

			buffers[i] = (*C.void)(unsafe.Pointer(&buf.Bytes()[0]))
		}
		out.buffers = (*unsafe.Pointer)(unsafe.Pointer(&buffers[0]))
	}

	arr.Data().Retain()
	h := cgo.NewHandle(arr.Data())
	out.private_data = createHandle(h)
	out.release = (*[0]byte)(C.goReleaseArray)
	switch arr := arr.(type) {
	case array.ListLike:
		out.n_children = 1
		childPtrs := allocateArrowArrayPtrArr(1)
		children := allocateArrowArrayArr(1)
		exportArray(arr.ListValues(), &children[0], nil)
		childPtrs[0] = &children[0]
		out.children = (**CArrowArray)(unsafe.Pointer(&childPtrs[0]))
	case *array.FixedSizeList:
		out.n_children = 1
		childPtrs := allocateArrowArrayPtrArr(1)
		children := allocateArrowArrayArr(1)
		exportArray(arr.ListValues(), &children[0], nil)
		childPtrs[0] = &children[0]
		out.children = (**CArrowArray)(unsafe.Pointer(&childPtrs[0]))
	case *array.Struct:
		out.n_children = C.int64_t(arr.NumField())
		childPtrs := allocateArrowArrayPtrArr(arr.NumField())
		children := allocateArrowArrayArr(arr.NumField())
		for i := 0; i < arr.NumField(); i++ {
			exportArray(arr.Field(i), &children[i], nil)
			childPtrs[i] = &children[i]
		}
		out.children = (**CArrowArray)(unsafe.Pointer(&childPtrs[0]))
	case *array.Dictionary:
		out.dictionary = (*CArrowArray)(C.malloc(C.sizeof_struct_ArrowArray))
		exportArray(arr.Dictionary(), out.dictionary, nil)
	case array.Union:
		out.n_children = C.int64_t(arr.NumFields())
		childPtrs := allocateArrowArrayPtrArr(arr.NumFields())
		children := allocateArrowArrayArr(arr.NumFields())
		for i := 0; i < arr.NumFields(); i++ {
			exportArray(arr.Field(i), &children[i], nil)
			childPtrs[i] = &children[i]
		}
		out.children = (**CArrowArray)(unsafe.Pointer(&childPtrs[0]))
	default:
		out.n_children = 0
		out.children = nil
	}
}

type cRecordReader struct {
	rdr array.RecordReader
	err *C.char
}

func (rr cRecordReader) getSchema(out *CArrowSchema) int {
	schema := rr.rdr.Schema()
	if schema == nil {
		return rr.maybeError()
	}
	ExportArrowSchema(schema, out)
	return 0
}

func (rr cRecordReader) next(out *CArrowArray) int {
	if rr.rdr.Next() {
		ExportArrowRecordBatch(rr.rdr.Record(), out, nil)
		return 0
	}
	C.ArrowArrayMarkReleased(out)
	return rr.maybeError()
}

func (rr cRecordReader) maybeError() int {
	err := rr.rdr.Err()
	if err != nil {
		return C.EIO
	}
	return 0
}

func (rr cRecordReader) getLastError() *C.char {
	err := rr.rdr.Err()
	if err != nil {
		if rr.err != nil {
			C.free(unsafe.Pointer(rr.err))
		}
		rr.err = C.CString(err.Error())
	}
	return rr.err
}

func (rr cRecordReader) release() {
	if rr.err != nil {
		C.free(unsafe.Pointer(rr.err))
	}
	rr.rdr.Release()
}
