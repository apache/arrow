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

package ipc_test

import (
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func genRecord(mem memory.Allocator) array.Record {
	meta := arrow.NewMetadata(
		[]string{"k1", "k2", "k3"},
		[]string{"v1", "v2", "v3"},
	)

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "bools", Type: arrow.FixedWidthTypes.Boolean},
			arrow.Field{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
			arrow.Field{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
			arrow.Field{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
			arrow.Field{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
			arrow.Field{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
			arrow.Field{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
			arrow.Field{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
			arrow.Field{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
		},
		&meta,
	)

	mask := []bool{true, true, false, false, true, true, true, true, true, false}

	colBs := func() array.Interface {
		ib := array.NewBooleanBuilder(mem)
		defer ib.Release()

		ib.AppendValues([]bool{true, false, true, false, true, false, true, false, true, false}, mask)
		return ib.NewBooleanArray()
	}()
	defer colBs.Release()

	colI8 := func() array.Interface {
		ib := array.NewInt8Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, mask)
		return ib.NewInt8Array()
	}()
	defer colI8.Release()

	colI16 := func() array.Interface {
		ib := array.NewInt16Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, mask)
		return ib.NewInt16Array()
	}()
	defer colI16.Release()

	colI32 := func() array.Interface {
		ib := array.NewInt32Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, mask)
		return ib.NewInt32Array()
	}()
	defer colI32.Release()

	colI64 := func() array.Interface {
		ib := array.NewInt64Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, mask)
		return ib.NewInt64Array()
	}()
	defer colI64.Release()

	colU8 := func() array.Interface {
		ib := array.NewUint8Builder(mem)
		defer ib.Release()

		ib.AppendValues([]uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, mask)
		return ib.NewUint8Array()
	}()
	defer colU8.Release()

	colU16 := func() array.Interface {
		ib := array.NewUint16Builder(mem)
		defer ib.Release()

		ib.AppendValues([]uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, mask)
		return ib.NewUint16Array()
	}()
	defer colU16.Release()

	colU32 := func() array.Interface {
		ib := array.NewUint32Builder(mem)
		defer ib.Release()

		ib.AppendValues([]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, mask)
		return ib.NewUint32Array()
	}()
	defer colU32.Release()

	colU64 := func() array.Interface {
		ib := array.NewUint64Builder(mem)
		defer ib.Release()

		ib.AppendValues([]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, mask)
		return ib.NewUint64Array()
	}()
	defer colU64.Release()

	colF32 := func() array.Interface {
		b := array.NewFloat32Builder(mem)
		defer b.Release()

		b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, mask)
		return b.NewFloat32Array()
	}()
	defer colF32.Release()

	colF64 := func() array.Interface {
		b := array.NewFloat64Builder(mem)
		defer b.Release()

		b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, mask)
		return b.NewFloat64Array()
	}()
	defer colF64.Release()

	cols := []array.Interface{
		colBs,
		colI8, colI16, colI32, colI64,
		colU8, colU16, colU32, colU64,
		colF32, colF64,
	}
	return array.NewRecord(schema, cols, -1)
}

func cmpRecs(r1, r2 array.Record) bool {
	// FIXME(sbinet): impl+use arrow.Record.Equal ?

	if !r1.Schema().Equal(r2.Schema()) {
		return false
	}
	if r1.NumCols() != r2.NumCols() {
		return false
	}
	if r1.NumRows() != r2.NumRows() {
		return false
	}

	var (
		txt1 = new(strings.Builder)
		txt2 = new(strings.Builder)
	)

	printRec(txt1, r1)
	printRec(txt2, r2)

	return txt1.String() == txt2.String()
}

func printRec(w io.Writer, rec array.Record) {
	for i, col := range rec.Columns() {
		fmt.Fprintf(w, "  col[%d] %q: %v\n", i, rec.ColumnName(i), col)
	}
}
