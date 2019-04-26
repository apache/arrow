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
	col1 := func() array.Interface {
		ib := array.NewInt8Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewInt8Array()
	}()
	defer col1.Release()

	col2 := func() array.Interface {
		ib := array.NewInt16Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewInt16Array()
	}()
	defer col2.Release()

	col3 := func() array.Interface {
		ib := array.NewInt32Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewInt32Array()
	}()
	defer col3.Release()

	col4 := func() array.Interface {
		ib := array.NewInt64Builder(mem)
		defer ib.Release()

		ib.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewInt64Array()
	}()
	defer col4.Release()

	col5 := func() array.Interface {
		ib := array.NewUint8Builder(mem)
		defer ib.Release()

		ib.AppendValues([]uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewUint8Array()
	}()
	defer col5.Release()

	col6 := func() array.Interface {
		ib := array.NewUint16Builder(mem)
		defer ib.Release()

		ib.AppendValues([]uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewUint16Array()
	}()
	defer col6.Release()

	col7 := func() array.Interface {
		ib := array.NewUint32Builder(mem)
		defer ib.Release()

		ib.AppendValues([]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewUint32Array()
	}()
	defer col7.Release()

	col8 := func() array.Interface {
		ib := array.NewUint64Builder(mem)
		defer ib.Release()

		ib.AppendValues([]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return ib.NewUint64Array()
	}()
	defer col8.Release()

	col9 := func() array.Interface {
		b := array.NewFloat32Builder(mem)
		defer b.Release()

		b.AppendValues([]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return b.NewFloat32Array()
	}()
	defer col9.Release()

	col10 := func() array.Interface {
		b := array.NewFloat64Builder(mem)
		defer b.Release()

		b.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
		return b.NewFloat64Array()
	}()
	defer col10.Release()

	cols := []array.Interface{
		col1,
		col2,
		col3,
		col4,
		col5,
		col6,
		col7,
		col8,
		col9,
		col10,
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
