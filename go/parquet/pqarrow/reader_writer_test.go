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

package pqarrow_test

import (
	"bytes"
	"context"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

const alternateOrNA = -1
const SIZELEN = 1024 * 1024

func randomUint8(size, truePct int, sampleVals [2]uint8, seed uint64) []uint8 {
	ret := make([]uint8, size)
	if truePct == alternateOrNA {
		for idx := range ret {
			ret[idx] = uint8(idx % 2)
		}
		return ret
	}

	dist := distuv.Bernoulli{
		P:   float64(truePct) / 100.0,
		Src: rand.NewSource(seed),
	}

	for idx := range ret {
		ret[idx] = sampleVals[int(dist.Rand())]
	}
	return ret
}

func randomInt32(size, truePct int, sampleVals [2]int32, seed uint64) []int32 {
	ret := make([]int32, size)
	if truePct == alternateOrNA {
		for idx := range ret {
			ret[idx] = int32(idx % 2)
		}
		return ret
	}

	dist := distuv.Bernoulli{
		P:   float64(truePct) / 100.0,
		Src: rand.NewSource(seed),
	}

	for idx := range ret {
		ret[idx] = sampleVals[int(dist.Rand())]
	}
	return ret
}

func tableFromVec(dt arrow.DataType, size int, data interface{}, nullable bool, nullPct int) arrow.Table {
	if !nullable && nullPct != alternateOrNA {
		panic("bad check")
	}

	var valid []bool
	if nullable {
		// true values select index 1 of sample values
		validBytes := randomUint8(size, nullPct, [2]uint8{1, 0}, 500)
		valid = *(*[]bool)(unsafe.Pointer(&validBytes))
	}

	bldr := array.NewBuilder(memory.DefaultAllocator, dt)
	defer bldr.Release()

	switch v := data.(type) {
	case []int32:
		bldr.(*array.Int32Builder).AppendValues(v, valid)
	case []int64:
		bldr.(*array.Int64Builder).AppendValues(v, valid)
	case []float32:
		bldr.(*array.Float32Builder).AppendValues(v, valid)
	case []float64:
		bldr.(*array.Float64Builder).AppendValues(v, valid)
	}

	arr := bldr.NewArray()

	field := arrow.Field{Name: "column", Type: dt, Nullable: nullable}
	sc := arrow.NewSchema([]arrow.Field{field}, nil)
	col := arrow.NewColumnFromArr(field, arr)
	defer col.Release()
	return array.NewTable(sc, []arrow.Column{col}, int64(size))
}

func BenchmarkWriteColumn(b *testing.B) {
	int32Values := make([]int32, SIZELEN)
	int64Values := make([]int64, SIZELEN)
	float32Values := make([]float32, SIZELEN)
	float64Values := make([]float64, SIZELEN)
	for i := 0; i < SIZELEN; i++ {
		int32Values[i] = 128
		int64Values[i] = 128
		float32Values[i] = 128
		float64Values[i] = 128
	}

	tests := []struct {
		name     string
		dt       arrow.DataType
		values   interface{}
		nullable bool
		nbytes   int64
	}{
		{"int32 not nullable", arrow.PrimitiveTypes.Int32, int32Values, false, int64(arrow.Int32Traits.BytesRequired(SIZELEN))},
		{"int32 nullable", arrow.PrimitiveTypes.Int32, int32Values, true, int64(arrow.Int32Traits.BytesRequired(SIZELEN))},
		{"int64 not nullable", arrow.PrimitiveTypes.Int64, int64Values, false, int64(arrow.Int64Traits.BytesRequired(SIZELEN))},
		{"int64 nullable", arrow.PrimitiveTypes.Int64, int64Values, true, int64(arrow.Int64Traits.BytesRequired(SIZELEN))},
		{"float32 not nullable", arrow.PrimitiveTypes.Float32, float32Values, false, int64(arrow.Float32Traits.BytesRequired(SIZELEN))},
		{"float32 nullable", arrow.PrimitiveTypes.Float32, float32Values, true, int64(arrow.Float32Traits.BytesRequired(SIZELEN))},
		{"float64 not nullable", arrow.PrimitiveTypes.Float64, float64Values, false, int64(arrow.Float64Traits.BytesRequired(SIZELEN))},
		{"float64 nullable", arrow.PrimitiveTypes.Float64, float64Values, true, int64(arrow.Float64Traits.BytesRequired(SIZELEN))},
	}

	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	arrProps := pqarrow.DefaultWriterProps()

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			tbl := tableFromVec(tt.dt, SIZELEN, tt.values, tt.nullable, alternateOrNA)
			b.Cleanup(func() { tbl.Release() })
			var buf bytes.Buffer
			buf.Grow(int(tt.nbytes))
			b.ResetTimer()
			b.SetBytes(tt.nbytes)

			for i := 0; i < b.N; i++ {
				buf.Reset()
				err := pqarrow.WriteTable(tbl, &buf, SIZELEN, props, arrProps)
				if err != nil {
					b.Error(err)
				}
			}
		})
	}
}

func benchReadTable(b *testing.B, name string, tbl arrow.Table, nbytes int64) {
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	arrProps := pqarrow.DefaultWriterProps()

	var buf bytes.Buffer
	if err := pqarrow.WriteTable(tbl, &buf, SIZELEN, props, arrProps); err != nil {
		b.Error(err)
	}
	ctx := context.Background()

	b.ResetTimer()
	b.Run(name, func(b *testing.B) {
		b.SetBytes(nbytes)

		for i := 0; i < b.N; i++ {
			pf, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				b.Error(err)
			}

			reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
			if err != nil {
				b.Error(err)
			}

			tbl, err := reader.ReadTable(ctx)
			if err != nil {
				b.Error(err)
			}
			defer tbl.Release()
		}
	})
}

func BenchmarkReadColumnInt32(b *testing.B) {
	tests := []struct {
		name     string
		nullable bool
		nullPct  int
		fvPct    int
	}{
		{"int32 not null 1pct", false, alternateOrNA, 1},
		{"int32 not null 10pct", false, alternateOrNA, 10},
		{"int32 not null 50pct", false, alternateOrNA, 50},
		{"int32 nullable alt", true, alternateOrNA, 0},
		{"int32 nullable 1pct 1pct", true, 1, 1},
		{"int32 nullable 10pct 10pct", true, 10, 10},
		{"int32 nullable 25pct 5pct", true, 25, 5},
		{"int32 nullable 50pct 50pct", true, 50, 50},
		{"int32 nullable 50pct 0pct", true, 50, 0},
		{"int32 nullable 99pct 50pct", true, 99, 50},
		{"int32 nullable 99pct 0pct", true, 99, 0},
	}

	for _, tt := range tests {
		values := randomInt32(SIZELEN, tt.fvPct, [2]int32{127, 128}, 500)
		tbl := tableFromVec(arrow.PrimitiveTypes.Int32, SIZELEN, values, tt.nullable, tt.nullPct)
		benchReadTable(b, tt.name, tbl, int64(arrow.Int32Traits.BytesRequired(SIZELEN)))
	}
}

func BenchmarkReadColumnInt64(b *testing.B) {
	tests := []struct {
		name     string
		nullable bool
		nullPct  int
		fvPct    int
	}{
		{"int64 not null 1pct", false, alternateOrNA, 1},
		{"int64 not null 10pct", false, alternateOrNA, 10},
		{"int64 not null 50pct", false, alternateOrNA, 50},
		{"int64 nullable alt", true, alternateOrNA, 0},
		{"int64 nullable 1pct 1pct", true, 1, 1},
		{"int64 nullable 5pct 5pct", true, 5, 5},
		{"int64 nullable 10pct 5pct", true, 10, 5},
		{"int64 nullable 25pct 10pct", true, 25, 10},
		{"int64 nullable 30pct 10pct", true, 30, 10},
		{"int64 nullable 35pct 10pct", true, 35, 10},
		{"int64 nullable 45pct 25pct", true, 45, 25},
		{"int64 nullable 50pct 50pct", true, 50, 50},
		{"int64 nullable 50pct 1pct", true, 50, 1},
		{"int64 nullable 75pct 1pct", true, 75, 1},
		{"int64 nullable 99pct 50pct", true, 99, 50},
		{"int64 nullable 99pct 0pct", true, 99, 0},
	}

	for _, tt := range tests {
		values := randomInt32(SIZELEN, tt.fvPct, [2]int32{127, 128}, 500)
		tbl := tableFromVec(arrow.PrimitiveTypes.Int32, SIZELEN, values, tt.nullable, tt.nullPct)
		benchReadTable(b, tt.name, tbl, int64(arrow.Int32Traits.BytesRequired(SIZELEN)))
	}
}

func BenchmarkReadColumnFloat64(b *testing.B) {
	tests := []struct {
		name     string
		nullable bool
		nullPct  int
		fvPct    int
	}{
		{"double not null 1pct", false, alternateOrNA, 0},
		{"double not null 20pct", false, alternateOrNA, 20},
		{"double nullable alt", true, alternateOrNA, 0},
		{"double nullable 10pct 50pct", true, 10, 50},
		{"double nullable 25pct 25pct", true, 25, 25},
	}

	for _, tt := range tests {
		values := randomInt32(SIZELEN, tt.fvPct, [2]int32{127, 128}, 500)
		tbl := tableFromVec(arrow.PrimitiveTypes.Int32, SIZELEN, values, tt.nullable, tt.nullPct)
		benchReadTable(b, tt.name, tbl, int64(arrow.Int32Traits.BytesRequired(SIZELEN)))
	}
}
