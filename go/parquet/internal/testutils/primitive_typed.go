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

package testutils

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/metadata"
	"github.com/apache/arrow/go/v13/parquet/schema"
)

type PrimitiveTypedTest struct {
	Node   schema.Node
	Schema *schema.Schema

	Typ reflect.Type

	DefLevels []int16
	RepLevels []int16
	Buffer    *memory.Buffer
	Values    interface{}

	ValuesOut    interface{}
	DefLevelsOut []int16
	RepLevelsOut []int16
}

func NewPrimitiveTypedTest(typ reflect.Type) PrimitiveTypedTest {
	return PrimitiveTypedTest{Typ: typ}
}

func (p *PrimitiveTypedTest) SetupValuesOut(nvalues int64) {
	p.ValuesOut = reflect.MakeSlice(reflect.SliceOf(p.Typ), int(nvalues), int(nvalues)).Interface()
	p.DefLevelsOut = make([]int16, nvalues)
	p.RepLevelsOut = make([]int16, nvalues)
}

func (p *PrimitiveTypedTest) GenerateData(nvalues int64) {
	p.DefLevels = make([]int16, nvalues)
	p.Values = reflect.MakeSlice(reflect.SliceOf(p.Typ), int(nvalues), int(nvalues)).Interface()
	InitValues(p.Values, p.Buffer)
	for idx := range p.DefLevels {
		p.DefLevels[idx] = 1
	}
}

func (p *PrimitiveTypedTest) SetupSchema(rep parquet.Repetition, ncols int) {
	fields := make([]schema.Node, ncols)
	for i := 0; i < ncols; i++ {
		name := fmt.Sprintf("column_%d", i)
		fields[i], _ = schema.NewPrimitiveNode(name, rep, TypeToParquetType(p.Typ), -1, 12)
	}
	p.Node, _ = schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	p.Schema = schema.NewSchema(p.Node.(*schema.GroupNode))
	p.Buffer = memory.NewResizableBuffer(memory.DefaultAllocator)
}

func (p *PrimitiveTypedTest) UpdateStats(stat metadata.TypedStatistics, numNull int64) {
	nvalues := int64(len(p.DefLevels))
	switch s := stat.(type) {
	case *metadata.Int32Statistics:
		s.Update(p.Values.([]int32)[:nvalues-numNull], numNull)
	case *metadata.Int64Statistics:
		s.Update(p.Values.([]int64)[:nvalues-numNull], numNull)
	case *metadata.Float32Statistics:
		s.Update(p.Values.([]float32)[:nvalues-numNull], numNull)
	case *metadata.Float64Statistics:
		s.Update(p.Values.([]float64)[:nvalues-numNull], numNull)
	case *metadata.Int96Statistics:
		s.Update(p.Values.([]parquet.Int96)[:nvalues-numNull], numNull)
	case *metadata.ByteArrayStatistics:
		s.Update(p.Values.([]parquet.ByteArray)[:nvalues-numNull], numNull)
	case *metadata.BooleanStatistics:
		s.Update(p.Values.([]bool)[:nvalues-numNull], numNull)
	case *metadata.FixedLenByteArrayStatistics:
		s.Update(p.Values.([]parquet.FixedLenByteArray)[:nvalues-numNull], numNull)
	default:
		panic("unimplemented")
	}
}

func (p *PrimitiveTypedTest) UpdateStatsSpaced(stat metadata.TypedStatistics, numNull int64, validBits []byte, validBitsOffset int64) {
	switch s := stat.(type) {
	case *metadata.Int32Statistics:
		s.UpdateSpaced(p.Values.([]int32), validBits, validBitsOffset, numNull)
	case *metadata.Int64Statistics:
		s.UpdateSpaced(p.Values.([]int64), validBits, validBitsOffset, numNull)
	case *metadata.Float32Statistics:
		s.UpdateSpaced(p.Values.([]float32), validBits, validBitsOffset, numNull)
	case *metadata.Float64Statistics:
		s.UpdateSpaced(p.Values.([]float64), validBits, validBitsOffset, numNull)
	case *metadata.Int96Statistics:
		s.UpdateSpaced(p.Values.([]parquet.Int96), validBits, validBitsOffset, numNull)
	case *metadata.ByteArrayStatistics:
		s.UpdateSpaced(p.Values.([]parquet.ByteArray), validBits, validBitsOffset, numNull)
	case *metadata.BooleanStatistics:
		s.UpdateSpaced(p.Values.([]bool), validBits, validBitsOffset, numNull)
	case *metadata.FixedLenByteArrayStatistics:
		s.UpdateSpaced(p.Values.([]parquet.FixedLenByteArray), validBits, validBitsOffset, numNull)
	default:
		panic("uninplemented")
	}
}

func (p *PrimitiveTypedTest) GetMinMax(stat metadata.TypedStatistics) (min, max interface{}) {
	switch s := stat.(type) {
	case *metadata.Int32Statistics:
		min = s.Min()
		max = s.Max()
	case *metadata.Int64Statistics:
		min = s.Min()
		max = s.Max()
	case *metadata.Float32Statistics:
		min = s.Min()
		max = s.Max()
	case *metadata.Float64Statistics:
		min = s.Min()
		max = s.Max()
	case *metadata.ByteArrayStatistics:
		min = s.Min()
		max = s.Max()
	case *metadata.Int96Statistics:
		min = s.Min()
		max = s.Max()
	case *metadata.BooleanStatistics:
		min = s.Min()
		max = s.Max()
	case *metadata.FixedLenByteArrayStatistics:
		min = s.Min()
		max = s.Max()
	default:
		panic("unimplemented")
	}
	return
}

func (p *PrimitiveTypedTest) WriteBatchValues(writer file.ColumnChunkWriter, defLevels, repLevels []int16) (int64, error) {
	switch w := writer.(type) {
	case *file.Int32ColumnChunkWriter:
		return w.WriteBatch(p.Values.([]int32), defLevels, repLevels)
	case *file.Int64ColumnChunkWriter:
		return w.WriteBatch(p.Values.([]int64), defLevels, repLevels)
	case *file.Float32ColumnChunkWriter:
		return w.WriteBatch(p.Values.([]float32), defLevels, repLevels)
	case *file.Float64ColumnChunkWriter:
		return w.WriteBatch(p.Values.([]float64), defLevels, repLevels)
	case *file.Int96ColumnChunkWriter:
		return w.WriteBatch(p.Values.([]parquet.Int96), defLevels, repLevels)
	case *file.ByteArrayColumnChunkWriter:
		return w.WriteBatch(p.Values.([]parquet.ByteArray), defLevels, repLevels)
	case *file.BooleanColumnChunkWriter:
		return w.WriteBatch(p.Values.([]bool), defLevels, repLevels)
	case *file.FixedLenByteArrayColumnChunkWriter:
		return w.WriteBatch(p.Values.([]parquet.FixedLenByteArray), defLevels, repLevels)
	default:
		panic("unimplemented")
	}
}

func (p *PrimitiveTypedTest) WriteBatchSubset(batch, offset int, writer file.ColumnChunkWriter, defLevels, repLevels []int16) (int64, error) {
	switch w := writer.(type) {
	case *file.Int32ColumnChunkWriter:
		return w.WriteBatch(p.Values.([]int32)[offset:batch+offset], defLevels, repLevels)
	case *file.Int64ColumnChunkWriter:
		return w.WriteBatch(p.Values.([]int64)[offset:batch+offset], defLevels, repLevels)
	case *file.Float32ColumnChunkWriter:
		return w.WriteBatch(p.Values.([]float32)[offset:batch+offset], defLevels, repLevels)
	case *file.Float64ColumnChunkWriter:
		return w.WriteBatch(p.Values.([]float64)[offset:batch+offset], defLevels, repLevels)
	case *file.Int96ColumnChunkWriter:
		return w.WriteBatch(p.Values.([]parquet.Int96)[offset:batch+offset], defLevels, repLevels)
	case *file.ByteArrayColumnChunkWriter:
		return w.WriteBatch(p.Values.([]parquet.ByteArray)[offset:batch+offset], defLevels, repLevels)
	case *file.BooleanColumnChunkWriter:
		return w.WriteBatch(p.Values.([]bool)[offset:batch+offset], defLevels, repLevels)
	case *file.FixedLenByteArrayColumnChunkWriter:
		return w.WriteBatch(p.Values.([]parquet.FixedLenByteArray)[offset:batch+offset], defLevels, repLevels)
	default:
		panic("unimplemented")
	}
}

func (p *PrimitiveTypedTest) WriteBatchValuesSpaced(writer file.ColumnChunkWriter, defLevels, repLevels []int16, validBits []byte, validBitsOffset int64) {
	switch w := writer.(type) {
	case *file.Int32ColumnChunkWriter:
		w.WriteBatchSpaced(p.Values.([]int32), defLevels, repLevels, validBits, validBitsOffset)
	case *file.Int64ColumnChunkWriter:
		w.WriteBatchSpaced(p.Values.([]int64), defLevels, repLevels, validBits, validBitsOffset)
	case *file.Float32ColumnChunkWriter:
		w.WriteBatchSpaced(p.Values.([]float32), defLevels, repLevels, validBits, validBitsOffset)
	case *file.Float64ColumnChunkWriter:
		w.WriteBatchSpaced(p.Values.([]float64), defLevels, repLevels, validBits, validBitsOffset)
	case *file.Int96ColumnChunkWriter:
		w.WriteBatchSpaced(p.Values.([]parquet.Int96), defLevels, repLevels, validBits, validBitsOffset)
	case *file.ByteArrayColumnChunkWriter:
		w.WriteBatchSpaced(p.Values.([]parquet.ByteArray), defLevels, repLevels, validBits, validBitsOffset)
	case *file.BooleanColumnChunkWriter:
		w.WriteBatchSpaced(p.Values.([]bool), defLevels, repLevels, validBits, validBitsOffset)
	case *file.FixedLenByteArrayColumnChunkWriter:
		w.WriteBatchSpaced(p.Values.([]parquet.FixedLenByteArray), defLevels, repLevels, validBits, validBitsOffset)
	default:
		panic("unimplemented")
	}
}

func (p *PrimitiveTypedTest) ReadBatch(reader file.ColumnChunkReader, batch, valuesRead int64, defLevels, repLevels []int16) int64 {
	switch r := reader.(type) {
	case *file.Int32ColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, p.ValuesOut.([]int32)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.Int64ColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, p.ValuesOut.([]int64)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.Float32ColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, p.ValuesOut.([]float32)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.Float64ColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, p.ValuesOut.([]float64)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.Int96ColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, p.ValuesOut.([]parquet.Int96)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.ByteArrayColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, p.ValuesOut.([]parquet.ByteArray)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.BooleanColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, p.ValuesOut.([]bool)[valuesRead:], defLevels, repLevels)
		return int64(read)
	case *file.FixedLenByteArrayColumnChunkReader:
		_, read, _ := r.ReadBatch(batch, p.ValuesOut.([]parquet.FixedLenByteArray)[valuesRead:], defLevels, repLevels)
		return int64(read)
	default:
		panic("unimplemented")
	}
}

func Min(v1, v2 interface{}) interface{} {
	switch n1 := v1.(type) {
	case int32:
		if n1 < v2.(int32) {
			return n1
		}
		return v2
	case int64:
		if n1 < v2.(int64) {
			return n1
		}
		return v2
	case float32:
		if n1 < v2.(float32) {
			return n1
		}
		return v2
	case float64:
		if n1 < v2.(float64) {
			return n1
		}
		return v2
	}
	panic("min utility only implemented for int32, int64, float32, float64")
}

func Max(v1, v2 interface{}) interface{} {
	switch n1 := v1.(type) {
	case int32:
		if n1 < v2.(int32) {
			return v2
		}
		return n1
	case int64:
		if n1 < v2.(int64) {
			return v2
		}
		return n1
	case float32:
		if n1 < v2.(float32) {
			return v2
		}
		return n1
	case float64:
		if n1 < v2.(float64) {
			return v2
		}
		return n1
	}
	panic("max utility only implemented for int32, int64, float32, float64")
}
