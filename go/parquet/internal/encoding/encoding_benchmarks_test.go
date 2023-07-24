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

package encoding_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/internal/hashing"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/internal/encoding"
	"github.com/apache/arrow/go/v13/parquet/internal/testutils"
	"github.com/apache/arrow/go/v13/parquet/schema"
)

const (
	MINSIZE = 1024
	MAXSIZE = 65536
)

func BenchmarkPlainEncodingBoolean(b *testing.B) {
	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
			values := make([]bool, sz)
			for idx := range values {
				values[idx] = true
			}
			encoder := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.Plain,
				false, nil, memory.DefaultAllocator).(encoding.BooleanEncoder)
			b.ResetTimer()
			b.SetBytes(int64(len(values)))
			for n := 0; n < b.N; n++ {
				encoder.Put(values)
				buf, _ := encoder.FlushValues()
				buf.Release()
			}
		})
	}
}

func BenchmarkPlainEncodingInt32(b *testing.B) {
	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
			values := make([]int32, sz)
			for idx := range values {
				values[idx] = 64
			}
			encoder := encoding.NewEncoder(parquet.Types.Int32, parquet.Encodings.Plain,
				false, nil, memory.DefaultAllocator).(encoding.Int32Encoder)
			b.ResetTimer()
			b.SetBytes(int64(len(values) * arrow.Int32SizeBytes))
			for n := 0; n < b.N; n++ {
				encoder.Put(values)
				buf, _ := encoder.FlushValues()
				buf.Release()
			}
		})
	}
}

func BenchmarkPlainEncodingInt64(b *testing.B) {
	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
			values := make([]int64, sz)
			for idx := range values {
				values[idx] = 64
			}
			encoder := encoding.NewEncoder(parquet.Types.Int64, parquet.Encodings.Plain,
				false, nil, memory.DefaultAllocator).(encoding.Int64Encoder)
			b.ResetTimer()
			b.SetBytes(int64(len(values) * arrow.Int64SizeBytes))
			for n := 0; n < b.N; n++ {
				encoder.Put(values)
				buf, _ := encoder.FlushValues()
				buf.Release()
			}
		})
	}
}

func BenchmarkPlainEncodingFloat32(b *testing.B) {
	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
			values := make([]float32, sz)
			for idx := range values {
				values[idx] = 64.0
			}
			encoder := encoding.NewEncoder(parquet.Types.Float, parquet.Encodings.Plain,
				false, nil, memory.DefaultAllocator).(encoding.Float32Encoder)
			b.ResetTimer()
			b.SetBytes(int64(len(values) * arrow.Float32SizeBytes))
			for n := 0; n < b.N; n++ {
				encoder.Put(values)
				buf, _ := encoder.FlushValues()
				buf.Release()
			}
		})
	}
}

func BenchmarkPlainEncodingFloat64(b *testing.B) {
	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
			values := make([]float64, sz)
			for idx := range values {
				values[idx] = 64
			}
			encoder := encoding.NewEncoder(parquet.Types.Double, parquet.Encodings.Plain,
				false, nil, memory.DefaultAllocator).(encoding.Float64Encoder)
			b.ResetTimer()
			b.SetBytes(int64(len(values) * arrow.Float64SizeBytes))
			for n := 0; n < b.N; n++ {
				encoder.Put(values)
				buf, _ := encoder.FlushValues()
				buf.Release()
			}
		})
	}
}

func BenchmarkPlainDecodingBoolean(b *testing.B) {
	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
			output := make([]bool, sz)
			values := make([]bool, sz)
			for idx := range values {
				values[idx] = true
			}
			encoder := encoding.NewEncoder(parquet.Types.Boolean, parquet.Encodings.Plain,
				false, nil, memory.DefaultAllocator).(encoding.BooleanEncoder)
			encoder.Put(values)
			buf, _ := encoder.FlushValues()
			defer buf.Release()

			decoder := encoding.NewDecoder(parquet.Types.Boolean, parquet.Encodings.Plain, nil, memory.DefaultAllocator)
			b.ResetTimer()
			b.SetBytes(int64(len(values)))
			for n := 0; n < b.N; n++ {
				decoder.SetData(sz, buf.Bytes())
				decoder.(encoding.BooleanDecoder).Decode(output)
			}
		})
	}
}

func BenchmarkPlainDecodingInt32(b *testing.B) {
	for sz := MINSIZE; sz < MAXSIZE+1; sz *= 2 {
		b.Run(fmt.Sprintf("len %d", sz), func(b *testing.B) {
			output := make([]int32, sz)
			values := make([]int32, sz)
			for idx := range values {
				values[idx] = 64
			}
			encoder := encoding.NewEncoder(parquet.Types.Int32, parquet.Encodings.Plain,
				false, nil, memory.DefaultAllocator).(encoding.Int32Encoder)
			encoder.Put(values)
			buf, _ := encoder.FlushValues()
			defer buf.Release()

			decoder := encoding.NewDecoder(parquet.Types.Int32, parquet.Encodings.Plain, nil, memory.DefaultAllocator)
			b.ResetTimer()
			b.SetBytes(int64(len(values)))
			for n := 0; n < b.N; n++ {
				decoder.SetData(sz, buf.Bytes())
				decoder.(encoding.Int32Decoder).Decode(output)
			}
		})
	}
}

func BenchmarkMemoTableFloat64(b *testing.B) {
	tests := []struct {
		nunique int32
		nvalues int64
	}{
		{100, 65535},
		{1000, 65535},
		{5000, 65535},
	}

	for _, tt := range tests {
		b.Run(fmt.Sprintf("%d unique n %d", tt.nunique, tt.nvalues), func(b *testing.B) {
			rag := testutils.NewRandomArrayGenerator(0)
			dict := rag.Float64(int64(tt.nunique), 0)
			indices := rag.Int32(tt.nvalues, 0, int32(tt.nunique)-1, 0)

			values := make([]float64, tt.nvalues)
			for idx := range values {
				values[idx] = dict.Value(int(indices.Value(idx)))
			}

			b.ResetTimer()
			b.Run("go map", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tbl := encoding.NewFloat64MemoTable(memory.DefaultAllocator)
					for _, v := range values {
						tbl.GetOrInsert(v)
					}
					if tbl.Size() != int(tt.nunique) {
						b.Fatal(tbl.Size(), tt.nunique)
					}
				}
			})
			b.ResetTimer()
			b.Run("xxh3", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tbl := hashing.NewFloat64MemoTable(0)
					for _, v := range values {
						tbl.GetOrInsert(v)
					}
					if tbl.Size() != int(tt.nunique) {
						b.Fatal(tbl.Size(), tt.nunique)
					}
				}
			})
		})
	}
}

func BenchmarkMemoTableInt32(b *testing.B) {
	tests := []struct {
		nunique int32
		nvalues int64
	}{
		{100, 65535},
		{1000, 65535},
		{5000, 65535},
	}

	for _, tt := range tests {
		b.Run(fmt.Sprintf("%d unique n %d", tt.nunique, tt.nvalues), func(b *testing.B) {
			rag := testutils.NewRandomArrayGenerator(0)
			dict := rag.Int32(int64(tt.nunique), 0, math.MaxInt32-1, 0)
			indices := rag.Int32(tt.nvalues, 0, int32(tt.nunique)-1, 0)

			values := make([]int32, tt.nvalues)
			for idx := range values {
				values[idx] = dict.Value(int(indices.Value(idx)))
			}
			b.ResetTimer()
			b.Run("xxh3", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tbl := hashing.NewInt32MemoTable(0)
					for _, v := range values {
						tbl.GetOrInsert(v)
					}
					if tbl.Size() != int(tt.nunique) {
						b.Fatal(tbl.Size(), tt.nunique)
					}
				}
			})

			b.Run("go map", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tbl := encoding.NewInt32MemoTable(memory.DefaultAllocator)
					for _, v := range values {
						tbl.GetOrInsert(v)
					}
					if tbl.Size() != int(tt.nunique) {
						b.Fatal(tbl.Size(), tt.nunique)
					}
				}
			})
		})
	}
}

func BenchmarkMemoTable(b *testing.B) {
	tests := []struct {
		nunique int32
		minLen  int32
		maxLen  int32
		nvalues int64
	}{
		{100, 32, 32, 65535},
		{100, 8, 32, 65535},
		{1000, 32, 32, 65535},
		{1000, 8, 32, 65535},
		{5000, 32, 32, 65535},
		{5000, 8, 32, 65535},
	}

	for _, tt := range tests {
		b.Run(fmt.Sprintf("%d unique len %d-%d n %d", tt.nunique, tt.minLen, tt.maxLen, tt.nvalues), func(b *testing.B) {

			rag := testutils.NewRandomArrayGenerator(0)
			dict := rag.ByteArray(int64(tt.nunique), tt.minLen, tt.maxLen, 0).(*array.String)
			indices := rag.Int32(tt.nvalues, 0, int32(tt.nunique)-1, 0)

			values := make([]parquet.ByteArray, tt.nvalues)
			for idx := range values {
				values[idx] = []byte(dict.Value(int(indices.Value(idx))))
			}

			b.ResetTimer()

			b.Run("xxh3", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tbl := hashing.NewBinaryMemoTable(0, -1, array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary))
					for _, v := range values {
						tbl.GetOrInsert(v)
					}
					if tbl.Size() != int(tt.nunique) {
						b.Fatal(tbl.Size(), tt.nunique)
					}
					tbl.Release()
				}
			})
			b.ResetTimer()
			b.Run("go map", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tbl := encoding.NewBinaryMemoTable(memory.DefaultAllocator)
					for _, v := range values {
						tbl.GetOrInsert(v)
					}
					if tbl.Size() != int(tt.nunique) {
						b.Fatal(tbl.Size(), tt.nunique)
					}
					tbl.Release()
				}
			})
		})
	}
}

func BenchmarkMemoTableAllUnique(b *testing.B) {
	tests := []struct {
		minLen  int32
		maxLen  int32
		nvalues int64
	}{
		{32, 32, 1024},
		{8, 32, 1024},
		{32, 32, 32767},
		{8, 32, 32767},
		{32, 32, 65535},
		{8, 32, 65535},
	}
	for _, tt := range tests {
		b.Run(fmt.Sprintf("values %d len %d-%d", tt.nvalues, tt.minLen, tt.maxLen), func(b *testing.B) {

			rag := testutils.NewRandomArrayGenerator(0)
			dict := rag.ByteArray(tt.nvalues, tt.minLen, tt.maxLen, 0).(*array.String)

			values := make([]parquet.ByteArray, tt.nvalues)
			for idx := range values {
				values[idx] = []byte(dict.Value(idx))
			}

			b.ResetTimer()
			b.Run("go map", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tbl := encoding.NewBinaryMemoTable(memory.DefaultAllocator)
					for _, v := range values {
						tbl.GetOrInsert(v)
					}
					if tbl.Size() != int(tt.nvalues) {
						b.Fatal(tbl.Size(), tt.nvalues)
					}
					tbl.Release()
				}
			})

			b.Run("xxh3", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tbl := hashing.NewBinaryMemoTable(0, -1, array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary))
					for _, v := range values {
						tbl.GetOrInsert(v)
					}
					if tbl.Size() != int(tt.nvalues) {
						b.Fatal(tbl.Size(), tt.nvalues)
					}
					tbl.Release()
				}
			})
		})
	}

}

func BenchmarkEncodeDictByteArray(b *testing.B) {
	const (
		nunique = 100
		minLen  = 8
		maxLen  = 32
		nvalues = 65535
	)

	rag := testutils.NewRandomArrayGenerator(0)
	dict := rag.ByteArray(nunique, minLen, maxLen, 0).(*array.String)
	indices := rag.Int32(nvalues, 0, nunique-1, 0)

	values := make([]parquet.ByteArray, nvalues)
	for idx := range values {
		values[idx] = []byte(dict.Value(int(indices.Value(idx))))
	}
	col := schema.NewColumn(schema.NewByteArrayNode("bytearray", parquet.Repetitions.Required, -1), 0, 0)

	out := make([]byte, nunique*(maxLen+arrow.Uint32SizeBytes))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc := encoding.NewEncoder(parquet.Types.ByteArray, parquet.Encodings.PlainDict, true, col, memory.DefaultAllocator).(*encoding.DictByteArrayEncoder)
		enc.Put(values)
		enc.WriteDict(out)
	}
}

func BenchmarkDecodeDictByteArray(b *testing.B) {
	const (
		nunique = 100
		minLen  = 32
		maxLen  = 32
		nvalues = 65535
	)

	rag := testutils.NewRandomArrayGenerator(0)
	dict := rag.ByteArray(nunique, minLen, maxLen, 0).(*array.String)
	indices := rag.Int32(nvalues, 0, nunique-1, 0)

	values := make([]parquet.ByteArray, nvalues)
	for idx := range values {
		values[idx] = []byte(dict.Value(int(indices.Value(idx))))
	}

	col := schema.NewColumn(schema.NewByteArrayNode("bytearray", parquet.Repetitions.Required, -1), 0, 0)
	enc := encoding.NewEncoder(parquet.Types.ByteArray, parquet.Encodings.PlainDict, true, col, memory.DefaultAllocator).(*encoding.DictByteArrayEncoder)
	enc.Put(values)

	dictBuf := make([]byte, enc.DictEncodedSize())
	enc.WriteDict(dictBuf)

	idxBuf := make([]byte, enc.EstimatedDataEncodedSize())
	enc.WriteIndices(idxBuf)

	out := make([]parquet.ByteArray, nvalues)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dec := encoding.NewDecoder(parquet.Types.ByteArray, parquet.Encodings.Plain, col, memory.DefaultAllocator)
		dec.SetData(nunique, dictBuf)
		dictDec := encoding.NewDictDecoder(parquet.Types.ByteArray, col, memory.DefaultAllocator).(*encoding.DictByteArrayDecoder)
		dictDec.SetDict(dec)
		dictDec.SetData(nvalues, idxBuf)

		dictDec.Decode(out)
	}
}
