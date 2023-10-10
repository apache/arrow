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

// Package arrdata exports arrays and records data ready to be used for tests.
package arrdata

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/decimal128"
	"github.com/apache/arrow/go/v14/arrow/decimal256"
	"github.com/apache/arrow/go/v14/arrow/float16"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/internal/types"
)

var (
	Records     = make(map[string][]arrow.Record)
	RecordNames []string
)

func init() {
	Records["nulls"] = makeNullRecords()
	Records["primitives"] = makePrimitiveRecords()
	Records["structs"] = makeStructsRecords()
	Records["lists"] = makeListsRecords()
	Records["list_views"] = makeListViewsRecords()
	Records["strings"] = makeStringsRecords()
	Records["fixed_size_lists"] = makeFixedSizeListsRecords()
	Records["fixed_width_types"] = makeFixedWidthTypesRecords()
	Records["fixed_size_binaries"] = makeFixedSizeBinariesRecords()
	Records["intervals"] = makeIntervalsRecords()
	Records["durations"] = makeDurationsRecords()
	Records["decimal128"] = makeDecimal128sRecords()
	Records["decimal256"] = makeDecimal256sRecords()
	Records["maps"] = makeMapsRecords()
	Records["extension"] = makeExtensionRecords()
	Records["union"] = makeUnionRecords()
	Records["run_end_encoded"] = makeRunEndEncodedRecords()

	for k := range Records {
		RecordNames = append(RecordNames, k)
	}
	sort.Strings(RecordNames)
}

func makeNullRecords() []arrow.Record {
	mem := memory.NewGoAllocator()

	meta := arrow.NewMetadata(
		[]string{"k1", "k2", "k3"},
		[]string{"v1", "v2", "v3"},
	)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "nulls", Type: arrow.Null, Nullable: true},
		}, &meta,
	)

	mask := []bool{true, false, false, true, true}
	chunks := [][]arrow.Array{
		{
			arrayOf(mem, []nullT{null, null, null, null, null}, mask),
		},
		{
			arrayOf(mem, []nullT{null, null, null, null, null}, mask),
		},
		{
			arrayOf(mem, []nullT{null, null, null, null, null}, mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makePrimitiveRecords() []arrow.Record {
	mem := memory.NewGoAllocator()

	meta := arrow.NewMetadata(
		[]string{"k1", "k2", "k3"},
		[]string{"v1", "v2", "v3"},
	)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "bools", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			{Name: "int8s", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			{Name: "int16s", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
			{Name: "int32s", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "uint8s", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
			{Name: "uint16s", Type: arrow.PrimitiveTypes.Uint16, Nullable: true},
			{Name: "uint32s", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "uint64s", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
			{Name: "float32s", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
			{Name: "float64s", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, &meta,
	)

	mask := []bool{true, false, false, true, true}
	chunks := [][]arrow.Array{
		{
			arrayOf(mem, []bool{true, false, true, false, true}, mask),
			arrayOf(mem, []int8{-1, -2, -3, -4, -5}, mask),
			arrayOf(mem, []int16{-1, -2, -3, -4, -5}, mask),
			arrayOf(mem, []int32{-1, -2, -3, -4, -5}, mask),
			arrayOf(mem, []int64{-1, -2, -3, -4, -5}, mask),
			arrayOf(mem, []uint8{+1, +2, +3, +4, +5}, mask),
			arrayOf(mem, []uint16{+1, +2, +3, +4, +5}, mask),
			arrayOf(mem, []uint32{+1, +2, +3, +4, +5}, mask),
			arrayOf(mem, []uint64{+1, +2, +3, +4, +5}, mask),
			arrayOf(mem, []float32{+1, +2, +3, +4, +5}, mask),
			arrayOf(mem, []float64{+1, +2, +3, +4, +5}, mask),
		},
		{
			arrayOf(mem, []bool{true, false, true, false, true}, mask),
			arrayOf(mem, []int8{-11, -12, -13, -14, -15}, mask),
			arrayOf(mem, []int16{-11, -12, -13, -14, -15}, mask),
			arrayOf(mem, []int32{-11, -12, -13, -14, -15}, mask),
			arrayOf(mem, []int64{-11, -12, -13, -14, -15}, mask),
			arrayOf(mem, []uint8{+11, +12, +13, +14, +15}, mask),
			arrayOf(mem, []uint16{+11, +12, +13, +14, +15}, mask),
			arrayOf(mem, []uint32{+11, +12, +13, +14, +15}, mask),
			arrayOf(mem, []uint64{+11, +12, +13, +14, +15}, mask),
			arrayOf(mem, []float32{+11, +12, +13, +14, +15}, mask),
			arrayOf(mem, []float64{+11, +12, +13, +14, +15}, mask),
		},
		{
			arrayOf(mem, []bool{true, false, true, false, true}, mask),
			arrayOf(mem, []int8{-21, -22, -23, -24, -25}, mask),
			arrayOf(mem, []int16{-21, -22, -23, -24, -25}, mask),
			arrayOf(mem, []int32{-21, -22, -23, -24, -25}, mask),
			arrayOf(mem, []int64{-21, -22, -23, -24, -25}, mask),
			arrayOf(mem, []uint8{+21, +22, +23, +24, +25}, mask),
			arrayOf(mem, []uint16{+21, +22, +23, +24, +25}, mask),
			arrayOf(mem, []uint32{+21, +22, +23, +24, +25}, mask),
			arrayOf(mem, []uint64{+21, +22, +23, +24, +25}, mask),
			arrayOf(mem, []float32{+21, +22, +23, +24, +25}, mask),
			arrayOf(mem, []float64{+21, +22, +23, +24, +25}, mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeStructsRecords() []arrow.Record {
	mem := memory.NewGoAllocator()

	fields := []arrow.Field{
		{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
		{Name: "f2", Type: arrow.BinaryTypes.String},
	}
	dtype := arrow.StructOf(fields...)
	schema := arrow.NewSchema([]arrow.Field{{Name: "struct_nullable", Type: dtype, Nullable: true}}, nil)

	mask := []bool{true, false, false, true, true, true, false, true}
	chunks := [][]arrow.Array{
		{
			structOf(mem, dtype, [][]arrow.Array{
				{
					arrayOf(mem, []int32{-1, -2, -3, -4, -5}, mask[:5]),
					arrayOf(mem, []string{"111", "222", "333", "444", "555"}, mask[:5]),
				},
				{
					arrayOf(mem, []int32{-11, -12, -13, -14, -15}, mask[:5]),
					arrayOf(mem, []string{"1111", "1222", "1333", "1444", "1555"}, mask[:5]),
				},
				{
					arrayOf(mem, []int32{-21, -22, -23, -24, -25}, mask[:5]),
					arrayOf(mem, []string{"2111", "2222", "2333", "2444", "2555"}, mask[:5]),
				},
				{
					arrayOf(mem, []int32{-31, -32, -33, -34, -35}, mask[:5]),
					arrayOf(mem, []string{"3111", "3222", "3333", "3444", "3555"}, mask[:5]),
				},
				{
					arrayOf(mem, []int32{-41, -42, -43, -44, -45}, mask[:5]),
					arrayOf(mem, []string{"4111", "4222", "4333", "4444", "4555"}, mask[:5]),
				},
			}, []bool{true, false, true, true, true}),
		},
		{
			structOf(mem, dtype, [][]arrow.Array{
				{
					arrayOf(mem, []int32{1, 2, 3, 4, 5}, mask[:5]),
					arrayOf(mem, []string{"-111", "-222", "-333", "-444", "-555"}, mask[:5]),
				},
				{
					arrayOf(mem, []int32{11, 12, 13, 14, 15}, mask[:5]),
					arrayOf(mem, []string{"-1111", "-1222", "-1333", "-1444", "-1555"}, mask[:5]),
				},
				{
					arrayOf(mem, []int32{21, 22, 23, 24, 25}, mask[:5]),
					arrayOf(mem, []string{"-2111", "-2222", "-2333", "-2444", "-2555"}, mask[:5]),
				},
				{
					arrayOf(mem, []int32{31, 32, 33, 34, 35}, mask[:5]),
					arrayOf(mem, []string{"-3111", "-3222", "-3333", "-3444", "-3555"}, mask[:5]),
				},
				{
					arrayOf(mem, []int32{41, 42, 43, 44, 45}, mask[:5]),
					arrayOf(mem, []string{"-4111", "-4222", "-4333", "-4444", "-4555"}, mask[:5]),
				},
			}, []bool{true, false, false, true, true}),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeListsRecords() []arrow.Record {
	mem := memory.NewGoAllocator()
	dtype := arrow.ListOf(arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "list_nullable", Type: dtype, Nullable: true},
	}, nil)

	mask := []bool{true, false, false, true, true}

	chunks := [][]arrow.Array{
		{
			listOf(mem, []arrow.Array{
				arrayOf(mem, []int32{1, 2, 3, 4, 5}, mask),
				arrayOf(mem, []int32{11, 12, 13, 14, 15}, mask),
				arrayOf(mem, []int32{21, 22, 23, 24, 25}, mask),
			}, nil),
		},
		{
			listOf(mem, []arrow.Array{
				arrayOf(mem, []int32{-1, -2, -3, -4, -5}, mask),
				arrayOf(mem, []int32{-11, -12, -13, -14, -15}, mask),
				arrayOf(mem, []int32{-21, -22, -23, -24, -25}, mask),
			}, nil),
		},
		{
			listOf(mem, []arrow.Array{
				arrayOf(mem, []int32{-1, -2, -3, -4, -5}, mask),
				arrayOf(mem, []int32{-11, -12, -13, -14, -15}, mask),
				arrayOf(mem, []int32{-21, -22, -23, -24, -25}, mask),
			}, []bool{true, false, true}),
		},
		{
			func() arrow.Array {
				bldr := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
				defer bldr.Release()

				return bldr.NewListArray()
			}(),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeListViewsRecords() []arrow.Record {
	mem := memory.NewGoAllocator()
	dtype := arrow.ListViewOf(arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "list_view_nullable", Type: dtype, Nullable: true},
	}, nil)

	mask := []bool{true, false, false, true, true}

	chunks := [][]arrow.Array{
		{
			listViewOf(mem, []arrow.Array{
				arrayOf(mem, []int32{1, 2, 3, 4, 5}, mask),
				arrayOf(mem, []int32{11, 12, 13, 14, 15}, mask),
				arrayOf(mem, []int32{21, 22, 23, 24, 25}, mask),
			}, nil),
		},
		{
			listViewOf(mem, []arrow.Array{
				arrayOf(mem, []int32{-1, -2, -3, -4, -5}, mask),
				arrayOf(mem, []int32{-11, -12, -13, -14, -15}, mask),
				arrayOf(mem, []int32{-21, -22, -23, -24, -25}, mask),
			}, nil),
		},
		{
			listViewOf(mem, []arrow.Array{
				arrayOf(mem, []int32{-1, -2, -3, -4, -5}, mask),
				arrayOf(mem, []int32{}, []bool{}),
				arrayOf(mem, []int32{-21, -22, -23, -24, -25}, mask),
			}, []bool{true, false, true}),
		},
		{
			func() arrow.Array {
				bldr := array.NewListViewBuilder(mem, arrow.PrimitiveTypes.Int32)
				defer bldr.Release()

				return bldr.NewListViewArray()
			}(),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeFixedSizeListsRecords() []arrow.Record {
	mem := memory.NewGoAllocator()
	const N = 3
	dtype := arrow.FixedSizeListOf(N, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "fixed_size_list_nullable", Type: dtype, Nullable: true},
	}, nil)

	mask := []bool{true, false, true}

	chunks := [][]arrow.Array{
		{
			fixedSizeListOf(mem, N, []arrow.Array{
				arrayOf(mem, []int32{1, 2, 3}, mask),
				arrayOf(mem, []int32{11, 12, 13}, mask),
				arrayOf(mem, []int32{21, 22, 23}, mask),
			}, nil),
		},
		{
			fixedSizeListOf(mem, N, []arrow.Array{
				arrayOf(mem, []int32{-1, -2, -3}, mask),
				arrayOf(mem, []int32{-11, -12, -13}, mask),
				arrayOf(mem, []int32{-21, -22, -23}, mask),
			}, nil),
		},
		{
			fixedSizeListOf(mem, N, []arrow.Array{
				arrayOf(mem, []int32{-1, -2, -3}, mask),
				arrayOf(mem, []int32{-11, -12, -13}, mask),
				arrayOf(mem, []int32{-21, -22, -23}, mask),
			}, []bool{true, false, true}),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeStringsRecords() []arrow.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "strings", Type: arrow.BinaryTypes.String},
		{Name: "bytes", Type: arrow.BinaryTypes.Binary},
	}, nil)

	mask := []bool{true, false, false, true, true}
	chunks := [][]arrow.Array{
		{
			arrayOf(mem, []string{"1é", "2", "3", "4", "5"}, mask),
			arrayOf(mem, [][]byte{[]byte("1é"), []byte("2"), []byte("3"), []byte("4"), []byte("5")}, mask),
		},
		{
			arrayOf(mem, []string{"11", "22", "33", "44", "55"}, mask),
			arrayOf(mem, [][]byte{[]byte("11"), []byte("22"), []byte("33"), []byte("44"), []byte("55")}, mask),
		},
		{
			arrayOf(mem, []string{"111", "222", "333", "444", "555"}, mask),
			arrayOf(mem, [][]byte{[]byte("111"), []byte("222"), []byte("333"), []byte("444"), []byte("555")}, mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

type (
	nullT        struct{}
	time32s      arrow.Time32
	time32ms     arrow.Time32
	time64ns     arrow.Time64
	time64us     arrow.Time64
	timestamp_s  arrow.Timestamp
	timestamp_ms arrow.Timestamp
	timestamp_us arrow.Timestamp
	timestamp_ns arrow.Timestamp
)

var (
	null nullT
)

func makeFixedWidthTypesRecords() []arrow.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "float16s", Type: arrow.FixedWidthTypes.Float16, Nullable: true},
			{Name: "time32ms", Type: arrow.FixedWidthTypes.Time32ms, Nullable: true},
			{Name: "time32s", Type: arrow.FixedWidthTypes.Time32s, Nullable: true},
			{Name: "time64ns", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true},
			{Name: "time64us", Type: arrow.FixedWidthTypes.Time64us, Nullable: true},
			{Name: "timestamp_s", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
			{Name: "timestamp_ms", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true},
			{Name: "timestamp_us", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
			{Name: "timestamp_ns", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
			{Name: "date32s", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
			{Name: "date64s", Type: arrow.FixedWidthTypes.Date64, Nullable: true},
		}, nil,
	)

	float16s := func(vs []float32) []float16.Num {
		o := make([]float16.Num, len(vs))
		for i, v := range vs {
			o[i] = float16.New(v)
		}
		return o
	}

	mask := []bool{true, false, false, true, true}
	chunks := [][]arrow.Array{
		{
			arrayOf(mem, float16s([]float32{+1, +2, +3, +4, +5}), mask),
			arrayOf(mem, []time32ms{-2, -1, 0, +1, +2}, mask),
			arrayOf(mem, []time32s{-2, -1, 0, +1, +2}, mask),
			arrayOf(mem, []time64ns{-2, -1, 0, +1, +2}, mask),
			arrayOf(mem, []time64us{-2, -1, 0, +1, +2}, mask),
			arrayOf(mem, []timestamp_s{0, +1, +2, +3, +4}, mask),
			arrayOf(mem, []timestamp_ms{0, +1, +2, +3, +4}, mask),
			arrayOf(mem, []timestamp_us{0, +1, +2, +3, +4}, mask),
			arrayOf(mem, []timestamp_ns{0, +1, +2, +3, +4}, mask),
			arrayOf(mem, []arrow.Date32{-2, -1, 0, +1, +2}, mask),
			arrayOf(mem, []arrow.Date64{-2, -1, 0, +1, +2}, mask),
		},
		{
			arrayOf(mem, float16s([]float32{+11, +12, +13, +14, +15}), mask),
			arrayOf(mem, []time32ms{-12, -11, 10, +11, +12}, mask),
			arrayOf(mem, []time32s{-12, -11, 10, +11, +12}, mask),
			arrayOf(mem, []time64ns{-12, -11, 10, +11, +12}, mask),
			arrayOf(mem, []time64us{-12, -11, 10, +11, +12}, mask),
			arrayOf(mem, []timestamp_s{10, +11, +12, +13, +14}, mask),
			arrayOf(mem, []timestamp_ms{10, +11, +12, +13, +14}, mask),
			arrayOf(mem, []timestamp_us{10, +11, +12, +13, +14}, mask),
			arrayOf(mem, []timestamp_ns{10, +11, +12, +13, +14}, mask),
			arrayOf(mem, []arrow.Date32{-12, -11, 10, +11, +12}, mask),
			arrayOf(mem, []arrow.Date64{-12, -11, 10, +11, +12}, mask),
		},
		{
			arrayOf(mem, float16s([]float32{+21, +22, +23, +24, +25}), mask),
			arrayOf(mem, []time32ms{-22, -21, 20, +21, +22}, mask),
			arrayOf(mem, []time32s{-22, -21, 20, +21, +22}, mask),
			arrayOf(mem, []time64ns{-22, -21, 20, +21, +22}, mask),
			arrayOf(mem, []time64us{-22, -21, 20, +21, +22}, mask),
			arrayOf(mem, []timestamp_s{20, +21, +22, +23, +24}, mask),
			arrayOf(mem, []timestamp_ms{20, +21, +22, +23, +24}, mask),
			arrayOf(mem, []timestamp_us{20, +21, +22, +23, +24}, mask),
			arrayOf(mem, []timestamp_ns{20, +21, +22, +23, +24}, mask),
			arrayOf(mem, []arrow.Date32{-22, -21, 20, +21, +22}, mask),
			arrayOf(mem, []arrow.Date64{-22, -21, 20, +21, +22}, mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

type fsb3 string

func makeFixedSizeBinariesRecords() []arrow.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "fixed_size_binary_3", Type: &arrow.FixedSizeBinaryType{ByteWidth: 3}, Nullable: true},
		}, nil,
	)

	mask := []bool{true, false, false, true, true}
	chunks := [][]arrow.Array{
		{
			arrayOf(mem, []fsb3{"001", "002", "003", "004", "005"}, mask),
		},
		{
			arrayOf(mem, []fsb3{"011", "012", "013", "014", "015"}, mask),
		},
		{
			arrayOf(mem, []fsb3{"021", "022", "023", "024", "025"}, mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeIntervalsRecords() []arrow.Record {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "months", Type: arrow.FixedWidthTypes.MonthInterval, Nullable: true},
			{Name: "days", Type: arrow.FixedWidthTypes.DayTimeInterval, Nullable: true},
			{Name: "nanos", Type: arrow.FixedWidthTypes.MonthDayNanoInterval, Nullable: true},
		}, nil,
	)

	mask := []bool{true, false, false, true, true}
	chunks := [][]arrow.Array{
		{
			arrayOf(mem, []arrow.MonthInterval{1, 2, 3, 4, 5}, mask),
			arrayOf(mem, []arrow.DayTimeInterval{
				{Days: 1, Milliseconds: 1},
				{Days: 2, Milliseconds: 2},
				{Days: 3, Milliseconds: 3},
				{Days: 4, Milliseconds: 4},
				{Days: 5, Milliseconds: 5}},
				mask),
			arrayOf(mem, []arrow.MonthDayNanoInterval{
				{Months: 1, Days: 1, Nanoseconds: 1000},
				{Months: 2, Days: 2, Nanoseconds: 2000},
				{Months: 3, Days: 3, Nanoseconds: 3000},
				{Months: 4, Days: 4, Nanoseconds: 4000},
				{Months: 5, Days: 5, Nanoseconds: 5000}},
				mask),
		},
		{
			arrayOf(mem, []arrow.MonthInterval{-11, -12, -13, -14, -15}, mask),
			arrayOf(mem, []arrow.DayTimeInterval{
				{Days: -11, Milliseconds: -11},
				{Days: -12, Milliseconds: -12},
				{Days: -13, Milliseconds: -13},
				{Days: -14, Milliseconds: -14},
				{Days: -15, Milliseconds: -15}},
				mask),
			arrayOf(mem, []arrow.MonthDayNanoInterval{
				{Months: -11, Days: -11, Nanoseconds: -11000},
				{Months: -12, Days: -12, Nanoseconds: -12000},
				{Months: -13, Days: -13, Nanoseconds: -13000},
				{Months: -14, Days: -14, Nanoseconds: -14000},
				{Months: -15, Days: -15, Nanoseconds: -15000}}, mask),
		},
		{
			arrayOf(mem, []arrow.MonthInterval{21, 22, 23, 24, 25, 0}, append(mask, true)),
			arrayOf(mem, []arrow.DayTimeInterval{
				{Days: 21, Milliseconds: 21},
				{Days: 22, Milliseconds: 22},
				{Days: 23, Milliseconds: 23},
				{Days: 24, Milliseconds: 24},
				{Days: 25, Milliseconds: 25},
				{Days: 0, Milliseconds: 0}}, append(mask, true)),
			arrayOf(mem, []arrow.MonthDayNanoInterval{
				{Months: 21, Days: 21, Nanoseconds: 21000},
				{Months: 22, Days: 22, Nanoseconds: 22000},
				{Months: 23, Days: 23, Nanoseconds: 23000},
				{Months: 24, Days: 24, Nanoseconds: 24000},
				{Months: 25, Days: 25, Nanoseconds: 25000},
				{Months: 0, Days: 0, Nanoseconds: 0}}, append(mask, true)),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

type (
	duration_s  arrow.Duration
	duration_ms arrow.Duration
	duration_us arrow.Duration
	duration_ns arrow.Duration
)

func makeDurationsRecords() []arrow.Record {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "durations-s", Type: &arrow.DurationType{Unit: arrow.Second}, Nullable: true},
			{Name: "durations-ms", Type: &arrow.DurationType{Unit: arrow.Millisecond}, Nullable: true},
			{Name: "durations-us", Type: &arrow.DurationType{Unit: arrow.Microsecond}, Nullable: true},
			{Name: "durations-ns", Type: &arrow.DurationType{Unit: arrow.Nanosecond}, Nullable: true},
		}, nil,
	)

	mask := []bool{true, false, false, true, true}
	chunks := [][]arrow.Array{
		{
			arrayOf(mem, []duration_s{1, 2, 3, 4, 5}, mask),
			arrayOf(mem, []duration_ms{1, 2, 3, 4, 5}, mask),
			arrayOf(mem, []duration_us{1, 2, 3, 4, 5}, mask),
			arrayOf(mem, []duration_ns{1, 2, 3, 4, 5}, mask),
		},
		{
			arrayOf(mem, []duration_s{11, 12, 13, 14, 15}, mask),
			arrayOf(mem, []duration_ms{11, 12, 13, 14, 15}, mask),
			arrayOf(mem, []duration_us{11, 12, 13, 14, 15}, mask),
			arrayOf(mem, []duration_ns{11, 12, 13, 14, 15}, mask),
		},
		{
			arrayOf(mem, []duration_s{21, 22, 23, 24, 25}, mask),
			arrayOf(mem, []duration_ms{21, 22, 23, 24, 25}, mask),
			arrayOf(mem, []duration_us{21, 22, 23, 24, 25}, mask),
			arrayOf(mem, []duration_ns{21, 22, 23, 24, 25}, mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

var (
	decimal128Type = &arrow.Decimal128Type{Precision: 10, Scale: 1}
	decimal256Type = &arrow.Decimal256Type{Precision: 72, Scale: 2}
)

func makeDecimal128sRecords() []arrow.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "dec128s", Type: decimal128Type, Nullable: true},
		}, nil,
	)

	dec128s := func(vs []int64) []decimal128.Num {
		o := make([]decimal128.Num, len(vs))
		for i, v := range vs {
			o[i] = decimal128.New(v, uint64(v))
		}
		return o
	}

	mask := []bool{true, false, false, true, true}
	chunks := [][]arrow.Array{
		{
			arrayOf(mem, dec128s([]int64{31, 32, 33, 34, 35}), mask),
		},
		{
			arrayOf(mem, dec128s([]int64{41, 42, 43, 44, 45}), mask),
		},
		{
			arrayOf(mem, dec128s([]int64{51, 52, 53, 54, 55}), mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeDecimal256sRecords() []arrow.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "dec256s", Type: decimal256Type, Nullable: true},
		}, nil,
	)

	dec256s := func(vs []uint64) []decimal256.Num {
		o := make([]decimal256.Num, len(vs))
		for i, v := range vs {
			o[i] = decimal256.New(v, v, v, v)
		}
		return o
	}

	mask := []bool{true, false, false, true, true}
	chunks := [][]arrow.Array{
		{
			arrayOf(mem, dec256s([]uint64{21, 22, 23, 24, 25}), mask),
		},
		{
			arrayOf(mem, dec256s([]uint64{31, 32, 33, 34, 35}), mask),
		},
		{
			arrayOf(mem, dec256s([]uint64{41, 42, 43, 44, 45}), mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeMapsRecords() []arrow.Record {
	mem := memory.NewGoAllocator()
	dtype := arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String)
	dtype.KeysSorted = true
	schema := arrow.NewSchema([]arrow.Field{{Name: "map_int_utf8", Type: dtype, Nullable: true}}, nil)

	mask := []bool{true, false, false, true, true}
	chunks := [][]arrow.Array{
		{
			mapOf(mem, dtype.KeysSorted, []arrow.Array{
				structOf(mem, dtype.Elem().(*arrow.StructType), [][]arrow.Array{
					{
						arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil),
						arrayOf(mem, []string{"111", "222", "333", "444", "555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil),
						arrayOf(mem, []string{"1111", "1222", "1333", "1444", "1555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil),
						arrayOf(mem, []string{"2111", "2222", "2333", "2444", "2555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil),
						arrayOf(mem, []string{"3111", "3222", "3333", "3444", "3555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil),
						arrayOf(mem, []string{"4111", "4222", "4333", "4444", "4555"}, mask[:5]),
					},
				}, nil),
				structOf(mem, dtype.Elem().(*arrow.StructType), [][]arrow.Array{
					{
						arrayOf(mem, []int32{1, 2, 3, 4, 5}, nil),
						arrayOf(mem, []string{"-111", "-222", "-333", "-444", "-555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{1, 2, 3, 4, 5}, nil),
						arrayOf(mem, []string{"-1111", "-1222", "-1333", "-1444", "-1555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{1, 2, 3, 4, 5}, nil),
						arrayOf(mem, []string{"-2111", "-2222", "-2333", "-2444", "-2555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{1, 2, 3, 4, 5}, nil),
						arrayOf(mem, []string{"-3111", "-3222", "-3333", "-3444", "-3555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{1, 2, 3, 4, 5}, nil),
						arrayOf(mem, []string{"-4111", "-4222", "-4333", "-4444", "-4555"}, mask[:5]),
					},
				}, nil),
			}, []bool{true, false, true, true, true}),
		},
		{
			mapOf(mem, dtype.KeysSorted, []arrow.Array{
				structOf(mem, dtype.Elem().(*arrow.StructType), [][]arrow.Array{
					{
						arrayOf(mem, []int32{1, 2, 3, 4, 5}, nil),
						arrayOf(mem, []string{"-111", "-222", "-333", "-444", "-555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{1, 2, 3, 4, 5}, nil),
						arrayOf(mem, []string{"-1111", "-1222", "-1333", "-1444", "-1555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{1, 2, 3, 4, 5}, nil),
						arrayOf(mem, []string{"-2111", "-2222", "-2333", "-2444", "-2555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{1, 2, 3, 4, 5}, nil),
						arrayOf(mem, []string{"-3111", "-3222", "-3333", "-3444", "-3555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{1, 2, 3, 4, 5}, nil),
						arrayOf(mem, []string{"-4111", "-4222", "-4333", "-4444", "-4555"}, mask[:5]),
					},
				}, nil),
				structOf(mem, dtype.Elem().(*arrow.StructType), [][]arrow.Array{
					{
						arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil),
						arrayOf(mem, []string{"111", "222", "333", "444", "555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil),
						arrayOf(mem, []string{"1111", "1222", "1333", "1444", "1555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil),
						arrayOf(mem, []string{"2111", "2222", "2333", "2444", "2555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil),
						arrayOf(mem, []string{"3111", "3222", "3333", "3444", "3555"}, mask[:5]),
					},
					{
						arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil),
						arrayOf(mem, []string{"4111", "4222", "4333", "4444", "4555"}, mask[:5]),
					},
				}, nil),
			}, []bool{true, false, true, true, true}),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeExtensionRecords() []arrow.Record {
	mem := memory.NewGoAllocator()

	p1Type := types.NewParametric1Type(6)
	p2Type := types.NewParametric1Type(12)
	p3Type := types.NewParametric2Type(2)
	p4Type := types.NewParametric2Type(3)
	p5Type := types.NewExtStructType()

	arrow.RegisterExtensionType(p1Type)
	arrow.RegisterExtensionType(p3Type)
	arrow.RegisterExtensionType(p4Type)
	arrow.RegisterExtensionType(p5Type)

	meta := arrow.NewMetadata(
		[]string{"k1", "k2"},
		[]string{"v1", "v2"},
	)

	unregisteredMeta := arrow.NewMetadata(
		append(meta.Keys(), ipc.ExtensionTypeKeyName, ipc.ExtensionMetadataKeyName),
		append(meta.Values(), "unregistered", ""))

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "p1", Type: p1Type, Nullable: true, Metadata: meta},
			{Name: "p2", Type: p2Type, Nullable: true, Metadata: meta},
			{Name: "p3", Type: p3Type, Nullable: true, Metadata: meta},
			{Name: "p4", Type: p4Type, Nullable: true, Metadata: meta},
			{Name: "p5", Type: p5Type, Nullable: true, Metadata: meta},
			{Name: "unreg", Type: arrow.PrimitiveTypes.Int8, Nullable: true, Metadata: unregisteredMeta},
		}, nil)

	mask := []bool{true, false, true, true, false}
	chunks := [][]arrow.Array{
		{
			extArray(mem, p1Type, []int32{1, -1, 2, 3, -1}, mask),
			extArray(mem, p2Type, []int32{2, -1, 3, 4, -1}, mask),
			extArray(mem, p3Type, []int32{5, -1, 6, 7, 8}, mask),
			extArray(mem, p4Type, []int32{5, -1, 7, 9, -1}, mask),
			extArray(mem, p5Type, [][]arrow.Array{
				{
					arrayOf(mem, []int64{1, -1, 2, 3, -1}, mask),
					arrayOf(mem, []float64{0.1, -1, 0.2, 0.3, -1}, mask),
				},
			}, mask),
			arrayOf(mem, []int8{-1, -2, -3, -4, -5}, mask),
		},
		{
			extArray(mem, p1Type, []int32{10, -1, 20, 30, -1}, mask),
			extArray(mem, p2Type, []int32{20, -1, 30, 40, -1}, mask),
			extArray(mem, p3Type, []int32{50, -1, 60, 70, 8}, mask),
			extArray(mem, p4Type, []int32{50, -1, 70, 90, -1}, mask),
			extArray(mem, p5Type, [][]arrow.Array{
				{
					arrayOf(mem, []int64{10, -1, 20, 30, -1}, mask),
					arrayOf(mem, []float64{0.01, -1, 0.02, 0.03, -1}, mask),
				},
			}, mask),
			arrayOf(mem, []int8{-11, -12, -13, -14, -15}, mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeUnionRecords() []arrow.Record {
	mem := memory.NewGoAllocator()

	unionFields := []arrow.Field{
		{Name: "u0", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "u1", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
	}

	typeCodes := []arrow.UnionTypeCode{5, 10}
	sparseType := arrow.SparseUnionOf(unionFields, typeCodes)
	denseType := arrow.DenseUnionOf(unionFields, typeCodes)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "sparse", Type: sparseType, Nullable: true},
		{Name: "dense", Type: denseType, Nullable: true},
	}, nil)

	sparseChildren := make([]arrow.Array, 4)
	denseChildren := make([]arrow.Array, 4)

	const length = 7

	typeIDsBuffer := memory.NewBufferBytes(arrow.Uint8Traits.CastToBytes([]uint8{5, 10, 5, 5, 10, 10, 5}))
	sparseChildren[0] = arrayOf(mem, []int32{0, 1, 2, 3, 4, 5, 6},
		[]bool{true, true, true, false, true, true, true})
	defer sparseChildren[0].Release()
	sparseChildren[1] = arrayOf(mem, []uint8{10, 11, 12, 13, 14, 15, 16},
		nil)
	defer sparseChildren[1].Release()
	sparseChildren[2] = arrayOf(mem, []int32{0, -1, -2, -3, -4, -5, -6},
		[]bool{true, true, true, true, true, true, false})
	defer sparseChildren[2].Release()
	sparseChildren[3] = arrayOf(mem, []uint8{100, 101, 102, 103, 104, 105, 106},
		nil)
	defer sparseChildren[3].Release()

	denseChildren[0] = arrayOf(mem, []int32{0, 2, 3, 7}, []bool{true, false, true, true})
	defer denseChildren[0].Release()
	denseChildren[1] = arrayOf(mem, []uint8{11, 14, 15}, nil)
	defer denseChildren[1].Release()
	denseChildren[2] = arrayOf(mem, []int32{0, -2, -3, -7}, []bool{false, true, true, false})
	defer denseChildren[2].Release()
	denseChildren[3] = arrayOf(mem, []uint8{101, 104, 105}, nil)
	defer denseChildren[3].Release()

	offsetsBuffer := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, 0, 1, 2, 1, 2, 3}))
	sparse1 := array.NewSparseUnion(sparseType, length, sparseChildren[:2], typeIDsBuffer, 0)
	dense1 := array.NewDenseUnion(denseType, length, denseChildren[:2], typeIDsBuffer, offsetsBuffer, 0)

	sparse2 := array.NewSparseUnion(sparseType, length, sparseChildren[2:], typeIDsBuffer, 0)
	dense2 := array.NewDenseUnion(denseType, length, denseChildren[2:], typeIDsBuffer, offsetsBuffer, 0)

	defer sparse1.Release()
	defer dense1.Release()
	defer sparse2.Release()
	defer dense2.Release()

	return []arrow.Record{
		array.NewRecord(schema, []arrow.Array{sparse1, dense1}, -1),
		array.NewRecord(schema, []arrow.Array{sparse2, dense2}, -1)}
}

func makeRunEndEncodedRecords() []arrow.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ree16", Type: arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int16, arrow.BinaryTypes.String)},
		{Name: "ree32", Type: arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32)},
		{Name: "ree64", Type: arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int64, arrow.BinaryTypes.Binary)},
	}, nil)

	schema.Field(1).Type.(*arrow.RunEndEncodedType).ValueNullable = false
	isValid := []bool{true, false, true, false, true}
	chunks := [][]arrow.Array{
		{
			runEndEncodedOf(
				arrayOf(mem, []int16{5, 10, 20, 1020, 1120}, nil),
				arrayOf(mem, []string{"foo", "bar", "baz", "foo", ""}, isValid), 1100, 20),
			runEndEncodedOf(
				arrayOf(mem, []int32{100, 200, 800, 1000, 1100}, nil),
				arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil), 1100, 0),
			runEndEncodedOf(
				arrayOf(mem, []int64{100, 250, 450, 800, 1100}, nil),
				arrayOf(mem, [][]byte{{0xde, 0xad}, {0xbe, 0xef}, {0xde, 0xad, 0xbe, 0xef}, {}, {0xba, 0xad, 0xf0, 0x0d}}, isValid), 1100, 0),
		},
		{
			runEndEncodedOf(
				arrayOf(mem, []int16{110, 160, 170, 1070, 1120}, nil),
				arrayOf(mem, []string{"super", "dee", "", "duper", "doo"}, isValid), 1100, 20),
			runEndEncodedOf(
				arrayOf(mem, []int32{100, 120, 710, 810, 1100}, nil),
				arrayOf(mem, []int32{-1, -2, -3, -4, -5}, nil), 1100, 0),
			runEndEncodedOf(
				arrayOf(mem, []int64{100, 250, 450, 800, 1100}, nil),
				arrayOf(mem, [][]byte{{0xde, 0xad}, {0xbe, 0xef}, {0xde, 0xad, 0xbe, 0xef}, {}, {0xba, 0xad, 0xf0, 0x0d}}, isValid), 1100, 0),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func extArray(mem memory.Allocator, dt arrow.ExtensionType, a interface{}, valids []bool) arrow.Array {
	var storage arrow.Array
	switch st := dt.StorageType().(type) {
	case *arrow.StructType:
		storage = structOf(mem, st, a.([][]arrow.Array), valids)
	case *arrow.MapType:
		storage = mapOf(mem, false, a.([]arrow.Array), valids)
	case *arrow.ListType:
		storage = listOf(mem, a.([]arrow.Array), valids)
	default:
		storage = arrayOf(mem, a, valids)
	}
	defer storage.Release()

	return array.NewExtensionArrayWithStorage(dt, storage)
}

func arrayOf(mem memory.Allocator, a interface{}, valids []bool) arrow.Array {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	switch a := a.(type) {
	case []nullT:
		return array.NewNull(len(a))

	case []bool:
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewBooleanArray()

	case []int8:
		bldr := array.NewInt8Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt8Array()

	case []int16:
		bldr := array.NewInt16Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt16Array()

	case []int32:
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt32Array()

	case []int64:
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt64Array()

	case []uint8:
		bldr := array.NewUint8Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint8Array()

	case []uint16:
		bldr := array.NewUint16Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint16Array()

	case []uint32:
		bldr := array.NewUint32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint32Array()

	case []uint64:
		bldr := array.NewUint64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint64Array()

	case []float16.Num:
		bldr := array.NewFloat16Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat16Array()

	case []float32:
		bldr := array.NewFloat32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat32Array()

	case []float64:
		bldr := array.NewFloat64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat64Array()

	case []decimal128.Num:
		bldr := array.NewDecimal128Builder(mem, decimal128Type)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		aa := bldr.NewDecimal128Array()
		return aa

	case []decimal256.Num:
		bldr := array.NewDecimal256Builder(mem, decimal256Type)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		aa := bldr.NewDecimal256Array()
		return aa

	case []string:
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewStringArray()

	case [][]byte:
		bldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewBinaryArray()

	case []time32s:
		bldr := array.NewTime32Builder(mem, arrow.FixedWidthTypes.Time32s.(*arrow.Time32Type))
		defer bldr.Release()

		vs := make([]arrow.Time32, len(a))
		for i, v := range a {
			vs[i] = arrow.Time32(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []time32ms:
		bldr := array.NewTime32Builder(mem, arrow.FixedWidthTypes.Time32ms.(*arrow.Time32Type))
		defer bldr.Release()

		vs := make([]arrow.Time32, len(a))
		for i, v := range a {
			vs[i] = arrow.Time32(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []time64ns:
		bldr := array.NewTime64Builder(mem, arrow.FixedWidthTypes.Time64ns.(*arrow.Time64Type))
		defer bldr.Release()

		vs := make([]arrow.Time64, len(a))
		for i, v := range a {
			vs[i] = arrow.Time64(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []time64us:
		bldr := array.NewTime64Builder(mem, arrow.FixedWidthTypes.Time64us.(*arrow.Time64Type))
		defer bldr.Release()

		vs := make([]arrow.Time64, len(a))
		for i, v := range a {
			vs[i] = arrow.Time64(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []timestamp_s:
		bldr := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_s.(*arrow.TimestampType))
		defer bldr.Release()

		vs := make([]arrow.Timestamp, len(a))
		for i, v := range a {
			vs[i] = arrow.Timestamp(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []timestamp_ms:
		bldr := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ms.(*arrow.TimestampType))
		defer bldr.Release()

		vs := make([]arrow.Timestamp, len(a))
		for i, v := range a {
			vs[i] = arrow.Timestamp(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []timestamp_us:
		bldr := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
		defer bldr.Release()

		vs := make([]arrow.Timestamp, len(a))
		for i, v := range a {
			vs[i] = arrow.Timestamp(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []timestamp_ns:
		bldr := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
		defer bldr.Release()

		vs := make([]arrow.Timestamp, len(a))
		for i, v := range a {
			vs[i] = arrow.Timestamp(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []arrow.Date32:
		bldr := array.NewDate32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []arrow.Date64:
		bldr := array.NewDate64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []fsb3:
		bldr := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 3})
		defer bldr.Release()
		vs := make([][]byte, len(a))
		for i, v := range a {
			vs[i] = []byte(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []arrow.MonthInterval:
		bldr := array.NewMonthIntervalBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []arrow.DayTimeInterval:
		bldr := array.NewDayTimeIntervalBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []arrow.MonthDayNanoInterval:
		bldr := array.NewMonthDayNanoIntervalBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []duration_s:
		bldr := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Second})
		defer bldr.Release()
		vs := make([]arrow.Duration, len(a))
		for i, v := range a {
			vs[i] = arrow.Duration(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []duration_ms:
		bldr := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Millisecond})
		defer bldr.Release()
		vs := make([]arrow.Duration, len(a))
		for i, v := range a {
			vs[i] = arrow.Duration(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []duration_us:
		bldr := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Microsecond})
		defer bldr.Release()
		vs := make([]arrow.Duration, len(a))
		for i, v := range a {
			vs[i] = arrow.Duration(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	case []duration_ns:
		bldr := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Nanosecond})
		defer bldr.Release()
		vs := make([]arrow.Duration, len(a))
		for i, v := range a {
			vs[i] = arrow.Duration(v)
		}
		bldr.AppendValues(vs, valids)
		return bldr.NewArray()

	default:
		panic(fmt.Errorf("arrdata: invalid data slice type %T", a))
	}
}

func listOf(mem memory.Allocator, values []arrow.Array, valids []bool) *array.List {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	bldr := array.NewListBuilder(mem, values[0].DataType())
	defer bldr.Release()

	valid := func(i int) bool {
		return valids[i]
	}

	if valids == nil {
		valid = func(i int) bool { return true }
	}

	for i, value := range values {
		bldr.Append(valid(i))
		buildArray(bldr.ValueBuilder(), value)
	}

	return bldr.NewListArray()
}

func listViewOf(mem memory.Allocator, values []arrow.Array, valids []bool) *array.ListView {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	bldr := array.NewListViewBuilder(mem, values[0].DataType())
	defer bldr.Release()

	valid := func(i int) bool {
		return valids[i]
	}

	if valids == nil {
		valid = func(i int) bool { return true }
	}

	for i, value := range values {
		bldr.AppendWithSize(valid(i), value.Len())
		buildArray(bldr.ValueBuilder(), value)
	}

	return bldr.NewListViewArray()
}

func fixedSizeListOf(mem memory.Allocator, n int32, values []arrow.Array, valids []bool) *array.FixedSizeList {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	bldr := array.NewFixedSizeListBuilder(mem, n, values[0].DataType())
	defer bldr.Release()

	valid := func(i int) bool {
		return valids[i]
	}

	if valids == nil {
		valid = func(i int) bool { return true }
	}

	for i, value := range values {
		bldr.Append(valid(i))
		buildArray(bldr.ValueBuilder(), value)
	}

	return bldr.NewListArray()
}

func structOf(mem memory.Allocator, dtype *arrow.StructType, fields [][]arrow.Array, valids []bool) *array.Struct {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	bldr := array.NewStructBuilder(mem, dtype)
	defer bldr.Release()

	if valids == nil {
		valids = make([]bool, fields[0][0].Len())
		for i := range valids {
			valids[i] = true
		}
	}

	for i := range fields {
		bldr.AppendValues(valids)
		for j := range dtype.Fields() {
			fbldr := bldr.FieldBuilder(j)
			buildArray(fbldr, fields[i][j])
		}
	}

	return bldr.NewStructArray()
}

func mapOf(mem memory.Allocator, sortedKeys bool, values []arrow.Array, valids []bool) *array.Map {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	pairType := values[0].DataType().(*arrow.StructType)
	bldr := array.NewMapBuilder(mem, pairType.Field(0).Type, pairType.Field(1).Type, sortedKeys)
	defer bldr.Release()

	valid := func(i int) bool {
		return valids[i]
	}

	if valids == nil {
		valid = func(i int) bool { return true }
	}

	vb := bldr.ValueBuilder().(*array.StructBuilder)
	for i, value := range values {
		bldr.Append(valid(i))
		buildArray(vb.FieldBuilder(0), value.(*array.Struct).Field(0))
		buildArray(vb.FieldBuilder(1), value.(*array.Struct).Field(1))
	}

	return bldr.NewMapArray()
}

func runEndEncodedOf(runEnds, values arrow.Array, logicalLen, offset int) arrow.Array {
	defer runEnds.Release()
	defer values.Release()
	return array.NewRunEndEncodedArray(runEnds, values, logicalLen, offset)
}

func buildArray(bldr array.Builder, data arrow.Array) {
	defer data.Release()

	switch bldr := bldr.(type) {
	case *array.BooleanBuilder:
		data := data.(*array.Boolean)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Int8Builder:
		data := data.(*array.Int8)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Int16Builder:
		data := data.(*array.Int16)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Int32Builder:
		data := data.(*array.Int32)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Int64Builder:
		data := data.(*array.Int64)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Uint8Builder:
		data := data.(*array.Uint8)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Uint16Builder:
		data := data.(*array.Uint16)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Uint32Builder:
		data := data.(*array.Uint32)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Uint64Builder:
		data := data.(*array.Uint64)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Float32Builder:
		data := data.(*array.Float32)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.Float64Builder:
		data := data.(*array.Float64)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.StringBuilder:
		data := data.(*array.String)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}

	case *array.LargeStringBuilder:
		data := data.(*array.LargeString)
		for i := 0; i < data.Len(); i++ {
			switch {
			case data.IsValid(i):
				bldr.Append(data.Value(i))
			default:
				bldr.AppendNull()
			}
		}
	}
}
