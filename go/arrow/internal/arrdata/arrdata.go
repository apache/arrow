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
package arrdata // import "github.com/apache/arrow/go/arrow/internal/arrdata"

import (
	"fmt"
	"sort"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/apache/arrow/go/arrow/memory"
)

var (
	Records     = make(map[string][]array.Record)
	RecordNames []string
)

func init() {
	Records["primitives"] = makePrimitiveRecords()
	Records["structs"] = makeStructsRecords()
	Records["lists"] = makeListsRecords()
	Records["strings"] = makeStringsRecords()
	Records["fixed_size_lists"] = makeFixedSizeListsRecords()
	Records["fixed_width_types"] = makeFixedWidthTypesRecords()
	Records["fixed_size_binaries"] = makeFixedSizeBinariesRecords()
	Records["intervals"] = makeIntervalsRecords()
	Records["durations"] = makeDurationsRecords()
	Records["decimal128"] = makeDecimal128sRecords()

	for k := range Records {
		RecordNames = append(RecordNames, k)
	}
	sort.Strings(RecordNames)
}

func makePrimitiveRecords() []array.Record {
	mem := memory.NewGoAllocator()

	meta := arrow.NewMetadata(
		[]string{"k1", "k2", "k3"},
		[]string{"v1", "v2", "v3"},
	)

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "bools", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			arrow.Field{Name: "int8s", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			arrow.Field{Name: "int16s", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
			arrow.Field{Name: "int32s", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			arrow.Field{Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			arrow.Field{Name: "uint8s", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
			arrow.Field{Name: "uint16s", Type: arrow.PrimitiveTypes.Uint16, Nullable: true},
			arrow.Field{Name: "uint32s", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			arrow.Field{Name: "uint64s", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
			arrow.Field{Name: "float32s", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
			arrow.Field{Name: "float64s", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, &meta,
	)

	mask := []bool{true, false, false, true, true}
	chunks := [][]array.Interface{
		[]array.Interface{
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
		[]array.Interface{
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
		[]array.Interface{
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

	recs := make([]array.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeStructsRecords() []array.Record {
	mem := memory.NewGoAllocator()

	fields := []arrow.Field{
		{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
		{Name: "f2", Type: arrow.BinaryTypes.String},
	}
	dtype := arrow.StructOf(fields...)
	schema := arrow.NewSchema([]arrow.Field{{Name: "struct_nullable", Type: dtype, Nullable: true}}, nil)

	bldr := array.NewStructBuilder(mem, dtype)
	defer bldr.Release()

	mask := []bool{true, false, false, true, true, true, false, true}
	chunks := [][]array.Interface{
		[]array.Interface{
			structOf(mem, dtype, [][]array.Interface{
				[]array.Interface{
					arrayOf(mem, []int32{-1, -2, -3, -4, -5}, mask[:5]),
					arrayOf(mem, []string{"111", "222", "333", "444", "555"}, mask[:5]),
				},
				[]array.Interface{
					arrayOf(mem, []int32{-11, -12, -13, -14, -15}, mask[:5]),
					arrayOf(mem, []string{"1111", "1222", "1333", "1444", "1555"}, mask[:5]),
				},
				[]array.Interface{
					arrayOf(mem, []int32{-21, -22, -23, -24, -25}, mask[:5]),
					arrayOf(mem, []string{"2111", "2222", "2333", "2444", "2555"}, mask[:5]),
				},
				[]array.Interface{
					arrayOf(mem, []int32{-31, -32, -33, -34, -35}, mask[:5]),
					arrayOf(mem, []string{"3111", "3222", "3333", "3444", "3555"}, mask[:5]),
				},
				[]array.Interface{
					arrayOf(mem, []int32{-41, -42, -43, -44, -45}, mask[:5]),
					arrayOf(mem, []string{"4111", "4222", "4333", "4444", "4555"}, mask[:5]),
				},
			}, []bool{true, false, true, true, true}),
		},
		[]array.Interface{
			structOf(mem, dtype, [][]array.Interface{
				[]array.Interface{
					arrayOf(mem, []int32{1, 2, 3, 4, 5}, mask[:5]),
					arrayOf(mem, []string{"-111", "-222", "-333", "-444", "-555"}, mask[:5]),
				},
				[]array.Interface{
					arrayOf(mem, []int32{11, 12, 13, 14, 15}, mask[:5]),
					arrayOf(mem, []string{"-1111", "-1222", "-1333", "-1444", "-1555"}, mask[:5]),
				},
				[]array.Interface{
					arrayOf(mem, []int32{21, 22, 23, 24, 25}, mask[:5]),
					arrayOf(mem, []string{"-2111", "-2222", "-2333", "-2444", "-2555"}, mask[:5]),
				},
				[]array.Interface{
					arrayOf(mem, []int32{31, 32, 33, 34, 35}, mask[:5]),
					arrayOf(mem, []string{"-3111", "-3222", "-3333", "-3444", "-3555"}, mask[:5]),
				},
				[]array.Interface{
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

	recs := make([]array.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeListsRecords() []array.Record {
	mem := memory.NewGoAllocator()
	dtype := arrow.ListOf(arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "list_nullable", Type: dtype, Nullable: true},
	}, nil)

	mask := []bool{true, false, false, true, true}

	chunks := [][]array.Interface{
		[]array.Interface{
			listOf(mem, []array.Interface{
				arrayOf(mem, []int32{1, 2, 3, 4, 5}, mask),
				arrayOf(mem, []int32{11, 12, 13, 14, 15}, mask),
				arrayOf(mem, []int32{21, 22, 23, 24, 25}, mask),
			}, nil),
		},
		[]array.Interface{
			listOf(mem, []array.Interface{
				arrayOf(mem, []int32{-1, -2, -3, -4, -5}, mask),
				arrayOf(mem, []int32{-11, -12, -13, -14, -15}, mask),
				arrayOf(mem, []int32{-21, -22, -23, -24, -25}, mask),
			}, nil),
		},
		[]array.Interface{
			listOf(mem, []array.Interface{
				arrayOf(mem, []int32{-1, -2, -3, -4, -5}, mask),
				arrayOf(mem, []int32{-11, -12, -13, -14, -15}, mask),
				arrayOf(mem, []int32{-21, -22, -23, -24, -25}, mask),
			}, []bool{true, false, true}),
		},
		[]array.Interface{
			func() array.Interface {
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

	recs := make([]array.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeFixedSizeListsRecords() []array.Record {
	mem := memory.NewGoAllocator()
	const N = 3
	dtype := arrow.FixedSizeListOf(N, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "fixed_size_list_nullable", Type: dtype, Nullable: true},
	}, nil)

	mask := []bool{true, false, true}

	chunks := [][]array.Interface{
		[]array.Interface{
			fixedSizeListOf(mem, N, []array.Interface{
				arrayOf(mem, []int32{1, 2, 3}, mask),
				arrayOf(mem, []int32{11, 12, 13}, mask),
				arrayOf(mem, []int32{21, 22, 23}, mask),
			}, nil),
		},
		[]array.Interface{
			fixedSizeListOf(mem, N, []array.Interface{
				arrayOf(mem, []int32{-1, -2, -3}, mask),
				arrayOf(mem, []int32{-11, -12, -13}, mask),
				arrayOf(mem, []int32{-21, -22, -23}, mask),
			}, nil),
		},
		[]array.Interface{
			fixedSizeListOf(mem, N, []array.Interface{
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

	recs := make([]array.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeStringsRecords() []array.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "strings", Type: arrow.BinaryTypes.String},
		{Name: "bytes", Type: arrow.BinaryTypes.Binary},
	}, nil)

	mask := []bool{true, false, false, true, true}
	chunks := [][]array.Interface{
		[]array.Interface{
			arrayOf(mem, []string{"1é", "2", "3", "4", "5"}, mask),
			arrayOf(mem, [][]byte{[]byte("1é"), []byte("2"), []byte("3"), []byte("4"), []byte("5")}, mask),
		},
		[]array.Interface{
			arrayOf(mem, []string{"11", "22", "33", "44", "55"}, mask),
			arrayOf(mem, [][]byte{[]byte("11"), []byte("22"), []byte("33"), []byte("44"), []byte("55")}, mask),
		},
		[]array.Interface{
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

	recs := make([]array.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

type (
	time32s      arrow.Time32
	time32ms     arrow.Time32
	time64ns     arrow.Time64
	time64us     arrow.Time64
	timestamp_s  arrow.Timestamp
	timestamp_ms arrow.Timestamp
	timestamp_us arrow.Timestamp
	timestamp_ns arrow.Timestamp
)

func makeFixedWidthTypesRecords() []array.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "float16s", Type: arrow.FixedWidthTypes.Float16, Nullable: true},
			arrow.Field{Name: "time32ms", Type: arrow.FixedWidthTypes.Time32ms, Nullable: true},
			arrow.Field{Name: "time32s", Type: arrow.FixedWidthTypes.Time32s, Nullable: true},
			arrow.Field{Name: "time64ns", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true},
			arrow.Field{Name: "time64us", Type: arrow.FixedWidthTypes.Time64us, Nullable: true},
			arrow.Field{Name: "timestamp_s", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
			arrow.Field{Name: "timestamp_ms", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true},
			arrow.Field{Name: "timestamp_us", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
			arrow.Field{Name: "timestamp_ns", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
			arrow.Field{Name: "date32s", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
			arrow.Field{Name: "date64s", Type: arrow.FixedWidthTypes.Date64, Nullable: true},
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
	chunks := [][]array.Interface{
		[]array.Interface{
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
		[]array.Interface{
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
		[]array.Interface{
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

	recs := make([]array.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

type fsb3 string

func makeFixedSizeBinariesRecords() []array.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "fixed_size_binary_3", Type: &arrow.FixedSizeBinaryType{ByteWidth: 3}, Nullable: true},
		}, nil,
	)

	mask := []bool{true, false, false, true, true}
	chunks := [][]array.Interface{
		[]array.Interface{
			arrayOf(mem, []fsb3{"001", "002", "003", "004", "005"}, mask),
		},
		[]array.Interface{
			arrayOf(mem, []fsb3{"011", "012", "013", "014", "015"}, mask),
		},
		[]array.Interface{
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

	recs := make([]array.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func makeIntervalsRecords() []array.Record {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "months", Type: arrow.FixedWidthTypes.MonthInterval, Nullable: true},
			arrow.Field{Name: "days", Type: arrow.FixedWidthTypes.DayTimeInterval, Nullable: true},
		}, nil,
	)

	mask := []bool{true, false, false, true, true}
	chunks := [][]array.Interface{
		[]array.Interface{
			arrayOf(mem, []arrow.MonthInterval{1, 2, 3, 4, 5}, mask),
			arrayOf(mem, []arrow.DayTimeInterval{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}, mask),
		},
		[]array.Interface{
			arrayOf(mem, []arrow.MonthInterval{11, 12, 13, 14, 15}, mask),
			arrayOf(mem, []arrow.DayTimeInterval{{11, 11}, {12, 12}, {13, 13}, {14, 14}, {15, 15}}, mask),
		},
		[]array.Interface{
			arrayOf(mem, []arrow.MonthInterval{21, 22, 23, 24, 25}, mask),
			arrayOf(mem, []arrow.DayTimeInterval{{21, 21}, {22, 22}, {23, 23}, {24, 24}, {25, 25}}, mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]array.Record, len(chunks))
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

func makeDurationsRecords() []array.Record {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "durations-s", Type: &arrow.DurationType{Unit: arrow.Second}, Nullable: true},
			arrow.Field{Name: "durations-ms", Type: &arrow.DurationType{Unit: arrow.Millisecond}, Nullable: true},
			arrow.Field{Name: "durations-us", Type: &arrow.DurationType{Unit: arrow.Microsecond}, Nullable: true},
			arrow.Field{Name: "durations-ns", Type: &arrow.DurationType{Unit: arrow.Nanosecond}, Nullable: true},
		}, nil,
	)

	mask := []bool{true, false, false, true, true}
	chunks := [][]array.Interface{
		[]array.Interface{
			arrayOf(mem, []duration_s{1, 2, 3, 4, 5}, mask),
			arrayOf(mem, []duration_ms{1, 2, 3, 4, 5}, mask),
			arrayOf(mem, []duration_us{1, 2, 3, 4, 5}, mask),
			arrayOf(mem, []duration_ns{1, 2, 3, 4, 5}, mask),
		},
		[]array.Interface{
			arrayOf(mem, []duration_s{11, 12, 13, 14, 15}, mask),
			arrayOf(mem, []duration_ms{11, 12, 13, 14, 15}, mask),
			arrayOf(mem, []duration_us{11, 12, 13, 14, 15}, mask),
			arrayOf(mem, []duration_ns{11, 12, 13, 14, 15}, mask),
		},
		[]array.Interface{
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

	recs := make([]array.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

var (
	decimal128Type = &arrow.Decimal128Type{Precision: 10, Scale: 1}
)

func makeDecimal128sRecords() []array.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "dec128s", Type: decimal128Type, Nullable: true},
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
	chunks := [][]array.Interface{
		[]array.Interface{
			arrayOf(mem, dec128s([]int64{31, 32, 33, 34, 35}), mask),
		},
		[]array.Interface{
			arrayOf(mem, dec128s([]int64{41, 42, 43, 44, 45}), mask),
		},
		[]array.Interface{
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

	recs := make([]array.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}

func arrayOf(mem memory.Allocator, a interface{}, valids []bool) array.Interface {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	switch a := a.(type) {
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

func listOf(mem memory.Allocator, values []array.Interface, valids []bool) *array.List {
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

func fixedSizeListOf(mem memory.Allocator, n int32, values []array.Interface, valids []bool) *array.FixedSizeList {
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

func structOf(mem memory.Allocator, dtype *arrow.StructType, fields [][]array.Interface, valids []bool) *array.Struct {
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

	for i, valid := range valids {
		bldr.Append(valid)
		for j := range dtype.Fields() {
			fbldr := bldr.FieldBuilder(j)
			buildArray(fbldr, fields[i][j])
		}
	}

	return bldr.NewStructArray()
}

func buildArray(bldr array.Builder, data array.Interface) {
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
	}
}
