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
package driver

import (
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/decimal128"
	"github.com/apache/arrow/go/v14/arrow/decimal256"
	"github.com/apache/arrow/go/v14/arrow/float16"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

func Test_fromArrowType(t *testing.T) {
	fields := []arrow.Field{
		{Name: "f1-bool", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "f2-f16", Type: arrow.FixedWidthTypes.Float16},
		{Name: "f3-f32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "f4-f64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "f5-d128", Type: &arrow.Decimal128Type{}},
		{Name: "f6-d256", Type: &arrow.Decimal256Type{}},
		{Name: "f7-i8", Type: arrow.PrimitiveTypes.Int8},
		{Name: "f8-i16", Type: arrow.PrimitiveTypes.Int16},
		{Name: "f9-i32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "f10-i64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "f11-binary", Type: arrow.BinaryTypes.Binary},
		{Name: "f12-string", Type: arrow.BinaryTypes.String},
		{Name: "f13-t32s", Type: arrow.FixedWidthTypes.Time32s},
		{Name: "f14-t64us", Type: arrow.FixedWidthTypes.Time64us},
		{Name: "f15-ts_us", Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: "f16-d64", Type: arrow.FixedWidthTypes.Date64},
		{Name: "f17-dti", Type: arrow.FixedWidthTypes.DayTimeInterval},
	}

	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema(fields, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	testTime := time.Now()

	b.Field(0).(*array.BooleanBuilder).Append(true)
	b.Field(1).(*array.Float16Builder).Append(float16.New(1))
	b.Field(2).(*array.Float32Builder).Append(1)
	b.Field(3).(*array.Float64Builder).Append(1)
	b.Field(4).(*array.Decimal128Builder).Append(decimal128.FromBigInt(big.NewInt(1)))
	b.Field(5).(*array.Decimal256Builder).Append(decimal256.FromBigInt(big.NewInt(1)))
	b.Field(6).(*array.Int8Builder).Append(1)
	b.Field(7).(*array.Int16Builder).Append(1)
	b.Field(8).(*array.Int32Builder).Append(1)
	b.Field(9).(*array.Int64Builder).Append(1)
	b.Field(10).(*array.BinaryBuilder).Append([]byte("a"))
	b.Field(11).(*array.StringBuilder).Append("a")

	t32, err := arrow.Time32FromString("12:30:00", arrow.Second)
	if err != nil {
		t.Error(err)
	}

	b.Field(12).(*array.Time32Builder).Append(t32)

	t64, err := arrow.Time64FromString("12:00:00", arrow.Microsecond)
	if err != nil {
		t.Error(err)
	}

	b.Field(13).(*array.Time64Builder).Append(t64)

	ts, err := arrow.TimestampFromString("1970-01-01T12:00:00", arrow.Nanosecond)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(ts.ToTime(arrow.Nanosecond))

	b.Field(14).(*array.TimestampBuilder).Append(ts)
	b.Field(15).(*array.Date64Builder).Append(arrow.Date64FromTime(testTime))
	b.Field(16).(*array.DayTimeIntervalBuilder).Append(arrow.DayTimeInterval{Days: 1, Milliseconds: 1000})

	rec := b.NewRecord()
	defer rec.Release()

	tf := func(t *testing.T, idx int, want any) {
		t.Run(fmt.Sprintf("fromArrowType %v %s", fields[idx].Type, fields[idx].Name), func(t *testing.T) {
			v, err := fromArrowType(rec.Column(idx), 0)
			if err != nil {
				t.Fatalf("err when converting from arrow: %s", err)
			}
			if !reflect.DeepEqual(v, want) {
				t.Fatalf("test failed, wanted %T %v got %T %v", want, want, v, v)
			}
		})
	}

	tf(t, 0, true)
	tf(t, 1, float64(1))
	tf(t, 2, float64(1))
	tf(t, 3, float64(1))
	tf(t, 4, float64(1))
	tf(t, 5, float64(1))
	tf(t, 6, int64(1))
	tf(t, 7, int64(1))
	tf(t, 8, int64(1))
	tf(t, 9, int64(1))
	tf(t, 10, []byte("a"))
	tf(t, 11, "a")
	tf(t, 12, time.Date(1970, 1, 1, 12, 30, 0, 0, time.UTC))
	tf(t, 13, time.Date(1970, 1, 1, 12, 0, 0, 0, time.UTC))
	tf(t, 14, time.Date(1970, 1, 1, 12, 0, 0, 0, time.UTC))
	tf(t, 15, testTime.In(time.UTC).Truncate(24*time.Hour))
	tf(t, 16, time.Duration(24*time.Hour+time.Second))
}
