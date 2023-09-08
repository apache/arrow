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

package arrow_test

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/decimal128"
	"github.com/apache/arrow/go/v14/arrow/decimal256"
	"github.com/apache/arrow/go/v14/arrow/float16"
)

func TestBooleanTraits(t *testing.T) {
	for _, tc := range []struct {
		i, want int
	}{
		{0, 0},
		{1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}, {6, 1}, {7, 1}, {8, 1},
		{9, 2},
		{17, 3},
	} {
		t.Run(fmt.Sprintf("nbytes=%d", tc.i), func(t *testing.T) {
			got := arrow.BooleanTraits.BytesRequired(tc.i)
			if got != tc.want {
				t.Fatalf("got=%v, want=%v", got, tc.want)
			}
		})
	}
}

func TestFloat16Traits(t *testing.T) {
	const N = 10
	nbytes := arrow.Float16Traits.BytesRequired(N)
	b1 := arrow.Float16Traits.CastToBytes([]float16.Num{
		float16.New(0),
		float16.New(1),
		float16.New(2),
		float16.New(3),
		float16.New(4),
		float16.New(5),
		float16.New(6),
		float16.New(7),
		float16.New(8),
		float16.New(9),
	})

	b2 := make([]byte, nbytes)
	for i := 0; i < N; i++ {
		beg := i * arrow.Float16SizeBytes
		end := (i + 1) * arrow.Float16SizeBytes
		arrow.Float16Traits.PutValue(b2[beg:end], float16.New(float32(i)))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Float16Traits.CastFromBytes(b1)
		v2 := arrow.Float16Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Float16Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v.Float32(), float32(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]float16.Num, N)
	arrow.Float16Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestDecimal128Traits(t *testing.T) {
	const N = 10
	nbytes := arrow.Decimal128Traits.BytesRequired(N)
	b1 := arrow.Decimal128Traits.CastToBytes([]decimal128.Num{
		decimal128.New(0, 10),
		decimal128.New(1, 10),
		decimal128.New(2, 10),
		decimal128.New(3, 10),
		decimal128.New(4, 10),
		decimal128.New(5, 10),
		decimal128.New(6, 10),
		decimal128.New(7, 10),
		decimal128.New(8, 10),
		decimal128.New(9, 10),
	})

	b2 := make([]byte, nbytes)
	for i := 0; i < N; i++ {
		beg := i * arrow.Decimal128SizeBytes
		end := (i + 1) * arrow.Decimal128SizeBytes
		arrow.Decimal128Traits.PutValue(b2[beg:end], decimal128.New(int64(i), 10))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Decimal128Traits.CastFromBytes(b1)
		v2 := arrow.Decimal128Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Decimal128Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, decimal128.New(int64(i), 10); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]decimal128.Num, N)
	arrow.Decimal128Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestDecimal256Traits(t *testing.T) {
	const N = 10
	nbytes := arrow.Decimal256Traits.BytesRequired(N)
	b1 := arrow.Decimal256Traits.CastToBytes([]decimal256.Num{
		decimal256.New(0, 0, 0, 10),
		decimal256.New(1, 1, 1, 10),
		decimal256.New(2, 2, 2, 10),
		decimal256.New(3, 3, 3, 10),
		decimal256.New(4, 4, 4, 10),
		decimal256.New(5, 5, 5, 10),
		decimal256.New(6, 6, 6, 10),
		decimal256.New(7, 7, 7, 10),
		decimal256.New(8, 8, 8, 10),
		decimal256.New(9, 9, 9, 10),
	})

	b2 := make([]byte, nbytes)
	for i := 0; i < N; i++ {
		beg := i * arrow.Decimal256SizeBytes
		end := (i + 1) * arrow.Decimal256SizeBytes
		arrow.Decimal256Traits.PutValue(b2[beg:end], decimal256.New(uint64(i), uint64(i), uint64(i), 10))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Decimal256Traits.CastFromBytes(b1)
		v2 := arrow.Decimal256Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Decimal256Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, decimal256.New(uint64(i), uint64(i), uint64(i), 10); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]decimal256.Num, N)
	arrow.Decimal256Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestMonthIntervalTraits(t *testing.T) {
	const N = 10
	b1 := arrow.MonthIntervalTraits.CastToBytes([]arrow.MonthInterval{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.MonthIntervalTraits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.MonthIntervalSizeBytes
		end := (i + 1) * arrow.MonthIntervalSizeBytes
		arrow.MonthIntervalTraits.PutValue(b2[beg:end], arrow.MonthInterval(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.MonthIntervalTraits.CastFromBytes(b1)
		v2 := arrow.MonthIntervalTraits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.MonthIntervalTraits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, arrow.MonthInterval(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]arrow.MonthInterval, N)
	arrow.MonthIntervalTraits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestDayTimeIntervalTraits(t *testing.T) {
	const N = 10
	b1 := arrow.DayTimeIntervalTraits.CastToBytes([]arrow.DayTimeInterval{
		{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9},
	})

	b2 := make([]byte, arrow.DayTimeIntervalTraits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.DayTimeIntervalSizeBytes
		end := (i + 1) * arrow.DayTimeIntervalSizeBytes
		arrow.DayTimeIntervalTraits.PutValue(b2[beg:end], arrow.DayTimeInterval{int32(i), int32(i)})
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.DayTimeIntervalTraits.CastFromBytes(b1)
		v2 := arrow.DayTimeIntervalTraits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.DayTimeIntervalTraits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, (arrow.DayTimeInterval{int32(i), int32(i)}); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]arrow.DayTimeInterval, N)
	arrow.DayTimeIntervalTraits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestMonthDayNanoIntervalTraits(t *testing.T) {
	const N = 10
	b1 := arrow.MonthDayNanoIntervalTraits.CastToBytes([]arrow.MonthDayNanoInterval{
		{0, 0, 0}, {1, 1, 1000}, {2, 2, 2000}, {3, 3, 3000}, {4, 4, 4000}, {5, 5, 5000}, {6, 6, 6000}, {7, 7, 7000}, {8, 8, 8000}, {9, 9, 9000},
	})

	b2 := make([]byte, arrow.MonthDayNanoIntervalTraits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.MonthDayNanoIntervalSizeBytes
		end := (i + 1) * arrow.MonthDayNanoIntervalSizeBytes
		arrow.MonthDayNanoIntervalTraits.PutValue(b2[beg:end], arrow.MonthDayNanoInterval{int32(i), int32(i), int64(i) * 1000})
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.MonthDayNanoIntervalTraits.CastFromBytes(b1)
		v2 := arrow.MonthDayNanoIntervalTraits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.MonthDayNanoIntervalTraits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, (arrow.MonthDayNanoInterval{int32(i), int32(i), int64(i) * 1000}); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]arrow.MonthDayNanoInterval, N)
	arrow.MonthDayNanoIntervalTraits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestTimestampTraits(t *testing.T) {
	const N = 10
	b1 := arrow.TimestampTraits.CastToBytes([]arrow.Timestamp{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.TimestampTraits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.TimestampSizeBytes
		end := (i + 1) * arrow.TimestampSizeBytes
		arrow.TimestampTraits.PutValue(b2[beg:end], arrow.Timestamp(i))
	}

	if !bytes.Equal(b1, b2) {
		v1 := arrow.TimestampTraits.CastFromBytes(b1)
		v2 := arrow.TimestampTraits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.TimestampTraits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, arrow.Timestamp(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]arrow.Timestamp, N)
	arrow.TimestampTraits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}
