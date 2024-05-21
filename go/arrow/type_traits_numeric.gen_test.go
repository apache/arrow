// Code generated by type_traits_numeric.gen_test.go.tmpl. DO NOT EDIT.

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
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
)

func TestInt64Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Int64Traits.CastToBytes([]int64{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Int64Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Int64SizeBytes
		end := (i + 1) * arrow.Int64SizeBytes
		arrow.Int64Traits.PutValue(b2[beg:end], int64(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Int64Traits.CastFromBytes(b1)
		v2 := arrow.Int64Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Int64Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, int64(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]int64, N)
	arrow.Int64Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestUint64Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Uint64Traits.CastToBytes([]uint64{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Uint64Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Uint64SizeBytes
		end := (i + 1) * arrow.Uint64SizeBytes
		arrow.Uint64Traits.PutValue(b2[beg:end], uint64(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Uint64Traits.CastFromBytes(b1)
		v2 := arrow.Uint64Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Uint64Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, uint64(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]uint64, N)
	arrow.Uint64Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestFloat64Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Float64Traits.CastToBytes([]float64{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Float64Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Float64SizeBytes
		end := (i + 1) * arrow.Float64SizeBytes
		arrow.Float64Traits.PutValue(b2[beg:end], float64(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Float64Traits.CastFromBytes(b1)
		v2 := arrow.Float64Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Float64Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, float64(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]float64, N)
	arrow.Float64Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestInt32Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Int32Traits.CastToBytes([]int32{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Int32Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Int32SizeBytes
		end := (i + 1) * arrow.Int32SizeBytes
		arrow.Int32Traits.PutValue(b2[beg:end], int32(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Int32Traits.CastFromBytes(b1)
		v2 := arrow.Int32Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Int32Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, int32(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]int32, N)
	arrow.Int32Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestUint32Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Uint32Traits.CastToBytes([]uint32{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Uint32Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Uint32SizeBytes
		end := (i + 1) * arrow.Uint32SizeBytes
		arrow.Uint32Traits.PutValue(b2[beg:end], uint32(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Uint32Traits.CastFromBytes(b1)
		v2 := arrow.Uint32Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Uint32Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, uint32(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]uint32, N)
	arrow.Uint32Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestFloat32Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Float32Traits.CastToBytes([]float32{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Float32Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Float32SizeBytes
		end := (i + 1) * arrow.Float32SizeBytes
		arrow.Float32Traits.PutValue(b2[beg:end], float32(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Float32Traits.CastFromBytes(b1)
		v2 := arrow.Float32Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Float32Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, float32(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]float32, N)
	arrow.Float32Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestInt16Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Int16Traits.CastToBytes([]int16{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Int16Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Int16SizeBytes
		end := (i + 1) * arrow.Int16SizeBytes
		arrow.Int16Traits.PutValue(b2[beg:end], int16(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Int16Traits.CastFromBytes(b1)
		v2 := arrow.Int16Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Int16Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, int16(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]int16, N)
	arrow.Int16Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestUint16Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Uint16Traits.CastToBytes([]uint16{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Uint16Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Uint16SizeBytes
		end := (i + 1) * arrow.Uint16SizeBytes
		arrow.Uint16Traits.PutValue(b2[beg:end], uint16(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Uint16Traits.CastFromBytes(b1)
		v2 := arrow.Uint16Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Uint16Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, uint16(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]uint16, N)
	arrow.Uint16Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestInt8Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Int8Traits.CastToBytes([]int8{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Int8Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Int8SizeBytes
		end := (i + 1) * arrow.Int8SizeBytes
		arrow.Int8Traits.PutValue(b2[beg:end], int8(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Int8Traits.CastFromBytes(b1)
		v2 := arrow.Int8Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Int8Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, int8(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]int8, N)
	arrow.Int8Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestUint8Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Uint8Traits.CastToBytes([]uint8{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Uint8Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Uint8SizeBytes
		end := (i + 1) * arrow.Uint8SizeBytes
		arrow.Uint8Traits.PutValue(b2[beg:end], uint8(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Uint8Traits.CastFromBytes(b1)
		v2 := arrow.Uint8Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Uint8Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, uint8(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]uint8, N)
	arrow.Uint8Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestTime32Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Time32Traits.CastToBytes([]arrow.Time32{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Time32Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Time32SizeBytes
		end := (i + 1) * arrow.Time32SizeBytes
		arrow.Time32Traits.PutValue(b2[beg:end], arrow.Time32(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Time32Traits.CastFromBytes(b1)
		v2 := arrow.Time32Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Time32Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, arrow.Time32(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]arrow.Time32, N)
	arrow.Time32Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestTime64Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Time64Traits.CastToBytes([]arrow.Time64{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Time64Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Time64SizeBytes
		end := (i + 1) * arrow.Time64SizeBytes
		arrow.Time64Traits.PutValue(b2[beg:end], arrow.Time64(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Time64Traits.CastFromBytes(b1)
		v2 := arrow.Time64Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Time64Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, arrow.Time64(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]arrow.Time64, N)
	arrow.Time64Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestDate32Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Date32Traits.CastToBytes([]arrow.Date32{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Date32Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Date32SizeBytes
		end := (i + 1) * arrow.Date32SizeBytes
		arrow.Date32Traits.PutValue(b2[beg:end], arrow.Date32(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Date32Traits.CastFromBytes(b1)
		v2 := arrow.Date32Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Date32Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, arrow.Date32(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]arrow.Date32, N)
	arrow.Date32Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestDate64Traits(t *testing.T) {
	const N = 10
	b1 := arrow.Date64Traits.CastToBytes([]arrow.Date64{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.Date64Traits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.Date64SizeBytes
		end := (i + 1) * arrow.Date64SizeBytes
		arrow.Date64Traits.PutValue(b2[beg:end], arrow.Date64(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.Date64Traits.CastFromBytes(b1)
		v2 := arrow.Date64Traits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.Date64Traits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, arrow.Date64(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]arrow.Date64, N)
	arrow.Date64Traits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}

func TestDurationTraits(t *testing.T) {
	const N = 10
	b1 := arrow.DurationTraits.CastToBytes([]arrow.Duration{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	})

	b2 := make([]byte, arrow.DurationTraits.BytesRequired(N))
	for i := 0; i < N; i++ {
		beg := i * arrow.DurationSizeBytes
		end := (i + 1) * arrow.DurationSizeBytes
		arrow.DurationTraits.PutValue(b2[beg:end], arrow.Duration(i))
	}

	if !reflect.DeepEqual(b1, b2) {
		v1 := arrow.DurationTraits.CastFromBytes(b1)
		v2 := arrow.DurationTraits.CastFromBytes(b2)
		t.Fatalf("invalid values:\nb1=%v\nb2=%v\nv1=%v\nv2=%v\n", b1, b2, v1, v2)
	}

	v1 := arrow.DurationTraits.CastFromBytes(b1)
	for i, v := range v1 {
		if got, want := v, arrow.Duration(i); got != want {
			t.Fatalf("invalid value[%d]. got=%v, want=%v", i, got, want)
		}
	}

	v2 := make([]arrow.Duration, N)
	arrow.DurationTraits.Copy(v2, v1)

	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("invalid values:\nv1=%v\nv2=%v\n", v1, v2)
	}
}
