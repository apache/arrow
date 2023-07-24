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
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"golang.org/x/exp/rand"
)

// RandomNonNull generates a random arrow array of the requested type with length size with no nulls.
// Accepts float32, float64, all integer primitives, Date32, date64, string, binary, fixed_size_binary, bool and decimal.
//
// Always uses 0 as the seed with the following min/max restrictions:
// int16, uint16, int8, and uint8 will be min 0, max 64
// Date32 and Date64 will be between 0 and 24 * 86400000 in increments of 86400000
// String will all have the value "test-string"
// binary will have each value between length 2 and 12 but random bytes that are not limited to ascii
// fixed size binary will all be of length 10, random bytes are not limited to ascii
// bool will be approximately half false and half true randomly.
func RandomNonNull(mem memory.Allocator, dt arrow.DataType, size int) arrow.Array {
	switch dt.ID() {
	case arrow.FLOAT32:
		bldr := array.NewFloat32Builder(mem)
		defer bldr.Release()
		values := make([]float32, size)
		FillRandomFloat32(0, values)
		bldr.AppendValues(values, nil)
		return bldr.NewArray()
	case arrow.FLOAT64:
		bldr := array.NewFloat64Builder(mem)
		defer bldr.Release()
		values := make([]float64, size)
		FillRandomFloat64(0, values)
		bldr.AppendValues(values, nil)
		return bldr.NewArray()
	case arrow.INT64:
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()
		values := make([]int64, size)
		FillRandomInt64(0, values)
		bldr.AppendValues(values, nil)
		return bldr.NewArray()
	case arrow.UINT64:
		bldr := array.NewUint64Builder(mem)
		defer bldr.Release()
		values := make([]uint64, size)
		FillRandomUint64(0, values)
		bldr.AppendValues(values, nil)
		return bldr.NewArray()
	case arrow.INT32:
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()
		values := make([]int32, size)
		FillRandomInt32(0, values)
		bldr.AppendValues(values, nil)
		return bldr.NewArray()
	case arrow.UINT32:
		bldr := array.NewUint32Builder(mem)
		defer bldr.Release()
		values := make([]uint32, size)
		FillRandomUint32(0, values)
		bldr.AppendValues(values, nil)
		return bldr.NewArray()
	case arrow.INT16:
		bldr := array.NewInt16Builder(mem)
		defer bldr.Release()
		values := make([]int16, size)
		FillRandomInt16(0, 0, 64, values)
		bldr.AppendValues(values, nil)
		return bldr.NewArray()
	case arrow.UINT16:
		bldr := array.NewUint16Builder(mem)
		defer bldr.Release()
		values := make([]uint16, size)
		FillRandomUint16(0, 0, 64, values)
		bldr.AppendValues(values, nil)
		return bldr.NewArray()
	case arrow.INT8:
		bldr := array.NewInt8Builder(mem)
		defer bldr.Release()
		values := make([]int8, size)
		FillRandomInt8(0, 0, 64, values)
		bldr.AppendValues(values, nil)
		return bldr.NewArray()
	case arrow.UINT8:
		bldr := array.NewUint8Builder(mem)
		defer bldr.Release()
		values := make([]uint8, size)
		FillRandomUint8(0, 0, 64, values)
		bldr.AppendValues(values, nil)
		return bldr.NewArray()
	case arrow.DATE32:
		bldr := array.NewDate32Builder(mem)
		defer bldr.Release()
		values := make([]int32, size)
		FillRandomInt32Max(0, 24, values)

		dates := make([]arrow.Date32, size)
		for idx, val := range values {
			dates[idx] = arrow.Date32(val) * 86400000
		}
		bldr.AppendValues(dates, nil)
		return bldr.NewArray()
	case arrow.DATE64:
		bldr := array.NewDate64Builder(mem)
		defer bldr.Release()
		values := make([]int64, size)
		FillRandomInt64Max(0, 24, values)

		dates := make([]arrow.Date64, size)
		for idx, val := range values {
			dates[idx] = arrow.Date64(val) * 86400000
		}
		bldr.AppendValues(dates, nil)
		return bldr.NewArray()
	case arrow.STRING:
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append("test-string")
		}
		return bldr.NewArray()
	case arrow.LARGE_STRING:
		bldr := array.NewLargeStringBuilder(mem)
		defer bldr.Release()
		for i := 0; i < size; i++ {
			bldr.Append("test-large-string")
		}
		return bldr.NewArray()
	case arrow.BINARY, arrow.LARGE_BINARY:
		bldr := array.NewBinaryBuilder(mem, dt.(arrow.BinaryDataType))
		defer bldr.Release()

		buf := make([]byte, 12)
		r := rand.New(rand.NewSource(0))
		for i := 0; i < size; i++ {
			length := r.Intn(12-2+1) + 2
			r.Read(buf[:length])
			bldr.Append(buf[:length])
		}
		return bldr.NewArray()
	case arrow.FIXED_SIZE_BINARY:
		bldr := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 10})
		defer bldr.Release()

		buf := make([]byte, 10)
		r := rand.New(rand.NewSource(0))
		for i := 0; i < size; i++ {
			r.Read(buf)
			bldr.Append(buf)
		}
		return bldr.NewArray()
	case arrow.DECIMAL:
		dectype := dt.(*arrow.Decimal128Type)
		bldr := array.NewDecimal128Builder(mem, dectype)
		defer bldr.Release()

		data := RandomDecimals(int64(size), 0, dectype.Precision)
		bldr.AppendValues(arrow.Decimal128Traits.CastFromBytes(data), nil)
		return bldr.NewArray()
	case arrow.BOOL:
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()

		values := make([]bool, size)
		FillRandomBooleans(0.5, 0, values)
		bldr.AppendValues(values, nil)
		return bldr.NewArray()
	}
	return nil
}

// RandomNullable generates a random arrow array of length size with approximately numNulls,
// at most there can be size/2 nulls. Other than there being nulls, the values follow the same rules
// as described in the docs for RandomNonNull.
func RandomNullable(dt arrow.DataType, size int, numNulls int) arrow.Array {
	switch dt.ID() {
	case arrow.FLOAT32:
		bldr := array.NewFloat32Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]float32, size)
		FillRandomFloat32(0, values)

		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}
		bldr.AppendValues(values, valid)
		return bldr.NewArray()
	case arrow.FLOAT64:
		bldr := array.NewFloat64Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]float64, size)
		FillRandomFloat64(0, values)

		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}
		bldr.AppendValues(values, valid)
		return bldr.NewArray()
	case arrow.INT8:
		bldr := array.NewInt8Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]int8, size)
		FillRandomInt8(0, 0, 64, values)
		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		bldr.AppendValues(values, valid)
		return bldr.NewArray()
	case arrow.UINT8:
		bldr := array.NewUint8Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]uint8, size)
		FillRandomUint8(0, 0, 64, values)
		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		bldr.AppendValues(values, valid)
		return bldr.NewArray()
	case arrow.INT16:
		bldr := array.NewInt16Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]int16, size)
		FillRandomInt16(0, 0, 64, values)
		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		bldr.AppendValues(values, valid)
		return bldr.NewArray()
	case arrow.UINT16:
		bldr := array.NewUint16Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]uint16, size)
		FillRandomUint16(0, 0, 64, values)
		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		bldr.AppendValues(values, valid)
		return bldr.NewArray()
	case arrow.INT32:
		bldr := array.NewInt32Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]int32, size)
		FillRandomInt32Max(0, 64, values)
		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		bldr.AppendValues(values, valid)
		return bldr.NewArray()
	case arrow.UINT32:
		bldr := array.NewUint32Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]uint32, size)
		FillRandomUint32Max(0, 64, values)
		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		bldr.AppendValues(values, valid)
		return bldr.NewArray()

	case arrow.INT64:
		bldr := array.NewInt64Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]int64, size)
		FillRandomInt64Max(0, 64, values)
		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		bldr.AppendValues(values, valid)
		return bldr.NewArray()
	case arrow.UINT64:
		bldr := array.NewUint64Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]uint64, size)
		FillRandomUint64Max(0, 64, values)
		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		bldr.AppendValues(values, valid)
		return bldr.NewArray()
	case arrow.DATE32:
		bldr := array.NewDate32Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]int32, size)
		FillRandomInt32Max(0, 24, values)

		dates := make([]arrow.Date32, size)
		for idx, val := range values {
			dates[idx] = arrow.Date32(val) * 86400000
		}
		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}
		bldr.AppendValues(dates, valid)
		return bldr.NewArray()
	case arrow.DATE64:
		bldr := array.NewDate64Builder(memory.DefaultAllocator)
		defer bldr.Release()
		values := make([]int64, size)
		FillRandomInt64Max(0, 24, values)

		dates := make([]arrow.Date64, size)
		for idx, val := range values {
			dates[idx] = arrow.Date64(val) * 86400000
		}
		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}
		bldr.AppendValues(dates, valid)
		return bldr.NewArray()
	case arrow.BINARY:
		bldr := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		defer bldr.Release()

		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		buf := make([]byte, 12)
		r := rand.New(rand.NewSource(0))
		for i := 0; i < size; i++ {
			if !valid[i] {
				bldr.AppendNull()
				continue
			}

			length := r.Intn(12-2+1) + 2
			r.Read(buf[:length])
			bldr.Append(buf[:length])
		}
		return bldr.NewArray()
	case arrow.STRING:
		bldr := array.NewStringBuilder(memory.DefaultAllocator)
		defer bldr.Release()

		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		buf := make([]byte, 12)
		r := rand.New(rand.NewSource(0))
		for i := 0; i < size; i++ {
			if !valid[i] {
				bldr.AppendNull()
				continue
			}

			length := r.Intn(12-2+1) + 2
			r.Read(buf[:length])
			// trivially force data to be valid UTF8 by making it all ASCII
			for idx := range buf[:length] {
				buf[idx] &= 0x7f
			}
			bldr.Append(string(buf[:length]))
		}
		return bldr.NewArray()
	case arrow.FIXED_SIZE_BINARY:
		bldr := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: 10})
		defer bldr.Release()

		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		buf := make([]byte, 10)
		r := rand.New(rand.NewSource(0))
		for i := 0; i < size; i++ {
			if !valid[i] {
				bldr.AppendNull()
				continue
			}

			r.Read(buf)
			bldr.Append(buf)
		}
		return bldr.NewArray()
	case arrow.DECIMAL:
		dectype := dt.(*arrow.Decimal128Type)
		bldr := array.NewDecimal128Builder(memory.DefaultAllocator, dectype)
		defer bldr.Release()

		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		data := RandomDecimals(int64(size), 0, dectype.Precision)
		bldr.AppendValues(arrow.Decimal128Traits.CastFromBytes(data), valid)
		return bldr.NewArray()
	case arrow.BOOL:
		bldr := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer bldr.Release()

		valid := make([]bool, size)
		for idx := range valid {
			valid[idx] = true
		}
		for i := 0; i < numNulls; i++ {
			valid[i*2] = false
		}

		values := make([]bool, size)
		FillRandomBooleans(0.5, 0, values)
		bldr.AppendValues(values, valid)
		return bldr.NewArray()
	}
	return nil
}
