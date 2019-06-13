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

package array

import (
	"bytes"

	"github.com/apache/arrow/go/arrow"
	"github.com/pkg/errors"
)

// ArrayEquals reports whether the two provided arrays are equal.
func ArrayEquals(left, right Interface) bool {
	switch {
	case !baseArrayEquals(left, right):
		return false
	case left.Len() == 0:
		return true
	case left.NullN() == left.Len():
		return true
	}

	// at this point, we know both arrays have same type, same length, same number of nulls
	// and nulls at the same place.
	// compare the values.

	switch l := left.(type) {
	case *Null:
		return true
	case *Boolean:
		r := right.(*Boolean)
		return booleanArrayEquals(l, r)
	case *FixedSizeBinary:
		r := right.(*FixedSizeBinary)
		return fixedSizeBinaryArrayEquals(l, r)
	case *Binary:
		r := right.(*Binary)
		return binaryArrayEquals(l, r)
	case *String:
		r := right.(*String)
		return stringArrayEquals(l, r)
	case *Int8:
		r := right.(*Int8)
		return i8ArrayEquals(l, r)
	case *Int16:
		r := right.(*Int16)
		return i16ArrayEquals(l, r)
	case *Int32:
		r := right.(*Int32)
		return i32ArrayEquals(l, r)
	case *Int64:
		r := right.(*Int64)
		return i64ArrayEquals(l, r)
	case *Uint8:
		r := right.(*Uint8)
		return u8ArrayEquals(l, r)
	case *Uint16:
		r := right.(*Uint16)
		return u16ArrayEquals(l, r)
	case *Uint32:
		r := right.(*Uint32)
		return u32ArrayEquals(l, r)
	case *Uint64:
		r := right.(*Uint64)
		return u64ArrayEquals(l, r)
	case *Float16:
		r := right.(*Float16)
		return f16ArrayEquals(l, r)
	case *Float32:
		r := right.(*Float32)
		return f32ArrayEquals(l, r)
	case *Float64:
		r := right.(*Float64)
		return f64ArrayEquals(l, r)
	case *Date32:
		r := right.(*Date32)
		return date32ArrayEquals(l, r)
	case *Date64:
		r := right.(*Date64)
		return date64ArrayEquals(l, r)
	case *Time32:
		r := right.(*Time32)
		return time32ArrayEquals(l, r)
	case *Time64:
		r := right.(*Time64)
		return time64ArrayEquals(l, r)
	case *Timestamp:
		r := right.(*Timestamp)
		return timestampArrayEquals(l, r)
	case *List:
		r := right.(*List)
		return listArrayEquals(l, r)
	case *FixedSizeList:
		r := right.(*FixedSizeList)
		return fixedSizeListArrayEquals(l, r)
	case *Struct:
		r := right.(*Struct)
		return structArrayEquals(l, r)

	default:
		panic(errors.Errorf("arrow/array: unknown array type %T", l))
	}
}

func baseArrayEquals(left, right Interface) bool {
	switch {
	case left.Len() != right.Len():
		return false
	case left.NullN() != right.NullN():
		return false
	case !arrow.TypeEquals(left.DataType(), right.DataType()): // We do not check for metadata as in the C++ implementation.
		return false
	case !validityBitmapEquals(left, right):
		return false
	}
	return true
}

func validityBitmapEquals(left, right Interface) bool {
	// TODO(alexandreyc): make it faster by comparing byte slices of the validity bitmap?
	n := left.Len()
	if n != right.Len() {
		return false
	}
	for i := 0; i < n; i++ {
		if left.IsNull(i) != right.IsNull(i) {
			return false
		}
	}
	return true
}

func booleanArrayEquals(left, right *Boolean) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func fixedSizeBinaryArrayEquals(left, right *FixedSizeBinary) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if bytes.Compare(left.Value(i), right.Value(i)) != 0 {
			return false
		}
	}
	return true
}

func binaryArrayEquals(left, right *Binary) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if bytes.Compare(left.Value(i), right.Value(i)) != 0 {
			return false
		}
	}
	return true
}

func stringArrayEquals(left, right *String) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func i8ArrayEquals(left, right *Int8) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func i16ArrayEquals(left, right *Int16) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func i32ArrayEquals(left, right *Int32) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func i64ArrayEquals(left, right *Int64) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func u8ArrayEquals(left, right *Uint8) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func u16ArrayEquals(left, right *Uint16) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func u32ArrayEquals(left, right *Uint32) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func u64ArrayEquals(left, right *Uint64) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func f16ArrayEquals(left, right *Float16) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func f32ArrayEquals(left, right *Float32) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func f64ArrayEquals(left, right *Float64) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func date32ArrayEquals(left, right *Date32) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func date64ArrayEquals(left, right *Date64) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func time32ArrayEquals(left, right *Time32) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func time64ArrayEquals(left, right *Time64) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func timestampArrayEquals(left, right *Timestamp) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func listArrayEquals(left, right *List) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		o := func() bool {
			l := left.newListValue(i)
			defer l.Release()
			r := right.newListValue(i)
			defer r.Release()
			return ArrayEquals(l, r)
		}()
		if !o {
			return false
		}
	}
	return true
}

func fixedSizeListArrayEquals(left, right *FixedSizeList) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		o := func() bool {
			l := left.newListValue(i)
			defer l.Release()
			r := right.newListValue(i)
			defer r.Release()
			return ArrayEquals(l, r)
		}()
		if !o {
			return false
		}
	}
	return true
}

func structArrayEquals(left, right *Struct) bool {
	for i, lf := range left.fields {
		rf := right.fields[i]
		if !ArrayEquals(lf, rf) {
			return false
		}
	}
	return true
}
