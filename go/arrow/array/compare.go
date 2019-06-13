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

// ArrayEqual reports whether the two provided arrays are equal.
func ArrayEqual(left, right Interface) bool {
	switch {
	case !baseArrayEqual(left, right):
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
		return booleanArrayEqual(l, r)
	case *FixedSizeBinary:
		r := right.(*FixedSizeBinary)
		return fixedSizeBinaryArrayEqual(l, r)
	case *Binary:
		r := right.(*Binary)
		return binaryArrayEqual(l, r)
	case *String:
		r := right.(*String)
		return stringArrayEqual(l, r)
	case *Int8:
		r := right.(*Int8)
		return i8ArrayEqual(l, r)
	case *Int16:
		r := right.(*Int16)
		return i16ArrayEqual(l, r)
	case *Int32:
		r := right.(*Int32)
		return i32ArrayEqual(l, r)
	case *Int64:
		r := right.(*Int64)
		return i64ArrayEqual(l, r)
	case *Uint8:
		r := right.(*Uint8)
		return u8ArrayEqual(l, r)
	case *Uint16:
		r := right.(*Uint16)
		return u16ArrayEqual(l, r)
	case *Uint32:
		r := right.(*Uint32)
		return u32ArrayEqual(l, r)
	case *Uint64:
		r := right.(*Uint64)
		return u64ArrayEqual(l, r)
	case *Float16:
		r := right.(*Float16)
		return f16ArrayEqual(l, r)
	case *Float32:
		r := right.(*Float32)
		return f32ArrayEqual(l, r)
	case *Float64:
		r := right.(*Float64)
		return f64ArrayEqual(l, r)
	case *Date32:
		r := right.(*Date32)
		return date32ArrayEqual(l, r)
	case *Date64:
		r := right.(*Date64)
		return date64ArrayEqual(l, r)
	case *Time32:
		r := right.(*Time32)
		return time32ArrayEqual(l, r)
	case *Time64:
		r := right.(*Time64)
		return time64ArrayEqual(l, r)
	case *Timestamp:
		r := right.(*Timestamp)
		return timestampArrayEqual(l, r)
	case *List:
		r := right.(*List)
		return listArrayEqual(l, r)
	case *FixedSizeList:
		r := right.(*FixedSizeList)
		return fixedSizeListArrayEqual(l, r)
	case *Struct:
		r := right.(*Struct)
		return structArrayEqual(l, r)

	default:
		panic(errors.Errorf("arrow/array: unknown array type %T", l))
	}
}

func baseArrayEqual(left, right Interface) bool {
	switch {
	case left.Len() != right.Len():
		return false
	case left.NullN() != right.NullN():
		return false
	case !arrow.TypeEquals(left.DataType(), right.DataType()): // We do not check for metadata as in the C++ implementation.
		return false
	case !validityBitmapEqual(left, right):
		return false
	}
	return true
}

func validityBitmapEqual(left, right Interface) bool {
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

func booleanArrayEqual(left, right *Boolean) bool {
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

func fixedSizeBinaryArrayEqual(left, right *FixedSizeBinary) bool {
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

func binaryArrayEqual(left, right *Binary) bool {
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

func stringArrayEqual(left, right *String) bool {
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

func i8ArrayEqual(left, right *Int8) bool {
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

func i16ArrayEqual(left, right *Int16) bool {
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

func i32ArrayEqual(left, right *Int32) bool {
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

func i64ArrayEqual(left, right *Int64) bool {
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

func u8ArrayEqual(left, right *Uint8) bool {
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

func u16ArrayEqual(left, right *Uint16) bool {
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

func u32ArrayEqual(left, right *Uint32) bool {
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

func u64ArrayEqual(left, right *Uint64) bool {
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

func f16ArrayEqual(left, right *Float16) bool {
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

func f32ArrayEqual(left, right *Float32) bool {
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

func f64ArrayEqual(left, right *Float64) bool {
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

func date32ArrayEqual(left, right *Date32) bool {
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

func date64ArrayEqual(left, right *Date64) bool {
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

func time32ArrayEqual(left, right *Time32) bool {
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

func time64ArrayEqual(left, right *Time64) bool {
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

func timestampArrayEqual(left, right *Timestamp) bool {
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

func listArrayEqual(left, right *List) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		o := func() bool {
			l := left.newListValue(i)
			defer l.Release()
			r := right.newListValue(i)
			defer r.Release()
			return ArrayEqual(l, r)
		}()
		if !o {
			return false
		}
	}
	return true
}

func fixedSizeListArrayEqual(left, right *FixedSizeList) bool {
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) {
			continue
		}
		o := func() bool {
			l := left.newListValue(i)
			defer l.Release()
			r := right.newListValue(i)
			defer r.Release()
			return ArrayEqual(l, r)
		}()
		if !o {
			return false
		}
	}
	return true
}

func structArrayEqual(left, right *Struct) bool {
	for i, lf := range left.fields {
		rf := right.fields[i]
		if !ArrayEqual(lf, rf) {
			return false
		}
	}
	return true
}
