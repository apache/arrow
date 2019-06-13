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
		return arrayEqualBoolean(l, r)
	case *FixedSizeBinary:
		r := right.(*FixedSizeBinary)
		return arrayEqualFixedSizeBinary(l, r)
	case *Binary:
		r := right.(*Binary)
		return arrayEqualBinary(l, r)
	case *String:
		r := right.(*String)
		return arrayEqualString(l, r)
	case *Int8:
		r := right.(*Int8)
		return arrayEqualInt8(l, r)
	case *Int16:
		r := right.(*Int16)
		return arrayEqualInt16(l, r)
	case *Int32:
		r := right.(*Int32)
		return arrayEqualInt32(l, r)
	case *Int64:
		r := right.(*Int64)
		return arrayEqualInt64(l, r)
	case *Uint8:
		r := right.(*Uint8)
		return arrayEqualUint8(l, r)
	case *Uint16:
		r := right.(*Uint16)
		return arrayEqualUint16(l, r)
	case *Uint32:
		r := right.(*Uint32)
		return arrayEqualUint32(l, r)
	case *Uint64:
		r := right.(*Uint64)
		return arrayEqualUint64(l, r)
	case *Float16:
		r := right.(*Float16)
		return arrayEqualFloat16(l, r)
	case *Float32:
		r := right.(*Float32)
		return arrayEqualFloat32(l, r)
	case *Float64:
		r := right.(*Float64)
		return arrayEqualFloat64(l, r)
	case *Date32:
		r := right.(*Date32)
		return arrayEqualDate32(l, r)
	case *Date64:
		r := right.(*Date64)
		return arrayEqualDate64(l, r)
	case *Time32:
		r := right.(*Time32)
		return arrayEqualTime32(l, r)
	case *Time64:
		r := right.(*Time64)
		return arrayEqualTime64(l, r)
	case *Timestamp:
		r := right.(*Timestamp)
		return arrayEqualTimestamp(l, r)
	case *List:
		r := right.(*List)
		return arrayEqualList(l, r)
	case *FixedSizeList:
		r := right.(*FixedSizeList)
		return arrayEqualFixedSizeList(l, r)
	case *Struct:
		r := right.(*Struct)
		return arrayEqualStruct(l, r)

	default:
		panic(errors.Errorf("arrow/array: unknown array type %T", l))
	}
}

// ArraySliceEqual reports whether slices left[lbeg:lend] and right[rbeg:rend] are equal.
func ArraySliceEqual(left Interface, lbeg, lend int64, right Interface, rbeg, rend int64) bool {
	l := NewSlice(left, lbeg, lend)
	defer l.Release()
	r := NewSlice(right, rbeg, rend)
	defer r.Release()

	return ArrayEqual(l, r)
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
