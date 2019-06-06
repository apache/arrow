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
)

func ArrayEquals(left, right Interface) bool {
	switch {
	case !baseArrayEquals(left, right):
		return false
	case left.Len() == 0:
		return true
	case left.NullN() == left.Len():
		return true
	}

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

		// TODO(alexandreyc): numeric, list, struct
	}

	return true
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
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}

func fixedSizeBinaryArrayEquals(left, right *FixedSizeBinary) bool {
	for i := 0; i < left.Len(); i++ {
		if bytes.Compare(left.Value(i), right.Value(i)) != 0 {
			return false
		}
	}
	return true
}

func binaryArrayEquals(left, right *Binary) bool {
	for i := 0; i < left.Len(); i++ {
		if bytes.Compare(left.Value(i), right.Value(i)) != 0 {
			return false
		}
	}
	return true
}

func stringArrayEquals(left, right *String) bool {
	for i := 0; i < left.Len(); i++ {
		if left.Value(i) != right.Value(i) {
			return false
		}
	}
	return true
}
