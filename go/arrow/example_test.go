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
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

// This example demonstrates how to build an array of int64 values using a builder and Append.
// Whilst convenient for small arrays,
func Example_minimal() {
	// Create an allocator.
	pool := memory.NewGoAllocator()

	// Create an int64 array builder.
	builder := array.NewInt64Builder(pool)

	builder.Append(1)
	builder.Append(2)
	builder.Append(3)
	builder.AppendNull()
	builder.Append(5)
	builder.Append(6)
	builder.Append(7)
	builder.Append(8)

	// Finish building the int64 array and reset the builder.
	ints := builder.NewInt64Array()

	// Enumerate the values.
	for i, v := range ints.Int64Values() {
		fmt.Printf("ints[%d] = ", i)
		if ints.IsNull(i) {
			fmt.Println("(null)")
		} else {
			fmt.Println(v)
		}
	}

	// Output:
	// ints[0] = 1
	// ints[1] = 2
	// ints[2] = 3
	// ints[3] = (null)
	// ints[4] = 5
	// ints[5] = 6
	// ints[6] = 7
	// ints[7] = 8
}

// This example demonstrates creating an array, sourcing the values and
// null bitmaps directly from byte slices. The null count is set to
// UnknownNullCount, instructing the array to calculate the
// null count from the bitmap when NullN is called.
func Example_fromMemory() {
	// create LSB packed bits with the following pattern:
	// 01010011 11000101
	data := memory.NewBufferBytes([]byte{0xca, 0xa3})

	// create LSB packed validity (null) bitmap, where every 4th element is null:
	// 11101110 11101110
	nullBitmap := memory.NewBufferBytes([]byte{0x77, 0x77})

	// Create a boolean array and lazily determine NullN using UnknownNullCount
	bools := array.NewBoolean(16, data, nullBitmap, array.UnknownNullCount)

	// Show the null count
	fmt.Printf("NullN()  = %d\n", bools.NullN())

	// Enumerate the values.
	n := bools.Len()
	for i := 0; i < n; i++ {
		fmt.Printf("bools[%d] = ", i)
		if bools.IsNull(i) {
			fmt.Println("(null)")
		} else {
			fmt.Printf("%t\n", bools.Value(i))
		}
	}

	// Output:
	// NullN()  = 4
	// bools[0] = false
	// bools[1] = true
	// bools[2] = false
	// bools[3] = (null)
	// bools[4] = false
	// bools[5] = false
	// bools[6] = true
	// bools[7] = (null)
	// bools[8] = true
	// bools[9] = true
	// bools[10] = false
	// bools[11] = (null)
	// bools[12] = false
	// bools[13] = true
	// bools[14] = false
	// bools[15] = (null)
}

// This example shows how to create a List array.
// The resulting array should be:
//  [[0, 1, 2], [], [3], [4, 5], [6, 7, 8], [], [9]]
func Example_listArray() {
	pool := memory.NewGoAllocator()

	lb := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int64)
	defer lb.Release()

	vb := lb.ValueBuilder().(*array.Int64Builder)
	defer vb.Release()

	vb.Reserve(10)

	lb.Append(true)
	vb.Append(0)
	vb.Append(1)
	vb.Append(2)

	lb.AppendNull()

	lb.Append(true)
	vb.Append(3)

	lb.Append(true)
	vb.Append(4)
	vb.Append(5)

	lb.Append(true)
	vb.Append(6)
	vb.Append(7)
	vb.Append(8)

	lb.AppendNull()

	lb.Append(true)
	vb.Append(9)

	arr := lb.NewArray().(*array.List)
	fmt.Printf("NullN()   = %d\n", arr.NullN())
	fmt.Printf("Len()     = %d\n", arr.Len())
	fmt.Printf("Offsets() = %v\n", arr.Offsets())

	offsets := arr.Offsets()[1:]

	varr := arr.ListValues().(*array.Int64)
	pos := 0
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsValid(i) {
			fmt.Printf("List[%d]   = (null)\n", i)
			continue
		}
		fmt.Printf("List[%d]   = [", i)
		for j := pos; j < int(offsets[i]); j++ {
			if j != pos {
				fmt.Printf(", ")
			}
			fmt.Printf("%v", varr.Value(j))
		}
		pos = int(offsets[i])
		fmt.Printf("]\n")
	}

	// Output:
	// NullN()   = 2
	// Len()     = 7
	// Offsets() = [0 3 3 4 6 9 9 10]
	// List[0]   = [0, 1, 2]
	// List[1]   = (null)
	// List[2]   = [3]
	// List[3]   = [4, 5]
	// List[4]   = [6, 7, 8]
	// List[5]   = (null)
	// List[6]   = [9]
}
