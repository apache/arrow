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

package array_test

import (
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestMapArray(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		arr, equalArr, unequalArr *array.Map

		equalValid     = []bool{true, true, true, true, true, true, true}
		equalOffsets   = []int32{0, 1, 2, 5, 6, 7, 8, 10}
		equalKeys      = []string{"a", "a", "a", "b", "c", "a", "a", "a", "a", "b"}
		equalValues    = []int32{1, 2, 3, 4, 5, 2, 2, 2, 5, 6}
		unequalValid   = []bool{true, true, true}
		unequalOffsets = []int32{0, 1, 4, 7}
		unequalKeys    = []string{"a", "a", "b", "c", "a", "b", "c"}
		unequalValues  = []int32{1, 2, 2, 2, 3, 4, 5}
	)

	bldr := array.NewMapBuilder(pool, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32, false)
	defer bldr.Release()

	kb := bldr.KeyBuilder().(*array.StringBuilder)
	ib := bldr.ItemBuilder().(*array.Int32Builder)

	bldr.AppendValues(equalOffsets, equalValid)
	for _, k := range equalKeys {
		kb.Append(k)
	}
	ib.AppendValues(equalValues, nil)

	assert.Equal(t, len(equalValid), bldr.Len())
	assert.Zero(t, bldr.NullN())

	arr = bldr.NewMapArray()
	defer arr.Release()

	bldr.AppendValues(equalOffsets, equalValid)
	for _, k := range equalKeys {
		kb.Append(k)
	}
	ib.AppendValues(equalValues, nil)

	equalArr = bldr.NewMapArray()
	defer equalArr.Release()

	bldr.AppendValues(unequalOffsets, unequalValid)
	for _, k := range unequalKeys {
		kb.Append(k)
	}
	ib.AppendValues(unequalValues, nil)

	unequalArr = bldr.NewMapArray()
	defer unequalArr.Release()

	assert.True(t, array.Equal(arr, arr))
	assert.True(t, array.Equal(arr, equalArr))
	assert.True(t, array.Equal(equalArr, arr))
	assert.False(t, array.Equal(equalArr, unequalArr))
	assert.False(t, array.Equal(unequalArr, equalArr))

	assert.True(t, array.SliceEqual(arr, 0, 1, unequalArr, 0, 1))
	assert.False(t, array.SliceEqual(arr, 0, 2, unequalArr, 0, 2))
	assert.False(t, array.SliceEqual(arr, 1, 2, unequalArr, 1, 2))
	assert.True(t, array.SliceEqual(arr, 2, 3, unequalArr, 2, 3))

	t.Run("items non nullable", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)

		dt := arrow.MapOf(arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int16)
		dt.KeysSorted = true
		dt.SetItemNullable(false)

		bldr := array.NewBuilder(pool, dt).(*array.MapBuilder)
		defer bldr.Release()

		kb := bldr.KeyBuilder().(*array.Int16Builder)
		ib := bldr.ItemBuilder().(*array.Int16Builder)

		bldr.Append(true)
		kb.Append(1)
		ib.AppendNull()

		assert.Panics(t, func() {
			_ = bldr.NewArray()
		})
	})
}

func TestMapArrayBuildIntToInt(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		dtype      = arrow.MapOf(arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int16)
		keys       = []int16{0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5}
		items      = []int16{1, 1, 2, 3, 5, 8, -1, -1, 0, 1, -1, 2}
		validItems = []bool{true, true, true, true, true, true, false, false, true, true, false, true}
		offsets    = []int32{0, 6, 6, 12, 12}
		validMaps  = []bool{true, false, true, true}
	)

	bldr := array.NewBuilder(pool, dtype).(*array.MapBuilder)
	defer bldr.Release()

	bldr.Reserve(len(validMaps))

	kb := bldr.KeyBuilder().(*array.Int16Builder)
	ib := bldr.ItemBuilder().(*array.Int16Builder)

	bldr.Append(true)
	kb.AppendValues(keys[:6], nil)
	ib.AppendValues(items[:6], nil)

	bldr.AppendNull()
	bldr.Append(true)
	kb.AppendValues(keys[6:], nil)
	ib.AppendValues(items[6:], []bool{false, false, true, true, false, true})

	bldr.Append(true)
	arr := bldr.NewArray().(*array.Map)
	defer arr.Release()

	assert.Equal(t, arrow.MAP, arr.DataType().ID())
	assert.EqualValues(t, len(validMaps), arr.Len())

	for i, ex := range validMaps {
		assert.Equal(t, ex, arr.IsValid(i))
		assert.Equal(t, !ex, arr.IsNull(i))
	}

	assert.Equal(t, offsets, arr.Offsets())
	assert.Equal(t, keys, arr.Keys().(*array.Int16).Int16Values())

	itemArr := arr.Items().(*array.Int16)
	for i, ex := range validItems {
		if ex {
			assert.True(t, itemArr.IsValid(i))
			assert.False(t, itemArr.IsNull(i))
			assert.Equal(t, items[i], itemArr.Value(i))
		} else {
			assert.False(t, itemArr.IsValid(i))
			assert.True(t, itemArr.IsNull(i))
		}
	}

	assert.Equal(t, "[{[0 1 2 3 4 5] [1 1 2 3 5 8]} (null) {[0 1 2 3 4 5] [(null) (null) 0 1 (null) 2]} {[] []}]", arr.String())
}

func TestMapStringRoundTrip(t *testing.T) {
	// 1. create array
	dt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	b := array.NewMapBuilderWithType(mem, dt)
	defer b.Release()

	kb := b.KeyBuilder().(*array.StringBuilder)
	ib := b.ItemBuilder().(*array.Int32Builder)

	for n := 0; n < 10; n++ {
		b.AppendNull()
		b.Append(true)

		for r := 'a'; r <= 'z'; r++ {
			kb.Append(string(r) + strconv.Itoa(n))
			if (n+int(r))%2 == 0 {
				ib.AppendNull()
			} else {
				ib.Append(int32(n + int(r)))
			}
		}
	}

	arr := b.NewArray().(*array.Map)
	defer arr.Release()

	// 2. create array via AppendValueFromString
	b1 := array.NewMapBuilderWithType(mem, dt)
	defer b1.Release()

	for i := 0; i < arr.Len(); i++ {
		assert.NoError(t, b1.AppendValueFromString(arr.ValueStr(i)))
	}

	arr1 := b1.NewArray().(*array.Map)
	defer arr1.Release()

	assert.True(t, array.Equal(arr, arr1))
}

func TestMapBuilder_SetNull(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	var (
		arr          *array.Map
		equalValid   = []bool{true, true, true, true, true, true, true}
		equalOffsets = []int32{0, 1, 2, 5, 6, 7, 8, 10}
		equalKeys    = []string{"a", "a", "a", "b", "c", "a", "a", "a", "a", "b"}
		equalValues  = []int32{1, 2, 3, 4, 5, 2, 2, 2, 5, 6}
	)

	bldr := array.NewMapBuilder(pool, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32, false)
	defer bldr.Release()

	kb := bldr.KeyBuilder().(*array.StringBuilder)
	ib := bldr.ItemBuilder().(*array.Int32Builder)

	bldr.AppendValues(equalOffsets, equalValid)
	for _, k := range equalKeys {
		kb.Append(k)
	}
	ib.AppendValues(equalValues, nil)

	bldr.SetNull(0)
	bldr.SetNull(3)

	arr = bldr.NewMapArray()
	defer arr.Release()

	assert.True(t, arr.IsNull(0))
	assert.True(t, arr.IsValid(1))
	assert.True(t, arr.IsNull(3))
}
