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

package util_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/arrow/util"
	"github.com/stretchr/testify/assert"
)

func TestTotalArrayReusedBuffers(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	bldr := array.NewBooleanBuilder(mem)
	defer bldr.Release()
	bldr.Append(true)
	arr := bldr.NewArray()
	defer arr.Release()

	rec := array.NewRecord(arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean},
	}, nil), []arrow.Array{arr, arr}, 1)
	defer rec.Release()

	assert.Equal(t, int64(5), util.TotalRecordSize(rec))

	rec1 := array.NewRecord(arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.FixedWidthTypes.Boolean},
	}, nil), []arrow.Array{arr}, 1)
	defer rec1.Release()

	// both records should have the same size as rec is using the same buffer
	assert.Equal(t, int64(5), util.TotalRecordSize(rec1))
}

func TestTotalArraySizeBasic(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	noNulls, _, err := array.FromJSON(mem,
		arrow.PrimitiveTypes.Int16,
		strings.NewReader("[1, 2, 3]"))
	assert.NoError(t, err)
	defer noNulls.Release()
	assert.Equal(t, int64(10), util.TotalArraySize(noNulls))

	withNulls, _, err := array.FromJSON(mem,
		arrow.PrimitiveTypes.Int16,
		strings.NewReader("[1, 2, 3, 4, null, 6, 7, 8, 9]"))
	assert.NoError(t, err)
	defer withNulls.Release()
	assert.Equal(t, int64(22), util.TotalArraySize(withNulls))

	bldr := array.NewBooleanBuilder(mem)
	defer bldr.Release()

	arr := bldr.NewArray()
	defer arr.Release()

	assert.Equal(t, int64(0), util.TotalArraySize(arr))
}

func TestTotalArraySizeNested(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	arrayWithChildren, _, err := array.FromJSON(mem,
		arrow.ListOf(arrow.PrimitiveTypes.Int64),
		strings.NewReader("[[0, 1, 2, 3, 4], [5], null]"))
	assert.NoError(t, err)
	defer arrayWithChildren.Release()
	assert.Equal(t, int64(72), util.TotalArraySize(arrayWithChildren))
}

func TestTotalArraySizeRecord(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	recordBldr := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	defer recordBldr.Release()
	recordBldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	recordBldr.Field(1).(*array.Int64Builder).AppendValues([]int64{4, 5, 6}, nil)
	record := recordBldr.NewRecord()
	defer record.Release()

	assert.Equal(t, int64(44), util.TotalRecordSize(record))
}
