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

package pqarrow

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNonNullableSingleList(t *testing.T) {
	// translates to the following parquet schema:
	// required group bag {
	//   repeated group [unseen] (List) {
	//		 required int64 Entires;
	//	 }
	// }
	// So:
	// def level 0: a null entry
	// def level 1: a non-null entry
	bldr := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
	defer bldr.Release()

	vb := bldr.ValueBuilder().(*array.Int64Builder)

	bldr.Append(true)
	vb.Append(1)

	bldr.Append(true)
	vb.Append(2)
	vb.Append(3)

	bldr.Append(true)
	vb.Append(4)
	vb.Append(5)
	vb.Append(6)

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, false)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{2, 2, 2, 2, 2, 2}, result.defLevels)
	assert.Equal(t, []int16{0, 0, 1, 0, 1, 1}, result.repLevels)
	assert.Len(t, result.postListVisitedElems, 1)
	assert.EqualValues(t, 0, result.postListVisitedElems[0].start)
	assert.EqualValues(t, 6, result.postListVisitedElems[0].end)
}

// next group of tests translates to the following parquet schema:
// optional group bag {
//   repeated group [unseen] (List) {
//		 optional int64 Entires;
//	 }
// }
// So:
// def level 0: a null list
// def level 1: an empty list
// def level 2: a null entry
// def level 3: a non-null entry

func TestNullableSingleListAllNulls(t *testing.T) {
	bldr := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
	defer bldr.Release()

	bldr.AppendNull()
	bldr.AppendNull()
	bldr.AppendNull()
	bldr.AppendNull()

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{0, 0, 0, 0}, result.defLevels)
	assert.Equal(t, []int16{0, 0, 0, 0}, result.repLevels)
}

func TestNullableSingleListAllEmpty(t *testing.T) {
	bldr := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
	defer bldr.Release()

	bldr.Append(true)
	bldr.Append(true)
	bldr.Append(true)
	bldr.Append(true)

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{1, 1, 1, 1}, result.defLevels)
	assert.Equal(t, []int16{0, 0, 0, 0}, result.repLevels)
}

func TestNullableSingleListAllNullEntries(t *testing.T) {
	bldr := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
	defer bldr.Release()

	vb := bldr.ValueBuilder().(*array.Int64Builder)

	bldr.Append(true)
	vb.AppendNull()
	bldr.Append(true)
	vb.AppendNull()
	bldr.Append(true)
	vb.AppendNull()
	bldr.Append(true)
	vb.AppendNull()

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{2, 2, 2, 2}, result.defLevels)
	assert.Equal(t, []int16{0, 0, 0, 0}, result.repLevels)
	assert.Len(t, result.postListVisitedElems, 1)
	assert.EqualValues(t, 0, result.postListVisitedElems[0].start)
	assert.EqualValues(t, 4, result.postListVisitedElems[0].end)
}

func TestNullableSingleListAllPresentEntries(t *testing.T) {
	bldr := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
	defer bldr.Release()

	vb := bldr.ValueBuilder().(*array.Int64Builder)

	bldr.Append(true)
	bldr.Append(true)
	bldr.Append(true)
	vb.Append(1)
	bldr.Append(true)
	bldr.Append(true)
	vb.Append(2)
	vb.Append(3)

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{1, 1, 3, 1, 3, 3}, result.defLevels)
	assert.Equal(t, []int16{0, 0, 0, 0, 0, 1}, result.repLevels)
	assert.Len(t, result.postListVisitedElems, 1)
	assert.EqualValues(t, 0, result.postListVisitedElems[0].start)
	assert.EqualValues(t, 3, result.postListVisitedElems[0].end)
}

func TestNullableSingleListSomeNullEntriesSomeNullLists(t *testing.T) {
	bldr := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
	defer bldr.Release()

	vb := bldr.ValueBuilder().(*array.Int64Builder)

	bldr.Append(false)
	bldr.Append(true)
	vb.AppendValues([]int64{1, 2, 3}, nil)
	bldr.Append(true)
	bldr.Append(true)
	bldr.AppendNull()
	bldr.AppendNull()
	bldr.Append(true)
	vb.AppendValues([]int64{4, 5}, nil)
	bldr.Append(true)
	vb.AppendNull()

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{0, 3, 3, 3, 1, 1, 0, 0, 3, 3, 2}, result.defLevels)
	assert.Equal(t, []int16{0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0}, result.repLevels)
}

// next group of tests translate to the following parquet schema:
//
// optional group bag {
//   repeated group outer_list (List) {
//     optional group nullable {
//       repeated group inner_list (List) {
//         optional int64 Entries;
//       }
//     }
//   }
// }
// So:
// def level 0: null outer list
// def level 1: empty outer list
// def level 2: null inner list
// def level 3: empty inner list
// def level 4: null entry
// def level 5: non-null entry

func TestNestedListsWithSomeEntries(t *testing.T) {
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int64)
	bldr := array.NewListBuilder(memory.DefaultAllocator, listType)
	defer bldr.Release()

	nestedBldr := bldr.ValueBuilder().(*array.ListBuilder)
	vb := nestedBldr.ValueBuilder().(*array.Int64Builder)

	// produce: [null, [[1, 2, 3], [4, 5]], [[], [], []], []]

	bldr.AppendNull()
	bldr.Append(true)
	nestedBldr.Append(true)
	vb.AppendValues([]int64{1, 2, 3}, nil)
	nestedBldr.Append(true)
	vb.AppendValues([]int64{4, 5}, nil)

	bldr.Append(true)
	nestedBldr.Append(true)
	nestedBldr.Append(true)
	nestedBldr.Append(true)
	bldr.Append(true)

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{0, 5, 5, 5, 5, 5, 3, 3, 3, 1}, result.defLevels)
	assert.Equal(t, []int16{0, 0, 2, 2, 1, 2, 0, 1, 1, 0}, result.repLevels)
}

func TestNestedListsWithSomeNulls(t *testing.T) {
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int64)
	bldr := array.NewListBuilder(memory.DefaultAllocator, listType)
	defer bldr.Release()

	nestedBldr := bldr.ValueBuilder().(*array.ListBuilder)
	vb := nestedBldr.ValueBuilder().(*array.Int64Builder)

	// produce: [null, [[1, null, 3], null, null], [[4, 5]]]

	bldr.AppendNull()
	bldr.Append(true)
	nestedBldr.Append(true)
	vb.AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
	nestedBldr.AppendNull()
	nestedBldr.AppendNull()
	bldr.Append(true)
	nestedBldr.Append(true)
	vb.AppendValues([]int64{4, 5}, nil)

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{0, 5, 4, 5, 2, 2, 5, 5}, result.defLevels)
	assert.Equal(t, []int16{0, 0, 2, 2, 1, 1, 0, 2}, result.repLevels)
}

func TestNestedListsSomeNullsSomeEmpty(t *testing.T) {
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int64)
	bldr := array.NewListBuilder(memory.DefaultAllocator, listType)
	defer bldr.Release()

	nestedBldr := bldr.ValueBuilder().(*array.ListBuilder)
	vb := nestedBldr.ValueBuilder().(*array.Int64Builder)

	// produce: [null, [[1, null, 3], [], []], [[4, 5]]]

	bldr.AppendNull()
	bldr.Append(true)
	nestedBldr.Append(true)
	vb.AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
	nestedBldr.Append(true)
	nestedBldr.Append(true)
	bldr.Append(true)
	nestedBldr.Append(true)
	vb.AppendValues([]int64{4, 5}, nil)

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{0, 5, 4, 5, 3, 3, 5, 5}, result.defLevels)
	assert.Equal(t, []int16{0, 0, 2, 2, 1, 1, 0, 2}, result.repLevels)
}

func TestNestedExtensionListsWithSomeNulls(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	listType := arrow.ListOf(types.NewUUIDType())
	bldr := array.NewListBuilder(mem, listType)
	defer bldr.Release()

	nestedBldr := bldr.ValueBuilder().(*array.ListBuilder)
	vb := nestedBldr.ValueBuilder().(*types.UUIDBuilder)

	uuid1 := uuid.New()
	uuid3 := uuid.New()
	uuid4 := uuid.New()
	uuid5 := uuid.New()

	// produce: [null, [[uuid1, null, uuid3], null, null], [[uuid4, uuid5]]]

	bldr.AppendNull()
	bldr.Append(true)
	nestedBldr.Append(true)
	vb.Append(uuid1)
	vb.AppendNull()
	vb.Append(uuid3)
	nestedBldr.AppendNull()
	nestedBldr.AppendNull()
	bldr.Append(true)
	nestedBldr.Append(true)
	vb.AppendValues([]uuid.UUID{uuid4, uuid5}, nil)

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{0, 5, 4, 5, 2, 2, 5, 5}, result.defLevels)
	assert.Equal(t, []int16{0, 0, 2, 2, 1, 1, 0, 2}, result.repLevels)
	assert.Equal(t, result.leafArr.NullN(), 1)
}

// triplenested translates to parquet:
//
// optional group bag {
//   repeated group outer_list (List) {
//     option group nullable {
//       repeated group middle_list (List) {
//         option group nullable {
//           repeated group inner_list (List) {
//              optional int64 Entries;
//           }
//         }
//       }
//     }
//   }
// }
// So:
// def level 0: a outer list
// def level 1: an empty outer list
// def level 2: a null middle list
// def level 3: an empty middle list
// def level 4: an null inner list
// def level 5: an empty inner list
// def level 6: a null entry
// def level 7: a non-null entry

func TestTripleNestedAllPresent(t *testing.T) {
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int64)
	nestedListType := arrow.ListOf(listType)
	bldr := array.NewListBuilder(memory.DefaultAllocator, nestedListType)
	defer bldr.Release()

	dblNestedBldr := bldr.ValueBuilder().(*array.ListBuilder)
	nestedBldr := dblNestedBldr.ValueBuilder().(*array.ListBuilder)
	vb := nestedBldr.ValueBuilder().(*array.Int64Builder)

	// produce: [ [[[1, 2, 3], [4, 5, 6]], [[7, 8, 9]]] ]
	bldr.Append(true)
	dblNestedBldr.Append(true)
	nestedBldr.Append(true)
	vb.AppendValues([]int64{1, 2, 3}, nil)
	nestedBldr.Append(true)
	vb.AppendValues([]int64{4, 5, 6}, nil)

	dblNestedBldr.Append(true)
	nestedBldr.Append(true)
	vb.AppendValues([]int64{7, 8, 9}, nil)

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{7, 7, 7, 7, 7, 7, 7, 7, 7}, result.defLevels)
	assert.Equal(t, []int16{0, 3, 3, 2, 3, 3, 1, 3, 3}, result.repLevels)
}

func TestTripleNestedSomeNullsSomeEmpty(t *testing.T) {
	listType := arrow.ListOf(arrow.PrimitiveTypes.Int64)
	nestedListType := arrow.ListOf(listType)
	bldr := array.NewListBuilder(memory.DefaultAllocator, nestedListType)
	defer bldr.Release()

	dblNestedBldr := bldr.ValueBuilder().(*array.ListBuilder)
	nestedBldr := dblNestedBldr.ValueBuilder().(*array.ListBuilder)
	vb := nestedBldr.ValueBuilder().(*array.Int64Builder)

	// produce: [
	//	  [null, [[1, null, 3], []], []],     first row
	//    [[[]], [[], [1, 2]], null, [[3]]],  second row
	//    null,                               third row
	//    []                                  fourth row
	//  ]

	// first row
	bldr.Append(true)
	dblNestedBldr.AppendNull()
	dblNestedBldr.Append(true)
	nestedBldr.Append(true)
	vb.AppendValues([]int64{1, 0, 3}, []bool{true, false, true})
	nestedBldr.Append(true)
	dblNestedBldr.Append(true)

	// second row
	bldr.Append(true)
	dblNestedBldr.Append(true)
	nestedBldr.Append(true)
	dblNestedBldr.Append(true)
	nestedBldr.Append(true)
	nestedBldr.Append(true)
	vb.AppendValues([]int64{1, 2}, nil)
	dblNestedBldr.AppendNull()
	dblNestedBldr.Append(true)
	nestedBldr.Append(true)
	vb.Append(3)

	// third row
	bldr.AppendNull()

	// fourth row
	bldr.Append(true)

	arr := bldr.NewListArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Equal(t, []int16{
		2, 7, 6, 7, 5, 3, // first row
		5, 5, 7, 7, 2, 7, // second row
		0, // third row
		1,
	}, result.defLevels)
	assert.Equal(t, []int16{
		0, 1, 3, 3, 2, 1, // first row
		0, 1, 2, 3, 1, 1, // second row
		0, 0,
	}, result.repLevels)
}

func TestStruct(t *testing.T) {
	structType := arrow.StructOf(arrow.Field{Name: "list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		arrow.Field{Name: "Entries", Type: arrow.PrimitiveTypes.Int64, Nullable: true})

	bldr := array.NewStructBuilder(memory.DefaultAllocator, structType)
	defer bldr.Release()

	entryBldr := bldr.FieldBuilder(1).(*array.Int64Builder)
	listBldr := bldr.FieldBuilder(0).(*array.ListBuilder)
	vb := listBldr.ValueBuilder().(*array.Int64Builder)

	// produce: [ {"Entries": 1, "list": [2, 3]}, {"Entries": 4, "list": [5, 6]}, null]

	bldr.Append(true)
	entryBldr.Append(1)
	listBldr.Append(true)
	vb.AppendValues([]int64{2, 3}, nil)

	bldr.Append(true)
	entryBldr.Append(4)
	listBldr.Append(true)
	vb.AppendValues([]int64{5, 6}, nil)

	bldr.AppendNull()

	arr := bldr.NewArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.writeAll(ctx)
	require.NoError(t, err)

	assert.Len(t, result, 2)
	assert.Equal(t, []int16{4, 4, 4, 4, 0}, result[0].defLevels)
	assert.Equal(t, []int16{0, 1, 0, 1, 0}, result[0].repLevels)

	assert.Equal(t, []int16{2, 2, 0}, result[1].defLevels)
	assert.Nil(t, result[1].repLevels)
}

func TestFixedSizeListNullableElems(t *testing.T) {
	bldr := array.NewFixedSizeListBuilder(memory.DefaultAllocator, 2, arrow.PrimitiveTypes.Int64)
	defer bldr.Release()

	vb := bldr.ValueBuilder().(*array.Int64Builder)
	bldr.AppendValues([]bool{false, true, true, false})
	vb.AppendValues([]int64{2, 3, 4, 5}, nil)

	// produce: [null, [2, 3], [4, 5], null]

	arr := bldr.NewArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.writeAll(ctx)
	require.NoError(t, err)

	assert.Len(t, result, 1)
	assert.Equal(t, []int16{0, 3, 3, 3, 3, 0}, result[0].defLevels)
	assert.Equal(t, []int16{0, 0, 1, 0, 1, 0}, result[0].repLevels)

	// null slots take up space in a fixed size list (they can in variable
	// size lists as well) but the actual written values are only the middle
	// elements
	assert.Len(t, result[0].postListVisitedElems, 1)
	assert.EqualValues(t, 2, result[0].postListVisitedElems[0].start)
	assert.EqualValues(t, 6, result[0].postListVisitedElems[0].end)
}

func TestFixedSizeListMissingMiddleTwoVisitedRanges(t *testing.T) {
	bldr := array.NewFixedSizeListBuilder(memory.DefaultAllocator, 2, arrow.PrimitiveTypes.Int64)
	defer bldr.Release()

	vb := bldr.ValueBuilder().(*array.Int64Builder)
	bldr.AppendValues([]bool{true, false, true})
	vb.AppendValues([]int64{0, 1, 2, 3}, nil)

	// produce: [[0, 1], null, [2, 3]]

	arr := bldr.NewArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, true)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.writeAll(ctx)
	require.NoError(t, err)

	assert.Len(t, result, 1)
	assert.Equal(t, []int16{3, 3, 0, 3, 3}, result[0].defLevels)
	assert.Equal(t, []int16{0, 1, 0, 0, 1}, result[0].repLevels)

	// null slots take up space in a fixed size list (they can in variable
	// size lists as well) but the actual written values are only the middle
	// elements
	assert.Len(t, result[0].postListVisitedElems, 2)
	assert.EqualValues(t, 0, result[0].postListVisitedElems[0].start)
	assert.EqualValues(t, 2, result[0].postListVisitedElems[0].end)

	assert.EqualValues(t, 4, result[0].postListVisitedElems[1].start)
	assert.EqualValues(t, 6, result[0].postListVisitedElems[1].end)
}

func TestPrimitiveNonNullable(t *testing.T) {
	bldr := array.NewInt64Builder(memory.DefaultAllocator)
	defer bldr.Release()

	bldr.AppendValues([]int64{1, 2, 3, 4}, nil)

	arr := bldr.NewArray()
	defer arr.Release()

	mp, err := newMultipathLevelBuilder(arr, false)
	require.NoError(t, err)
	defer mp.Release()

	ctx := arrowCtxFromContext(NewArrowWriteContext(context.Background(), nil))
	result, err := mp.write(0, ctx)
	require.NoError(t, err)

	assert.Nil(t, result.defLevels)
	assert.Nil(t, result.repLevels)

	assert.Len(t, result.postListVisitedElems, 1)
	assert.EqualValues(t, 0, result.postListVisitedElems[0].start)
	assert.EqualValues(t, 4, result.postListVisitedElems[0].end)
}
