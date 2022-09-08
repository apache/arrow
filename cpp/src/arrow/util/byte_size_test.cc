// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/byte_size.h"

#include <gtest/gtest.h>

#include <unordered_map>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace util {

TEST(TotalBufferSize, Arrays) {
  std::shared_ptr<Array> no_nulls = ArrayFromJSON(int16(), "[1, 2, 3]");
  ASSERT_EQ(6, TotalBufferSize(*no_nulls->data()));

  std::shared_ptr<Array> with_nulls =
      ArrayFromJSON(int16(), "[1, 2, 3, 4, null, 6, 7, 8, 9]");
  ASSERT_EQ(20, TotalBufferSize(*with_nulls->data()));
}

TEST(TotalBufferSize, NestedArray) {
  std::shared_ptr<Array> array_with_children =
      ArrayFromJSON(list(int64()), "[[0, 1, 2, 3, 4], [5], null]");
  // The offsets array will have 4 4-byte offsets      (16)
  // The child array will have 6 8-byte values         (48)
  // The child array will not have a validity bitmap
  // The list array will have a 1 byte validity bitmap  (1)
  ASSERT_EQ(65, TotalBufferSize(*array_with_children));
}

TEST(TotalBufferSize, ArrayWithOffset) {
  std::shared_ptr<Array> base_array =
      ArrayFromJSON(int16(), "[1, 2, 3, 4, null, 6, 7, 8, 9]");
  std::shared_ptr<Array> sliced = base_array->Slice(8, 1);
  ASSERT_EQ(20, TotalBufferSize(*sliced));
}

TEST(TotalBufferSize, ArrayWithDict) {
  std::shared_ptr<Array> arr = ArrayFromJSON(dictionary(int32(), int8()), "[0, 0, 0]");
  ASSERT_EQ(13, TotalBufferSize(*arr));
}

TEST(TotalBufferSize, ChunkedArray) {
  ArrayVector arrays;
  for (int i = 0; i < 10; i++) {
    arrays.push_back(ConstantArrayGenerator::Zeroes(5, int32()));
  }
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> chunked_array,
                       ChunkedArray::Make(std::move(arrays)));
  ASSERT_EQ(5 * 4 * 10, TotalBufferSize(*chunked_array));
}

TEST(TotalBufferSize, RecordBatch) {
  std::shared_ptr<RecordBatch> record_batch = ConstantArrayGenerator::Zeroes(
      10, schema({field("a", int32()), field("b", int64())}));
  ASSERT_EQ(10 * 4 + 10 * 8, TotalBufferSize(*record_batch));
}

TEST(TotalBufferSize, Table) {
  ArrayVector c1_arrays, c2_arrays;
  for (int i = 0; i < 5; i++) {
    c1_arrays.push_back(ConstantArrayGenerator::Zeroes(10, int32()));
  }
  for (int i = 0; i < 10; i++) {
    c2_arrays.push_back(ConstantArrayGenerator::Zeroes(5, int64()));
  }
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> c1,
                       ChunkedArray::Make(std::move(c1_arrays)));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> c2,
                       ChunkedArray::Make(std::move(c2_arrays)));
  std::shared_ptr<Schema> schm = schema({field("a", int32()), field("b", int64())});
  std::shared_ptr<Table> table = Table::Make(std::move(schm), {c1, c2});
  ASSERT_EQ(5 * 10 * 4 + 10 * 5 * 8, TotalBufferSize(*table));
}

TEST(TotalBufferSize, SharedBuffers) {
  std::shared_ptr<Array> shared = ArrayFromJSON(int16(), "[1, 2, 3]");
  std::shared_ptr<Array> first = shared->Slice(0, 2);
  std::shared_ptr<Array> second = shared->Slice(1, 2);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> combined,
                       ChunkedArray::Make({first, second}));
  ASSERT_EQ(6, TotalBufferSize(*combined));
}

struct ExpectedRange {
  // The index of the expected buffer.  If an array shares buffers then multiple
  // ExpectedRange objects will have the same index.
  int index;
  // The start of the expected range, as an offset from the source buffer start
  uint64_t offset;
  uint64_t length;
};

std::shared_ptr<Array> ExpectedRangesToArray(
    const std::vector<ExpectedRange>& ranges,
    const std::function<uint64_t(const ExpectedRange&)>& key_func) {
  UInt64Builder builder;
  for (const auto& range : ranges) {
    ARROW_EXPECT_OK(builder.Append(key_func(range)));
  }
  std::shared_ptr<Array> arr;
  ARROW_EXPECT_OK(builder.Finish(&arr));
  return arr;
}

std::shared_ptr<Array> RangesToOffsets(const std::vector<ExpectedRange>& ranges) {
  return ExpectedRangesToArray(ranges,
                               [](const ExpectedRange& range) { return range.offset; });
}

std::shared_ptr<Array> RangesToLengths(const std::vector<ExpectedRange>& ranges) {
  return ExpectedRangesToArray(ranges,
                               [](const ExpectedRange& range) { return range.length; });
}

// We can't validate the buffer addresses exactly because they are unpredictable pointer
// values.  However, when multiple ranges come from the same buffer we can validate that
// the buffers are the same.
void CheckBufferRangeStarts(const std::shared_ptr<Array>& starts,
                            const std::vector<ExpectedRange>& expected) {
  const std::shared_ptr<UInt64Array>& starts_uint64 =
      checked_pointer_cast<UInt64Array>(starts);
  std::unordered_map<int, uint64_t> previous_buffer_starts;
  ASSERT_NE(nullptr, starts_uint64);
  const uint64_t* starts_data = starts_uint64->raw_values();
  for (std::size_t i = 0; i < expected.size(); i++) {
    const auto& previous_buffer_start = previous_buffer_starts.find(expected[i].index);
    if (previous_buffer_start == previous_buffer_starts.end()) {
      previous_buffer_starts.insert({expected[i].index, starts_data[i]});
    } else {
      ASSERT_EQ(starts_data[i], previous_buffer_start->second);
    }
  }
}

void CheckBufferRanges(const std::shared_ptr<Array>& input,
                       const std::vector<ExpectedRange>& expected) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result, ReferencedRanges(*input->data()));
  std::shared_ptr<StructArray> result_struct = checked_pointer_cast<StructArray>(result);
  ASSERT_NE(nullptr, result_struct);
  AssertArraysEqual(*result_struct->field(1), *RangesToOffsets(expected));
  AssertArraysEqual(*result_struct->field(2), *RangesToLengths(expected));
  CheckBufferRangeStarts(result_struct->field(0), expected);
}

void CheckFixedWidthStarts(const std::shared_ptr<Array>& input) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result, ReferencedRanges(*input->data()));
  uint64_t expected_values_start =
      reinterpret_cast<uint64_t>(input->data()->buffers[1]->data());
  uint64_t expected_validity_start =
      reinterpret_cast<uint64_t>(input->null_bitmap_data());
  std::shared_ptr<StructArray> result_struct = checked_pointer_cast<StructArray>(result);
  const uint64_t* raw_starts =
      checked_pointer_cast<UInt64Array>(result_struct->field(0))->raw_values();
  ASSERT_EQ(expected_validity_start, raw_starts[0]);
  ASSERT_EQ(expected_values_start, raw_starts[1]);
}

TEST(ByteRanges, StartValue) {
  std::shared_ptr<Array> bool_arr = ArrayFromJSON(
      boolean(), "[true, true, true, null, null, null, true, true, true, true]");
  CheckFixedWidthStarts(bool_arr);
  CheckFixedWidthStarts(bool_arr->Slice(9, 1));

  std::shared_ptr<Array> ts_arr =
      ArrayFromJSON(timestamp(TimeUnit::SECOND),
                    R"(["1970-01-01","2000-02-29","3989-07-14","1900-02-28", null])");
  CheckFixedWidthStarts(ts_arr);
  CheckFixedWidthStarts(ts_arr->Slice(2, 1));
}

TEST(ByteRanges, FixedWidthTypes) {
  std::shared_ptr<Array> bool_arr = ArrayFromJSON(
      boolean(), "[true, true, true, null, null, null, true, true, true, true]");
  CheckBufferRanges(bool_arr, {{0, 0, 2}, {1, 0, 2}});
  CheckBufferRanges(bool_arr->Slice(1, 8), {{0, 0, 2}, {1, 0, 2}});
  CheckBufferRanges(bool_arr->Slice(1, 5), {{0, 0, 1}, {1, 0, 1}});
  CheckBufferRanges(bool_arr->Slice(5, 5), {{0, 0, 2}, {1, 0, 2}});
  CheckBufferRanges(bool_arr->Slice(9, 1), {{0, 1, 1}, {1, 1, 1}});

  std::shared_ptr<Array> bool_arr_no_validity = ArrayFromJSON(boolean(), "[true, true]");
  CheckBufferRanges(bool_arr_no_validity, {{0, 0, 1}});

  std::shared_ptr<Array> fsb_arr =
      ArrayFromJSON(fixed_size_binary(4), R"(["foox", "barz", null])");
  CheckBufferRanges(fsb_arr, {{0, 0, 1}, {1, 0, 12}});
  CheckBufferRanges(fsb_arr->Slice(1, 1), {{0, 0, 1}, {1, 4, 4}});
}

TEST(ByteRanges, DictionaryArray) {
  std::shared_ptr<Array> dict_arr =
      ArrayFromJSON(dictionary(int16(), utf8()), R"(["x", "abc", "x", null])");
  CheckBufferRanges(dict_arr, {{0, 0, 1}, {1, 0, 8}, {2, 0, 8}, {3, 0, 4}});
  CheckBufferRanges(dict_arr->Slice(2, 2), {{0, 0, 1}, {1, 4, 4}, {2, 0, 8}, {3, 0, 4}});
}

template <typename Type>
class ByteRangesVariableBinary : public ::testing::Test {};
TYPED_TEST_SUITE(ByteRangesVariableBinary, BaseBinaryArrowTypes);

TYPED_TEST(ByteRangesVariableBinary, Basic) {
  using offset_type = typename TypeParam::offset_type;
  auto type = TypeTraits<TypeParam>::type_singleton();
  std::shared_ptr<Array> str_arr = ArrayFromJSON(type, R"(["a", "bb", "ccc", "dddd"])");
  CheckBufferRanges(str_arr, {{0, 0, 4 * sizeof(offset_type)}, {1, 0, 10}});
  CheckBufferRanges(str_arr->Slice(0, 1), {{0, 0, sizeof(offset_type)}, {1, 0, 1}});
  CheckBufferRanges(str_arr->Slice(1, 1),
                    {{0, sizeof(offset_type), sizeof(offset_type)}, {1, 1, 2}});
  CheckBufferRanges(str_arr->Slice(2, 2),
                    {{0, 2 * sizeof(offset_type), 2 * sizeof(offset_type)}, {1, 3, 7}});

  std::shared_ptr<Array> str_with_null_arr =
      ArrayFromJSON(type, R"(["a", null, "ccc", null])");
  CheckBufferRanges(str_with_null_arr,
                    {{0, 0, 1}, {1, 0, 4 * sizeof(offset_type)}, {2, 0, 4}});
  CheckBufferRanges(str_with_null_arr->Slice(0, 1),
                    {{0, 0, 1}, {1, 0, sizeof(offset_type)}, {2, 0, 1}});
  CheckBufferRanges(
      str_with_null_arr->Slice(1, 1),
      {{0, 0, 1}, {1, sizeof(offset_type), sizeof(offset_type)}, {2, 1, 0}});
  CheckBufferRanges(
      str_with_null_arr->Slice(2, 2),
      {{0, 0, 1}, {1, 2 * sizeof(offset_type), 2 * sizeof(offset_type)}, {2, 1, 3}});
}

template <typename Type>
class ByteRangesList : public ::testing::Test {};
TYPED_TEST_SUITE(ByteRangesList, ListArrowTypes);

TYPED_TEST(ByteRangesList, Basic) {
  using offset_type = typename TypeParam::offset_type;
  std::shared_ptr<DataType> type = std::make_shared<TypeParam>(int32());
  std::shared_ptr<Array> list_arr = ArrayFromJSON(type, "[[1, 2], [3], [0]]");
  CheckBufferRanges(list_arr, {{0, 0, 3 * sizeof(offset_type)}, {1, 0, 16}});
  CheckBufferRanges(list_arr->Slice(2, 1),
                    {{0, 2 * sizeof(offset_type), sizeof(offset_type)}, {1, 12, 4}});

  std::shared_ptr<Array> list_arr_with_nulls_in_items =
      ArrayFromJSON(type, "[[1, null], [3], [0]]");
  CheckBufferRanges(list_arr_with_nulls_in_items,
                    {{0, 0, 3 * sizeof(offset_type)}, {1, 0, 1}, {2, 0, 16}});
  CheckBufferRanges(list_arr_with_nulls_in_items->Slice(0, 1),
                    {{0, 0, sizeof(offset_type)}, {1, 0, 1}, {2, 0, 8}});

  std::shared_ptr<Array> list_arr_with_nulls =
      ArrayFromJSON(type, "[[1, null], null, [0]]");
  CheckBufferRanges(list_arr_with_nulls,
                    {{0, 0, 1}, {1, 0, 3 * sizeof(offset_type)}, {2, 0, 1}, {3, 0, 12}});
  CheckBufferRanges(list_arr_with_nulls->Slice(0, 2),
                    {{0, 0, 1}, {1, 0, 2 * sizeof(offset_type)}, {2, 0, 1}, {3, 0, 8}});
}

TYPED_TEST(ByteRangesList, NestedList) {
  using offset_type = typename TypeParam::offset_type;
  std::shared_ptr<DataType> type =
      std::make_shared<TypeParam>(std::make_shared<TypeParam>(int32()));
  std::shared_ptr<Array> list_arr =
      ArrayFromJSON(type, "[[[1], [2, 3, 4]], null, [[null]], [null, [5]]]");
  CheckBufferRanges(list_arr, {{0, 0, 1},
                               {1, 0, 4 * sizeof(offset_type)},
                               {2, 0, 1},
                               {3, 0, 5 * sizeof(offset_type)},
                               {4, 0, 1},
                               {5, 0, 24}});
  CheckBufferRanges(list_arr->Slice(2, 2),
                    {{0, 0, 1},
                     {1, 2 * sizeof(offset_type), 2 * sizeof(offset_type)},
                     {2, 0, 1},
                     {3, 2 * sizeof(offset_type), 3 * sizeof(offset_type)},
                     {4, 0, 1},
                     {5, 16, 8}});
}

TEST(ByteRanges, FixedSizeList) {
  std::shared_ptr<Array> list_arr = ArrayFromJSON(
      fixed_size_list(int8(), 2), "[[0, 1], [2, 3], [4, 5], [6, 7], [9, 10]]");
  CheckBufferRanges(list_arr, {{0, 0, 10}});
  CheckBufferRanges(list_arr->Slice(2, 2), {{0, 4, 4}});

  std::shared_ptr<Array> list_arr_nulls_in_items = ArrayFromJSON(
      fixed_size_list(int8(), 2), "[[0, null], [null, null], [4, 5], [6, 7], [9, 10]]");
  CheckBufferRanges(list_arr_nulls_in_items, {{0, 0, 2}, {1, 0, 10}});
  CheckBufferRanges(list_arr_nulls_in_items->Slice(4, 1), {{0, 1, 1}, {1, 8, 2}});

  std::shared_ptr<Array> list_arr_with_nulls = ArrayFromJSON(
      fixed_size_list(int8(), 2), "[[0, null], null, [4, 5], null, [9, 10]]");
  CheckBufferRanges(list_arr_with_nulls, {{0, 0, 1}, {1, 0, 2}, {2, 0, 10}});
  CheckBufferRanges(list_arr_with_nulls->Slice(4, 1), {{0, 0, 1}, {1, 1, 1}, {2, 8, 2}});
}

TEST(ByteRanges, Map) {
  std::shared_ptr<Array> map_arr = ArrayFromJSON(
      map(utf8(), uint16()), R"([[["x", 1], ["y", 2]], [["x", 3], ["y", 4]]])");
  CheckBufferRanges(map_arr, {{0, 0, 8},    // 2 32-bit outer list indices
                              {1, 0, 16},   // 4 32-bit inner list indices
                              {2, 0, 4},    // 4 keys
                              {3, 0, 8}});  // 4 values
  CheckBufferRanges(map_arr->Slice(1, 1), {{0, 4, 4}, {1, 8, 8}, {2, 2, 2}, {3, 4, 4}});
}

TEST(ByteRanges, DenseUnion) {
  std::shared_ptr<DataType> type =
      dense_union({field("a", int64()), field("b", utf8())}, {2, 5});
  std::shared_ptr<Array> union_array = ArrayFromJSON(type, R"([
      [2, null],
      [2, 222],
      [5, "hello"],
      [5, "eh"],
      [2, null],
      [2, 111],
      [5, null]
    ])");
  CheckBufferRanges(union_array, {
                                     {0, 0, 7},   // Types buffer, 1 byte per row
                                     {1, 0, 28},  // Offsets buffer, 4 bytes per row
                                     {2, 0, 1},   // int64 validity
                                     {3, 0, 32},  // int64 values
                                     {4, 0, 1},   // string validity
                                     {5, 0, 12},  // string offsets
                                     {6, 0, 7}    // string values
                                 });
  CheckBufferRanges(union_array->Slice(3, 3),
                    {
                        {0, 3, 3},    // Types buffer, 1 byte per row
                        {1, 12, 12},  // Offsets buffer, 4 bytes per row
                        {2, 0, 1},    // int64 validity
                        {3, 16, 16},  // int64 values
                        {4, 0, 1},    // string validity
                        {5, 4, 4},    // string offsets
                        {6, 5, 2}     // string values
                    });
}

TEST(ByteRanges, SparseUnion) {
  std::shared_ptr<DataType> type =
      sparse_union({field("a", int64()), field("b", utf8())}, {2, 5});
  std::shared_ptr<Array> union_array = ArrayFromJSON(type, R"([
      [2, null],
      [2, 222],
      [5, "hello"],
      [5, "eh"],
      [2, null],
      [2, 111],
      [5, null]
    ])");
  CheckBufferRanges(union_array, {
                                     {0, 0, 7},   // Types buffer, 1 byte per row
                                     {1, 0, 1},   // int64 validity
                                     {2, 0, 56},  // int64 values
                                     {3, 0, 1},   // string validity
                                     {4, 0, 28},  // string offsets
                                     {5, 0, 7}    // string values
                                 });
  CheckBufferRanges(union_array->Slice(3, 3),
                    {
                        {0, 3, 3},    // Types buffer, 1 byte per row
                        {1, 0, 1},    // int64 validity
                        {2, 24, 24},  // int64 values
                        {3, 0, 1},    // string validity
                        {4, 12, 12},  // string offsets
                        {5, 5, 2}     // string values
                    });
}

TEST(ByteRanges, ExtensionArray) {
  std::shared_ptr<Array> ext_arr = ExampleUuid();
  CheckBufferRanges(ext_arr, {{0, 0, 1}, {1, 0, 64}});
}

TEST(ByteRanges, NullArray) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> null_array, MakeArrayOfNull(null(), 42));
  CheckBufferRanges(null_array, {});
  CheckBufferRanges(null_array->Slice(15, 10), {});
}

TEST(ByteRanges, SharedArrayRange) {
  std::shared_ptr<Array> shared_arr = ArrayFromJSON(int16(), "[1, 2, 3]");
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> struct_arr,
                       StructArray::Make({shared_arr, shared_arr},
                                         {field("a", int16()), field("b", int16())}));
  CheckBufferRanges(struct_arr, {{0, 0, 6}, {0, 0, 6}});
  auto sliced = checked_pointer_cast<StructArray>(struct_arr->Slice(2, 1));
  CheckBufferRanges(struct_arr->Slice(2, 1), {{0, 4, 2}, {0, 4, 2}});
}

TEST(ByteRanges, PartialOverlapArrayRange) {
  std::shared_ptr<Array> shared_arr = ArrayFromJSON(int16(), "[1, 2, 3]");
  std::shared_ptr<Array> first = shared_arr->Slice(0, 2);
  std::shared_ptr<Array> second = shared_arr->Slice(1, 2);
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Array> struct_arr,
      StructArray::Make({first, second}, {field("a", int16()), field("b", int16())}));
  CheckBufferRanges(struct_arr, {{0, 0, 4}, {0, 2, 4}});
}

TEST(ByteRanges, ChunkedArrayNoOverlap) {
  std::shared_ptr<Array> first = ArrayFromJSON(int16(), "[1, null, 3]");
  std::shared_ptr<Array> second = ArrayFromJSON(int16(), "[4, 5, 6]");
  std::shared_ptr<ChunkedArray> chunked =
      std::make_shared<ChunkedArray>(ArrayVector{first, second});
  ASSERT_OK_AND_EQ(13, ReferencedBufferSize(*chunked));
}

TEST(ByteRanges, RecordBatchNoOverlap) {
  std::shared_ptr<Array> first = ArrayFromJSON(int16(), "[1, null, 3]");
  std::shared_ptr<Array> second = ArrayFromJSON(int16(), "[4, 5, 6]");
  std::shared_ptr<RecordBatch> record_batch =
      RecordBatch::Make(schema({field("a", int16()), field("b", int16())}),
                        first->length(), {first, second});
  ASSERT_OK_AND_EQ(13, ReferencedBufferSize(*record_batch));
}

TEST(ByteRanges, TableNoOverlap) {
  std::shared_ptr<Table> table =
      TableFromJSON(schema({field("a", int16()), field("b", int16())}),
                    {"[[1, null], [2, 2]]", "[[3, 4]]"});
  ASSERT_OK_AND_EQ(13, ReferencedBufferSize(*table));
}

}  // namespace util
}  // namespace arrow
