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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/ipc/test-common.h"
#include "arrow/memory_pool.h"
#include "arrow/pretty_print.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"

namespace arrow {
namespace ipc {
namespace test {

void CompareArraysDetailed(int index, const Array& result, const Array& expected) {
  if (!expected.Equals(result)) {
    std::stringstream pp_result;
    std::stringstream pp_expected;

    ASSERT_OK(PrettyPrint(expected, 0, &pp_expected));
    ASSERT_OK(PrettyPrint(result, 0, &pp_result));

    FAIL() << "Index: " << index << " Expected: " << pp_expected.str()
           << "\nGot: " << pp_result.str();
  }
}

void CompareBatchColumnsDetailed(const RecordBatch& result, const RecordBatch& expected) {
  for (int i = 0; i < expected.num_columns(); ++i) {
    auto left = result.column(i);
    auto right = expected.column(i);
    CompareArraysDetailed(i, *left, *right);
  }
}

Status MakeRandomInt32Array(int64_t length, bool include_nulls, MemoryPool* pool,
                            std::shared_ptr<Array>* out, uint32_t seed) {
  random::RandomArrayGenerator rand(seed);
  const double null_probability = include_nulls ? 0.5 : 0.0;

  *out = rand.Int32(length, 0, 1000, null_probability);

  return Status::OK();
}

Status MakeRandomListArray(const std::shared_ptr<Array>& child_array, int num_lists,
                           bool include_nulls, MemoryPool* pool,
                           std::shared_ptr<Array>* out) {
  // Create the null list values
  std::vector<uint8_t> valid_lists(num_lists);
  const double null_percent = include_nulls ? 0.1 : 0;
  random_null_bytes(num_lists, null_percent, valid_lists.data());

  // Create list offsets
  const int max_list_size = 10;

  std::vector<int32_t> list_sizes(num_lists, 0);
  std::vector<int32_t> offsets(
      num_lists + 1, 0);  // +1 so we can shift for nulls. See partial sum below.
  const uint32_t seed = static_cast<uint32_t>(child_array->length());

  if (num_lists > 0) {
    rand_uniform_int(num_lists, seed, 0, max_list_size, list_sizes.data());
    // make sure sizes are consistent with null
    std::transform(list_sizes.begin(), list_sizes.end(), valid_lists.begin(),
                   list_sizes.begin(),
                   [](int32_t size, int32_t valid) { return valid == 0 ? 0 : size; });
    std::partial_sum(list_sizes.begin(), list_sizes.end(), ++offsets.begin());

    // Force invariants
    const int32_t child_length = static_cast<int32_t>(child_array->length());
    offsets[0] = 0;
    std::replace_if(offsets.begin(), offsets.end(),
                    [child_length](int32_t offset) { return offset > child_length; },
                    child_length);
  }

  offsets[num_lists] = static_cast<int32_t>(child_array->length());

  /// TODO(wesm): Implement support for nulls in ListArray::FromArrays
  std::shared_ptr<Buffer> null_bitmap, offsets_buffer;
  RETURN_NOT_OK(GetBitmapFromVector(valid_lists, &null_bitmap));
  RETURN_NOT_OK(CopyBufferFromVector(offsets, pool, &offsets_buffer));

  *out = std::make_shared<ListArray>(list(child_array->type()), num_lists, offsets_buffer,
                                     child_array, null_bitmap, kUnknownNullCount);
  return ValidateArray(**out);
}

Status MakeRandomBooleanArray(const int length, bool include_nulls,
                              std::shared_ptr<Array>* out) {
  std::vector<uint8_t> values(length);
  random_null_bytes(length, 0.5, values.data());
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(BitUtil::BytesToBits(values, default_memory_pool(), &data));

  if (include_nulls) {
    std::vector<uint8_t> valid_bytes(length);
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(BitUtil::BytesToBits(valid_bytes, default_memory_pool(), &null_bitmap));
    random_null_bytes(length, 0.1, valid_bytes.data());
    *out = std::make_shared<BooleanArray>(length, data, null_bitmap, -1);
  } else {
    *out = std::make_shared<BooleanArray>(length, data, NULLPTR, 0);
  }
  return Status::OK();
}

Status MakeBooleanBatchSized(const int length, std::shared_ptr<RecordBatch>* out) {
  // Make the schema
  auto f0 = field("f0", boolean());
  auto f1 = field("f1", boolean());
  auto schema = ::arrow::schema({f0, f1});

  std::shared_ptr<Array> a0, a1;
  RETURN_NOT_OK(MakeRandomBooleanArray(length, true, &a0));
  RETURN_NOT_OK(MakeRandomBooleanArray(length, false, &a1));
  *out = RecordBatch::Make(schema, length, {a0, a1});
  return Status::OK();
}

Status MakeBooleanBatch(std::shared_ptr<RecordBatch>* out) {
  return MakeBooleanBatchSized(1000, out);
}

Status MakeIntBatchSized(int length, std::shared_ptr<RecordBatch>* out, uint32_t seed) {
  // Make the schema
  auto f0 = field("f0", int32());
  auto f1 = field("f1", int32());
  auto schema = ::arrow::schema({f0, f1});

  // Example data
  std::shared_ptr<Array> a0, a1;
  MemoryPool* pool = default_memory_pool();
  RETURN_NOT_OK(MakeRandomInt32Array(length, false, pool, &a0, seed));
  RETURN_NOT_OK(MakeRandomInt32Array(length, true, pool, &a1, seed + 1));
  *out = RecordBatch::Make(schema, length, {a0, a1});
  return Status::OK();
}

Status MakeIntRecordBatch(std::shared_ptr<RecordBatch>* out) {
  return MakeIntBatchSized(10, out);
}

Status MakeRandomStringArray(int64_t length, bool include_nulls, MemoryPool* pool,
                             std::shared_ptr<Array>* out) {
  const std::vector<std::string> values = {"",    "",          "abc",  "123",
                                           "efg", "456!@#!@#", "12312"};
  StringBuilder builder(pool);
  const size_t values_len = values.size();
  for (int64_t i = 0; i < length; ++i) {
    int64_t values_index = i % values_len;
    if (include_nulls && values_index == 0) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      const auto& value = values[values_index];
      RETURN_NOT_OK(builder.Append(value));
    }
  }
  return builder.Finish(out);
}

template <class Builder, class RawType>
static Status MakeBinaryArrayWithUniqueValues(int64_t length, bool include_nulls,
                                              MemoryPool* pool,
                                              std::shared_ptr<Array>* out) {
  Builder builder(pool);
  for (int64_t i = 0; i < length; ++i) {
    if (include_nulls && (i % 7 == 0)) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      const std::string value = std::to_string(i);
      RETURN_NOT_OK(builder.Append(reinterpret_cast<const RawType*>(value.data()),
                                   static_cast<int32_t>(value.size())));
    }
  }
  return builder.Finish(out);
}

Status MakeStringTypesRecordBatch(std::shared_ptr<RecordBatch>* out, bool with_nulls) {
  const int64_t length = 500;
  auto string_type = utf8();
  auto binary_type = binary();
  auto f0 = field("f0", string_type);
  auto f1 = field("f1", binary_type);
  auto schema = ::arrow::schema({f0, f1});

  std::shared_ptr<Array> a0, a1;
  MemoryPool* pool = default_memory_pool();

  // Quirk with RETURN_NOT_OK macro and templated functions
  {
    auto s = MakeBinaryArrayWithUniqueValues<StringBuilder, char>(length, with_nulls,
                                                                  pool, &a0);
    RETURN_NOT_OK(s);
  }

  {
    auto s = MakeBinaryArrayWithUniqueValues<BinaryBuilder, uint8_t>(length, with_nulls,
                                                                     pool, &a1);
    RETURN_NOT_OK(s);
  }
  *out = RecordBatch::Make(schema, length, {a0, a1});
  return Status::OK();
}

Status MakeStringTypesRecordBatchWithNulls(std::shared_ptr<RecordBatch>* out) {
  return MakeStringTypesRecordBatch(out, true);
}

Status MakeNullRecordBatch(std::shared_ptr<RecordBatch>* out) {
  const int64_t length = 500;
  auto f0 = field("f0", null());
  auto schema = ::arrow::schema({f0});
  std::shared_ptr<Array> a0 = std::make_shared<NullArray>(length);
  *out = RecordBatch::Make(schema, length, {a0});
  return Status::OK();
}

Status MakeListRecordBatch(std::shared_ptr<RecordBatch>* out) {
  // Make the schema
  auto f0 = field("f0", list(int32()));
  auto f1 = field("f1", list(list(int32())));
  auto f2 = field("f2", int32());
  auto schema = ::arrow::schema({f0, f1, f2});

  // Example data

  MemoryPool* pool = default_memory_pool();
  const int length = 200;
  std::shared_ptr<Array> leaf_values, list_array, list_list_array, flat_array;
  const bool include_nulls = true;
  RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool, &leaf_values));
  RETURN_NOT_OK(
      MakeRandomListArray(leaf_values, length, include_nulls, pool, &list_array));
  RETURN_NOT_OK(
      MakeRandomListArray(list_array, length, include_nulls, pool, &list_list_array));
  RETURN_NOT_OK(MakeRandomInt32Array(length, include_nulls, pool, &flat_array));
  *out = RecordBatch::Make(schema, length, {list_array, list_list_array, flat_array});
  return Status::OK();
}

Status MakeFixedSizeListRecordBatch(std::shared_ptr<RecordBatch>* out) {
  // Make the schema
  auto f0 = field("f0", fixed_size_list(int32(), 1));
  auto f1 = field("f1", fixed_size_list(list(int32()), 3));
  auto f2 = field("f2", int32());
  auto schema = ::arrow::schema({f0, f1, f2});

  // Example data

  MemoryPool* pool = default_memory_pool();
  const int length = 200;
  std::shared_ptr<Array> leaf_values, list_array, list_list_array, flat_array;
  const bool include_nulls = true;
  RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool, &leaf_values));
  RETURN_NOT_OK(
      MakeRandomListArray(leaf_values, length * 3, include_nulls, pool, &list_array));
  list_list_array = std::make_shared<FixedSizeListArray>(f1->type(), length, list_array);
  list_array = std::make_shared<FixedSizeListArray>(f0->type(), length,
                                                    leaf_values->Slice(0, length));
  RETURN_NOT_OK(MakeRandomInt32Array(length, include_nulls, pool, &flat_array));
  *out = RecordBatch::Make(schema, length, {list_array, list_list_array, flat_array});
  return Status::OK();
}

Status MakeZeroLengthRecordBatch(std::shared_ptr<RecordBatch>* out) {
  // Make the schema
  auto f0 = field("f0", list(int32()));
  auto f1 = field("f1", list(list(int32())));
  auto f2 = field("f2", int32());
  auto schema = ::arrow::schema({f0, f1, f2});

  // Example data
  MemoryPool* pool = default_memory_pool();
  const bool include_nulls = true;
  std::shared_ptr<Array> leaf_values, list_array, list_list_array, flat_array;
  RETURN_NOT_OK(MakeRandomInt32Array(0, include_nulls, pool, &leaf_values));
  RETURN_NOT_OK(MakeRandomListArray(leaf_values, 0, include_nulls, pool, &list_array));
  RETURN_NOT_OK(
      MakeRandomListArray(list_array, 0, include_nulls, pool, &list_list_array));
  RETURN_NOT_OK(MakeRandomInt32Array(0, include_nulls, pool, &flat_array));
  *out = RecordBatch::Make(schema, 0, {list_array, list_list_array, flat_array});
  return Status::OK();
}

Status MakeNonNullRecordBatch(std::shared_ptr<RecordBatch>* out) {
  // Make the schema
  auto f0 = field("f0", list(int32()));
  auto f1 = field("f1", list(list(int32())));
  auto f2 = field("f2", int32());
  auto schema = ::arrow::schema({f0, f1, f2});

  // Example data
  MemoryPool* pool = default_memory_pool();
  const int length = 50;
  std::shared_ptr<Array> leaf_values, list_array, list_list_array, flat_array;

  RETURN_NOT_OK(MakeRandomInt32Array(1000, true, pool, &leaf_values));
  bool include_nulls = false;
  RETURN_NOT_OK(
      MakeRandomListArray(leaf_values, length, include_nulls, pool, &list_array));
  RETURN_NOT_OK(
      MakeRandomListArray(list_array, length, include_nulls, pool, &list_list_array));
  RETURN_NOT_OK(MakeRandomInt32Array(length, include_nulls, pool, &flat_array));
  *out = RecordBatch::Make(schema, length, {list_array, list_list_array, flat_array});
  return Status::OK();
}

Status MakeDeeplyNestedList(std::shared_ptr<RecordBatch>* out) {
  const int batch_length = 5;
  auto type = int32();

  MemoryPool* pool = default_memory_pool();
  std::shared_ptr<Array> array;
  const bool include_nulls = true;
  RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool, &array));
  for (int i = 0; i < 63; ++i) {
    type = std::static_pointer_cast<DataType>(list(type));
    RETURN_NOT_OK(MakeRandomListArray(array, batch_length, include_nulls, pool, &array));
  }

  auto f0 = field("f0", type);
  auto schema = ::arrow::schema({f0});
  std::vector<std::shared_ptr<Array>> arrays = {array};
  *out = RecordBatch::Make(schema, batch_length, arrays);
  return Status::OK();
}

Status MakeStruct(std::shared_ptr<RecordBatch>* out) {
  // reuse constructed list columns
  std::shared_ptr<RecordBatch> list_batch;
  RETURN_NOT_OK(MakeListRecordBatch(&list_batch));
  std::vector<std::shared_ptr<Array>> columns = {
      list_batch->column(0), list_batch->column(1), list_batch->column(2)};
  auto list_schema = list_batch->schema();

  // Define schema
  std::shared_ptr<DataType> type(new StructType(
      {list_schema->field(0), list_schema->field(1), list_schema->field(2)}));
  auto f0 = field("non_null_struct", type);
  auto f1 = field("null_struct", type);
  auto schema = ::arrow::schema({f0, f1});

  // construct individual nullable/non-nullable struct arrays
  std::shared_ptr<Array> no_nulls(new StructArray(type, list_batch->num_rows(), columns));
  std::vector<uint8_t> null_bytes(list_batch->num_rows(), 1);
  null_bytes[0] = 0;
  std::shared_ptr<Buffer> null_bitmask;
  RETURN_NOT_OK(BitUtil::BytesToBits(null_bytes, default_memory_pool(), &null_bitmask));
  std::shared_ptr<Array> with_nulls(
      new StructArray(type, list_batch->num_rows(), columns, null_bitmask, 1));

  // construct batch
  std::vector<std::shared_ptr<Array>> arrays = {no_nulls, with_nulls};
  *out = RecordBatch::Make(schema, list_batch->num_rows(), arrays);
  return Status::OK();
}

Status MakeUnion(std::shared_ptr<RecordBatch>* out) {
  // Define schema
  std::vector<std::shared_ptr<Field>> union_types(
      {field("u0", int32()), field("u1", uint8())});

  std::vector<uint8_t> type_codes = {5, 10};
  auto sparse_type =
      std::make_shared<UnionType>(union_types, type_codes, UnionMode::SPARSE);

  auto dense_type =
      std::make_shared<UnionType>(union_types, type_codes, UnionMode::DENSE);

  auto f0 = field("sparse_nonnull", sparse_type, false);
  auto f1 = field("sparse", sparse_type);
  auto f2 = field("dense", dense_type);

  auto schema = ::arrow::schema({f0, f1, f2});

  // Create data
  std::vector<std::shared_ptr<Array>> sparse_children(2);
  std::vector<std::shared_ptr<Array>> dense_children(2);

  const int64_t length = 7;

  std::shared_ptr<Buffer> type_ids_buffer;
  std::vector<uint8_t> type_ids = {5, 10, 5, 5, 10, 10, 5};
  RETURN_NOT_OK(CopyBufferFromVector(type_ids, default_memory_pool(), &type_ids_buffer));

  std::vector<int32_t> u0_values = {0, 1, 2, 3, 4, 5, 6};
  ArrayFromVector<Int32Type, int32_t>(u0_values, &sparse_children[0]);

  std::vector<uint8_t> u1_values = {10, 11, 12, 13, 14, 15, 16};
  ArrayFromVector<UInt8Type, uint8_t>(u1_values, &sparse_children[1]);

  // dense children
  u0_values = {0, 2, 3, 7};
  ArrayFromVector<Int32Type, int32_t>(u0_values, &dense_children[0]);

  u1_values = {11, 14, 15};
  ArrayFromVector<UInt8Type, uint8_t>(u1_values, &dense_children[1]);

  std::shared_ptr<Buffer> offsets_buffer;
  std::vector<int32_t> offsets = {0, 0, 1, 2, 1, 2, 3};
  RETURN_NOT_OK(CopyBufferFromVector(offsets, default_memory_pool(), &offsets_buffer));

  std::vector<uint8_t> null_bytes(length, 1);
  null_bytes[2] = 0;
  std::shared_ptr<Buffer> null_bitmask;
  RETURN_NOT_OK(BitUtil::BytesToBits(null_bytes, default_memory_pool(), &null_bitmask));

  // construct individual nullable/non-nullable struct arrays
  auto sparse_no_nulls =
      std::make_shared<UnionArray>(sparse_type, length, sparse_children, type_ids_buffer);
  auto sparse = std::make_shared<UnionArray>(sparse_type, length, sparse_children,
                                             type_ids_buffer, NULLPTR, null_bitmask, 1);

  auto dense =
      std::make_shared<UnionArray>(dense_type, length, dense_children, type_ids_buffer,
                                   offsets_buffer, null_bitmask, 1);

  // construct batch
  std::vector<std::shared_ptr<Array>> arrays = {sparse_no_nulls, sparse, dense};
  *out = RecordBatch::Make(schema, length, arrays);
  return Status::OK();
}

Status MakeDictionary(std::shared_ptr<RecordBatch>* out) {
  const int64_t length = 6;

  std::vector<bool> is_valid = {true, true, false, true, true, true};

  auto dict1 = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\"]");
  auto dict2 = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\", \"qux\"]");

  auto f0_type = arrow::dictionary(arrow::int32(), dict1);
  auto f1_type = arrow::dictionary(arrow::int8(), dict1, true);
  auto f2_type = arrow::dictionary(arrow::int32(), dict2);

  std::shared_ptr<Array> indices0, indices1, indices2;
  std::vector<int32_t> indices0_values = {1, 2, -1, 0, 2, 0};
  std::vector<int8_t> indices1_values = {0, 0, 2, 2, 1, 1};
  std::vector<int32_t> indices2_values = {3, 0, 2, 1, 0, 2};

  ArrayFromVector<Int32Type, int32_t>(is_valid, indices0_values, &indices0);
  ArrayFromVector<Int8Type, int8_t>(is_valid, indices1_values, &indices1);
  ArrayFromVector<Int32Type, int32_t>(is_valid, indices2_values, &indices2);

  auto a0 = std::make_shared<DictionaryArray>(f0_type, indices0);
  auto a1 = std::make_shared<DictionaryArray>(f1_type, indices1);
  auto a2 = std::make_shared<DictionaryArray>(f2_type, indices2);

  // Lists of dictionary-encoded strings
  auto f3_type = list(f1_type);

  auto indices3 = ArrayFromJSON(int8(), "[0, 1, 2, 0, 1, 1, 2, 1, 0]");
  auto offsets3 = ArrayFromJSON(int32(), "[0, 0, 2, 2, 5, 6, 9]");

  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(GetBitmapFromVector(is_valid, &null_bitmap));

  std::shared_ptr<Array> a3 = std::make_shared<ListArray>(
      f3_type, length, std::static_pointer_cast<PrimitiveArray>(offsets3)->values(),
      std::make_shared<DictionaryArray>(f1_type, indices3), null_bitmap, 1);

  // Dictionary-encoded lists of integers
  auto dict4 = ArrayFromJSON(list(int8()), "[[44, 55], [], [66]]");
  auto f4_type = dictionary(int8(), dict4);

  auto indices4 = ArrayFromJSON(int8(), "[0, 1, 2, 0, 2, 2]");
  auto a4 = std::make_shared<DictionaryArray>(f4_type, indices4);

  // construct batch
  auto schema = ::arrow::schema(
      {field("dict1", f0_type), field("dict2", f1_type), field("dict3", f2_type),
       field("list<encoded utf8>", f3_type), field("encoded list<int8>", f4_type)});

  std::vector<std::shared_ptr<Array>> arrays = {a0, a1, a2, a3, a4};

  *out = RecordBatch::Make(schema, length, arrays);
  return Status::OK();
}

Status MakeDictionaryFlat(std::shared_ptr<RecordBatch>* out) {
  const int64_t length = 6;

  std::vector<bool> is_valid = {true, true, false, true, true, true};

  auto dict1 = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\"]");
  auto dict2 = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\", \"qux\"]");

  auto f0_type = arrow::dictionary(arrow::int32(), dict1);
  auto f1_type = arrow::dictionary(arrow::int8(), dict1);
  auto f2_type = arrow::dictionary(arrow::int32(), dict2);

  std::shared_ptr<Array> indices0, indices1, indices2;
  std::vector<int32_t> indices0_values = {1, 2, -1, 0, 2, 0};
  std::vector<int8_t> indices1_values = {0, 0, 2, 2, 1, 1};
  std::vector<int32_t> indices2_values = {3, 0, 2, 1, 0, 2};

  ArrayFromVector<Int32Type, int32_t>(is_valid, indices0_values, &indices0);
  ArrayFromVector<Int8Type, int8_t>(is_valid, indices1_values, &indices1);
  ArrayFromVector<Int32Type, int32_t>(is_valid, indices2_values, &indices2);

  auto a0 = std::make_shared<DictionaryArray>(f0_type, indices0);
  auto a1 = std::make_shared<DictionaryArray>(f1_type, indices1);
  auto a2 = std::make_shared<DictionaryArray>(f2_type, indices2);

  // construct batch
  auto schema = ::arrow::schema(
      {field("dict1", f0_type), field("dict2", f1_type), field("dict3", f2_type)});

  std::vector<std::shared_ptr<Array>> arrays = {a0, a1, a2};
  *out = RecordBatch::Make(schema, length, arrays);
  return Status::OK();
}

Status MakeDates(std::shared_ptr<RecordBatch>* out) {
  std::vector<bool> is_valid = {true, true, true, false, true, true, true};
  auto f0 = field("f0", date32());
  auto f1 = field("f1", date64());
  auto schema = ::arrow::schema({f0, f1});

  std::vector<int32_t> date32_values = {0, 1, 2, 3, 4, 5, 6};
  std::shared_ptr<Array> date32_array;
  ArrayFromVector<Date32Type, int32_t>(is_valid, date32_values, &date32_array);

  std::vector<int64_t> date64_values = {1489269000000, 1489270000000, 1489271000000,
                                        1489272000000, 1489272000000, 1489273000000,
                                        1489274000000};
  std::shared_ptr<Array> date64_array;
  ArrayFromVector<Date64Type, int64_t>(is_valid, date64_values, &date64_array);

  *out = RecordBatch::Make(schema, date32_array->length(), {date32_array, date64_array});
  return Status::OK();
}

Status MakeTimestamps(std::shared_ptr<RecordBatch>* out) {
  std::vector<bool> is_valid = {true, true, true, false, true, true, true};
  auto f0 = field("f0", timestamp(TimeUnit::MILLI));
  auto f1 = field("f1", timestamp(TimeUnit::NANO, "America/New_York"));
  auto f2 = field("f2", timestamp(TimeUnit::SECOND));
  auto schema = ::arrow::schema({f0, f1, f2});

  std::vector<int64_t> ts_values = {1489269000000, 1489270000000, 1489271000000,
                                    1489272000000, 1489272000000, 1489273000000};

  std::shared_ptr<Array> a0, a1, a2;
  ArrayFromVector<TimestampType, int64_t>(f0->type(), is_valid, ts_values, &a0);
  ArrayFromVector<TimestampType, int64_t>(f1->type(), is_valid, ts_values, &a1);
  ArrayFromVector<TimestampType, int64_t>(f2->type(), is_valid, ts_values, &a2);

  *out = RecordBatch::Make(schema, a0->length(), {a0, a1, a2});
  return Status::OK();
}

Status MakeIntervals(std::shared_ptr<RecordBatch>* out) {
  std::vector<bool> is_valid = {true, true, true, false, true, true, true};
  auto f0 = field("f0", duration(TimeUnit::MILLI));
  auto f1 = field("f1", duration(TimeUnit::NANO));
  auto f2 = field("f2", duration(TimeUnit::SECOND));
  auto f3 = field("f3", day_time_interval());
  auto f4 = field("f4", month_interval());
  auto schema = ::arrow::schema({f0, f1, f2, f3, f4});

  std::vector<int64_t> ts_values = {1489269000000, 1489270000000, 1489271000000,
                                    1489272000000, 1489272000000, 1489273000000};

  std::shared_ptr<Array> a0, a1, a2, a3, a4;
  ArrayFromVector<DurationType, int64_t>(f0->type(), is_valid, ts_values, &a0);
  ArrayFromVector<DurationType, int64_t>(f1->type(), is_valid, ts_values, &a1);
  ArrayFromVector<DurationType, int64_t>(f2->type(), is_valid, ts_values, &a2);
  ArrayFromVector<DayTimeIntervalType, DayTimeIntervalType::DayMilliseconds>(
      f3->type(), is_valid, {{0, 0}, {0, 1}, {1, 1}, {2, 1}, {3, 4}, {-1, -1}}, &a3);
  ArrayFromVector<MonthIntervalType, int32_t>(f4->type(), is_valid, {0, -1, 1, 2, -2, 24},
                                              &a4);

  *out = RecordBatch::Make(schema, a0->length(), {a0, a1, a2, a3, a4});
  return Status::OK();
}

Status MakeTimes(std::shared_ptr<RecordBatch>* out) {
  std::vector<bool> is_valid = {true, true, true, false, true, true, true};
  auto f0 = field("f0", time32(TimeUnit::MILLI));
  auto f1 = field("f1", time64(TimeUnit::NANO));
  auto f2 = field("f2", time32(TimeUnit::SECOND));
  auto f3 = field("f3", time64(TimeUnit::NANO));
  auto schema = ::arrow::schema({f0, f1, f2, f3});

  std::vector<int32_t> t32_values = {1489269000, 1489270000, 1489271000,
                                     1489272000, 1489272000, 1489273000};
  std::vector<int64_t> t64_values = {1489269000000, 1489270000000, 1489271000000,
                                     1489272000000, 1489272000000, 1489273000000};

  std::shared_ptr<Array> a0, a1, a2, a3;
  ArrayFromVector<Time32Type, int32_t>(f0->type(), is_valid, t32_values, &a0);
  ArrayFromVector<Time64Type, int64_t>(f1->type(), is_valid, t64_values, &a1);
  ArrayFromVector<Time32Type, int32_t>(f2->type(), is_valid, t32_values, &a2);
  ArrayFromVector<Time64Type, int64_t>(f3->type(), is_valid, t64_values, &a3);

  *out = RecordBatch::Make(schema, a0->length(), {a0, a1, a2, a3});
  return Status::OK();
}

template <typename BuilderType, typename T>
static void AppendValues(const std::vector<bool>& is_valid, const std::vector<T>& values,
                         BuilderType* builder) {
  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid[i]) {
      ASSERT_OK(builder->Append(values[i]));
    } else {
      ASSERT_OK(builder->AppendNull());
    }
  }
}

Status MakeFWBinary(std::shared_ptr<RecordBatch>* out) {
  std::vector<bool> is_valid = {true, true, true, false};
  auto f0 = field("f0", fixed_size_binary(4));
  auto f1 = field("f1", fixed_size_binary(0));
  auto schema = ::arrow::schema({f0, f1});

  std::shared_ptr<Array> a1, a2;

  FixedSizeBinaryBuilder b1(f0->type());
  FixedSizeBinaryBuilder b2(f1->type());

  std::vector<std::string> values1 = {"foo1", "foo2", "foo3", "foo4"};
  AppendValues(is_valid, values1, &b1);

  std::vector<std::string> values2 = {"", "", "", ""};
  AppendValues(is_valid, values2, &b2);

  RETURN_NOT_OK(b1.Finish(&a1));
  RETURN_NOT_OK(b2.Finish(&a2));

  *out = RecordBatch::Make(schema, a1->length(), {a1, a2});
  return Status::OK();
}

Status MakeDecimal(std::shared_ptr<RecordBatch>* out) {
  constexpr int kDecimalPrecision = 38;
  auto type = decimal(kDecimalPrecision, 4);
  auto f0 = field("f0", type);
  auto f1 = field("f1", type);
  auto schema = ::arrow::schema({f0, f1});

  constexpr int kDecimalSize = 16;
  constexpr int length = 10;

  std::shared_ptr<Buffer> data, is_valid;
  std::vector<uint8_t> is_valid_bytes(length);

  RETURN_NOT_OK(AllocateBuffer(kDecimalSize * length, &data));

  random_decimals(length, 1, kDecimalPrecision, data->mutable_data());
  random_null_bytes(length, 0.1, is_valid_bytes.data());

  RETURN_NOT_OK(BitUtil::BytesToBits(is_valid_bytes, default_memory_pool(), &is_valid));

  auto a1 = std::make_shared<Decimal128Array>(f0->type(), length, data, is_valid,
                                              kUnknownNullCount);

  auto a2 = std::make_shared<Decimal128Array>(f1->type(), length, data);

  *out = RecordBatch::Make(schema, length, {a1, a2});
  return Status::OK();
}

Status MakeNull(std::shared_ptr<RecordBatch>* out) {
  auto f0 = field("f0", null());

  // Also put a non-null field to make sure we handle the null array buffers properly
  auto f1 = field("f1", int64());

  auto schema = ::arrow::schema({f0, f1});

  auto a1 = std::make_shared<NullArray>(10);

  std::vector<int64_t> int_values = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<bool> is_valid = {true, true, true, false, false,
                                true, true, true, true,  true};
  std::shared_ptr<Array> a2;
  ArrayFromVector<Int64Type, int64_t>(f1->type(), is_valid, int_values, &a2);

  *out = RecordBatch::Make(schema, a1->length(), {a1, a2});
  return Status::OK();
}

}  // namespace test
}  // namespace ipc
}  // namespace arrow
