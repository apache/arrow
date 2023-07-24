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
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/ipc/test_common.h"
#include "arrow/pretty_print.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_builders.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow {

using internal::checked_cast;

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
                            std::shared_ptr<Array>* out, uint32_t seed, int32_t min,
                            int32_t max) {
  random::RandomArrayGenerator rand(seed);
  const double null_probability = include_nulls ? 0.5 : 0.0;

  *out = rand.Int32(length, min, max, null_probability);

  return Status::OK();
}

Status MakeRandomInt64Array(int64_t length, bool include_nulls, MemoryPool* pool,
                            std::shared_ptr<Array>* out, uint32_t seed) {
  random::RandomArrayGenerator rand(seed);
  const double null_probability = include_nulls ? 0.5 : 0.0;

  *out = rand.Int64(length, 0, 1000, null_probability);

  return Status::OK();
}

namespace {

template <typename ArrayType>
Status MakeRandomArray(int64_t length, bool include_nulls, MemoryPool* pool,
                       std::shared_ptr<Array>* out, uint32_t seed) {
  random::RandomArrayGenerator rand(seed);
  const double null_probability = include_nulls ? 0.5 : 0.0;

  *out = rand.Numeric<ArrayType>(length, 0, 1000, null_probability);

  return Status::OK();
}

template <>
Status MakeRandomArray<Int8Type>(int64_t length, bool include_nulls, MemoryPool* pool,
                                 std::shared_ptr<Array>* out, uint32_t seed) {
  random::RandomArrayGenerator rand(seed);
  const double null_probability = include_nulls ? 0.5 : 0.0;

  *out = rand.Numeric<Int8Type>(length, 0, 127, null_probability);

  return Status::OK();
}

template <>
Status MakeRandomArray<UInt8Type>(int64_t length, bool include_nulls, MemoryPool* pool,
                                  std::shared_ptr<Array>* out, uint32_t seed) {
  random::RandomArrayGenerator rand(seed);
  const double null_probability = include_nulls ? 0.5 : 0.0;

  *out = rand.Numeric<UInt8Type>(length, 0, 127, null_probability);

  return Status::OK();
}

template <typename TypeClass>
Status MakeListArray(const std::shared_ptr<Array>& child_array, int num_lists,
                     bool include_nulls, MemoryPool* pool, std::shared_ptr<Array>* out) {
  using offset_type = typename TypeClass::offset_type;
  using ArrayType = typename TypeTraits<TypeClass>::ArrayType;

  // Create the null list values
  std::vector<uint8_t> valid_lists(num_lists);
  const double null_percent = include_nulls ? 0.1 : 0;
  random_null_bytes(num_lists, null_percent, valid_lists.data());

  // Create list offsets
  const int max_list_size = 10;

  std::vector<offset_type> list_sizes(num_lists, 0);
  std::vector<offset_type> offsets(
      num_lists + 1, 0);  // +1 so we can shift for nulls. See partial sum below.
  const auto seed = static_cast<uint32_t>(child_array->length());

  if (num_lists > 0) {
    rand_uniform_int(num_lists, seed, 0, max_list_size, list_sizes.data());
    // make sure sizes are consistent with null
    std::transform(list_sizes.begin(), list_sizes.end(), valid_lists.begin(),
                   list_sizes.begin(),
                   [](offset_type size, uint8_t valid) { return valid == 0 ? 0 : size; });
    std::partial_sum(list_sizes.begin(), list_sizes.end(), ++offsets.begin());

    // Force invariants
    const auto child_length = static_cast<offset_type>(child_array->length());
    offsets[0] = 0;
    std::replace_if(
        offsets.begin(), offsets.end(),
        [child_length](offset_type offset) { return offset > child_length; },
        child_length);
  }

  offsets[num_lists] = static_cast<offset_type>(child_array->length());

  /// TODO(wesm): Implement support for nulls in ListArray::FromArrays
  std::shared_ptr<Buffer> null_bitmap, offsets_buffer;
  RETURN_NOT_OK(GetBitmapFromVector(valid_lists, &null_bitmap));
  RETURN_NOT_OK(CopyBufferFromVector(offsets, pool, &offsets_buffer));

  *out = std::make_shared<ArrayType>(std::make_shared<TypeClass>(child_array->type()),
                                     num_lists, offsets_buffer, child_array, null_bitmap,
                                     kUnknownNullCount);
  return (**out).Validate();
}

}  // namespace

Status MakeRandomListArray(const std::shared_ptr<Array>& child_array, int num_lists,
                           bool include_nulls, MemoryPool* pool,
                           std::shared_ptr<Array>* out) {
  return MakeListArray<ListType>(child_array, num_lists, include_nulls, pool, out);
}

Status MakeRandomLargeListArray(const std::shared_ptr<Array>& child_array, int num_lists,
                                bool include_nulls, MemoryPool* pool,
                                std::shared_ptr<Array>* out) {
  return MakeListArray<LargeListType>(child_array, num_lists, include_nulls, pool, out);
}

Status MakeRandomMapArray(const std::shared_ptr<Array>& key_array,
                          const std::shared_ptr<Array>& item_array, int num_maps,
                          bool include_nulls, MemoryPool* pool,
                          std::shared_ptr<Array>* out) {
  auto pair_type = struct_(
      {field("key", key_array->type(), false), field("value", item_array->type())});

  auto pair_array = std::make_shared<StructArray>(pair_type, key_array->length(),
                                                  ArrayVector{key_array, item_array});

  RETURN_NOT_OK(MakeRandomListArray(pair_array, num_maps, include_nulls, pool, out));
  auto map_data = (*out)->data();
  map_data->type = map(key_array->type(), item_array->type());
  out->reset(new MapArray(map_data));
  return (**out).Validate();
}

Status MakeRandomBooleanArray(const int length, bool include_nulls,
                              std::shared_ptr<Array>* out) {
  std::vector<uint8_t> values(length);
  random_null_bytes(length, 0.5, values.data());
  ARROW_ASSIGN_OR_RAISE(auto data, internal::BytesToBits(values));

  if (include_nulls) {
    std::vector<uint8_t> valid_bytes(length);
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap, internal::BytesToBits(valid_bytes));
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
  auto f0 = field("f0", int8());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());
  auto f3 = field("f3", uint16());
  auto f4 = field("f4", int32());
  auto f5 = field("f5", uint32());
  auto f6 = field("f6", int64());
  auto f7 = field("f7", uint64());
  auto schema = ::arrow::schema({f0, f1, f2, f3, f4, f5, f6, f7});

  // Example data
  std::shared_ptr<Array> a0, a1, a2, a3, a4, a5, a6, a7;
  MemoryPool* pool = default_memory_pool();
  RETURN_NOT_OK(MakeRandomArray<Int8Type>(length, false, pool, &a0, seed));
  RETURN_NOT_OK(MakeRandomArray<UInt8Type>(length, true, pool, &a1, seed));
  RETURN_NOT_OK(MakeRandomArray<Int16Type>(length, true, pool, &a2, seed));
  RETURN_NOT_OK(MakeRandomArray<UInt16Type>(length, false, pool, &a3, seed));
  RETURN_NOT_OK(MakeRandomArray<Int32Type>(length, false, pool, &a4, seed));
  RETURN_NOT_OK(MakeRandomArray<UInt32Type>(length, true, pool, &a5, seed));
  RETURN_NOT_OK(MakeRandomArray<Int64Type>(length, true, pool, &a6, seed));
  RETURN_NOT_OK(MakeRandomArray<UInt64Type>(length, false, pool, &a7, seed));
  *out = RecordBatch::Make(schema, length, {a0, a1, a2, a3, a4, a5, a6, a7});
  return Status::OK();
}

Status MakeIntRecordBatch(std::shared_ptr<RecordBatch>* out) {
  return MakeIntBatchSized(10, out);
}

Status MakeFloat3264BatchSized(int length, std::shared_ptr<RecordBatch>* out,
                               uint32_t seed) {
  // Make the schema
  auto f0 = field("f0", float32());
  auto f1 = field("f1", float64());
  auto schema = ::arrow::schema({f0, f1});

  // Example data
  std::shared_ptr<Array> a0, a1;
  MemoryPool* pool = default_memory_pool();
  RETURN_NOT_OK(MakeRandomArray<FloatType>(length, false, pool, &a0, seed));
  RETURN_NOT_OK(MakeRandomArray<DoubleType>(length, true, pool, &a1, seed + 1));
  *out = RecordBatch::Make(schema, length, {a0, a1});
  return Status::OK();
}

Status MakeFloat3264Batch(std::shared_ptr<RecordBatch>* out) {
  return MakeFloat3264BatchSized(10, out);
}

Status MakeFloatBatchSized(int length, std::shared_ptr<RecordBatch>* out, uint32_t seed) {
  // Make the schema
  auto f0 = field("f0", float16());
  auto f1 = field("f1", float32());
  auto f2 = field("f2", float64());
  auto schema = ::arrow::schema({f0, f1, f2});

  // Example data
  std::shared_ptr<Array> a0, a1, a2;
  MemoryPool* pool = default_memory_pool();
  RETURN_NOT_OK(MakeRandomArray<HalfFloatType>(length, false, pool, &a0, seed));
  RETURN_NOT_OK(MakeRandomArray<FloatType>(length, false, pool, &a1, seed + 1));
  RETURN_NOT_OK(MakeRandomArray<DoubleType>(length, true, pool, &a2, seed + 2));
  *out = RecordBatch::Make(schema, length, {a0, a1, a2});
  return Status::OK();
}

Status MakeFloatBatch(std::shared_ptr<RecordBatch>* out) {
  return MakeFloatBatchSized(10, out);
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

template <class BuilderType>
static Status MakeBinaryArrayWithUniqueValues(int64_t length, bool include_nulls,
                                              MemoryPool* pool,
                                              std::shared_ptr<Array>* out) {
  BuilderType builder(pool);
  for (int64_t i = 0; i < length; ++i) {
    if (include_nulls && (i % 7 == 0)) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      RETURN_NOT_OK(builder.Append(std::to_string(i)));
    }
  }
  return builder.Finish(out);
}

Status MakeStringTypesRecordBatch(std::shared_ptr<RecordBatch>* out, bool with_nulls) {
  const int64_t length = 500;
  auto f0 = field("strings", utf8());
  auto f1 = field("binaries", binary());
  auto f2 = field("large_strings", large_utf8());
  auto f3 = field("large_binaries", large_binary());
  auto schema = ::arrow::schema({f0, f1, f2, f3});

  std::shared_ptr<Array> a0, a1, a2, a3;
  MemoryPool* pool = default_memory_pool();

  // Quirk with RETURN_NOT_OK macro and templated functions
  {
    auto s =
        MakeBinaryArrayWithUniqueValues<StringBuilder>(length, with_nulls, pool, &a0);
    RETURN_NOT_OK(s);
  }
  {
    auto s =
        MakeBinaryArrayWithUniqueValues<BinaryBuilder>(length, with_nulls, pool, &a1);
    RETURN_NOT_OK(s);
  }
  {
    auto s = MakeBinaryArrayWithUniqueValues<LargeStringBuilder>(length, with_nulls, pool,
                                                                 &a2);
    RETURN_NOT_OK(s);
  }
  {
    auto s = MakeBinaryArrayWithUniqueValues<LargeBinaryBuilder>(length, with_nulls, pool,
                                                                 &a3);
    RETURN_NOT_OK(s);
  }
  *out = RecordBatch::Make(schema, length, {a0, a1, a2, a3});
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
  auto f2 = field("f2", large_list(int32()));
  auto schema = ::arrow::schema({f0, f1, f2});

  // Example data

  MemoryPool* pool = default_memory_pool();
  const int length = 200;
  std::shared_ptr<Array> leaf_values, list_array, list_list_array, large_list_array;
  const bool include_nulls = true;
  RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool, &leaf_values));
  RETURN_NOT_OK(
      MakeRandomListArray(leaf_values, length, include_nulls, pool, &list_array));
  RETURN_NOT_OK(
      MakeRandomListArray(list_array, length, include_nulls, pool, &list_list_array));
  RETURN_NOT_OK(MakeRandomLargeListArray(leaf_values, length, include_nulls, pool,
                                         &large_list_array));
  *out =
      RecordBatch::Make(schema, length, {list_array, list_list_array, large_list_array});
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
  ARROW_ASSIGN_OR_RAISE(auto null_bitmap, internal::BytesToBits(null_bytes));
  std::shared_ptr<Array> with_nulls(
      new StructArray(type, list_batch->num_rows(), columns, null_bitmap, 1));

  // construct batch
  std::vector<std::shared_ptr<Array>> arrays = {no_nulls, with_nulls};
  *out = RecordBatch::Make(schema, list_batch->num_rows(), arrays);
  return Status::OK();
}

Status AddArtificialOffsetInChildArray(ArrayData* array, int64_t offset) {
  auto& child = array->child_data[1];
  auto builder = MakeBuilder(child->type).ValueOrDie();
  ARROW_RETURN_NOT_OK(builder->AppendNulls(offset));
  ARROW_RETURN_NOT_OK(builder->AppendArraySlice(ArraySpan(*child), 0, child->length));
  array->child_data[1] = builder->Finish().ValueOrDie()->Slice(offset)->data();
  return Status::OK();
}

Status MakeRunEndEncoded(std::shared_ptr<RecordBatch>* out) {
  const int64_t logical_length = 10000;
  const int64_t slice_offset = 2000;
  random::RandomArrayGenerator rand(/*seed =*/1);
  std::vector<std::shared_ptr<Array>> all_arrays;
  std::vector<std::shared_ptr<Field>> all_fields;
  for (const bool sliced : {false, true}) {
    const int64_t generate_length =
        sliced ? logical_length + 2 * slice_offset : logical_length;

    std::vector<std::shared_ptr<Array>> arrays = {
        rand.RunEndEncoded(int32(), generate_length, 0.5),
        rand.RunEndEncoded(int32(), generate_length, 0),
        rand.RunEndEncoded(utf8(), generate_length, 0.5),
        rand.RunEndEncoded(list(int32()), generate_length, 0.5),
    };
    std::vector<std::shared_ptr<Field>> fields = {
        field("ree_int32", run_end_encoded(int32(), int32())),
        field("ree_int32_not_null", run_end_encoded(int32(), int32()), false),
        field("ree_string", run_end_encoded(int32(), utf8())),
        field("ree_list", run_end_encoded(int32(), list(int32()))),
    };

    if (sliced) {
      for (auto& array : arrays) {
        ARROW_RETURN_NOT_OK(
            AddArtificialOffsetInChildArray(array->data().get(), slice_offset));
        array = array->Slice(slice_offset, logical_length);
      }
      for (auto& item : fields) {
        item = field(item->name() + "_sliced", item->type(), item->nullable(),
                     item->metadata());
      }
    }

    all_arrays.insert(all_arrays.end(), arrays.begin(), arrays.end());
    all_fields.insert(all_fields.end(), fields.begin(), fields.end());
  }
  *out = RecordBatch::Make(schema(all_fields), logical_length, all_arrays);
  return Status::OK();
}

Status MakeUnion(std::shared_ptr<RecordBatch>* out) {
  // Define schema
  std::vector<std::shared_ptr<Field>> union_fields(
      {field("u0", int32()), field("u1", uint8())});

  std::vector<int8_t> type_codes = {5, 10};
  auto sparse_type = sparse_union(union_fields, type_codes);
  auto dense_type = dense_union(union_fields, type_codes);

  auto f0 = field("sparse", sparse_type);
  auto f1 = field("dense", dense_type);

  auto schema = ::arrow::schema({f0, f1});

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

  auto sparse = std::make_shared<SparseUnionArray>(sparse_type, length, sparse_children,
                                                   type_ids_buffer);
  auto dense = std::make_shared<DenseUnionArray>(dense_type, length, dense_children,
                                                 type_ids_buffer, offsets_buffer);

  // construct batch
  std::vector<std::shared_ptr<Array>> arrays = {sparse, dense};
  *out = RecordBatch::Make(schema, length, arrays);
  return Status::OK();
}

Status MakeDictionary(std::shared_ptr<RecordBatch>* out) {
  const int64_t length = 6;

  std::vector<bool> is_valid = {true, true, false, true, true, true};

  auto dict_ty = utf8();

  auto dict1 = ArrayFromJSON(dict_ty, "[\"foo\", \"bar\", \"baz\"]");
  auto dict2 = ArrayFromJSON(dict_ty, "[\"fo\", \"bap\", \"bop\", \"qup\"]");

  auto f0_type = arrow::dictionary(arrow::int32(), dict_ty);
  auto f1_type = arrow::dictionary(arrow::int8(), dict_ty, true);
  auto f2_type = arrow::dictionary(arrow::int32(), dict_ty);

  std::shared_ptr<Array> indices0, indices1, indices2;
  std::vector<int32_t> indices0_values = {1, 2, -1, 0, 2, 0};
  std::vector<int8_t> indices1_values = {0, 0, 2, 2, 1, 1};
  std::vector<int32_t> indices2_values = {3, 0, 2, 1, 0, 2};

  ArrayFromVector<Int32Type, int32_t>(is_valid, indices0_values, &indices0);
  ArrayFromVector<Int8Type, int8_t>(is_valid, indices1_values, &indices1);
  ArrayFromVector<Int32Type, int32_t>(is_valid, indices2_values, &indices2);

  auto a0 = std::make_shared<DictionaryArray>(f0_type, indices0, dict1);
  auto a1 = std::make_shared<DictionaryArray>(f1_type, indices1, dict1);
  auto a2 = std::make_shared<DictionaryArray>(f2_type, indices2, dict2);

  // Lists of dictionary-encoded strings
  auto f3_type = list(f1_type);

  auto indices3 = ArrayFromJSON(int8(), "[0, 1, 2, 0, 1, 1, 2, 1, 0]");
  auto offsets3 = ArrayFromJSON(int32(), "[0, 0, 2, 2, 5, 6, 9]");

  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(GetBitmapFromVector(is_valid, &null_bitmap));

  std::shared_ptr<Array> a3 = std::make_shared<ListArray>(
      f3_type, length, std::static_pointer_cast<PrimitiveArray>(offsets3)->values(),
      std::make_shared<DictionaryArray>(f1_type, indices3, dict1), null_bitmap, 1);

  // Dictionary-encoded lists of integers
  auto dict4_ty = list(int8());
  auto f4_type = dictionary(int8(), dict4_ty);

  auto indices4 = ArrayFromJSON(int8(), "[0, 1, 2, 0, 2, 2]");
  auto dict4 = ArrayFromJSON(dict4_ty, "[[44, 55], [], [66]]");
  auto a4 = std::make_shared<DictionaryArray>(f4_type, indices4, dict4);

  std::vector<std::shared_ptr<Field>> fields = {
      field("dict1", f0_type), field("dict2", f1_type), field("dict3", f2_type),
      field("list<encoded utf8>", f3_type), field("encoded list<int8>", f4_type)};
  std::vector<std::shared_ptr<Array>> arrays = {a0, a1, a2, a3, a4};

  // Ensure all dictionary index types are represented
  int field_index = 5;
  for (auto index_ty : all_dictionary_index_types()) {
    std::stringstream ss;
    ss << "dict" << field_index++;
    auto ty = arrow::dictionary(index_ty, dict_ty);
    auto indices = ArrayFromJSON(index_ty, "[0, 1, 2, 0, 2, 2]");
    fields.push_back(field(ss.str(), ty));
    arrays.push_back(std::make_shared<DictionaryArray>(ty, indices, dict1));
  }

  // construct batch
  *out = RecordBatch::Make(::arrow::schema(fields), length, arrays);
  return Status::OK();
}

Status MakeDictionaryFlat(std::shared_ptr<RecordBatch>* out) {
  const int64_t length = 6;

  std::vector<bool> is_valid = {true, true, false, true, true, true};

  auto dict_ty = utf8();
  auto dict1 = ArrayFromJSON(dict_ty, "[\"foo\", \"bar\", \"baz\"]");
  auto dict2 = ArrayFromJSON(dict_ty, "[\"foo\", \"bar\", \"baz\", \"qux\"]");

  auto f0_type = arrow::dictionary(arrow::int32(), dict_ty);
  auto f1_type = arrow::dictionary(arrow::int8(), dict_ty);
  auto f2_type = arrow::dictionary(arrow::int32(), dict_ty);

  std::shared_ptr<Array> indices0, indices1, indices2;
  std::vector<int32_t> indices0_values = {1, 2, -1, 0, 2, 0};
  std::vector<int8_t> indices1_values = {0, 0, 2, 2, 1, 1};
  std::vector<int32_t> indices2_values = {3, 0, 2, 1, 0, 2};

  ArrayFromVector<Int32Type, int32_t>(is_valid, indices0_values, &indices0);
  ArrayFromVector<Int8Type, int8_t>(is_valid, indices1_values, &indices1);
  ArrayFromVector<Int32Type, int32_t>(is_valid, indices2_values, &indices2);

  auto a0 = std::make_shared<DictionaryArray>(f0_type, indices0, dict1);
  auto a1 = std::make_shared<DictionaryArray>(f1_type, indices1, dict1);
  auto a2 = std::make_shared<DictionaryArray>(f2_type, indices2, dict2);

  // construct batch
  auto schema = ::arrow::schema(
      {field("dict1", f0_type), field("dict2", f1_type), field("dict3", f2_type)});

  std::vector<std::shared_ptr<Array>> arrays = {a0, a1, a2};
  *out = RecordBatch::Make(schema, length, arrays);
  return Status::OK();
}

Status MakeNestedDictionary(std::shared_ptr<RecordBatch>* out) {
  const int64_t length = 7;

  auto values0 = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\"]");
  auto values1 = ArrayFromJSON(int64(), "[1234567890, 987654321]");

  // NOTE: it is important to test several levels of nesting, with non-trivial
  // numbers of child fields, to exercise structural mapping of fields to dict ids.

  // Field 0: dict(int32, list(dict(int8, utf8)))
  ARROW_ASSIGN_OR_RAISE(auto inner0,
                        DictionaryArray::FromArrays(
                            dictionary(int8(), values0->type()),
                            /*indices=*/ArrayFromJSON(int8(), "[0, 1, 2, null, 2, 1, 0]"),
                            /*dictionary=*/values0));

  ARROW_ASSIGN_OR_RAISE(auto nested_values0,
                        ListArray::FromArrays(
                            /*offsets=*/*ArrayFromJSON(int32(), "[0, 3, 3, 6, 7]"),
                            /*values=*/*inner0));
  ARROW_ASSIGN_OR_RAISE(
      auto outer0, DictionaryArray::FromArrays(
                       dictionary(int32(), nested_values0->type()),
                       /*indices=*/ArrayFromJSON(int32(), "[0, 1, 3, 3, null, 3, 2]"),
                       /*dictionary=*/nested_values0));
  DCHECK_EQ(outer0->length(), length);

  // Field 1: struct(a: dict(int8, int64), b: dict(int16, utf8))
  ARROW_ASSIGN_OR_RAISE(
      auto inner1, DictionaryArray::FromArrays(
                       dictionary(int8(), values1->type()),
                       /*indices=*/ArrayFromJSON(int8(), "[0, 1, 1, null, null, 1, 0]"),
                       /*dictionary=*/values1));
  ARROW_ASSIGN_OR_RAISE(
      auto inner2, DictionaryArray::FromArrays(
                       dictionary(int16(), values0->type()),
                       /*indices=*/ArrayFromJSON(int16(), "[2, 1, null, null, 2, 1, 0]"),
                       /*dictionary=*/values0));
  ARROW_ASSIGN_OR_RAISE(
      auto outer1, StructArray::Make({inner1, inner2}, {field("a", inner1->type()),
                                                        field("b", inner2->type())}));
  DCHECK_EQ(outer1->length(), length);

  // Field 2: dict(int8, struct(c: dict(int8, int64), d: dict(int16, list(dict(int8,
  // utf8)))))
  ARROW_ASSIGN_OR_RAISE(auto nested_values2,
                        ListArray::FromArrays(
                            /*offsets=*/*ArrayFromJSON(int32(), "[0, 1, 5, 5, 7]"),
                            /*values=*/*inner0));
  ARROW_ASSIGN_OR_RAISE(
      auto inner3, DictionaryArray::FromArrays(
                       dictionary(int16(), nested_values2->type()),
                       /*indices=*/ArrayFromJSON(int16(), "[0, 1, 3, null, 3, 2, 1]"),
                       /*dictionary=*/nested_values2));
  ARROW_ASSIGN_OR_RAISE(
      auto inner4, StructArray::Make({inner1, inner3}, {field("c", inner1->type()),
                                                        field("d", inner3->type())}));
  ARROW_ASSIGN_OR_RAISE(auto outer2,
                        DictionaryArray::FromArrays(
                            dictionary(int8(), inner4->type()),
                            /*indices=*/ArrayFromJSON(int8(), "[0, 2, 4, 6, 1, 3, 5]"),
                            /*dictionary=*/inner4));
  DCHECK_EQ(outer2->length(), length);

  auto schema = ::arrow::schema({
      field("f0", outer0->type()),
      field("f1", outer1->type()),
      field("f2", outer2->type()),
  });
  *out = RecordBatch::Make(schema, length, {outer0, outer1, outer2});
  return Status::OK();
}

Status MakeMap(std::shared_ptr<RecordBatch>* out) {
  constexpr int64_t kNumRows = 3;
  std::shared_ptr<Array> a0, a1;

  auto key_array = ArrayFromJSON(utf8(), R"(["k1", "k2", "k1", "k3", "k1", "k4"])");
  auto item_array = ArrayFromJSON(int16(), "[0, -1, 2, -3, 4, null]");
  RETURN_NOT_OK(MakeRandomMapArray(key_array, item_array, kNumRows,
                                   /*include_nulls=*/false, default_memory_pool(), &a0));
  RETURN_NOT_OK(MakeRandomMapArray(key_array, item_array, kNumRows,
                                   /*include_nulls=*/true, default_memory_pool(), &a1));
  auto f0 = field("f0", a0->type());
  auto f1 = field("f1", a1->type());
  *out = RecordBatch::Make(::arrow::schema({f0, f1}), kNumRows, {a0, a1});
  return Status::OK();
}

Status MakeMapOfDictionary(std::shared_ptr<RecordBatch>* out) {
  // Exercises ARROW-9660
  constexpr int64_t kNumRows = 3;
  std::shared_ptr<Array> a0, a1;

  auto key_array = DictArrayFromJSON(dictionary(int32(), utf8()), "[0, 1, 0, 2, 0, 3]",
                                     R"(["k1", "k2", "k3", "k4"])");
  auto item_array = ArrayFromJSON(int16(), "[0, -1, 2, -3, 4, null]");
  RETURN_NOT_OK(MakeRandomMapArray(key_array, item_array, kNumRows,
                                   /*include_nulls=*/false, default_memory_pool(), &a0));
  RETURN_NOT_OK(MakeRandomMapArray(key_array, item_array, kNumRows,
                                   /*include_nulls=*/true, default_memory_pool(), &a1));
  auto f0 = field("f0", a0->type());
  auto f1 = field("f1", a1->type());
  *out = RecordBatch::Make(::arrow::schema({f0, f1}), kNumRows, {a0, a1});
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

  std::vector<int64_t> date64_values = {86400000,  172800000, 259200000, 1489272000000,
                                        345600000, 432000000, 518400000};
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
  auto f5 = field("f5", month_day_nano_interval());
  auto schema = ::arrow::schema({f0, f1, f2, f3, f4, f5});

  std::vector<int64_t> ts_values = {1489269000000, 1489270000000, 1489271000000,
                                    1489272000000, 1489272000000, 1489273000000};

  std::shared_ptr<Array> a0, a1, a2, a3, a4, a5;
  ArrayFromVector<DurationType, int64_t>(f0->type(), is_valid, ts_values, &a0);
  ArrayFromVector<DurationType, int64_t>(f1->type(), is_valid, ts_values, &a1);
  ArrayFromVector<DurationType, int64_t>(f2->type(), is_valid, ts_values, &a2);
  ArrayFromVector<DayTimeIntervalType, DayTimeIntervalType::DayMilliseconds>(
      f3->type(), is_valid, {{0, 0}, {0, 1}, {1, 1}, {2, 1}, {3, 4}, {-1, -1}}, &a3);
  ArrayFromVector<MonthIntervalType, int32_t>(f4->type(), is_valid, {0, -1, 1, 2, -2, 24},
                                              &a4);
  ArrayFromVector<MonthDayNanoIntervalType, MonthDayNanoIntervalType::MonthDayNanos>(
      f5->type(), is_valid,
      {{0, 0, 0}, {0, 0, 1}, {-1, 0, 1}, {-1, -2, -3}, {2, 4, 6}, {-3, -4, -5}}, &a5);

  *out = RecordBatch::Make(schema, a0->length(), {a0, a1, a2, a3, a4, a5});
  return Status::OK();
}

Status MakeTimes(std::shared_ptr<RecordBatch>* out) {
  std::vector<bool> is_valid = {true, true, true, false, true, true, true};
  auto f0 = field("f0", time32(TimeUnit::MILLI));
  auto f1 = field("f1", time64(TimeUnit::NANO));
  auto f2 = field("f2", time32(TimeUnit::SECOND));
  auto f3 = field("f3", time64(TimeUnit::NANO));
  auto schema = ::arrow::schema({f0, f1, f2, f3});

  std::vector<int32_t> t32_values = {14896, 14897, 14892, 1489272000, 14893, 14895};
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
  constexpr int kLength = 10;
  auto type = decimal128(38, 4);
  auto f0 = field("f0", type);
  auto f1 = field("f1", type);
  auto schema = ::arrow::schema({f0, f1});

  auto gen = random::RandomArrayGenerator(/*seed=*/1);
  auto a1 = gen.Decimal128(type, kLength, /*null_probability=*/0.1);
  auto a2 = std::make_shared<Decimal128Array>(type, kLength, a1->data()->buffers[1]);

  *out = RecordBatch::Make(schema, kLength, {a1, a2});
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

Status MakeUuid(std::shared_ptr<RecordBatch>* out) {
  auto uuid_type = uuid();
  auto storage_type = checked_cast<const ExtensionType&>(*uuid_type).storage_type();

  auto f0 = field("f0", uuid_type);
  auto f1 = field("f1", uuid_type, /*nullable=*/false);
  auto schema = ::arrow::schema({f0, f1});

  auto a0 = std::make_shared<UuidArray>(
      uuid_type, ArrayFromJSON(storage_type, R"(["0123456789abcdef", null])"));
  auto a1 = std::make_shared<UuidArray>(
      uuid_type,
      ArrayFromJSON(storage_type, R"(["ZYXWVUTSRQPONMLK", "JIHGFEDBA9876543"])"));

  *out = RecordBatch::Make(schema, a1->length(), {a0, a1});
  return Status::OK();
}

Status MakeComplex128(std::shared_ptr<RecordBatch>* out) {
  auto type = complex128();
  auto storage_type = checked_cast<const ExtensionType&>(*type).storage_type();

  auto f0 = field("f0", type);
  auto f1 = field("f1", type, /*nullable=*/false);
  auto schema = ::arrow::schema({f0, f1});

  auto a0 = ExtensionType::WrapArray(complex128(),
                                     ArrayFromJSON(storage_type, "[[1.0, -2.5], null]"));
  auto a1 = ExtensionType::WrapArray(
      complex128(), ArrayFromJSON(storage_type, "[[1.0, -2.5], [3.0, -4.0]]"));

  *out = RecordBatch::Make(schema, a1->length(), {a0, a1});
  return Status::OK();
}

Status MakeDictExtension(std::shared_ptr<RecordBatch>* out) {
  auto type = dict_extension_type();
  auto storage_type = checked_cast<const ExtensionType&>(*type).storage_type();

  auto f0 = field("f0", type);
  auto f1 = field("f1", type, /*nullable=*/false);
  auto schema = ::arrow::schema({f0, f1});

  auto storage0 = std::make_shared<DictionaryArray>(
      storage_type, ArrayFromJSON(int8(), "[1, 0, null, 1, 1]"),
      ArrayFromJSON(utf8(), R"(["foo", "bar"])"));
  auto a0 = std::make_shared<ExtensionArray>(type, storage0);

  auto storage1 = std::make_shared<DictionaryArray>(
      storage_type, ArrayFromJSON(int8(), "[2, 0, 0, 1, 1]"),
      ArrayFromJSON(utf8(), R"(["arrow", "parquet", "gandiva"])"));
  auto a1 = std::make_shared<ExtensionArray>(type, storage1);

  *out = RecordBatch::Make(schema, a1->length(), {a0, a1});
  return Status::OK();
}

namespace {

template <typename CValueType, typename SeedType, typename DistributionType>
void FillRandomData(CValueType* data, size_t n, CValueType min, CValueType max,
                    SeedType seed) {
  std::default_random_engine rng(seed);
  DistributionType dist(min, max);
  std::generate(data, data + n,
                [&dist, &rng] { return static_cast<CValueType>(dist(rng)); });
}

template <typename CValueType, typename SeedType>
enable_if_t<std::is_integral<CValueType>::value && std::is_signed<CValueType>::value,
            void>
FillRandomData(CValueType* data, size_t n, SeedType seed) {
  FillRandomData<CValueType, SeedType, std::uniform_int_distribution<CValueType>>(
      data, n, -1000, 1000, seed);
}

template <typename CValueType, typename SeedType>
enable_if_t<std::is_integral<CValueType>::value && std::is_unsigned<CValueType>::value,
            void>
FillRandomData(CValueType* data, size_t n, SeedType seed) {
  FillRandomData<CValueType, SeedType, std::uniform_int_distribution<CValueType>>(
      data, n, 0, 1000, seed);
}

template <typename CValueType, typename SeedType>
enable_if_t<std::is_floating_point<CValueType>::value, void> FillRandomData(
    CValueType* data, size_t n, SeedType seed) {
  FillRandomData<CValueType, SeedType, std::uniform_real_distribution<CValueType>>(
      data, n, -1000, 1000, seed);
}

}  // namespace

Status MakeRandomTensor(const std::shared_ptr<DataType>& type,
                        const std::vector<int64_t>& shape, bool row_major_p,
                        std::shared_ptr<Tensor>* out, uint32_t seed) {
  const auto& element_type = internal::checked_cast<const FixedWidthType&>(*type);
  std::vector<int64_t> strides;
  if (row_major_p) {
    RETURN_NOT_OK(internal::ComputeRowMajorStrides(element_type, shape, &strides));
  } else {
    RETURN_NOT_OK(internal::ComputeColumnMajorStrides(element_type, shape, &strides));
  }

  const int64_t element_size = element_type.bit_width() / CHAR_BIT;
  const int64_t len =
      std::accumulate(shape.begin(), shape.end(), int64_t(1), std::multiplies<int64_t>());

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> buf, AllocateBuffer(element_size * len));

  switch (type->id()) {
    case Type::INT8:
      FillRandomData<int8_t, uint32_t, std::uniform_int_distribution<int16_t>>(
          reinterpret_cast<int8_t*>(buf->mutable_data()), len, -128, 127, seed);
      break;
    case Type::UINT8:
      FillRandomData<uint8_t, uint32_t, std::uniform_int_distribution<uint16_t>>(
          reinterpret_cast<uint8_t*>(buf->mutable_data()), len, 0, 255, seed);
      break;
    case Type::INT16:
      FillRandomData(reinterpret_cast<int16_t*>(buf->mutable_data()), len, seed);
      break;
    case Type::UINT16:
      FillRandomData(reinterpret_cast<uint16_t*>(buf->mutable_data()), len, seed);
      break;
    case Type::INT32:
      FillRandomData(reinterpret_cast<int32_t*>(buf->mutable_data()), len, seed);
      break;
    case Type::UINT32:
      FillRandomData(reinterpret_cast<uint32_t*>(buf->mutable_data()), len, seed);
      break;
    case Type::INT64:
      FillRandomData(reinterpret_cast<int64_t*>(buf->mutable_data()), len, seed);
      break;
    case Type::UINT64:
      FillRandomData(reinterpret_cast<uint64_t*>(buf->mutable_data()), len, seed);
      break;
    case Type::HALF_FLOAT:
      FillRandomData(reinterpret_cast<int16_t*>(buf->mutable_data()), len, seed);
      break;
    case Type::FLOAT:
      FillRandomData(reinterpret_cast<float*>(buf->mutable_data()), len, seed);
      break;
    case Type::DOUBLE:
      FillRandomData(reinterpret_cast<double*>(buf->mutable_data()), len, seed);
      break;
    default:
      return Status::Invalid(type->ToString(), " is not valid data type for a tensor");
  }

  return Tensor::Make(type, buf, shape, strides).Value(out);
}

}  // namespace test
}  // namespace ipc
}  // namespace arrow
