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

#ifndef ARROW_IPC_TEST_COMMON_H
#define ARROW_IPC_TEST_COMMON_H

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/table.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"

namespace arrow {
namespace ipc {

const auto kListInt32 = list(int32());
const auto kListListInt32 = list(kListInt32);

Status MakeRandomInt32Array(
    int32_t length, bool include_nulls, MemoryPool* pool, std::shared_ptr<Array>* out) {
  std::shared_ptr<PoolBuffer> data;
  test::MakeRandomInt32PoolBuffer(length, pool, &data);
  Int32Builder builder(pool, int32());
  if (include_nulls) {
    std::shared_ptr<PoolBuffer> valid_bytes;
    test::MakeRandomBytePoolBuffer(length, pool, &valid_bytes);
    RETURN_NOT_OK(builder.Append(
        reinterpret_cast<const int32_t*>(data->data()), length, valid_bytes->data()));
    return builder.Finish(out);
  }
  RETURN_NOT_OK(builder.Append(reinterpret_cast<const int32_t*>(data->data()), length));
  return builder.Finish(out);
}

Status MakeRandomListArray(const std::shared_ptr<Array>& child_array, int num_lists,
    bool include_nulls, MemoryPool* pool, std::shared_ptr<Array>* out) {
  // Create the null list values
  std::vector<uint8_t> valid_lists(num_lists);
  const double null_percent = include_nulls ? 0.1 : 0;
  test::random_null_bytes(num_lists, null_percent, valid_lists.data());

  // Create list offsets
  const int max_list_size = 10;

  std::vector<int32_t> list_sizes(num_lists, 0);
  std::vector<int32_t> offsets(
      num_lists + 1, 0);  // +1 so we can shift for nulls. See partial sum below.
  const int seed = child_array->length();
  if (num_lists > 0) {
    test::rand_uniform_int(num_lists, seed, 0, max_list_size, list_sizes.data());
    // make sure sizes are consistent with null
    std::transform(list_sizes.begin(), list_sizes.end(), valid_lists.begin(),
        list_sizes.begin(),
        [](int32_t size, int32_t valid) { return valid == 0 ? 0 : size; });
    std::partial_sum(list_sizes.begin(), list_sizes.end(), ++offsets.begin());

    // Force invariants
    const int child_length = child_array->length();
    offsets[0] = 0;
    std::replace_if(offsets.begin(), offsets.end(),
        [child_length](int32_t offset) { return offset > child_length; }, child_length);
  }
  ListBuilder builder(pool, child_array);
  RETURN_NOT_OK(builder.Append(offsets.data(), num_lists, valid_lists.data()));
  RETURN_NOT_OK(builder.Finish(out));
  return (*out)->Validate();
}

typedef Status MakeRecordBatch(std::shared_ptr<RecordBatch>* out);

Status MakeIntRecordBatch(std::shared_ptr<RecordBatch>* out) {
  const int length = 1000;

  // Make the schema
  auto f0 = std::make_shared<Field>("f0", int32());
  auto f1 = std::make_shared<Field>("f1", int32());
  std::shared_ptr<Schema> schema(new Schema({f0, f1}));

  // Example data
  std::shared_ptr<Array> a0, a1;
  MemoryPool* pool = default_memory_pool();
  RETURN_NOT_OK(MakeRandomInt32Array(length, false, pool, &a0));
  RETURN_NOT_OK(MakeRandomInt32Array(length, true, pool, &a1));
  out->reset(new RecordBatch(schema, length, {a0, a1}));
  return Status::OK();
}

template <class Builder, class RawType>
Status MakeRandomBinaryArray(
    const TypePtr& type, int32_t length, MemoryPool* pool, std::shared_ptr<Array>* out) {
  const std::vector<std::string> values = {
      "", "", "abc", "123", "efg", "456!@#!@#", "12312"};
  Builder builder(pool, type);
  const auto values_len = values.size();
  for (int32_t i = 0; i < length; ++i) {
    int values_index = i % values_len;
    if (values_index == 0) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      const std::string& value = values[values_index];
      RETURN_NOT_OK(
          builder.Append(reinterpret_cast<const RawType*>(value.data()), value.size()));
    }
  }
  return builder.Finish(out);
}

Status MakeStringTypesRecordBatch(std::shared_ptr<RecordBatch>* out) {
  const int32_t length = 500;
  auto string_type = utf8();
  auto binary_type = binary();
  auto f0 = std::make_shared<Field>("f0", string_type);
  auto f1 = std::make_shared<Field>("f1", binary_type);
  std::shared_ptr<Schema> schema(new Schema({f0, f1}));

  std::shared_ptr<Array> a0, a1;
  MemoryPool* pool = default_memory_pool();

  {
    auto status =
        MakeRandomBinaryArray<StringBuilder, char>(string_type, length, pool, &a0);
    RETURN_NOT_OK(status);
  }
  {
    auto status =
        MakeRandomBinaryArray<BinaryBuilder, uint8_t>(binary_type, length, pool, &a1);
    RETURN_NOT_OK(status);
  }
  out->reset(new RecordBatch(schema, length, {a0, a1}));
  return Status::OK();
}

Status MakeListRecordBatch(std::shared_ptr<RecordBatch>* out) {
  // Make the schema
  auto f0 = std::make_shared<Field>("f0", kListInt32);
  auto f1 = std::make_shared<Field>("f1", kListListInt32);
  auto f2 = std::make_shared<Field>("f2", int32());
  std::shared_ptr<Schema> schema(new Schema({f0, f1, f2}));

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
  out->reset(new RecordBatch(schema, length, {list_array, list_list_array, flat_array}));
  return Status::OK();
}

Status MakeZeroLengthRecordBatch(std::shared_ptr<RecordBatch>* out) {
  // Make the schema
  auto f0 = std::make_shared<Field>("f0", kListInt32);
  auto f1 = std::make_shared<Field>("f1", kListListInt32);
  auto f2 = std::make_shared<Field>("f2", int32());
  std::shared_ptr<Schema> schema(new Schema({f0, f1, f2}));

  // Example data
  MemoryPool* pool = default_memory_pool();
  const int length = 200;
  const bool include_nulls = true;
  std::shared_ptr<Array> leaf_values, list_array, list_list_array, flat_array;
  RETURN_NOT_OK(MakeRandomInt32Array(0, include_nulls, pool, &leaf_values));
  RETURN_NOT_OK(MakeRandomListArray(leaf_values, 0, include_nulls, pool, &list_array));
  RETURN_NOT_OK(
      MakeRandomListArray(list_array, 0, include_nulls, pool, &list_list_array));
  RETURN_NOT_OK(MakeRandomInt32Array(0, include_nulls, pool, &flat_array));
  out->reset(new RecordBatch(schema, length, {list_array, list_list_array, flat_array}));
  return Status::OK();
}

Status MakeNonNullRecordBatch(std::shared_ptr<RecordBatch>* out) {
  // Make the schema
  auto f0 = std::make_shared<Field>("f0", kListInt32);
  auto f1 = std::make_shared<Field>("f1", kListListInt32);
  auto f2 = std::make_shared<Field>("f2", int32());
  std::shared_ptr<Schema> schema(new Schema({f0, f1, f2}));

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
  out->reset(new RecordBatch(schema, length, {list_array, list_list_array, flat_array}));
  return Status::OK();
}

Status MakeDeeplyNestedList(std::shared_ptr<RecordBatch>* out) {
  const int batch_length = 5;
  TypePtr type = int32();

  MemoryPool* pool = default_memory_pool();
  std::shared_ptr<Array> array;
  const bool include_nulls = true;
  RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool, &array));
  for (int i = 0; i < 63; ++i) {
    type = std::static_pointer_cast<DataType>(list(type));
    RETURN_NOT_OK(MakeRandomListArray(array, batch_length, include_nulls, pool, &array));
  }

  auto f0 = std::make_shared<Field>("f0", type);
  std::shared_ptr<Schema> schema(new Schema({f0}));
  std::vector<std::shared_ptr<Array>> arrays = {array};
  out->reset(new RecordBatch(schema, batch_length, arrays));
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
  auto f0 = std::make_shared<Field>("non_null_struct", type);
  auto f1 = std::make_shared<Field>("null_struct", type);
  std::shared_ptr<Schema> schema(new Schema({f0, f1}));

  // construct individual nullable/non-nullable struct arrays
  std::shared_ptr<Array> no_nulls(new StructArray(type, list_batch->num_rows(), columns));
  std::vector<uint8_t> null_bytes(list_batch->num_rows(), 1);
  null_bytes[0] = 0;
  std::shared_ptr<Buffer> null_bitmask;
  RETURN_NOT_OK(BitUtil::BytesToBits(null_bytes, &null_bitmask));
  std::shared_ptr<Array> with_nulls(
      new StructArray(type, list_batch->num_rows(), columns, 1, null_bitmask));

  // construct batch
  std::vector<std::shared_ptr<Array>> arrays = {no_nulls, with_nulls};
  out->reset(new RecordBatch(schema, list_batch->num_rows(), arrays));
  return Status::OK();
}

Status MakeUnion(std::shared_ptr<RecordBatch>* out) {
  // Define schema
  std::vector<std::shared_ptr<Field>> union_types(
      {std::make_shared<Field>("u0", int32()), std::make_shared<Field>("u1", uint8())});

  std::vector<uint8_t> type_codes = {5, 10};
  auto sparse_type =
      std::make_shared<UnionType>(union_types, type_codes, UnionMode::SPARSE);

  auto dense_type =
      std::make_shared<UnionType>(union_types, type_codes, UnionMode::DENSE);

  auto f0 = std::make_shared<Field>("sparse_nonnull", sparse_type, false);
  auto f1 = std::make_shared<Field>("sparse", sparse_type);
  auto f2 = std::make_shared<Field>("dense", dense_type);

  std::shared_ptr<Schema> schema(new Schema({f0, f1, f2}));

  // Create data
  std::vector<std::shared_ptr<Array>> sparse_children(2);
  std::vector<std::shared_ptr<Array>> dense_children(2);

  const int32_t length = 7;

  std::shared_ptr<Buffer> type_ids_buffer;
  std::vector<uint8_t> type_ids = {5, 10, 5, 5, 10, 10, 5};
  RETURN_NOT_OK(test::CopyBufferFromVector(type_ids, &type_ids_buffer));

  std::vector<int32_t> u0_values = {0, 1, 2, 3, 4, 5, 6};
  ArrayFromVector<Int32Type, int32_t>(
      sparse_type->child(0)->type, u0_values, &sparse_children[0]);

  std::vector<uint8_t> u1_values = {10, 11, 12, 13, 14, 15, 16};
  ArrayFromVector<UInt8Type, uint8_t>(
      sparse_type->child(1)->type, u1_values, &sparse_children[1]);

  // dense children
  u0_values = {0, 2, 3, 7};
  ArrayFromVector<Int32Type, int32_t>(
      dense_type->child(0)->type, u0_values, &dense_children[0]);

  u1_values = {11, 14, 15};
  ArrayFromVector<UInt8Type, uint8_t>(
      dense_type->child(1)->type, u1_values, &dense_children[1]);

  std::shared_ptr<Buffer> offsets_buffer;
  std::vector<int32_t> offsets = {0, 0, 1, 2, 1, 2, 3};
  RETURN_NOT_OK(test::CopyBufferFromVector(offsets, &offsets_buffer));

  std::vector<uint8_t> null_bytes(length, 1);
  null_bytes[2] = 0;
  std::shared_ptr<Buffer> null_bitmask;
  RETURN_NOT_OK(BitUtil::BytesToBits(null_bytes, &null_bitmask));

  // construct individual nullable/non-nullable struct arrays
  auto sparse_no_nulls =
      std::make_shared<UnionArray>(sparse_type, length, sparse_children, type_ids_buffer);
  auto sparse = std::make_shared<UnionArray>(
      sparse_type, length, sparse_children, type_ids_buffer, nullptr, 1, null_bitmask);

  auto dense = std::make_shared<UnionArray>(dense_type, length, dense_children,
      type_ids_buffer, offsets_buffer, 1, null_bitmask);

  // construct batch
  std::vector<std::shared_ptr<Array>> arrays = {sparse_no_nulls, sparse, dense};
  out->reset(new RecordBatch(schema, length, arrays));
  return Status::OK();
}

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_TEST_COMMON_H
