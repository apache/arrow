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

#include "arrow/testing/generator.h"

#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"

namespace arrow {

template <typename ArrowType, typename CType = typename TypeTraits<ArrowType>::CType,
          typename BuilderType = typename TypeTraits<ArrowType>::BuilderType>
static inline std::shared_ptr<Array> ConstantArray(int64_t size, CType value) {
  auto type = TypeTraits<ArrowType>::type_singleton();
  auto builder_fn = [&](BuilderType* builder) { builder->UnsafeAppend(value); };
  return ArrayFromBuilderVisitor(type, size, builder_fn).ValueOrDie();
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Boolean(int64_t size, bool value) {
  return ConstantArray<BooleanType>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt8(int64_t size, uint8_t value) {
  return ConstantArray<UInt8Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int8(int64_t size, int8_t value) {
  return ConstantArray<Int8Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt16(int64_t size,
                                                             uint16_t value) {
  return ConstantArray<UInt16Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int16(int64_t size, int16_t value) {
  return ConstantArray<Int16Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt32(int64_t size,
                                                             uint32_t value) {
  return ConstantArray<UInt32Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int32(int64_t size, int32_t value) {
  return ConstantArray<Int32Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt64(int64_t size,
                                                             uint64_t value) {
  return ConstantArray<UInt64Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int64(int64_t size, int64_t value) {
  return ConstantArray<Int64Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Float32(int64_t size, float value) {
  return ConstantArray<FloatType>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Float64(int64_t size,
                                                              double value) {
  return ConstantArray<DoubleType>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::String(int64_t size,
                                                             std::string value) {
  return ConstantArray<StringType>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Zeroes(
    int64_t size, const std::shared_ptr<DataType>& type) {
  switch (type->id()) {
    case Type::NA:
      return std::make_shared<NullArray>(size);
    case Type::BOOL:
      return Boolean(size);
    case Type::UINT8:
      return UInt8(size);
    case Type::INT8:
      return Int8(size);
    case Type::UINT16:
      return UInt16(size);
    case Type::INT16:
      return Int16(size);
    case Type::UINT32:
      return UInt32(size);
    case Type::INT32:
      return Int32(size);
    case Type::UINT64:
      return UInt64(size);
    case Type::INT64:
      return Int64(size);
    case Type::TIME64:
    case Type::DATE64:
    case Type::TIMESTAMP: {
      EXPECT_OK_AND_ASSIGN(auto viewed, Int64(size)->View(type));
      return viewed;
    }
    case Type::INTERVAL_DAY_TIME:
    case Type::INTERVAL_MONTHS:
    case Type::TIME32:
    case Type::DATE32: {
      EXPECT_OK_AND_ASSIGN(auto viewed, Int32(size)->View(type));
      return viewed;
    }
    case Type::FLOAT:
      return Float32(size);
    case Type::DOUBLE:
      return Float64(size);
    case Type::STRING:
      return String(size);
    default:
      return nullptr;
  }
}

std::shared_ptr<RecordBatch> ConstantArrayGenerator::Zeroes(
    int64_t size, const std::shared_ptr<Schema>& schema) {
  std::vector<std::shared_ptr<Array>> arrays;

  for (const auto& field : schema->fields()) {
    arrays.emplace_back(Zeroes(size, field->type()));
  }

  return RecordBatch::Make(schema, size, arrays);
}

std::shared_ptr<RecordBatchReader> ConstantArrayGenerator::Repeat(
    int64_t n_batch, const std::shared_ptr<RecordBatch> batch) {
  std::vector<std::shared_ptr<RecordBatch>> batches(static_cast<size_t>(n_batch), batch);
  return *RecordBatchReader::Make(batches);
}

std::shared_ptr<RecordBatchReader> ConstantArrayGenerator::Zeroes(
    int64_t n_batch, int64_t batch_size, const std::shared_ptr<Schema>& schema) {
  return Repeat(n_batch, Zeroes(batch_size, schema));
}

Result<std::shared_ptr<Array>> ScalarVectorToArray(const ScalarVector& scalars) {
  if (scalars.empty()) {
    return Status::NotImplemented("ScalarVectorToArray with no scalars");
  }
  std::unique_ptr<arrow::ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(default_memory_pool(), scalars[0]->type, &builder));
  RETURN_NOT_OK(builder->AppendScalars(scalars));
  std::shared_ptr<Array> out;
  RETURN_NOT_OK(builder->Finish(&out));
  return out;
}

}  // namespace arrow
