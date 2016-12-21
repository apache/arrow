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

#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/test-util.h"

namespace parquet {
namespace arrow {

using ::arrow::Array;
using ::arrow::Status;

template <typename ArrowType>
using is_arrow_float = std::is_floating_point<typename ArrowType::c_type>;

template <typename ArrowType>
using is_arrow_int = std::is_integral<typename ArrowType::c_type>;

template <typename ArrowType>
using is_arrow_string = std::is_same<ArrowType, ::arrow::StringType>;

template <typename ArrowType>
using is_arrow_binary = std::is_same<ArrowType, ::arrow::BinaryType>;

template <typename ArrowType>
using is_arrow_bool = std::is_same<ArrowType, ::arrow::BooleanType>;

template <class ArrowType>
typename std::enable_if<is_arrow_float<ArrowType>::value, Status>::type NonNullArray(
    size_t size, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;
  ::arrow::test::random_real<typename ArrowType::c_type>(size, 0, 0, 1, &values);
  ::arrow::NumericBuilder<ArrowType> builder(
      ::arrow::default_memory_pool(), std::make_shared<ArrowType>());
  builder.Append(values.data(), values.size());
  return builder.Finish(out);
}

template <class ArrowType>
typename std::enable_if<is_arrow_int<ArrowType>::value, Status>::type NonNullArray(
    size_t size, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;
  ::arrow::test::randint<typename ArrowType::c_type>(size, 0, 64, &values);
  ::arrow::NumericBuilder<ArrowType> builder(
      ::arrow::default_memory_pool(), std::make_shared<ArrowType>());
  builder.Append(values.data(), values.size());
  return builder.Finish(out);
}

template <class ArrowType>
typename std::enable_if<
    is_arrow_string<ArrowType>::value || is_arrow_binary<ArrowType>::value, Status>::type
NonNullArray(size_t size, std::shared_ptr<Array>* out) {
  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  BuilderType builder(::arrow::default_memory_pool(), std::make_shared<ArrowType>());
  for (size_t i = 0; i < size; i++) {
    builder.Append("test-string");
  }
  return builder.Finish(out);
}

template <class ArrowType>
typename std::enable_if<is_arrow_bool<ArrowType>::value, Status>::type NonNullArray(
    size_t size, std::shared_ptr<Array>* out) {
  std::vector<uint8_t> values;
  ::arrow::test::randint<uint8_t>(size, 0, 1, &values);
  ::arrow::BooleanBuilder builder(
      ::arrow::default_memory_pool(), std::make_shared<::arrow::BooleanType>());
  builder.Append(values.data(), values.size());
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls.
template <typename ArrowType>
typename std::enable_if<is_arrow_float<ArrowType>::value, Status>::type NullableArray(
    size_t size, size_t num_nulls, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;
  ::arrow::test::random_real<typename ArrowType::c_type>(size, 0, 0, 1, &values);
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  ::arrow::NumericBuilder<ArrowType> builder(
      ::arrow::default_memory_pool(), std::make_shared<ArrowType>());
  builder.Append(values.data(), values.size(), valid_bytes.data());
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls.
template <typename ArrowType>
typename std::enable_if<is_arrow_int<ArrowType>::value, Status>::type NullableArray(
    size_t size, size_t num_nulls, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;
  ::arrow::test::randint<typename ArrowType::c_type>(size, 0, 64, &values);
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  ::arrow::NumericBuilder<ArrowType> builder(
      ::arrow::default_memory_pool(), std::make_shared<ArrowType>());
  builder.Append(values.data(), values.size(), valid_bytes.data());
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls yet.
template <typename ArrowType>
typename std::enable_if<
    is_arrow_string<ArrowType>::value || is_arrow_binary<ArrowType>::value, Status>::type
NullableArray(size_t size, size_t num_nulls, std::shared_ptr<::arrow::Array>* out) {
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  BuilderType builder(::arrow::default_memory_pool(), std::make_shared<ArrowType>());
  for (size_t i = 0; i < size; i++) {
    builder.Append("test-string");
  }
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls yet.
template <class ArrowType>
typename std::enable_if<is_arrow_bool<ArrowType>::value, Status>::type NullableArray(
    size_t size, size_t num_nulls, std::shared_ptr<Array>* out) {
  std::vector<uint8_t> values;
  ::arrow::test::randint<uint8_t>(size, 0, 1, &values);
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  ::arrow::BooleanBuilder builder(
      ::arrow::default_memory_pool(), std::make_shared<::arrow::BooleanType>());
  builder.Append(values.data(), values.size(), valid_bytes.data());
  return builder.Finish(out);
}

std::shared_ptr<::arrow::Column> MakeColumn(
    const std::string& name, const std::shared_ptr<Array>& array, bool nullable) {
  auto field = std::make_shared<::arrow::Field>(name, array->type(), nullable);
  return std::make_shared<::arrow::Column>(field, array);
}

std::shared_ptr<::arrow::Table> MakeSimpleTable(
    const std::shared_ptr<Array>& values, bool nullable) {
  std::shared_ptr<::arrow::Column> column = MakeColumn("col", values, nullable);
  std::vector<std::shared_ptr<::arrow::Column>> columns({column});
  std::vector<std::shared_ptr<::arrow::Field>> fields({column->field()});
  auto schema = std::make_shared<::arrow::Schema>(fields);
  return std::make_shared<::arrow::Table>("table", schema, columns);
}

template <typename T>
void ExpectArray(T* expected, Array* result) {
  auto p_array = static_cast<::arrow::PrimitiveArray*>(result);
  for (int i = 0; i < result->length(); i++) {
    EXPECT_EQ(expected[i], reinterpret_cast<const T*>(p_array->data()->data())[i]);
  }
}

template <typename ArrowType>
void ExpectArrayT(void* expected, Array* result) {
  ::arrow::PrimitiveArray* p_array = static_cast<::arrow::PrimitiveArray*>(result);
  for (int64_t i = 0; i < result->length(); i++) {
    EXPECT_EQ(reinterpret_cast<typename ArrowType::c_type*>(expected)[i],
        reinterpret_cast<const typename ArrowType::c_type*>(p_array->data()->data())[i]);
  }
}

template <>
void ExpectArrayT<::arrow::BooleanType>(void* expected, Array* result) {
  ::arrow::BooleanBuilder builder(
      ::arrow::default_memory_pool(), std::make_shared<::arrow::BooleanType>());
  builder.Append(reinterpret_cast<uint8_t*>(expected), result->length());

  std::shared_ptr<Array> expected_array;
  EXPECT_OK(builder.Finish(&expected_array));
  EXPECT_TRUE(result->Equals(expected_array));
}

}  // namespace arrow

}  // namespace parquet
