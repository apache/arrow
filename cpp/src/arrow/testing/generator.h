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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/visibility.h"
#include "arrow/type.h"

namespace arrow {

class ARROW_TESTING_EXPORT ConstantArrayGenerator {
 public:
  /// \brief Generates a constant BooleanArray
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> Boolean(int64_t size, bool value = false);

  /// \brief Generates a constant UInt8Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> UInt8(int64_t size, uint8_t value = 0);

  /// \brief Generates a constant Int8Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> Int8(int64_t size, int8_t value = 0);

  /// \brief Generates a constant UInt16Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> UInt16(int64_t size, uint16_t value = 0);

  /// \brief Generates a constant UInt16Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> Int16(int64_t size, int16_t value = 0);

  /// \brief Generates a constant UInt32Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> UInt32(int64_t size, uint32_t value = 0);

  /// \brief Generates a constant UInt32Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> Int32(int64_t size, int32_t value = 0);

  /// \brief Generates a constant UInt64Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> UInt64(int64_t size, uint64_t value = 0);

  /// \brief Generates a constant UInt64Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> Int64(int64_t size, int64_t value = 0);

  /// \brief Generates a constant Float32Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> Float32(int64_t size, float value = 0);

  /// \brief Generates a constant Float64Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> Float64(int64_t size, double value = 0);

  /// \brief Generates a constant StringArray
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] value to repeat
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> String(int64_t size, std::string value = "");

  template <typename ArrowType, typename CType = typename ArrowType::c_type>
  static std::shared_ptr<arrow::Array> Numeric(int64_t size, CType value = 0) {
    switch (ArrowType::type_id) {
      case Type::BOOL:
        return Boolean(size, static_cast<bool>(value));
      case Type::UINT8:
        return UInt8(size, static_cast<uint8_t>(value));
      case Type::INT8:
        return Int8(size, static_cast<int8_t>(value));
      case Type::UINT16:
        return UInt16(size, static_cast<uint16_t>(value));
      case Type::INT16:
        return Int16(size, static_cast<int16_t>(value));
      case Type::UINT32:
        return UInt32(size, static_cast<uint32_t>(value));
      case Type::INT32:
        return Int32(size, static_cast<int32_t>(value));
      case Type::UINT64:
        return UInt64(size, static_cast<uint64_t>(value));
      case Type::INT64:
        return Int64(size, static_cast<int64_t>(value));
      case Type::FLOAT:
        return Float32(size, static_cast<float>(value));
      case Type::DOUBLE:
        return Float64(size, static_cast<double>(value));
      default:
        return nullptr;
    }
  }

  /// \brief Generates a constant Array of zeroes
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] type the type of the Array
  ///
  /// \return a generated Array
  static std::shared_ptr<arrow::Array> Zeroes(int64_t size,
                                              const std::shared_ptr<DataType>& type) {
    switch (type->id()) {
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

  /// \brief Generates a RecordBatch of zeroes
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] schema to conform to
  ///
  /// This function is handy to return of RecordBatch of a desired shape.
  ///
  /// \return a generated RecordBatch
  static std::shared_ptr<arrow::RecordBatch> Zeroes(
      int64_t size, const std::shared_ptr<Schema>& schema) {
    std::vector<std::shared_ptr<Array>> arrays;

    for (const auto& field : schema->fields()) {
      arrays.emplace_back(Zeroes(size, field->type()));
    }

    return RecordBatch::Make(schema, size, arrays);
  }

  /// \brief Generates a RecordBatchReader by repeating a RecordBatch
  ///
  /// \param[in] n_batch the number of times it repeats batch
  /// \param[in] batch the RecordBatch to repeat
  ///
  /// \return a generated RecordBatchReader
  static std::shared_ptr<arrow::RecordBatchReader> Repeat(
      int64_t n_batch, const std::shared_ptr<RecordBatch> batch) {
    std::vector<std::shared_ptr<RecordBatch>> batches(static_cast<size_t>(n_batch),
                                                      batch);
    return *RecordBatchReader::Make(batches);
  }

  /// \brief Generates a RecordBatchReader of zeroes batches
  ///
  /// \param[in] n_batch the number of RecordBatch
  /// \param[in] batch_size the size of each RecordBatch
  /// \param[in] schema to conform to
  ///
  /// \return a generated RecordBatchReader
  static std::shared_ptr<arrow::RecordBatchReader> Zeroes(
      int64_t n_batch, int64_t batch_size, const std::shared_ptr<Schema>& schema) {
    return Repeat(n_batch, Zeroes(batch_size, schema));
  }
};

}  // namespace arrow
