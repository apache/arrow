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

// This module defines an abstract interface for iterating through pages in a
// Parquet column chunk within a row group. It could be extended in the future
// to iterate through all data pages in all chunks in a file.

#ifndef PARQUET_COLUMN_TEST_SPECIALIZATION_H
#define PARQUET_COLUMN_TEST_SPECIALIZATION_H

#include <algorithm>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "parquet/test-util.h"

namespace parquet {

namespace test {

template <>
void InitValues<bool>(int num_values, vector<bool>& values, vector<uint8_t>& buffer) {
  values = flip_coins(num_values, 0);
}

template <>
void InitValues<ByteArray>(int num_values, vector<ByteArray>& values,
                           vector<uint8_t>& buffer) {
  int max_byte_array_len = 12;
  int num_bytes = static_cast<int>(max_byte_array_len + sizeof(uint32_t));
  size_t nbytes = num_values * num_bytes;
  buffer.resize(nbytes);
  random_byte_array(num_values, 0, buffer.data(), values.data(), max_byte_array_len);
}

template <>
void InitValues<FLBA>(int num_values, vector<FLBA>& values, vector<uint8_t>& buffer) {
  size_t nbytes = num_values * FLBA_LENGTH;
  buffer.resize(nbytes);
  random_fixed_byte_array(num_values, 0, buffer.data(), FLBA_LENGTH, values.data());
}

template <>
void InitValues<Int96>(int num_values, vector<Int96>& values, vector<uint8_t>& buffer) {
  random_Int96_numbers(num_values, 0, std::numeric_limits<int32_t>::min(),
                       std::numeric_limits<int32_t>::max(), values.data());
}

inline std::string TestColumnName(int i) {
  std::stringstream col_name;
  col_name << "column_" << i;
  return col_name.str();
}

// This class lives here because of its dependency on the InitValues specializations.
template <typename TestType>
class PrimitiveTypedTest : public ::testing::Test {
 public:
  typedef typename TestType::c_type T;

  void SetUpSchema(Repetition::type repetition, int num_columns = 1) {
    std::vector<schema::NodePtr> fields;

    for (int i = 0; i < num_columns; ++i) {
      std::string name = TestColumnName(i);
      fields.push_back(schema::PrimitiveNode::Make(name, repetition, TestType::type_num,
                                                   LogicalType::NONE, FLBA_LENGTH));
    }
    node_ = schema::GroupNode::Make("schema", Repetition::REQUIRED, fields);
    schema_.Init(node_);
  }

  void GenerateData(int64_t num_values);
  void SetupValuesOut(int64_t num_values);
  void SyncValuesOut();

 protected:
  schema::NodePtr node_;
  SchemaDescriptor schema_;

  // Input buffers
  std::vector<T> values_;

  std::vector<int16_t> def_levels_;

  std::vector<uint8_t> buffer_;
  // Pointer to the values, needed as we cannot use vector<bool>::data()
  T* values_ptr_;
  std::vector<uint8_t> bool_buffer_;

  // Output buffers
  std::vector<T> values_out_;
  std::vector<uint8_t> bool_buffer_out_;
  T* values_out_ptr_;
};

template <typename TestType>
void PrimitiveTypedTest<TestType>::SyncValuesOut() {}

template <>
void PrimitiveTypedTest<BooleanType>::SyncValuesOut() {
  std::vector<uint8_t>::const_iterator source_iterator = bool_buffer_out_.begin();
  std::vector<T>::iterator destination_iterator = values_out_.begin();
  while (source_iterator != bool_buffer_out_.end()) {
    *destination_iterator++ = *source_iterator++ != 0;
  }
}

template <typename TestType>
void PrimitiveTypedTest<TestType>::SetupValuesOut(int64_t num_values) {
  values_out_.clear();
  values_out_.resize(num_values);
  values_out_ptr_ = values_out_.data();
}

template <>
void PrimitiveTypedTest<BooleanType>::SetupValuesOut(int64_t num_values) {
  values_out_.clear();
  values_out_.resize(num_values);

  bool_buffer_out_.clear();
  bool_buffer_out_.resize(num_values);
  // Write once to all values so we can copy it without getting Valgrind errors
  // about uninitialised values.
  std::fill(bool_buffer_out_.begin(), bool_buffer_out_.end(), true);
  values_out_ptr_ = reinterpret_cast<bool*>(bool_buffer_out_.data());
}

template <typename TestType>
void PrimitiveTypedTest<TestType>::GenerateData(int64_t num_values) {
  def_levels_.resize(num_values);
  values_.resize(num_values);

  InitValues<T>(static_cast<int>(num_values), values_, buffer_);
  values_ptr_ = values_.data();

  std::fill(def_levels_.begin(), def_levels_.end(), 1);
}

template <>
void PrimitiveTypedTest<BooleanType>::GenerateData(int64_t num_values) {
  def_levels_.resize(num_values);
  values_.resize(num_values);

  InitValues<T>(static_cast<int>(num_values), values_, buffer_);
  bool_buffer_.resize(num_values);
  std::copy(values_.begin(), values_.end(), bool_buffer_.begin());
  values_ptr_ = reinterpret_cast<bool*>(bool_buffer_.data());

  std::fill(def_levels_.begin(), def_levels_.end(), 1);
}
}  // namespace test

}  // namespace parquet

#endif  // PARQUET_COLUMN_TEST_SPECIALIZATION_H
