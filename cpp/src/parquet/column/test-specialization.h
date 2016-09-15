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
#include <vector>

#include "parquet/column/test-util.h"

namespace parquet {

namespace test {

template <>
void InitValues<bool>(int num_values, vector<bool>& values, vector<uint8_t>& buffer) {
  values = flip_coins(num_values, 0);
}

template <>
void InitValues<ByteArray>(
    int num_values, vector<ByteArray>& values, vector<uint8_t>& buffer) {
  int max_byte_array_len = 12;
  int num_bytes = max_byte_array_len + sizeof(uint32_t);
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

// This class lives here because of its dependency on the InitValues specializations.
template <typename TestType>
class PrimitiveTypedTest : public ::testing::Test {
 public:
  typedef typename TestType::c_type T;

  void SetUpSchemaRequired() {
    primitive_node_ = schema::PrimitiveNode::Make("column", Repetition::REQUIRED,
        TestType::type_num, LogicalType::NONE, FLBA_LENGTH);
    descr_ = std::make_shared<ColumnDescriptor>(primitive_node_, 0, 0);
    node_ = schema::GroupNode::Make(
        "schema", Repetition::REQUIRED, std::vector<schema::NodePtr>({primitive_node_}));
    schema_.Init(node_);
  }

  void SetUpSchemaOptional() {
    primitive_node_ = schema::PrimitiveNode::Make("column", Repetition::OPTIONAL,
        TestType::type_num, LogicalType::NONE, FLBA_LENGTH);
    descr_ = std::make_shared<ColumnDescriptor>(primitive_node_, 1, 0);
    node_ = schema::GroupNode::Make(
        "schema", Repetition::REQUIRED, std::vector<schema::NodePtr>({primitive_node_}));
    schema_.Init(node_);
  }

  void SetUpSchemaRepeated() {
    primitive_node_ = schema::PrimitiveNode::Make("column", Repetition::REPEATED,
        TestType::type_num, LogicalType::NONE, FLBA_LENGTH);
    descr_ = std::make_shared<ColumnDescriptor>(primitive_node_, 1, 1);
    node_ = schema::GroupNode::Make(
        "schema", Repetition::REQUIRED, std::vector<schema::NodePtr>({primitive_node_}));
    schema_.Init(node_);
  }

  void GenerateData(int64_t num_values);
  void SetupValuesOut(int64_t num_values);
  void SyncValuesOut();
  void SetUp() { SetUpSchemaRequired(); }

 protected:
  schema::NodePtr primitive_node_;
  schema::NodePtr node_;
  SchemaDescriptor schema_;
  std::shared_ptr<ColumnDescriptor> descr_;

  // Input buffers
  std::vector<T> values_;
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
  std::copy(bool_buffer_out_.begin(), bool_buffer_out_.end(), values_out_.begin());
}

template <typename TestType>
void PrimitiveTypedTest<TestType>::SetupValuesOut(int64_t num_values) {
  values_out_.resize(num_values);
  values_out_ptr_ = values_out_.data();
}

template <>
void PrimitiveTypedTest<BooleanType>::SetupValuesOut(int64_t num_values) {
  values_out_.resize(num_values);
  bool_buffer_out_.resize(num_values);
  // Write once to all values so we can copy it without getting Valgrind errors
  // about uninitialised values.
  std::fill(bool_buffer_out_.begin(), bool_buffer_out_.end(), true);
  values_out_ptr_ = reinterpret_cast<bool*>(bool_buffer_out_.data());
}

template <typename TestType>
void PrimitiveTypedTest<TestType>::GenerateData(int64_t num_values) {
  values_.resize(num_values);
  InitValues<T>(num_values, values_, buffer_);
  values_ptr_ = values_.data();
}

template <>
void PrimitiveTypedTest<BooleanType>::GenerateData(int64_t num_values) {
  values_.resize(num_values);
  InitValues<T>(num_values, values_, buffer_);
  bool_buffer_.resize(num_values);
  std::copy(values_.begin(), values_.end(), bool_buffer_.begin());
  values_ptr_ = reinterpret_cast<bool*>(bool_buffer_.data());
}
}  // namespace test

}  // namespace parquet

#endif  // PARQUET_COLUMN_TEST_SPECIALIZATION_H
