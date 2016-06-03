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

}  // namespace test

}  // namespace parquet

#endif  // PARQUET_COLUMN_TEST_SPECIALIZATION_H
