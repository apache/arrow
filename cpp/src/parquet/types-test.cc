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

#include <gtest/gtest.h>

#include <string>

#include "parquet/types.h"

namespace parquet_cpp {

TEST(TestTypeToString, PhysicalTypes) {
  ASSERT_STREQ("BOOLEAN", type_to_string(Type::BOOLEAN).c_str());
  ASSERT_STREQ("INT32", type_to_string(Type::INT32).c_str());
  ASSERT_STREQ("INT64", type_to_string(Type::INT64).c_str());
  ASSERT_STREQ("INT96", type_to_string(Type::INT96).c_str());
  ASSERT_STREQ("FLOAT", type_to_string(Type::FLOAT).c_str());
  ASSERT_STREQ("DOUBLE", type_to_string(Type::DOUBLE).c_str());
  ASSERT_STREQ("BYTE_ARRAY", type_to_string(Type::BYTE_ARRAY).c_str());
  ASSERT_STREQ("FIXED_LEN_BYTE_ARRAY",
      type_to_string(Type::FIXED_LEN_BYTE_ARRAY).c_str());
}

TEST(TestLogicalTypeToString, LogicalTypes) {
  ASSERT_STREQ("NONE",
      logical_type_to_string(LogicalType::NONE).c_str());
  ASSERT_STREQ("UTF8",
      logical_type_to_string(LogicalType::UTF8).c_str());
  ASSERT_STREQ("MAP_KEY_VALUE",
      logical_type_to_string(LogicalType::MAP_KEY_VALUE).c_str());
  ASSERT_STREQ("LIST",
      logical_type_to_string(LogicalType::LIST).c_str());
  ASSERT_STREQ("ENUM",
      logical_type_to_string(LogicalType::ENUM).c_str());
  ASSERT_STREQ("DECIMAL",
      logical_type_to_string(LogicalType::DECIMAL).c_str());
  ASSERT_STREQ("DATE",
      logical_type_to_string(LogicalType::DATE).c_str());
  ASSERT_STREQ("TIME_MILLIS",
      logical_type_to_string(LogicalType::TIME_MILLIS).c_str());
  ASSERT_STREQ("TIMESTAMP_MILLIS",
      logical_type_to_string(LogicalType::TIMESTAMP_MILLIS).c_str());
  ASSERT_STREQ("UINT_8",
      logical_type_to_string(LogicalType::UINT_8).c_str());
  ASSERT_STREQ("UINT_16",
      logical_type_to_string(LogicalType::UINT_16).c_str());
  ASSERT_STREQ("UINT_32",
      logical_type_to_string(LogicalType::UINT_32).c_str());
  ASSERT_STREQ("UINT_64",
      logical_type_to_string(LogicalType::UINT_64).c_str());
  ASSERT_STREQ("INT_8",
      logical_type_to_string(LogicalType::INT_8).c_str());
  ASSERT_STREQ("INT_16",
      logical_type_to_string(LogicalType::INT_16).c_str());
  ASSERT_STREQ("INT_32",
      logical_type_to_string(LogicalType::INT_32).c_str());
  ASSERT_STREQ("INT_64",
      logical_type_to_string(LogicalType::INT_64).c_str());
  ASSERT_STREQ("JSON",
      logical_type_to_string(LogicalType::JSON).c_str());
  ASSERT_STREQ("BSON",
      logical_type_to_string(LogicalType::BSON).c_str());
  ASSERT_STREQ("INTERVAL",
      logical_type_to_string(LogicalType::INTERVAL).c_str());
}


} // namespace parquet_cpp
