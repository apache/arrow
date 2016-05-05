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

namespace parquet {

TEST(TestTypeToString, PhysicalTypes) {
  ASSERT_STREQ("BOOLEAN", type_to_string(Type::BOOLEAN).c_str());
  ASSERT_STREQ("INT32", type_to_string(Type::INT32).c_str());
  ASSERT_STREQ("INT64", type_to_string(Type::INT64).c_str());
  ASSERT_STREQ("INT96", type_to_string(Type::INT96).c_str());
  ASSERT_STREQ("FLOAT", type_to_string(Type::FLOAT).c_str());
  ASSERT_STREQ("DOUBLE", type_to_string(Type::DOUBLE).c_str());
  ASSERT_STREQ("BYTE_ARRAY", type_to_string(Type::BYTE_ARRAY).c_str());
  ASSERT_STREQ(
      "FIXED_LEN_BYTE_ARRAY", type_to_string(Type::FIXED_LEN_BYTE_ARRAY).c_str());
}

TEST(TestLogicalTypeToString, LogicalTypes) {
  ASSERT_STREQ("NONE", logical_type_to_string(LogicalType::NONE).c_str());
  ASSERT_STREQ("UTF8", logical_type_to_string(LogicalType::UTF8).c_str());
  ASSERT_STREQ(
      "MAP_KEY_VALUE", logical_type_to_string(LogicalType::MAP_KEY_VALUE).c_str());
  ASSERT_STREQ("LIST", logical_type_to_string(LogicalType::LIST).c_str());
  ASSERT_STREQ("ENUM", logical_type_to_string(LogicalType::ENUM).c_str());
  ASSERT_STREQ("DECIMAL", logical_type_to_string(LogicalType::DECIMAL).c_str());
  ASSERT_STREQ("DATE", logical_type_to_string(LogicalType::DATE).c_str());
  ASSERT_STREQ("TIME_MILLIS", logical_type_to_string(LogicalType::TIME_MILLIS).c_str());
  ASSERT_STREQ(
      "TIMESTAMP_MILLIS", logical_type_to_string(LogicalType::TIMESTAMP_MILLIS).c_str());
  ASSERT_STREQ("UINT_8", logical_type_to_string(LogicalType::UINT_8).c_str());
  ASSERT_STREQ("UINT_16", logical_type_to_string(LogicalType::UINT_16).c_str());
  ASSERT_STREQ("UINT_32", logical_type_to_string(LogicalType::UINT_32).c_str());
  ASSERT_STREQ("UINT_64", logical_type_to_string(LogicalType::UINT_64).c_str());
  ASSERT_STREQ("INT_8", logical_type_to_string(LogicalType::INT_8).c_str());
  ASSERT_STREQ("INT_16", logical_type_to_string(LogicalType::INT_16).c_str());
  ASSERT_STREQ("INT_32", logical_type_to_string(LogicalType::INT_32).c_str());
  ASSERT_STREQ("INT_64", logical_type_to_string(LogicalType::INT_64).c_str());
  ASSERT_STREQ("JSON", logical_type_to_string(LogicalType::JSON).c_str());
  ASSERT_STREQ("BSON", logical_type_to_string(LogicalType::BSON).c_str());
  ASSERT_STREQ("INTERVAL", logical_type_to_string(LogicalType::INTERVAL).c_str());
}

TEST(TypePrinter, PhysicalTypes) {
  std::string smin;
  std::string smax;
  int32_t int_min = 1024;
  int32_t int_max = 2048;
  smin = std::string(reinterpret_cast<char*>(&int_min), sizeof(int32_t));
  smax = std::string(reinterpret_cast<char*>(&int_max), sizeof(int32_t));
  ASSERT_STREQ("1024", FormatValue(Type::INT32, smin.c_str(), 0).c_str());
  ASSERT_STREQ("2048", FormatValue(Type::INT32, smax.c_str(), 0).c_str());

  int64_t int64_min = 10240000000000;
  int64_t int64_max = 20480000000000;
  smin = std::string(reinterpret_cast<char*>(&int64_min), sizeof(int64_t));
  smax = std::string(reinterpret_cast<char*>(&int64_max), sizeof(int64_t));
  ASSERT_STREQ("10240000000000", FormatValue(Type::INT64, smin.c_str(), 0).c_str());
  ASSERT_STREQ("20480000000000", FormatValue(Type::INT64, smax.c_str(), 0).c_str());

  float float_min = 1.024;
  float float_max = 2.048;
  smin = std::string(reinterpret_cast<char*>(&float_min), sizeof(float));
  smax = std::string(reinterpret_cast<char*>(&float_max), sizeof(float));
  ASSERT_STREQ("1.024", FormatValue(Type::FLOAT, smin.c_str(), 0).c_str());
  ASSERT_STREQ("2.048", FormatValue(Type::FLOAT, smax.c_str(), 0).c_str());

  double double_min = 1.0245;
  double double_max = 2.0489;
  smin = std::string(reinterpret_cast<char*>(&double_min), sizeof(double));
  smax = std::string(reinterpret_cast<char*>(&double_max), sizeof(double));
  ASSERT_STREQ("1.0245", FormatValue(Type::DOUBLE, smin.c_str(), 0).c_str());
  ASSERT_STREQ("2.0489", FormatValue(Type::DOUBLE, smax.c_str(), 0).c_str());

  Int96 Int96_min = {{1024, 2048, 4096}};
  Int96 Int96_max = {{2048, 4096, 8192}};
  smin = std::string(reinterpret_cast<char*>(&Int96_min), sizeof(Int96));
  smax = std::string(reinterpret_cast<char*>(&Int96_max), sizeof(Int96));
  ASSERT_STREQ("1024 2048 4096 ", FormatValue(Type::INT96, smin.c_str(), 0).c_str());
  ASSERT_STREQ("2048 4096 8192 ", FormatValue(Type::INT96, smax.c_str(), 0).c_str());

  ByteArray BA_min;
  ByteArray BA_max;
  BA_min.ptr = reinterpret_cast<const uint8_t*>("abcdef");
  BA_min.len = 6;
  BA_max.ptr = reinterpret_cast<const uint8_t*>("ijklmnop");
  BA_max.len = 8;
  smin = std::string(reinterpret_cast<char*>(&BA_min), sizeof(ByteArray));
  smax = std::string(reinterpret_cast<char*>(&BA_max), sizeof(ByteArray));
  ASSERT_STREQ("a b c d e f ", FormatValue(Type::BYTE_ARRAY, smin.c_str(), 0).c_str());
  ASSERT_STREQ(
      "i j k l m n o p ", FormatValue(Type::BYTE_ARRAY, smax.c_str(), 0).c_str());

  FLBA FLBA_min;
  FLBA FLBA_max;
  FLBA_min.ptr = reinterpret_cast<const uint8_t*>("abcdefgh");
  FLBA_max.ptr = reinterpret_cast<const uint8_t*>("ijklmnop");
  int len = 8;
  smin = std::string(reinterpret_cast<char*>(&FLBA_min), sizeof(FLBA));
  smax = std::string(reinterpret_cast<char*>(&FLBA_max), sizeof(FLBA));
  ASSERT_STREQ("a b c d e f g h ",
      FormatValue(Type::FIXED_LEN_BYTE_ARRAY, smin.c_str(), len).c_str());
  ASSERT_STREQ("i j k l m n o p ",
      FormatValue(Type::FIXED_LEN_BYTE_ARRAY, smax.c_str(), len).c_str());
}

}  // namespace parquet
