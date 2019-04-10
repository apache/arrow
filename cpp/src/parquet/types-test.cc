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
  ASSERT_STREQ("BOOLEAN", TypeToString(Type::BOOLEAN).c_str());
  ASSERT_STREQ("INT32", TypeToString(Type::INT32).c_str());
  ASSERT_STREQ("INT64", TypeToString(Type::INT64).c_str());
  ASSERT_STREQ("INT96", TypeToString(Type::INT96).c_str());
  ASSERT_STREQ("FLOAT", TypeToString(Type::FLOAT).c_str());
  ASSERT_STREQ("DOUBLE", TypeToString(Type::DOUBLE).c_str());
  ASSERT_STREQ("BYTE_ARRAY", TypeToString(Type::BYTE_ARRAY).c_str());
  ASSERT_STREQ("FIXED_LEN_BYTE_ARRAY", TypeToString(Type::FIXED_LEN_BYTE_ARRAY).c_str());
}

TEST(TestLogicalTypeToString, LogicalTypes) {
  ASSERT_STREQ("NONE", LogicalTypeToString(LogicalType::NONE).c_str());
  ASSERT_STREQ("UTF8", LogicalTypeToString(LogicalType::UTF8).c_str());
  ASSERT_STREQ("MAP_KEY_VALUE", LogicalTypeToString(LogicalType::MAP_KEY_VALUE).c_str());
  ASSERT_STREQ("LIST", LogicalTypeToString(LogicalType::LIST).c_str());
  ASSERT_STREQ("ENUM", LogicalTypeToString(LogicalType::ENUM).c_str());
  ASSERT_STREQ("DECIMAL", LogicalTypeToString(LogicalType::DECIMAL).c_str());
  ASSERT_STREQ("DATE", LogicalTypeToString(LogicalType::DATE).c_str());
  ASSERT_STREQ("TIME_MILLIS", LogicalTypeToString(LogicalType::TIME_MILLIS).c_str());
  ASSERT_STREQ("TIME_MICROS", LogicalTypeToString(LogicalType::TIME_MICROS).c_str());
  ASSERT_STREQ("TIMESTAMP_MILLIS",
               LogicalTypeToString(LogicalType::TIMESTAMP_MILLIS).c_str());
  ASSERT_STREQ("TIMESTAMP_MICROS",
               LogicalTypeToString(LogicalType::TIMESTAMP_MICROS).c_str());
  ASSERT_STREQ("UINT_8", LogicalTypeToString(LogicalType::UINT_8).c_str());
  ASSERT_STREQ("UINT_16", LogicalTypeToString(LogicalType::UINT_16).c_str());
  ASSERT_STREQ("UINT_32", LogicalTypeToString(LogicalType::UINT_32).c_str());
  ASSERT_STREQ("UINT_64", LogicalTypeToString(LogicalType::UINT_64).c_str());
  ASSERT_STREQ("INT_8", LogicalTypeToString(LogicalType::INT_8).c_str());
  ASSERT_STREQ("INT_16", LogicalTypeToString(LogicalType::INT_16).c_str());
  ASSERT_STREQ("INT_32", LogicalTypeToString(LogicalType::INT_32).c_str());
  ASSERT_STREQ("INT_64", LogicalTypeToString(LogicalType::INT_64).c_str());
  ASSERT_STREQ("JSON", LogicalTypeToString(LogicalType::JSON).c_str());
  ASSERT_STREQ("BSON", LogicalTypeToString(LogicalType::BSON).c_str());
  ASSERT_STREQ("INTERVAL", LogicalTypeToString(LogicalType::INTERVAL).c_str());
}

TEST(TestCompressionToString, Compression) {
  ASSERT_STREQ("UNCOMPRESSED", CompressionToString(Compression::UNCOMPRESSED).c_str());
  ASSERT_STREQ("SNAPPY", CompressionToString(Compression::SNAPPY).c_str());
  ASSERT_STREQ("GZIP", CompressionToString(Compression::GZIP).c_str());
  ASSERT_STREQ("LZO", CompressionToString(Compression::LZO).c_str());
  ASSERT_STREQ("BROTLI", CompressionToString(Compression::BROTLI).c_str());
  ASSERT_STREQ("LZ4", CompressionToString(Compression::LZ4).c_str());
  ASSERT_STREQ("ZSTD", CompressionToString(Compression::ZSTD).c_str());
}

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#elif defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4996)
#endif

TEST(TypePrinter, StatisticsTypes) {
  std::string smin;
  std::string smax;
  int32_t int_min = 1024;
  int32_t int_max = 2048;
  smin = std::string(reinterpret_cast<char*>(&int_min), sizeof(int32_t));
  smax = std::string(reinterpret_cast<char*>(&int_max), sizeof(int32_t));
  ASSERT_STREQ("1024", FormatStatValue(Type::INT32, smin).c_str());
  ASSERT_STREQ("1024", FormatStatValue(Type::INT32, smin.c_str()).c_str());
  ASSERT_STREQ("2048", FormatStatValue(Type::INT32, smax).c_str());
  ASSERT_STREQ("2048", FormatStatValue(Type::INT32, smax.c_str()).c_str());

  int64_t int64_min = 10240000000000;
  int64_t int64_max = 20480000000000;
  smin = std::string(reinterpret_cast<char*>(&int64_min), sizeof(int64_t));
  smax = std::string(reinterpret_cast<char*>(&int64_max), sizeof(int64_t));
  ASSERT_STREQ("10240000000000", FormatStatValue(Type::INT64, smin).c_str());
  ASSERT_STREQ("10240000000000", FormatStatValue(Type::INT64, smin.c_str()).c_str());
  ASSERT_STREQ("20480000000000", FormatStatValue(Type::INT64, smax).c_str());
  ASSERT_STREQ("20480000000000", FormatStatValue(Type::INT64, smax.c_str()).c_str());

  float float_min = 1.024f;
  float float_max = 2.048f;
  smin = std::string(reinterpret_cast<char*>(&float_min), sizeof(float));
  smax = std::string(reinterpret_cast<char*>(&float_max), sizeof(float));
  ASSERT_STREQ("1.024", FormatStatValue(Type::FLOAT, smin).c_str());
  ASSERT_STREQ("1.024", FormatStatValue(Type::FLOAT, smin.c_str()).c_str());
  ASSERT_STREQ("2.048", FormatStatValue(Type::FLOAT, smax).c_str());
  ASSERT_STREQ("2.048", FormatStatValue(Type::FLOAT, smax.c_str()).c_str());

  double double_min = 1.0245;
  double double_max = 2.0489;
  smin = std::string(reinterpret_cast<char*>(&double_min), sizeof(double));
  smax = std::string(reinterpret_cast<char*>(&double_max), sizeof(double));
  ASSERT_STREQ("1.0245", FormatStatValue(Type::DOUBLE, smin).c_str());
  ASSERT_STREQ("1.0245", FormatStatValue(Type::DOUBLE, smin.c_str()).c_str());
  ASSERT_STREQ("2.0489", FormatStatValue(Type::DOUBLE, smax).c_str());
  ASSERT_STREQ("2.0489", FormatStatValue(Type::DOUBLE, smax.c_str()).c_str());

  Int96 Int96_min = {{1024, 2048, 4096}};
  Int96 Int96_max = {{2048, 4096, 8192}};
  smin = std::string(reinterpret_cast<char*>(&Int96_min), sizeof(Int96));
  smax = std::string(reinterpret_cast<char*>(&Int96_max), sizeof(Int96));
  ASSERT_STREQ("1024 2048 4096", FormatStatValue(Type::INT96, smin).c_str());
  ASSERT_STREQ("1024 2048 4096", FormatStatValue(Type::INT96, smin.c_str()).c_str());
  ASSERT_STREQ("2048 4096 8192", FormatStatValue(Type::INT96, smax).c_str());
  ASSERT_STREQ("2048 4096 8192", FormatStatValue(Type::INT96, smax.c_str()).c_str());

  smin = std::string("abcdef");
  smax = std::string("ijklmnop");
  ASSERT_STREQ("abcdef", FormatStatValue(Type::BYTE_ARRAY, smin).c_str());
  ASSERT_STREQ("abcdef", FormatStatValue(Type::BYTE_ARRAY, smin.c_str()).c_str());
  ASSERT_STREQ("ijklmnop", FormatStatValue(Type::BYTE_ARRAY, smax).c_str());
  ASSERT_STREQ("ijklmnop", FormatStatValue(Type::BYTE_ARRAY, smax.c_str()).c_str());

  // PARQUET-1357: FormatStatValue truncates binary statistics on zero character
  smax.push_back('\0');
  ASSERT_EQ(smax, FormatStatValue(Type::BYTE_ARRAY, smax));
  // This fails, thus the call to FormatStatValue(.., const char*) was deprecated.
  // ASSERT_EQ(smax, FormatStatValue(Type::BYTE_ARRAY, smax.c_str()));

  smin = std::string("abcdefgh");
  smax = std::string("ijklmnop");
  ASSERT_STREQ("abcdefgh", FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smin).c_str());
  ASSERT_STREQ("abcdefgh",
               FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smin.c_str()).c_str());
  ASSERT_STREQ("ijklmnop", FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smax).c_str());
  ASSERT_STREQ("ijklmnop",
               FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smax.c_str()).c_str());
}

#if !(defined(_WIN32) || defined(__CYGWIN__))
#pragma GCC diagnostic pop
#elif _MSC_VER
#pragma warning(pop)
#endif

}  // namespace parquet
