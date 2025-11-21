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
#include "parquet/endian_internal.h"

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

TEST(TestConvertedTypeToString, ConvertedTypes) {
  ASSERT_STREQ("NONE", ConvertedTypeToString(ConvertedType::NONE).c_str());
  ASSERT_STREQ("UTF8", ConvertedTypeToString(ConvertedType::UTF8).c_str());
  ASSERT_STREQ("MAP", ConvertedTypeToString(ConvertedType::MAP).c_str());
  ASSERT_STREQ("MAP_KEY_VALUE",
               ConvertedTypeToString(ConvertedType::MAP_KEY_VALUE).c_str());
  ASSERT_STREQ("LIST", ConvertedTypeToString(ConvertedType::LIST).c_str());
  ASSERT_STREQ("ENUM", ConvertedTypeToString(ConvertedType::ENUM).c_str());
  ASSERT_STREQ("DECIMAL", ConvertedTypeToString(ConvertedType::DECIMAL).c_str());
  ASSERT_STREQ("DATE", ConvertedTypeToString(ConvertedType::DATE).c_str());
  ASSERT_STREQ("TIME_MILLIS", ConvertedTypeToString(ConvertedType::TIME_MILLIS).c_str());
  ASSERT_STREQ("TIME_MICROS", ConvertedTypeToString(ConvertedType::TIME_MICROS).c_str());
  ASSERT_STREQ("TIMESTAMP_MILLIS",
               ConvertedTypeToString(ConvertedType::TIMESTAMP_MILLIS).c_str());
  ASSERT_STREQ("TIMESTAMP_MICROS",
               ConvertedTypeToString(ConvertedType::TIMESTAMP_MICROS).c_str());
  ASSERT_STREQ("UINT_8", ConvertedTypeToString(ConvertedType::UINT_8).c_str());
  ASSERT_STREQ("UINT_16", ConvertedTypeToString(ConvertedType::UINT_16).c_str());
  ASSERT_STREQ("UINT_32", ConvertedTypeToString(ConvertedType::UINT_32).c_str());
  ASSERT_STREQ("UINT_64", ConvertedTypeToString(ConvertedType::UINT_64).c_str());
  ASSERT_STREQ("INT_8", ConvertedTypeToString(ConvertedType::INT_8).c_str());
  ASSERT_STREQ("INT_16", ConvertedTypeToString(ConvertedType::INT_16).c_str());
  ASSERT_STREQ("INT_32", ConvertedTypeToString(ConvertedType::INT_32).c_str());
  ASSERT_STREQ("INT_64", ConvertedTypeToString(ConvertedType::INT_64).c_str());
  ASSERT_STREQ("JSON", ConvertedTypeToString(ConvertedType::JSON).c_str());
  ASSERT_STREQ("BSON", ConvertedTypeToString(ConvertedType::BSON).c_str());
  ASSERT_STREQ("INTERVAL", ConvertedTypeToString(ConvertedType::INTERVAL).c_str());
}

#ifdef __GNUC__
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#elif defined(_MSC_VER)
#  pragma warning(push)
#  pragma warning(disable : 4996)
#endif

TEST(TypePrinter, StatisticsTypes) {
  std::string smin;
  std::string smax;
  int32_t int_min = 1024;
  int32_t int_max = 2048;
  int_min = internal::ToLittleEndianValue(int_min);
  int_max = internal::ToLittleEndianValue(int_max);
  smin = std::string(reinterpret_cast<char*>(&int_min), sizeof(int_min));
  smax = std::string(reinterpret_cast<char*>(&int_max), sizeof(int_max));
  ASSERT_STREQ("1024", FormatStatValue(Type::INT32, smin).c_str());
  ASSERT_STREQ("2048", FormatStatValue(Type::INT32, smax).c_str());

  int64_t int64_min = 10240000000000;
  int64_t int64_max = 20480000000000;
  int64_min = internal::ToLittleEndianValue(int64_min);
  int64_max = internal::ToLittleEndianValue(int64_max);
  smin = std::string(reinterpret_cast<char*>(&int64_min), sizeof(int64_min));
  smax = std::string(reinterpret_cast<char*>(&int64_max), sizeof(int64_max));
  ASSERT_STREQ("10240000000000", FormatStatValue(Type::INT64, smin).c_str());
  ASSERT_STREQ("20480000000000", FormatStatValue(Type::INT64, smax).c_str());

  float float_min = 1.024f;
  float float_max = 2.048f;
  float_min = internal::ToLittleEndianValue(float_min);
  float_max = internal::ToLittleEndianValue(float_max);
  smin = std::string(reinterpret_cast<char*>(&float_min), sizeof(float_min));
  smax = std::string(reinterpret_cast<char*>(&float_max), sizeof(float_max));
  ASSERT_STREQ("1.024", FormatStatValue(Type::FLOAT, smin).c_str());
  ASSERT_STREQ("2.048", FormatStatValue(Type::FLOAT, smax).c_str());

  double double_min = 1.0245;
  double double_max = 2.0489;
  double_min = internal::ToLittleEndianValue(double_min);
  double_max = internal::ToLittleEndianValue(double_max);
  smin = std::string(reinterpret_cast<char*>(&double_min), sizeof(double_min));
  smax = std::string(reinterpret_cast<char*>(&double_max), sizeof(double_max));
  ASSERT_STREQ("1.0245", FormatStatValue(Type::DOUBLE, smin).c_str());
  ASSERT_STREQ("2.0489", FormatStatValue(Type::DOUBLE, smax).c_str());

  Int96 Int96_min = {{internal::ToLittleEndianValue<uint32_t>(1024),
                      internal::ToLittleEndianValue<uint32_t>(2048),
                      internal::ToLittleEndianValue<uint32_t>(4096)}};
  Int96 Int96_max = {{internal::ToLittleEndianValue<uint32_t>(2048),
                      internal::ToLittleEndianValue<uint32_t>(4096),
                      internal::ToLittleEndianValue<uint32_t>(8192)}};
  smin = std::string(reinterpret_cast<char*>(&Int96_min), sizeof(Int96));
  smax = std::string(reinterpret_cast<char*>(&Int96_max), sizeof(Int96));
  ASSERT_STREQ("1024 2048 4096", FormatStatValue(Type::INT96, smin).c_str());
  ASSERT_STREQ("2048 4096 8192", FormatStatValue(Type::INT96, smax).c_str());

  smin = std::string("abcdef");
  smax = std::string("ijklmnop");
  ASSERT_EQ(smin, FormatStatValue(Type::BYTE_ARRAY, smin, LogicalType::String()));
  ASSERT_EQ(smax, FormatStatValue(Type::BYTE_ARRAY, smax, LogicalType::String()));
  ASSERT_EQ("0x616263646566", FormatStatValue(Type::BYTE_ARRAY, smin));
  ASSERT_EQ("0x696a6b6c6d6e6f70", FormatStatValue(Type::BYTE_ARRAY, smax));

  // PARQUET-1357: FormatStatValue truncates binary statistics on zero character
  smax.push_back('\0');
  ASSERT_EQ(smax, FormatStatValue(Type::BYTE_ARRAY, smax, LogicalType::String()));
  ASSERT_EQ("0x696a6b6c6d6e6f7000", FormatStatValue(Type::BYTE_ARRAY, smax));

  // String
  smin = std::string("abcdefgh");
  smax = std::string("ijklmnop");
  ASSERT_EQ(smin,
            FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smin, LogicalType::String()));
  ASSERT_EQ(smax,
            FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smax, LogicalType::String()));
  ASSERT_EQ("0x6162636465666768", FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smin));
  ASSERT_EQ("0x696a6b6c6d6e6f70", FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smax));

  // Decimal

  // If the physical type is INT32 or INT64, the decimal storage is little-endian.
  int32_t int32_decimal = 1024;
  int32_decimal = internal::ToLittleEndianValue(int32_decimal);
  smin = std::string(reinterpret_cast<char*>(&int32_decimal), sizeof(int32_t));
  ASSERT_EQ("10.24", FormatStatValue(Type::INT32, smin, LogicalType::Decimal(6, 2)));

  int64_t int64_decimal = 102'400'000'000;
  int64_decimal = internal::ToLittleEndianValue(int64_decimal);
  smin = std::string(reinterpret_cast<char*>(&int64_decimal), sizeof(int64_t));
  ASSERT_EQ("10240000.0000",
            FormatStatValue(Type::INT64, smin, LogicalType::Decimal(18, 4)));

  // If the physical type is BYTE_ARRAY or FLBA, the decimal storage is big-endian.
  std::vector<uint8_t> bytes = {0x11, 0x22, 0x33, 0x44};
  smin = std::string(bytes.begin(), bytes.end());
  ASSERT_EQ("28745.4020",
            FormatStatValue(Type::BYTE_ARRAY, smin, LogicalType::Decimal(10, 4)));
  ASSERT_EQ("28745.4020", FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smin,
                                          LogicalType::Decimal(10, 4)));
  ASSERT_EQ("0x11223344", FormatStatValue(Type::BYTE_ARRAY, smin));
  ASSERT_EQ("0x11223344", FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smin));

  bytes = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
           0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
           0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xcf, 0xc7};
  smin = std::string(bytes.begin(), bytes.end());
  ASSERT_EQ("-12.345",
            FormatStatValue(Type::BYTE_ARRAY, smin, LogicalType::Decimal(40, 3)));
  ASSERT_EQ("-12.345", FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smin,
                                       LogicalType::Decimal(40, 3)));
  ASSERT_EQ("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcfc7",
            FormatStatValue(Type::BYTE_ARRAY, smin));
  ASSERT_EQ("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcfc7",
            FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smin));

  // Float16
  bytes = {0x1c, 0x50};
  smin = std::string(bytes.begin(), bytes.end());
  ASSERT_EQ("32.875",
            FormatStatValue(Type::FIXED_LEN_BYTE_ARRAY, smin, LogicalType::Float16()));
}

TEST(TestInt96Timestamp, Decoding) {
  auto check = [](int32_t julian_day, uint64_t nanoseconds) {
    Int96 i96{
        internal::ToLittleEndianValue(static_cast<uint32_t>(nanoseconds)),
        internal::ToLittleEndianValue(static_cast<uint32_t>(nanoseconds >> 32)),
        internal::ToLittleEndianValue(static_cast<uint32_t>(julian_day))};
    // Official formula according to https://github.com/apache/parquet-format/pull/49
    int64_t expected =
        (julian_day - 2440588) * (86400LL * 1000 * 1000 * 1000) + nanoseconds;
    int64_t actual = Int96GetNanoSeconds(i96);
    ASSERT_EQ(expected, actual);
  };

  // [2333837, 2547339] is the range of Julian days that can be converted to
  // 64-bit Unix timestamps.
  check(2333837, 0);
  check(2333855, 0);
  check(2547330, 0);
  check(2547338, 0);
  check(2547339, 0);

  check(2547330, 13);
  check(2547330, 32769);
  check(2547330, 87654);
  check(2547330, 0x123456789abcdefULL);
  check(2547330, 0xfedcba9876543210ULL);
  check(2547339, 0xffffffffffffffffULL);
}

#if !(defined(_WIN32) || defined(__CYGWIN__))
#  pragma GCC diagnostic pop
#elif _MSC_VER
#  pragma warning(pop)
#endif

}  // namespace parquet
