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

#include "arrow/flight/sql/odbc/flight_sql/accessors/string_array_accessor.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/encoding.h"
#include "arrow/testing/builder.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {
using arrow::StringType;
using odbcabstraction::OdbcVersion;

using arrow::ArrayFromVector;
using odbcabstraction::GetSqlWCharSize;
using odbcabstraction::Utf8ToWcs;

TEST(StringArrayAccessor, Test_CDataType_CHAR_Basic) {
  std::vector<std::string> values = {"foo", "barx", "baz123"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  StringArrayFlightSqlAccessor<odbcabstraction::CDataType_CHAR, char> accessor(
      array.get());

  size_t max_strlen = 64;
  std::vector<char> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(odbcabstraction::CDataType_CHAR, 0, 0, buffer.data(), max_strlen,
                        strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(values[i].length(), strlen_buffer[i]);
    ASSERT_EQ(values[i], std::string(buffer.data() + i * max_strlen));
  }
}

TEST(StringArrayAccessor, Test_CDataType_CHAR_Truncation) {
  std::vector<std::string> values = {"ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEF"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  StringArrayFlightSqlAccessor<odbcabstraction::CDataType_CHAR, char> accessor(
      array.get());

  size_t max_strlen = 8;
  std::vector<char> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(odbcabstraction::CDataType_CHAR, 0, 0, buffer.data(), max_strlen,
                        strlen_buffer.data());

  std::stringstream ss;
  int64_t value_offset = 0;

  // Construct the whole string by concatenating smaller chunks from
  // GetColumnarData
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  do {
    diagnostics.Clear();
    int64_t original_value_offset = value_offset;
    ASSERT_EQ(1, accessor.GetColumnarData(&binding, 0, 1, value_offset, true, diagnostics,
                                          nullptr));
    ASSERT_EQ(values[0].length() - original_value_offset, strlen_buffer[0]);

    ss << buffer.data();
  } while (value_offset < static_cast<int64_t>(values[0].length()) && value_offset != -1);

  ASSERT_EQ(values[0], ss.str());
}

TEST(StringArrayAccessor, Test_CDataType_WCHAR_Basic) {
  std::vector<std::string> values = {"foo", "barx", "baz123"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  auto accessor = CreateWCharStringArrayAccessor(array.get());

  size_t max_strlen = 64;
  std::vector<uint8_t> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(odbcabstraction::CDataType_WCHAR, 0, 0, buffer.data(), max_strlen,
                        strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor->GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                      diagnostics, nullptr));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(values[i].length() * GetSqlWCharSize(), strlen_buffer[i]);
    std::vector<uint8_t> expected;
    Utf8ToWcs(values[i].c_str(), &expected);
    uint8_t* start = buffer.data() + i * max_strlen;
    auto actual = std::vector<uint8_t>(start, start + strlen_buffer[i]);
    ASSERT_EQ(expected, actual);
  }
}

TEST(StringArrayAccessor, Test_CDataType_WCHAR_Truncation) {
  std::vector<std::string> values = {"ABCDEFA"};
  std::shared_ptr<Array> array;
  ArrayFromVector<StringType, std::string>(values, &array);

  auto accessor = CreateWCharStringArrayAccessor(array.get());

  size_t max_strlen = 8;
  std::vector<uint8_t> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(odbcabstraction::CDataType_WCHAR, 0, 0, buffer.data(), max_strlen,
                        strlen_buffer.data());

  std::basic_stringstream<uint8_t> ss;
  int64_t value_offset = 0;

  // Construct the whole string by concatenating smaller chunks from
  // GetColumnarData
  std::vector<uint8_t> finalStr;
  driver::odbcabstraction::Diagnostics diagnostics("Dummy", "Dummy",
                                                   odbcabstraction::V_3);
  do {
    int64_t original_value_offset = value_offset;
    ASSERT_EQ(1, accessor->GetColumnarData(&binding, 0, 1, value_offset, true,
                                           diagnostics, nullptr));
    ASSERT_EQ(values[0].length() * GetSqlWCharSize() - original_value_offset,
              strlen_buffer[0]);

    size_t length = value_offset - original_value_offset;
    if (value_offset == -1) {
      length = buffer.size();
    }
    finalStr.insert(finalStr.end(), buffer.data(), buffer.data() + length);
  } while (value_offset < static_cast<int64_t>(values[0].length() * GetSqlWCharSize()) &&
           value_offset != -1);

  // Trim final null bytes
  finalStr.resize(values[0].length() * GetSqlWCharSize());

  std::vector<uint8_t> expected;
  Utf8ToWcs(values[0].c_str(), &expected);
  ASSERT_EQ(expected, finalStr);
}

}  // namespace flight_sql
}  // namespace driver
