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

#include "arrow/flight/sql/odbc/flight_sql/accessors/binary_array_accessor.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

using arrow::BinaryType;
using odbcabstraction::OdbcVersion;

using arrow::ArrayFromVector;

TEST(BinaryArrayAccessor, Test_CDataType_BINARY_Basic) {
  std::vector<std::string> values = {"foo", "barx", "baz123"};
  std::shared_ptr<Array> array;
  ArrayFromVector<BinaryType, std::string>(values, &array);

  BinaryArrayFlightSqlAccessor<odbcabstraction::CDataType_BINARY> accessor(array.get());

  size_t max_strlen = 64;
  std::vector<char> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(odbcabstraction::CDataType_BINARY, 0, 0, buffer.data(),
                        max_strlen, strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(values[i].length(), strlen_buffer[i]);
    // Beware that CDataType_BINARY values are not null terminated.
    // It's safe to create a std::string from this data because we know it's
    // ASCII, this doesn't work with arbitrary binary data.
    ASSERT_EQ(values[i], std::string(buffer.data() + i * max_strlen,
                                     buffer.data() + i * max_strlen + strlen_buffer[i]));
  }
}

TEST(BinaryArrayAccessor, Test_CDataType_BINARY_Truncation) {
  std::vector<std::string> values = {"ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEF"};
  std::shared_ptr<Array> array;
  ArrayFromVector<BinaryType, std::string>(values, &array);

  BinaryArrayFlightSqlAccessor<odbcabstraction::CDataType_BINARY> accessor(array.get());

  size_t max_strlen = 8;
  std::vector<char> buffer(values.size() * max_strlen);
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(odbcabstraction::CDataType_BINARY, 0, 0, buffer.data(),
                        max_strlen, strlen_buffer.data());

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

    int64_t chunk_length = 0;
    if (value_offset == -1) {
      chunk_length = strlen_buffer[0];
    } else {
      chunk_length = max_strlen;
    }

    // Beware that CDataType_BINARY values are not null terminated.
    // It's safe to create a std::string from this data because we know it's
    // ASCII, this doesn't work with arbitrary binary data.
    ss << std::string(buffer.data(), buffer.data() + chunk_length);
  } while (value_offset < static_cast<int64_t>(values[0].length()) && value_offset != -1);

  ASSERT_EQ(values[0], ss.str());
}

}  // namespace flight_sql
}  // namespace driver
