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

#include "arrow/flight/sql/odbc/odbc_impl/accessors/boolean_array_accessor.h"
#include "arrow/testing/builder.h"
#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {

TEST(BooleanArrayFlightSqlAccessor, TestBooleanArrayCDataTypeBit) {
  const std::vector<bool> values = {true, false, true};
  std::shared_ptr<Array> array;
  ArrayFromVector<BooleanType>(values, &array);

  BooleanArrayFlightSqlAccessor<CDataType_BIT> accessor(array.get());

  std::vector<char> buffer(values.size());
  std::vector<ssize_t> str_len_buffer(values.size());

  ColumnBinding binding(CDataType_BIT, 0, 0, buffer.data(), 0, str_len_buffer.data());

  int64_t value_offset = 0;
  Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(unsigned char), str_len_buffer[i]);
    ASSERT_EQ(values[i] ? 1 : 0, buffer[i]);
  }
}

}  // namespace arrow::flight::sql::odbc
