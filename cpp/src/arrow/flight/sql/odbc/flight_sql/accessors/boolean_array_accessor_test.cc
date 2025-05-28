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

#include "arrow/flight/sql/odbc/flight_sql/accessors/boolean_array_accessor.h"
#include "arrow/testing/builder.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

using arrow::BooleanType;
using odbcabstraction::OdbcVersion;

using arrow::ArrayFromVector;

TEST(BooleanArrayFlightSqlAccessor, Test_BooleanArray_CDataType_BIT) {
  const std::vector<bool> values = {true, false, true};
  std::shared_ptr<Array> array;
  ArrayFromVector<BooleanType>(values, &array);

  BooleanArrayFlightSqlAccessor<odbcabstraction::CDataType_BIT> accessor(array.get());

  std::vector<char> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(odbcabstraction::CDataType_BIT, 0, 0, buffer.data(), 0,
                        strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (int i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(unsigned char), strlen_buffer[i]);
    ASSERT_EQ(values[i] ? 1 : 0, buffer[i]);
  }
}

}  // namespace flight_sql
}  // namespace driver
