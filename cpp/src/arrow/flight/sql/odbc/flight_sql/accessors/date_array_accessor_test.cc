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

#include "arrow/flight/sql/odbc/flight_sql/accessors/date_array_accessor.h"
#include "arrow/flight/sql/odbc/flight_sql/accessors/boolean_array_accessor.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/calendar_utils.h"
#include "arrow/testing/builder.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

using arrow::Date32Array;
using arrow::Date32Type;
using arrow::Date64Array;
using arrow::Date64Type;
using arrow::NumericArray;

using odbcabstraction::DATE_STRUCT;
using odbcabstraction::OdbcVersion;
using odbcabstraction::tagDATE_STRUCT;

using arrow::ArrayFromVector;
using odbcabstraction::GetTimeForSecondsSinceEpoch;

TEST(DateArrayAccessor, Test_Date32Array_CDataType_DATE) {
  std::vector<int32_t> values = {7589, 12320, 18980, 19095, -1, 0};
  std::vector<tagDATE_STRUCT> expected = {
      {1990, 10, 12}, {2003, 9, 25},  {2021, 12, 19},
      {2022, 4, 13},  {1969, 12, 31}, {1970, 1, 1},
  };

  std::shared_ptr<Array> array;
  ArrayFromVector<Date32Type, int32_t>(values, &array);

  DateArrayFlightSqlAccessor<odbcabstraction::CDataType_DATE, Date32Array> accessor(
      dynamic_cast<NumericArray<Date32Type>*>(array.get()));

  std::vector<tagDATE_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(odbcabstraction::CDataType_DATE, 0, 0, buffer.data(), 0,
                        strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(DATE_STRUCT), strlen_buffer[i]);

    ASSERT_EQ(expected[i].year, buffer[i].year);
    ASSERT_EQ(expected[i].month, buffer[i].month);
    ASSERT_EQ(expected[i].day, buffer[i].day);
  }
}

TEST(DateArrayAccessor, Test_Date64Array_CDataType_DATE) {
  std::vector<int64_t> values = {
      86400000,  172800000, 259200000,       1649793238110,  0, 345600000, 432000000,
      518400000, -86400000, -17987443200000, -24268068949000};
  std::vector<tagDATE_STRUCT> expected = {
      /* year(16), month(u16), day(u16) */
      {1970, 1, 2},   {1970, 1, 3}, {1970, 1, 4},   {2022, 4, 12},
      {1970, 1, 1},   {1970, 1, 5}, {1970, 1, 6},   {1970, 1, 7},
      {1969, 12, 31}, {1400, 1, 1}, {1200, 12, 22},
  };

  std::shared_ptr<Array> array;
  ArrayFromVector<Date64Type, int64_t>(values, &array);

  DateArrayFlightSqlAccessor<odbcabstraction::CDataType_DATE, Date64Array> accessor(
      dynamic_cast<NumericArray<Date64Type>*>(array.get()));

  std::vector<tagDATE_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  ColumnBinding binding(odbcabstraction::CDataType_DATE, 0, 0, buffer.data(), 0,
                        strlen_buffer.data());

  int64_t value_offset = 0;
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(DATE_STRUCT), strlen_buffer[i]);
    ASSERT_EQ(expected[i].year, buffer[i].year);
    ASSERT_EQ(expected[i].month, buffer[i].month);
    ASSERT_EQ(expected[i].day, buffer[i].day);
  }
}

}  // namespace flight_sql
}  // namespace driver
