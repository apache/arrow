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

#include "arrow/flight/sql/odbc/flight_sql/accessors/timestamp_array_accessor.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/testing/builder.h"
#include "gtest/gtest.h"
#include "odbcabstraction/calendar_utils.h"

namespace driver {
namespace flight_sql {

using arrow::ArrayFromVector;
using arrow::TimestampType;
using arrow::TimeUnit;

using odbcabstraction::OdbcVersion;
using odbcabstraction::TIMESTAMP_STRUCT;

using odbcabstraction::GetTimeForSecondsSinceEpoch;

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_MILLI) {
  std::vector<int64_t> values = {86400370,        172800000,      259200000,
                                 1649793238110LL, 345600000,      432000000,
                                 518400000,       -86399000,      0,
                                 -86399999,       -86399001,      86400001,
                                 86400999,        -3786912000000, -5364662400000,
                                 -1500,           -24268068949000};
  std::vector<TIMESTAMP_STRUCT> expected = {
      /* year(16), month(u16), day(u16), hour(u16), minute(u16), second(u16),
         fraction(u32) */
      {1970, 1, 2, 0, 0, 0, 370000000},
      {1970, 1, 3, 0, 0, 0, 0},
      {1970, 1, 4, 0, 0, 0, 0},
      {2022, 4, 12, 19, 53, 58, 110000000},
      {1970, 1, 5, 0, 0, 0, 0},
      {1970, 1, 6, 0, 0, 0, 0},
      {1970, 1, 7, 0, 0, 0, 0},
      {1969, 12, 31, 0, 0, 1, 0},
      {1970, 1, 1, 0, 0, 0, 0},
      /* Tests both ends of the fraction rounding range to ensure we don't tip the wrong
         way */
      {1969, 12, 31, 0, 0, 0, 1000000},
      {1969, 12, 31, 0, 0, 0, 999000000},
      {1970, 1, 2, 0, 0, 0, 1000000},
      {1970, 1, 2, 0, 0, 0, 999000000},
      {1849, 12, 31, 0, 0, 0, 0U},
      {1800, 1, 1, 0, 0, 0, 0U},
      {1969, 12, 31, 23, 59, 58, 500000000U},
      {1200, 12, 22, 13, 44, 11, 0U},
  };

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::MILLI));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(), values,
                                          &timestamp_array);

  TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP, TimeUnit::MILLI>
      accessor(timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(odbcabstraction::CDataType_TIMESTAMP, 0, 0, buffer.data(), 0,
                        strlen_buffer.data());
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);

    ASSERT_EQ(buffer[i].year, expected[i].year);
    ASSERT_EQ(buffer[i].month, expected[i].month);
    ASSERT_EQ(buffer[i].day, expected[i].day);
    ASSERT_EQ(buffer[i].hour, expected[i].hour);
    ASSERT_EQ(buffer[i].minute, expected[i].minute);
    ASSERT_EQ(buffer[i].second, expected[i].second);
    ASSERT_EQ(buffer[i].fraction, expected[i].fraction);
  }
}

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_SECONDS) {
  std::vector<int64_t> values = {86400,  172800, 259200, 1649793238,
                                 345600, 432000, 518400};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::SECOND));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(), values,
                                          &timestamp_array);

  TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP, TimeUnit::SECOND>
      accessor(timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(odbcabstraction::CDataType_TIMESTAMP, 0, 0, buffer.data(), 0,
                        strlen_buffer.data());
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);

  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);
    tm date{};

    auto converted_time = values[i];
    GetTimeForSecondsSinceEpoch(converted_time, date);

    ASSERT_EQ(buffer[i].year, 1900 + (date.tm_year));
    ASSERT_EQ(buffer[i].month, date.tm_mon + 1);
    ASSERT_EQ(buffer[i].day, date.tm_mday);
    ASSERT_EQ(buffer[i].hour, date.tm_hour);
    ASSERT_EQ(buffer[i].minute, date.tm_min);
    ASSERT_EQ(buffer[i].second, date.tm_sec);
    ASSERT_EQ(buffer[i].fraction, 0);
  }
}

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_MICRO) {
  std::vector<int64_t> values = {86400000000, 1649793238000000};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::MICRO));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(), values,
                                          &timestamp_array);

  TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP, TimeUnit::MICRO>
      accessor(timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(odbcabstraction::CDataType_TIMESTAMP, 0, 0, buffer.data(), 0,
                        strlen_buffer.data());
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);

  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);

    tm date{};

    auto converted_time = values[i] / odbcabstraction::MICRO_TO_SECONDS_DIVISOR;
    GetTimeForSecondsSinceEpoch(converted_time, date);

    ASSERT_EQ(buffer[i].year, 1900 + (date.tm_year));
    ASSERT_EQ(buffer[i].month, date.tm_mon + 1);
    ASSERT_EQ(buffer[i].day, date.tm_mday);
    ASSERT_EQ(buffer[i].hour, date.tm_hour);
    ASSERT_EQ(buffer[i].minute, date.tm_min);
    ASSERT_EQ(buffer[i].second, date.tm_sec);
    constexpr uint32_t MICROS_PER_NANO = 1000;
    ASSERT_EQ(buffer[i].fraction,
              (values[i] % odbcabstraction::MICRO_TO_SECONDS_DIVISOR) * MICROS_PER_NANO);
  }
}

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_NANO) {
  std::vector<int64_t> values = {86400000010000, 1649793238000000000};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::NANO));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(), values,
                                          &timestamp_array);

  TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP, TimeUnit::NANO>
      accessor(timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(odbcabstraction::CDataType_TIMESTAMP, 0, 0, buffer.data(), 0,
                        strlen_buffer.data());

  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);
    tm date{};

    auto converted_time = values[i] / odbcabstraction::NANO_TO_SECONDS_DIVISOR;
    GetTimeForSecondsSinceEpoch(converted_time, date);

    ASSERT_EQ(buffer[i].year, 1900 + (date.tm_year));
    ASSERT_EQ(buffer[i].month, date.tm_mon + 1);
    ASSERT_EQ(buffer[i].day, date.tm_mday);
    ASSERT_EQ(buffer[i].hour, date.tm_hour);
    ASSERT_EQ(buffer[i].minute, date.tm_min);
    ASSERT_EQ(buffer[i].second, date.tm_sec);
    ASSERT_EQ(buffer[i].fraction, (values[i] % odbcabstraction::NANO_TO_SECONDS_DIVISOR));
  }
}

}  // namespace flight_sql
}  // namespace driver
