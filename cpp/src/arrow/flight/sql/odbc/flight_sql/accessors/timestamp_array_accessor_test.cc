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

#include "timestamp_array_accessor.h"
#include "arrow/testing/builder.h"
#include "gtest/gtest.h"
#include "odbcabstraction/calendar_utils.h"
#include "utils.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_MILLI) {
  std::vector<int64_t> values = {86400370,  172800000, 259200000, 1649793238110LL,
                                 345600000, 432000000, 518400000};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::MILLI));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(), values,
                                          &timestamp_array);

  TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP, TimeUnit::MILLI> accessor(
      timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer.data(), 0,
                        strlen_buffer.data());
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);

    tm date{};

    auto converted_time = values[i] / MILLI_TO_SECONDS_DIVISOR;
    GetTimeForSecondsSinceEpoch(date, converted_time);

    ASSERT_EQ(buffer[i].year, 1900 + (date.tm_year));
    ASSERT_EQ(buffer[i].month, date.tm_mon + 1);
    ASSERT_EQ(buffer[i].day, date.tm_mday);
    ASSERT_EQ(buffer[i].hour, date.tm_hour);
    ASSERT_EQ(buffer[i].minute, date.tm_min);
    ASSERT_EQ(buffer[i].second, date.tm_sec);

    constexpr uint32_t NANOSECONDS_PER_MILLI = 1000000;
    ASSERT_EQ(buffer[i].fraction,
              (values[i] % MILLI_TO_SECONDS_DIVISOR) * NANOSECONDS_PER_MILLI);
  }
}

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_SECONDS) {
  std::vector<int64_t> values = {86400,  172800, 259200, 1649793238,
                                 345600, 432000, 518400};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::SECOND));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(), values,
                                          &timestamp_array);

  TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP, TimeUnit::SECOND> accessor(
      timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer.data(), 0,
                        strlen_buffer.data());
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);

  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);
    tm date{};

    auto converted_time = values[i];
    GetTimeForSecondsSinceEpoch(date, converted_time);

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

  TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP, TimeUnit::MICRO> accessor(
      timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer.data(), 0,
                        strlen_buffer.data());
  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);

  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);

    tm date{};

    auto converted_time = values[i] / MICRO_TO_SECONDS_DIVISOR;
    GetTimeForSecondsSinceEpoch(date, converted_time);

    ASSERT_EQ(buffer[i].year, 1900 + (date.tm_year));
    ASSERT_EQ(buffer[i].month, date.tm_mon + 1);
    ASSERT_EQ(buffer[i].day, date.tm_mday);
    ASSERT_EQ(buffer[i].hour, date.tm_hour);
    ASSERT_EQ(buffer[i].minute, date.tm_min);
    ASSERT_EQ(buffer[i].second, date.tm_sec);
    constexpr uint32_t MICROS_PER_NANO = 1000;
    ASSERT_EQ(buffer[i].fraction,
              (values[i] % MICRO_TO_SECONDS_DIVISOR) * MICROS_PER_NANO);
  }
}

TEST(TEST_TIMESTAMP, TIMESTAMP_WITH_NANO) {
  std::vector<int64_t> values = {86400000010000, 1649793238000000000};

  std::shared_ptr<Array> timestamp_array;

  auto timestamp_field = field("timestamp_field", timestamp(TimeUnit::NANO));
  ArrayFromVector<TimestampType, int64_t>(timestamp_field->type(), values,
                                          &timestamp_array);

  TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP, TimeUnit::NANO> accessor(
      timestamp_array.get());

  std::vector<TIMESTAMP_STRUCT> buffer(values.size());
  std::vector<ssize_t> strlen_buffer(values.size());

  int64_t value_offset = 0;
  ColumnBinding binding(CDataType_TIMESTAMP, 0, 0, buffer.data(), 0,
                        strlen_buffer.data());

  odbcabstraction::Diagnostics diagnostics("Foo", "Foo", OdbcVersion::V_3);
  ASSERT_EQ(values.size(),
            accessor.GetColumnarData(&binding, 0, values.size(), value_offset, false,
                                     diagnostics, nullptr));

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_EQ(sizeof(TIMESTAMP_STRUCT), strlen_buffer[i]);
    tm date{};

    auto converted_time = values[i] / NANO_TO_SECONDS_DIVISOR;
    GetTimeForSecondsSinceEpoch(date, converted_time);

    ASSERT_EQ(buffer[i].year, 1900 + (date.tm_year));
    ASSERT_EQ(buffer[i].month, date.tm_mon + 1);
    ASSERT_EQ(buffer[i].day, date.tm_mday);
    ASSERT_EQ(buffer[i].hour, date.tm_hour);
    ASSERT_EQ(buffer[i].minute, date.tm_min);
    ASSERT_EQ(buffer[i].second, date.tm_sec);
    ASSERT_EQ(buffer[i].fraction, (values[i] % NANO_TO_SECONDS_DIVISOR));
  }
}
}  // namespace flight_sql
}  // namespace driver
