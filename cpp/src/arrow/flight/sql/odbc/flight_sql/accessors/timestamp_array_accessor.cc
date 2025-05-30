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
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/calendar_utils.h"

using arrow::TimeUnit;

namespace {
int64_t GetConversionToSecondsDivisor(TimeUnit::type unit) {
  int64_t divisor = 1;
  switch (unit) {
    case TimeUnit::SECOND:
      divisor = 1;
      break;
    case TimeUnit::MILLI:
      divisor = driver::odbcabstraction::MILLI_TO_SECONDS_DIVISOR;
      break;
    case TimeUnit::MICRO:
      divisor = driver::odbcabstraction::MICRO_TO_SECONDS_DIVISOR;
      break;
    case TimeUnit::NANO:
      divisor = driver::odbcabstraction::NANO_TO_SECONDS_DIVISOR;
      break;
    default:
      assert(false);
      throw driver::odbcabstraction::DriverException("Unrecognized time unit value: " +
                                                     std::to_string(unit));
  }
  return divisor;
}

uint32_t CalculateFraction(TimeUnit::type unit, uint64_t units_since_epoch) {
  // Convert the given remainder and time unit to nanoseconds
  // since the fraction field on TIMESTAMP_STRUCT is in nanoseconds.
  switch (unit) {
    case TimeUnit::SECOND:
      return 0;
    case TimeUnit::MILLI:
      // 1000000 nanoseconds = 1 millisecond.
      return (units_since_epoch % driver::odbcabstraction::MILLI_TO_SECONDS_DIVISOR) *
             1000000;
    case TimeUnit::MICRO:
      // 1000 nanoseconds = 1 microsecond.
      return (units_since_epoch % driver::odbcabstraction::MICRO_TO_SECONDS_DIVISOR) *
             1000;
    case TimeUnit::NANO:
      // 1000 nanoseconds = 1 microsecond.
      return (units_since_epoch % driver::odbcabstraction::NANO_TO_SECONDS_DIVISOR);
  }
  return 0;
}
}  // namespace

namespace driver {
namespace flight_sql {

using odbcabstraction::TIMESTAMP_STRUCT;

using odbcabstraction::GetTimeForSecondsSinceEpoch;

template <CDataType TARGET_TYPE, TimeUnit::type UNIT>
TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>::TimestampArrayFlightSqlAccessor(
    Array* array)
    : FlightSqlAccessor<TimestampArray, TARGET_TYPE,
                        TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>>(array) {}

template <CDataType TARGET_TYPE, TimeUnit::type UNIT>
RowStatus TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>::MoveSingleCell_impl(
    ColumnBinding* binding, int64_t arrow_row, int64_t cell_counter,
    int64_t& value_offset, bool update_value_offset,
    odbcabstraction::Diagnostics& diagnostics) {
  auto* buffer = static_cast<TIMESTAMP_STRUCT*>(binding->buffer);

  int64_t value = this->GetArray()->Value(arrow_row);
  const auto divisor = GetConversionToSecondsDivisor(UNIT);
  const auto converted_result_seconds = value / divisor;
  tm timestamp = {0};

  GetTimeForSecondsSinceEpoch(timestamp, converted_result_seconds);

  buffer[cell_counter].year = 1900 + (timestamp.tm_year);
  buffer[cell_counter].month = timestamp.tm_mon + 1;
  buffer[cell_counter].day = timestamp.tm_mday;
  buffer[cell_counter].hour = timestamp.tm_hour;
  buffer[cell_counter].minute = timestamp.tm_min;
  buffer[cell_counter].second = timestamp.tm_sec;
  buffer[cell_counter].fraction = CalculateFraction(UNIT, value);

  if (binding->strlen_buffer) {
    binding->strlen_buffer[cell_counter] =
        static_cast<ssize_t>(GetCellLength_impl(binding));
  }

  return odbcabstraction::RowStatus_SUCCESS;
}

template <CDataType TARGET_TYPE, TimeUnit::type UNIT>
size_t TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>::GetCellLength_impl(
    ColumnBinding* binding) const {
  return sizeof(TIMESTAMP_STRUCT);
}

template class TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP,
                                               TimeUnit::SECOND>;
template class TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP,
                                               TimeUnit::MILLI>;
template class TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP,
                                               TimeUnit::MICRO>;
template class TimestampArrayFlightSqlAccessor<odbcabstraction::CDataType_TIMESTAMP,
                                               TimeUnit::NANO>;

}  // namespace flight_sql
}  // namespace driver
