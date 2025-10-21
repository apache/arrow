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

#include "arrow/flight/sql/odbc/odbc_impl/accessors/timestamp_array_accessor.h"

#include "arrow/flight/sql/odbc/odbc_impl/calendar_utils.h"

using arrow::TimeUnit;

namespace arrow::flight::sql::odbc {
namespace {

int64_t GetConversionToSecondsDivisor(TimeUnit::type unit) {
  int64_t divisor = 1;
  switch (unit) {
    case TimeUnit::SECOND:
      divisor = 1;
      break;
    case TimeUnit::MILLI:
      divisor = MILLI_TO_SECONDS_DIVISOR;
      break;
    case TimeUnit::MICRO:
      divisor = MICRO_TO_SECONDS_DIVISOR;
      break;
    case TimeUnit::NANO:
      divisor = NANO_TO_SECONDS_DIVISOR;
      break;
    default:
      assert(false);
      throw DriverException("Unrecognized time unit value: " + std::to_string(unit));
  }
  return divisor;
}

uint32_t CalculateFraction(TimeUnit::type unit, int64_t units_since_epoch) {
  /**
   * Convert the given remainder and time unit to nanoseconds
   * since the fraction field on TIMESTAMP_STRUCT is in nanoseconds.
   */
  if (unit == TimeUnit::SECOND) {
    return 0;
  }

  const int64_t divisor = GetConversionToSecondsDivisor(unit);
  const int64_t nano_divisor = GetConversionToSecondsDivisor(TimeUnit::NANO);

  // Safe remainder calculation that always gives a non-negative result
  int64_t remainder = units_since_epoch % divisor;
  if (remainder < 0) {
    remainder += divisor;
  }

  // Scale to nanoseconds
  return static_cast<uint32_t>(remainder * (nano_divisor / divisor));
}
}  // namespace

template <CDataType TARGET_TYPE, TimeUnit::type UNIT>
TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>::TimestampArrayFlightSqlAccessor(
    Array* array)
    : FlightSqlAccessor<TimestampArray, TARGET_TYPE,
                        TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>>(array) {}

template <CDataType TARGET_TYPE, TimeUnit::type UNIT>
RowStatus TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>::MoveSingleCellImpl(
    ColumnBinding* binding, int64_t arrow_row, int64_t cell_counter,
    int64_t& value_offset, bool update_value_offset, Diagnostics& diagnostics) {
  auto* buffer = static_cast<TIMESTAMP_STRUCT*>(binding->buffer);

  int64_t value = this->GetArray()->Value(arrow_row);
  const auto divisor = GetConversionToSecondsDivisor(UNIT);
  const auto converted_result_seconds =
      // We want floor division here; C++ will round towards zero
      (value < 0)
          /**
           * Floor division: Shift all "fractional" (not a multiple of divisor) values so
           * they round towards zero (and to the same value) along with the "floor" less
           * than them, then add 1 to get back to the floor.  Alternative we could shift
           * negatively by (divisor - 1) but this breaks near INT64_MIN causing underflow.
           */
          ? ((value + 1) / divisor) - 1
          // Towards zero is already floor
          : value / divisor;
  tm timestamp = {0};

  GetTimeForSecondsSinceEpoch(converted_result_seconds, timestamp);

  buffer[cell_counter].year = 1900 + (timestamp.tm_year);
  buffer[cell_counter].month = timestamp.tm_mon + 1;
  buffer[cell_counter].day = timestamp.tm_mday;
  buffer[cell_counter].hour = timestamp.tm_hour;
  buffer[cell_counter].minute = timestamp.tm_min;
  buffer[cell_counter].second = timestamp.tm_sec;
  buffer[cell_counter].fraction = CalculateFraction(UNIT, value);

  if (binding->str_len_buffer) {
    binding->str_len_buffer[cell_counter] =
        static_cast<ssize_t>(GetCellLengthImpl(binding));
  }

  return RowStatus_SUCCESS;
}

template <CDataType TARGET_TYPE, TimeUnit::type UNIT>
size_t TimestampArrayFlightSqlAccessor<TARGET_TYPE, UNIT>::GetCellLengthImpl(
    ColumnBinding* binding) const {
  return sizeof(TIMESTAMP_STRUCT);
}

template class TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP, TimeUnit::SECOND>;
template class TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP, TimeUnit::MILLI>;
template class TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP, TimeUnit::MICRO>;
template class TimestampArrayFlightSqlAccessor<CDataType_TIMESTAMP, TimeUnit::NANO>;

}  // namespace arrow::flight::sql::odbc
