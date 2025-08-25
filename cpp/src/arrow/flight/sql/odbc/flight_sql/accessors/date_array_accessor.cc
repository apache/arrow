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
#include <time.h>
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/calendar_utils.h"

using arrow::Date32Array;
using arrow::Date64Array;

namespace {
template <typename T>
int64_t convertDate(typename T::value_type value) {
  return value;
}

/// Converts the value from the array, which is in milliseconds, to seconds.
/// \param value    the value extracted from the array in milliseconds.
/// \return         the converted value in seconds.
template <>
int64_t convertDate<Date64Array>(int64_t value) {
  return value / driver::odbcabstraction::MILLI_TO_SECONDS_DIVISOR;
}

/// Converts the value from the array, which is in days, to seconds.
/// \param value    the value extracted from the array in days.
/// \return         the converted value in seconds.
template <>
int64_t convertDate<Date32Array>(int32_t value) {
  return value * driver::odbcabstraction::DAYS_TO_SECONDS_MULTIPLIER;
}
}  // namespace

namespace driver {
namespace flight_sql {

using odbcabstraction::DATE_STRUCT;
using odbcabstraction::RowStatus;

using odbcabstraction::GetTimeForSecondsSinceEpoch;

template <CDataType TARGET_TYPE, typename ARROW_ARRAY>
DateArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>::DateArrayFlightSqlAccessor(
    Array* array)
    : FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE,
                        DateArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>>(array) {}

template <CDataType TARGET_TYPE, typename ARROW_ARRAY>
RowStatus DateArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>::MoveSingleCell_impl(
    ColumnBinding* binding, int64_t arrow_row, int64_t cell_counter,
    int64_t& value_offset, bool update_value_offset,
    odbcabstraction::Diagnostics& diagnostics) {
  auto* buffer = static_cast<DATE_STRUCT*>(binding->buffer);
  auto value = convertDate<ARROW_ARRAY>(this->GetArray()->Value(arrow_row));
  tm date{};

  GetTimeForSecondsSinceEpoch(value, date);

  buffer[cell_counter].year = 1900 + (date.tm_year);
  buffer[cell_counter].month = date.tm_mon + 1;
  buffer[cell_counter].day = date.tm_mday;

  if (binding->strlen_buffer) {
    binding->strlen_buffer[cell_counter] =
        static_cast<ssize_t>(GetCellLength_impl(binding));
  }

  return odbcabstraction::RowStatus_SUCCESS;
}

template <CDataType TARGET_TYPE, typename ARROW_ARRAY>
size_t DateArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY>::GetCellLength_impl(
    ColumnBinding* binding) const {
  return sizeof(DATE_STRUCT);
}

template class DateArrayFlightSqlAccessor<odbcabstraction::CDataType_DATE, Date32Array>;
template class DateArrayFlightSqlAccessor<odbcabstraction::CDataType_DATE, Date64Array>;

}  // namespace flight_sql
}  // namespace driver
