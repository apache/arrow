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

#include "arrow/flight/sql/odbc/flight_sql/accessors/time_array_accessor.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/calendar_utils.h"

namespace driver {
namespace flight_sql {

using arrow::Array;
using arrow::Time32Array;
using arrow::Time64Array;
using arrow::TimeType;
using arrow::TimeUnit;

using odbcabstraction::DriverException;
using odbcabstraction::GetTimeForSecondsSinceEpoch;
using odbcabstraction::TIME_STRUCT;

Accessor* CreateTimeAccessor(arrow::Array* array, arrow::Type::type type) {
  auto time_type = arrow::internal::checked_pointer_cast<TimeType>(array->type());
  auto time_unit = time_type->unit();

  if (type == arrow::Type::TIME32) {
    switch (time_unit) {
      case TimeUnit::SECOND:
        return new TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                              Time32Array, TimeUnit::SECOND>(array);
      case TimeUnit::MILLI:
        return new TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                              Time32Array, TimeUnit::MILLI>(array);
      case TimeUnit::MICRO:
        return new TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                              Time32Array, TimeUnit::MICRO>(array);
      case TimeUnit::NANO:
        return new TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                              Time32Array, TimeUnit::NANO>(array);
    }
  } else if (type == arrow::Type::TIME64) {
    switch (time_unit) {
      case TimeUnit::SECOND:
        return new TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                              Time64Array, TimeUnit::SECOND>(array);
      case TimeUnit::MILLI:
        return new TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                              Time64Array, TimeUnit::MILLI>(array);
      case TimeUnit::MICRO:
        return new TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                              Time64Array, TimeUnit::MICRO>(array);
      case TimeUnit::NANO:
        return new TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME,
                                              Time64Array, TimeUnit::NANO>(array);
    }
  }
  assert(false);
  throw DriverException("Unsupported input supplied to CreateTimeAccessor");
}

namespace {
template <typename T>
int64_t ConvertTimeValue(typename T::value_type value, TimeUnit::type unit) {
  return value;
}

template <>
int64_t ConvertTimeValue<Time32Array>(int32_t value, TimeUnit::type unit) {
  return unit == TimeUnit::SECOND ? value
                                  : value / odbcabstraction::MILLI_TO_SECONDS_DIVISOR;
}

template <>
int64_t ConvertTimeValue<Time64Array>(int64_t value, TimeUnit::type unit) {
  return unit == TimeUnit::MICRO ? value / odbcabstraction::MICRO_TO_SECONDS_DIVISOR
                                 : value / odbcabstraction::NANO_TO_SECONDS_DIVISOR;
}
}  // namespace

template <CDataType TARGET_TYPE, typename ARROW_ARRAY, TimeUnit::type UNIT>
TimeArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY, UNIT>::TimeArrayFlightSqlAccessor(
    Array* array)
    : FlightSqlAccessor<ARROW_ARRAY, TARGET_TYPE,
                        TimeArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY, UNIT>>(
          array) {}

template <CDataType TARGET_TYPE, typename ARROW_ARRAY, TimeUnit::type UNIT>
RowStatus TimeArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY, UNIT>::MoveSingleCell_impl(
    ColumnBinding* binding, int64_t arrow_row, int64_t cell_counter,
    int64_t& value_offset, bool update_value_offset,
    odbcabstraction::Diagnostics& diagnostic) {
  auto* buffer = static_cast<TIME_STRUCT*>(binding->buffer);

  tm time{};

  auto converted_value_seconds =
      ConvertTimeValue<ARROW_ARRAY>(this->GetArray()->Value(arrow_row), UNIT);

  GetTimeForSecondsSinceEpoch(converted_value_seconds, time);

  buffer[cell_counter].hour = time.tm_hour;
  buffer[cell_counter].minute = time.tm_min;
  buffer[cell_counter].second = time.tm_sec;

  if (binding->strlen_buffer) {
    binding->strlen_buffer[cell_counter] =
        static_cast<ssize_t>(GetCellLength_impl(binding));
  }
  return odbcabstraction::RowStatus_SUCCESS;
}

template <CDataType TARGET_TYPE, typename ARROW_ARRAY, TimeUnit::type UNIT>
size_t TimeArrayFlightSqlAccessor<TARGET_TYPE, ARROW_ARRAY, UNIT>::GetCellLength_impl(
    ColumnBinding* binding) const {
  return sizeof(TIME_STRUCT);
}

template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME, Time32Array,
                                          TimeUnit::SECOND>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME, Time32Array,
                                          TimeUnit::MILLI>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME, Time32Array,
                                          TimeUnit::MICRO>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME, Time32Array,
                                          TimeUnit::NANO>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME, Time64Array,
                                          TimeUnit::SECOND>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME, Time64Array,
                                          TimeUnit::MILLI>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME, Time64Array,
                                          TimeUnit::MICRO>;
template class TimeArrayFlightSqlAccessor<odbcabstraction::CDataType_TIME, Time64Array,
                                          TimeUnit::NANO>;

}  // namespace flight_sql
}  // namespace driver
