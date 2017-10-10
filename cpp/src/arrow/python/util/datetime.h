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

#ifndef PYARROW_UTIL_DATETIME_H
#define PYARROW_UTIL_DATETIME_H

#include <algorithm>
#include <sstream>

#include <datetime.h>
#include "arrow/util/logging.h"
#include "arrow/python/platform.h"

namespace arrow {
namespace py {

// Days per month, regular year and leap year
static int _days_per_month_table[2][12] = {
    { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 },
    { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }
};

static int is_leapyear(int64_t year) {
    return (year & 0x3) == 0 && // year % 4 == 0
           ((year % 100) != 0 ||
            (year % 400) == 0);
}

// Calculates the days offset from the 1970 epoch.
static int64_t get_days_from_date(int64_t date_year,
                                  int64_t date_month,
                                  int64_t date_day) {
    int i, month;
    int64_t year, days = 0;
    int *month_lengths;

    year = date_year - 1970;
    days = year * 365;

    // Adjust for leap years
    if (days >= 0) {
        // 1968 is the closest leap year before 1970.
        // Exclude the current year, so add 1.
        year += 1;
        // Add one day for each 4 years
        days += year / 4;
        // 1900 is the closest previous year divisible by 100
        year += 68;
        // Subtract one day for each 100 years
        days -= year / 100;
        // 1600 is the closest previous year divisible by 400
        year += 300;
        // Add one day for each 400 years
        days += year / 400;
    } else {
        // 1972 is the closest later year after 1970.
        // Include the current year, so subtract 2.
        year -= 2;
        // Subtract one day for each 4 years
        days += year / 4;
        // 2000 is the closest later year divisible by 100
        year -= 28;
        // Add one day for each 100 years
        days -= year / 100;
        // 2000 is also the closest later year divisible by 400
        // Subtract one day for each 400 years
        days += year / 400;
    }

    month_lengths = _days_per_month_table[is_leapyear(date_year)];
    month = date_month - 1;

    // Add the months
    for (i = 0; i < month; ++i) {
        days += month_lengths[i];
    }

    // Add the days
    days += date_day - 1;

    return days;
}

static inline int64_t PyTime_to_us(PyObject* pytime) {
  return (static_cast<int64_t>(PyDateTime_TIME_GET_HOUR(pytime)) * 3600000000LL +
          static_cast<int64_t>(PyDateTime_TIME_GET_MINUTE(pytime)) * 60000000LL +
          static_cast<int64_t>(PyDateTime_TIME_GET_SECOND(pytime)) * 1000000LL +
          PyDateTime_TIME_GET_MICROSECOND(pytime));
}

static inline Status PyTime_from_int(int64_t val, const TimeUnit::type unit,
                                     PyObject** out) {
  int64_t hour = 0, minute = 0, second = 0, microsecond = 0;
  switch (unit) {
    case TimeUnit::NANO:
      if (val % 1000 != 0) {
        std::stringstream ss;
        ss << "Value " << val << " has non-zero nanoseconds";
        return Status::Invalid(ss.str());
      }
      val /= 1000;
    // fall through
    case TimeUnit::MICRO:
      microsecond = val - (val / 1000000LL) * 1000000LL;
      val /= 1000000LL;
      second = val - (val / 60) * 60;
      val /= 60;
      minute = val - (val / 60) * 60;
      hour = val / 60;
      break;
    case TimeUnit::MILLI:
      microsecond = (val - (val / 1000) * 1000) * 1000;
      val /= 1000;
    // fall through
    case TimeUnit::SECOND:
      second = val - (val / 60) * 60;
      val /= 60;
      minute = val - (val / 60) * 60;
      hour = val / 60;
      break;
    default:
      break;
  }
  *out = PyTime_FromTime(static_cast<int32_t>(hour), static_cast<int32_t>(minute),
                         static_cast<int32_t>(second), static_cast<int32_t>(microsecond));
  return Status::OK();
}

static inline Status PyDateTime_from_int(int64_t val, const TimeUnit::type unit,
                                         PyObject** out) {
  int64_t microsecond = 0;
  switch (unit) {
    case TimeUnit::NANO:
      if (val % 1000 != 0) {
        std::stringstream ss;
        ss << "Value " << val << " has non-zero nanoseconds";
        return Status::Invalid(ss.str());
      }
      val /= 1000;
    // fall through
    case TimeUnit::MICRO:
      microsecond = val - (val / 1000000LL) * 1000000LL;
      val /= 1000000LL;
      break;
    case TimeUnit::MILLI:
      microsecond = (val - (val / 1000) * 1000) * 1000;
      val /= 1000;
      break;
    case TimeUnit::SECOND:
      break;
    default:
      break;
  }
  // Now val is in seconds and we are going to do the inverse of what
  // PyDateTime_to_us does.
  time_t t = static_cast<time_t>(val);
  struct tm datetime;
  struct tm* result = gmtime_r(&t, &datetime);
  ARROW_CHECK(result != NULL);
  *out = PyDateTime_FromDateAndTime(datetime.tm_year + 1900, datetime.tm_mon + 1,
                                    datetime.tm_mday, datetime.tm_hour,
                                    datetime.tm_min, std::min(59, datetime.tm_sec),
                                    microsecond);
  return Status::OK();
}

static inline int64_t PyDate_to_ms(PyDateTime_Date* pydate) {
  int64_t total_seconds = 0;
  total_seconds += PyDateTime_DATE_GET_SECOND(pydate);
  total_seconds += PyDateTime_DATE_GET_MINUTE(pydate) * 60;
  total_seconds += PyDateTime_DATE_GET_HOUR(pydate) * 3600;
  int64_t days = get_days_from_date(PyDateTime_GET_YEAR(pydate),
                                    PyDateTime_GET_MONTH(pydate),
                                    PyDateTime_GET_DAY(pydate));
  total_seconds += days * 24 * 3600;
  return total_seconds * 1000;
}

static inline int64_t PyDateTime_to_us(PyDateTime_DateTime* pydatetime) {
  int64_t total_seconds = 0;
  total_seconds += PyDateTime_DATE_GET_SECOND(pydatetime);
  total_seconds += PyDateTime_DATE_GET_MINUTE(pydatetime) * 60;
  total_seconds += PyDateTime_DATE_GET_HOUR(pydatetime) * 3600;
  int64_t days = get_days_from_date(PyDateTime_GET_YEAR(pydatetime),
                                    PyDateTime_GET_MONTH(pydatetime),
                                    PyDateTime_GET_DAY(pydatetime));
  total_seconds += days * 24 * 3600;
  int us = PyDateTime_DATE_GET_MICROSECOND(pydatetime);

  return total_seconds * 1000000 + us;
}

static inline int32_t PyDate_to_days(PyDateTime_Date* pydate) {
  return static_cast<int32_t>(PyDate_to_ms(pydate) / 86400000LL);
}

}  // namespace py
}  // namespace arrow

#endif  // PYARROW_UTIL_DATETIME_H
