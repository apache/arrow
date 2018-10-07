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
#include "arrow/python/platform.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace py {

// The following code is adapted from
// https://github.com/numpy/numpy/blob/master/numpy/core/src/multiarray/datetime.c

// Days per month, regular year and leap year
static int64_t _days_per_month_table[2][12] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};

static bool is_leapyear(int64_t year) {
  return (year & 0x3) == 0 &&  // year % 4 == 0
         ((year % 100) != 0 || (year % 400) == 0);
}

// Calculates the days offset from the 1970 epoch.
static int64_t get_days_from_date(int64_t date_year, int64_t date_month,
                                  int64_t date_day) {
  int64_t i, month;
  int64_t year, days = 0;
  int64_t* month_lengths;

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

// Modifies '*days_' to be the day offset within the year,
// and returns the year.
static int64_t days_to_yearsdays(int64_t* days_) {
  const int64_t days_per_400years = (400 * 365 + 100 - 4 + 1);
  // Adjust so it's relative to the year 2000 (divisible by 400)
  int64_t days = (*days_) - (365 * 30 + 7);
  int64_t year;

  // Break down the 400 year cycle to get the year and day within the year
  if (days >= 0) {
    year = 400 * (days / days_per_400years);
    days = days % days_per_400years;
  } else {
    year = 400 * ((days - (days_per_400years - 1)) / days_per_400years);
    days = days % days_per_400years;
    if (days < 0) {
      days += days_per_400years;
    }
  }

  // Work out the year/day within the 400 year cycle
  if (days >= 366) {
    year += 100 * ((days - 1) / (100 * 365 + 25 - 1));
    days = (days - 1) % (100 * 365 + 25 - 1);
    if (days >= 365) {
      year += 4 * ((days + 1) / (4 * 365 + 1));
      days = (days + 1) % (4 * 365 + 1);
      if (days >= 366) {
        year += (days - 1) / 365;
        days = (days - 1) % 365;
      }
    }
  }

  *days_ = days;
  return year + 2000;
}

// Extracts the month and year and day number from a number of days
static void get_date_from_days(int64_t days, int64_t* date_year, int64_t* date_month,
                               int64_t* date_day) {
  int64_t *month_lengths, i;

  *date_year = days_to_yearsdays(&days);
  month_lengths = _days_per_month_table[is_leapyear(*date_year)];

  for (i = 0; i < 12; ++i) {
    if (days < month_lengths[i]) {
      *date_month = i + 1;
      *date_day = days + 1;
      return;
    } else {
      days -= month_lengths[i];
    }
  }

  // Should never get here
  return;
}

static inline int64_t PyTime_to_us(PyObject* pytime) {
  return (static_cast<int64_t>(PyDateTime_TIME_GET_HOUR(pytime)) * 3600000000LL +
          static_cast<int64_t>(PyDateTime_TIME_GET_MINUTE(pytime)) * 60000000LL +
          static_cast<int64_t>(PyDateTime_TIME_GET_SECOND(pytime)) * 1000000LL +
          PyDateTime_TIME_GET_MICROSECOND(pytime));
}

static inline int64_t PyTime_to_s(PyObject* pytime) {
  return PyTime_to_us(pytime) / 1000000;
}

static inline int64_t PyTime_to_ms(PyObject* pytime) {
  return PyTime_to_us(pytime) / 1000;
}

static inline int64_t PyTime_to_ns(PyObject* pytime) {
  return PyTime_to_us(pytime) * 1000;
}

// Splitting time quantities, for example splitting total seconds into
// minutes and remaining seconds. After we run
// int64_t remaining = split_time(total, quotient, &next)
// we have
// total = next * quotient + remaining. Handles negative values by propagating
// them: If total is negative, next will be negative and remaining will
// always be non-negative.
static inline int64_t split_time(int64_t total, int64_t quotient, int64_t* next) {
  int64_t r = total % quotient;
  if (r < 0) {
    *next = total / quotient - 1;
    return r + quotient;
  } else {
    *next = total / quotient;
    return r;
  }
}

static inline Status PyTime_convert_int(int64_t val, const TimeUnit::type unit,
                                        int64_t* hour, int64_t* minute, int64_t* second,
                                        int64_t* microsecond) {
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
      *microsecond = split_time(val, 1000000LL, &val);
      *second = split_time(val, 60, &val);
      *minute = split_time(val, 60, hour);
      break;
    case TimeUnit::MILLI:
      *microsecond = split_time(val, 1000, &val) * 1000;
    // fall through
    case TimeUnit::SECOND:
      *second = split_time(val, 60, &val);
      *minute = split_time(val, 60, hour);
      break;
    default:
      break;
  }
  return Status::OK();
}

static inline Status PyDate_convert_int(int64_t val, const DateUnit unit, int64_t* year,
                                        int64_t* month, int64_t* day) {
  switch (unit) {
    case DateUnit::MILLI:
      val /= 86400000LL;
    case DateUnit::DAY:
      get_date_from_days(val, year, month, day);
    default:
      break;
  }
  return Status::OK();
}

static inline Status PyTime_from_int(int64_t val, const TimeUnit::type unit,
                                     PyObject** out) {
  int64_t hour = 0, minute = 0, second = 0, microsecond = 0;
  RETURN_NOT_OK(PyTime_convert_int(val, unit, &hour, &minute, &second, &microsecond));
  *out = PyTime_FromTime(static_cast<int32_t>(hour), static_cast<int32_t>(minute),
                         static_cast<int32_t>(second), static_cast<int32_t>(microsecond));
  return Status::OK();
}

static inline Status PyDate_from_int(int64_t val, const DateUnit unit, PyObject** out) {
  int64_t year = 0, month = 0, day = 0;
  RETURN_NOT_OK(PyDate_convert_int(val, unit, &year, &month, &day));
  *out = PyDate_FromDate(static_cast<int32_t>(year), static_cast<int32_t>(month),
                         static_cast<int32_t>(day));
  return Status::OK();
}

static inline Status PyDateTime_from_int(int64_t val, const TimeUnit::type unit,
                                         PyObject** out) {
  int64_t hour = 0, minute = 0, second = 0, microsecond = 0;
  RETURN_NOT_OK(PyTime_convert_int(val, unit, &hour, &minute, &second, &microsecond));
  int64_t total_days = 0;
  hour = split_time(hour, 24, &total_days);
  int64_t year = 0, month = 0, day = 0;
  get_date_from_days(total_days, &year, &month, &day);
  *out = PyDateTime_FromDateAndTime(
      static_cast<int32_t>(year), static_cast<int32_t>(month), static_cast<int32_t>(day),
      static_cast<int32_t>(hour), static_cast<int32_t>(minute),
      static_cast<int32_t>(second), static_cast<int32_t>(microsecond));
  return Status::OK();
}

static inline int64_t PyDate_to_days(PyDateTime_Date* pydate) {
  return get_days_from_date(PyDateTime_GET_YEAR(pydate), PyDateTime_GET_MONTH(pydate),
                            PyDateTime_GET_DAY(pydate));
}

static inline int64_t PyDate_to_ms(PyDateTime_Date* pydate) {
  int64_t total_seconds = 0;
  total_seconds += PyDateTime_DATE_GET_SECOND(pydate);
  total_seconds += PyDateTime_DATE_GET_MINUTE(pydate) * 60;
  total_seconds += PyDateTime_DATE_GET_HOUR(pydate) * 3600;
  int64_t days =
      get_days_from_date(PyDateTime_GET_YEAR(pydate), PyDateTime_GET_MONTH(pydate),
                         PyDateTime_GET_DAY(pydate));
  total_seconds += days * 24 * 3600;
  return total_seconds * 1000;
}

static inline int64_t PyDateTime_to_s(PyDateTime_DateTime* pydatetime) {
  return PyDate_to_ms(reinterpret_cast<PyDateTime_Date*>(pydatetime)) / 1000LL;
}

static inline int64_t PyDateTime_to_ms(PyDateTime_DateTime* pydatetime) {
  int64_t date_ms = PyDate_to_ms(reinterpret_cast<PyDateTime_Date*>(pydatetime));
  int ms = PyDateTime_DATE_GET_MICROSECOND(pydatetime) / 1000;
  return date_ms + ms;
}

static inline int64_t PyDateTime_to_us(PyDateTime_DateTime* pydatetime) {
  int64_t ms = PyDate_to_ms(reinterpret_cast<PyDateTime_Date*>(pydatetime));
  int us = PyDateTime_DATE_GET_MICROSECOND(pydatetime);
  return ms * 1000 + us;
}

static inline int64_t PyDateTime_to_ns(PyDateTime_DateTime* pydatetime) {
  return PyDateTime_to_us(pydatetime) * 1000;
}

}  // namespace py
}  // namespace arrow

#endif  // PYARROW_UTIL_DATETIME_H
