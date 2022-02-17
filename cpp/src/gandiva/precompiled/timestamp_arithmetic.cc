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

#include "./epoch_time_point.h"

// The first row is for non-leap years
static int days_in_a_month[2][12] = {{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
                                     {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};

bool is_leap_year(int yy) {
  if ((yy % 4) != 0) {
    // not divisible by 4
    return false;
  }
  // yy = 4x
  if ((yy % 400) == 0) {
    // yy = 400x
    return true;
  }
  // yy = 4x, return true if yy != 100x
  return ((yy % 100) != 0);
}

bool is_last_day_of_month(const EpochTimePoint& tp) {
  int matrix_index = is_leap_year(tp.TmYear()) ? 1 : 0;

  return (tp.TmMday() == days_in_a_month[matrix_index][tp.TmMon()]);
}

bool did_days_overflow(arrow_vendored::date::year_month_day ymd) {
  int year = static_cast<int>(ymd.year());
  int month = static_cast<unsigned int>(ymd.month());
  int days = static_cast<unsigned int>(ymd.day());

  int matrix_index = is_leap_year(year) ? 1 : 0;

  return days > days_in_a_month[matrix_index][month - 1];
}

int last_possible_day_in_month(int year, int month) {
  int matrix_index = is_leap_year(year) ? 1 : 0;

  return days_in_a_month[matrix_index][month - 1];
}

extern "C" {

#include <time.h>

#include "./time_constants.h"
#include "./types.h"

#define TIMESTAMP_DIFF_FIXED_UNITS(TYPE, NAME, FROM_MILLIS)                          \
  FORCE_INLINE                                                                       \
  gdv_int32 NAME##_##TYPE##_##TYPE(gdv_##TYPE start_millis, gdv_##TYPE end_millis) { \
    return static_cast<int32_t>(FROM_MILLIS(end_millis - start_millis));             \
  }

#define SIGN_ADJUST_DIFF(is_positive, diff) ((is_positive) ? (diff) : -(diff))
#define MONTHS_TO_TIMEUNIT(diff, num_months) (diff) / (num_months)

// Assuming end_millis > start_millis, the algorithm to find the diff in months is:
// diff_in_months = year_diff * 12 + month_diff
// This is approximately correct, except when the last month has not fully elapsed
//
// a) If end_day > start_day, return diff_in_months     e.g. diff(2015-09-10, 2017-03-31)
// b) If end_day < start_day, return diff_in_months - 1 e.g. diff(2015-09-30, 2017-03-10)
// c) If end_day = start_day, check for millis          e.g. diff(2017-03-10, 2015-03-10)
// Need to check if end_millis_in_day > start_millis_in_day
// c1) If end_millis_in_day >= start_millis_in_day, return diff_in_months
// c2) else return diff_in_months - 1
#define TIMESTAMP_DIFF_MONTH_UNITS(TYPE, NAME, N_MONTHS)                              \
  FORCE_INLINE                                                                        \
  gdv_int32 NAME##_##TYPE##_##TYPE(gdv_##TYPE start_millis, gdv_##TYPE end_millis) {  \
    gdv_int32 diff;                                                                   \
    bool is_positive = (end_millis > start_millis);                                   \
    if (!is_positive) {                                                               \
      /* if end_millis < start_millis, swap and multiply by -1 at the end */          \
      gdv_##TYPE tmp = start_millis;                                                  \
      start_millis = end_millis;                                                      \
      end_millis = tmp;                                                               \
    }                                                                                 \
    EpochTimePoint start_tm(start_millis);                                            \
    EpochTimePoint end_tm(end_millis);                                                \
    gdv_int32 months_diff;                                                            \
    months_diff = static_cast<gdv_int32>(12 * (end_tm.TmYear() - start_tm.TmYear()) + \
                                         (end_tm.TmMon() - start_tm.TmMon()));        \
    if (end_tm.TmMday() > start_tm.TmMday()) {                                        \
      /* case a */                                                                    \
      diff = MONTHS_TO_TIMEUNIT(months_diff, N_MONTHS);                               \
      return SIGN_ADJUST_DIFF(is_positive, diff);                                     \
    }                                                                                 \
    if (end_tm.TmMday() < start_tm.TmMday()) {                                        \
      /* case b */                                                                    \
      months_diff += (is_last_day_of_month(end_tm) ? 1 : 0);                          \
      diff = MONTHS_TO_TIMEUNIT(months_diff - 1, N_MONTHS);                           \
      return SIGN_ADJUST_DIFF(is_positive, diff);                                     \
    }                                                                                 \
    gdv_int32 end_day_millis =                                                        \
        static_cast<gdv_int32>(end_tm.TmHour() * MILLIS_IN_HOUR +                     \
                               end_tm.TmMin() * MILLIS_IN_MIN + end_tm.TmSec());      \
    gdv_int32 start_day_millis =                                                      \
        static_cast<gdv_int32>(start_tm.TmHour() * MILLIS_IN_HOUR +                   \
                               start_tm.TmMin() * MILLIS_IN_MIN + start_tm.TmSec());  \
    if (end_day_millis >= start_day_millis) {                                         \
      /* case c1 */                                                                   \
      diff = MONTHS_TO_TIMEUNIT(months_diff, N_MONTHS);                               \
      return SIGN_ADJUST_DIFF(is_positive, diff);                                     \
    }                                                                                 \
    /* case c2 */                                                                     \
    diff = MONTHS_TO_TIMEUNIT(months_diff - 1, N_MONTHS);                             \
    return SIGN_ADJUST_DIFF(is_positive, diff);                                       \
  }

#define TIMESTAMP_DIFF(TYPE)                                            \
  TIMESTAMP_DIFF_FIXED_UNITS(TYPE, timestampdiffSecond, MILLIS_TO_SEC)  \
  TIMESTAMP_DIFF_FIXED_UNITS(TYPE, timestampdiffMinute, MILLIS_TO_MINS) \
  TIMESTAMP_DIFF_FIXED_UNITS(TYPE, timestampdiffHour, MILLIS_TO_HOUR)   \
  TIMESTAMP_DIFF_FIXED_UNITS(TYPE, timestampdiffDay, MILLIS_TO_DAY)     \
  TIMESTAMP_DIFF_FIXED_UNITS(TYPE, timestampdiffWeek, MILLIS_TO_WEEK)   \
  TIMESTAMP_DIFF_MONTH_UNITS(TYPE, timestampdiffMonth, 1)               \
  TIMESTAMP_DIFF_MONTH_UNITS(TYPE, timestampdiffQuarter, 3)             \
  TIMESTAMP_DIFF_MONTH_UNITS(TYPE, timestampdiffYear, 12)

TIMESTAMP_DIFF(timestamp)

#define ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(TYPE, NAME, TO_MILLIS)      \
  FORCE_INLINE                                                         \
  gdv_##TYPE NAME##_int32_##TYPE(gdv_int32 count, gdv_##TYPE millis) { \
    return millis + TO_MILLIS * static_cast<gdv_##TYPE>(count);        \
  }

// Documentation of mktime suggests that it handles
// TmMon() being negative, and also TmMon() being >= 12 by
// adjusting TmYear() accordingly
//
// Using gmtime_r() and timegm() instead of localtime_r() and mktime()
// since the input millis are since epoch
#define ADD_INT32_TO_TIMESTAMP_MONTH_UNITS(TYPE, NAME, N_MONTHS)                \
  FORCE_INLINE                                                                  \
  gdv_##TYPE NAME##_int32_##TYPE(gdv_int32 count, gdv_##TYPE millis) {          \
    EpochTimePoint tp(millis);                                                  \
    return tp.AddMonths(static_cast<int>(count * N_MONTHS)).MillisSinceEpoch(); \
  }

// TODO: Handle overflow while converting gdv_int64 to millis
#define ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(TYPE, NAME, TO_MILLIS)      \
  FORCE_INLINE                                                         \
  gdv_##TYPE NAME##_int64_##TYPE(gdv_int64 count, gdv_##TYPE millis) { \
    return millis + TO_MILLIS * static_cast<gdv_##TYPE>(count);        \
  }

#define ADD_INT64_TO_TIMESTAMP_MONTH_UNITS(TYPE, NAME, N_MONTHS)                \
  FORCE_INLINE                                                                  \
  gdv_##TYPE NAME##_int64_##TYPE(gdv_int64 count, gdv_##TYPE millis) {          \
    EpochTimePoint tp(millis);                                                  \
    return tp.AddMonths(static_cast<int>(count * N_MONTHS)).MillisSinceEpoch(); \
  }

#define ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(TYPE, NAME, TO_MILLIS)        \
  FORCE_INLINE                                                           \
  gdv_##TYPE NAME##_##TYPE##_int32(gdv_##TYPE millis, gdv_int32 count) { \
    return millis + TO_MILLIS * static_cast<gdv_##TYPE>(count);          \
  }

#define ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(TYPE, NAME, TO_MILLIS)        \
  FORCE_INLINE                                                           \
  gdv_##TYPE NAME##_##TYPE##_int64(gdv_##TYPE millis, gdv_int64 count) { \
    return millis + TO_MILLIS * static_cast<gdv_##TYPE>(count);          \
  }

#define ADD_TIMESTAMP_TO_INT32_MONTH_UNITS(TYPE, NAME, N_MONTHS)                \
  FORCE_INLINE                                                                  \
  gdv_##TYPE NAME##_##TYPE##_int32(gdv_##TYPE millis, gdv_int32 count) {        \
    EpochTimePoint tp(millis);                                                  \
    return tp.AddMonths(static_cast<int>(count * N_MONTHS)).MillisSinceEpoch(); \
  }

#define ADD_TIMESTAMP_TO_INT64_MONTH_UNITS(TYPE, NAME, N_MONTHS)                \
  FORCE_INLINE                                                                  \
  gdv_##TYPE NAME##_##TYPE##_int64(gdv_##TYPE millis, gdv_int64 count) {        \
    EpochTimePoint tp(millis);                                                  \
    return tp.AddMonths(static_cast<int>(count * N_MONTHS)).MillisSinceEpoch(); \
  }

#define ADD_TIMESTAMP_INT32_FIXEDUNITS(TYPE, NAME, TO_MILLIS) \
  ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(TYPE, NAME, TO_MILLIS)   \
  ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(TYPE, NAME, TO_MILLIS)

#define ADD_TIMESTAMP_INT32_MONTHUNITS(TYPE, NAME, N_MONTHS) \
  ADD_INT32_TO_TIMESTAMP_MONTH_UNITS(TYPE, NAME, N_MONTHS)   \
  ADD_TIMESTAMP_TO_INT32_MONTH_UNITS(TYPE, NAME, N_MONTHS)

#define TIMESTAMP_ADD_INT32(TYPE)                                         \
  ADD_TIMESTAMP_INT32_FIXEDUNITS(TYPE, timestampaddSecond, MILLIS_IN_SEC) \
  ADD_TIMESTAMP_INT32_FIXEDUNITS(TYPE, timestampaddMinute, MILLIS_IN_MIN) \
  ADD_TIMESTAMP_INT32_FIXEDUNITS(TYPE, timestampaddHour, MILLIS_IN_HOUR)  \
  ADD_TIMESTAMP_INT32_FIXEDUNITS(TYPE, timestampaddDay, MILLIS_IN_DAY)    \
  ADD_TIMESTAMP_INT32_FIXEDUNITS(TYPE, timestampaddWeek, MILLIS_IN_WEEK)  \
  ADD_TIMESTAMP_INT32_MONTHUNITS(TYPE, timestampaddMonth, 1)              \
  ADD_TIMESTAMP_INT32_MONTHUNITS(TYPE, timestampaddQuarter, 3)            \
  ADD_TIMESTAMP_INT32_MONTHUNITS(TYPE, timestampaddYear, 12)

#define ADD_TIMESTAMP_INT64_FIXEDUNITS(TYPE, NAME, TO_MILLIS) \
  ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(TYPE, NAME, TO_MILLIS)   \
  ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(TYPE, NAME, TO_MILLIS)

#define ADD_TIMESTAMP_INT64_MONTHUNITS(TYPE, NAME, N_MONTHS) \
  ADD_INT64_TO_TIMESTAMP_MONTH_UNITS(TYPE, NAME, N_MONTHS)   \
  ADD_TIMESTAMP_TO_INT64_MONTH_UNITS(TYPE, NAME, N_MONTHS)

#define TIMESTAMP_ADD_INT64(TYPE)                                         \
  ADD_TIMESTAMP_INT64_FIXEDUNITS(TYPE, timestampaddSecond, MILLIS_IN_SEC) \
  ADD_TIMESTAMP_INT64_FIXEDUNITS(TYPE, timestampaddMinute, MILLIS_IN_MIN) \
  ADD_TIMESTAMP_INT64_FIXEDUNITS(TYPE, timestampaddHour, MILLIS_IN_HOUR)  \
  ADD_TIMESTAMP_INT64_FIXEDUNITS(TYPE, timestampaddDay, MILLIS_IN_DAY)    \
  ADD_TIMESTAMP_INT64_FIXEDUNITS(TYPE, timestampaddWeek, MILLIS_IN_WEEK)  \
  ADD_TIMESTAMP_INT64_MONTHUNITS(TYPE, timestampaddMonth, 1)              \
  ADD_TIMESTAMP_INT64_MONTHUNITS(TYPE, timestampaddQuarter, 3)            \
  ADD_TIMESTAMP_INT64_MONTHUNITS(TYPE, timestampaddYear, 12)

#define TIMESTAMP_ADD_INT(TYPE) \
  TIMESTAMP_ADD_INT32(TYPE)     \
  TIMESTAMP_ADD_INT64(TYPE)

TIMESTAMP_ADD_INT(date64)
TIMESTAMP_ADD_INT(timestamp)

// add gdv_int32 to timestamp
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(date64, date_add, MILLIS_IN_DAY)
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(date64, add, MILLIS_IN_DAY)
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(timestamp, date_add, MILLIS_IN_DAY)
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(timestamp, add, MILLIS_IN_DAY)

// add gdv_int64 to timestamp
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(date64, date_add, MILLIS_IN_DAY)
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(date64, add, MILLIS_IN_DAY)
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(timestamp, date_add, MILLIS_IN_DAY)
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(timestamp, add, MILLIS_IN_DAY)

// date_sub, subtract, date_diff on gdv_int32
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(date64, date_sub, -1 * MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(date64, subtract, -1 * MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(date64, date_diff, -1 * MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(timestamp, date_sub, -1 * MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(timestamp, subtract, -1 * MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(timestamp, date_diff, -1 * MILLIS_IN_DAY)

// date_sub, subtract, date_diff on gdv_int64
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(date64, date_sub, -1 * MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(date64, subtract, -1 * MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(date64, date_diff, -1 * MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(timestamp, date_sub, -1 * MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(timestamp, subtract, -1 * MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(timestamp, date_diff, -1 * MILLIS_IN_DAY)

// add timestamp to gdv_int32
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(date64, date_add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(date64, add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(timestamp, date_add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(timestamp, add, MILLIS_IN_DAY)

// add timestamp to gdv_int64
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(date64, date_add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(date64, add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(timestamp, date_add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(timestamp, add, MILLIS_IN_DAY)

}  // extern "C"
