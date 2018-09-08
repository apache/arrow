// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "./epoch_time_point.h"

extern "C" {

#include <time.h>
#include "./time_constants.h"
#include "./types.h"

#define TIMESTAMP_DIFF_FIXED_UNITS(TYPE, NAME, FROM_MILLIS)          \
  FORCE_INLINE                                                       \
  int32 NAME##_##TYPE##_##TYPE(TYPE start_millis, TYPE end_millis) { \
    return FROM_MILLIS(end_millis - start_millis);                   \
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
#define TIMESTAMP_DIFF_MONTH_UNITS(TYPE, NAME, N_MONTHS)                          \
  FORCE_INLINE                                                                    \
  int32 NAME##_##TYPE##_##TYPE(TYPE start_millis, TYPE end_millis) {              \
    int32 diff;                                                                   \
    bool is_positive = (end_millis > start_millis);                               \
    if (!is_positive) {                                                           \
      /* if end_millis < start_millis, swap and multiply by -1 at the end */      \
      TYPE tmp = start_millis;                                                    \
      start_millis = end_millis;                                                  \
      end_millis = tmp;                                                           \
    }                                                                             \
    EpochTimePoint start_tm(start_millis);                                        \
    EpochTimePoint end_tm(end_millis);                                            \
    int32 months_diff;                                                            \
    months_diff = 12 * (end_tm.TmYear() - start_tm.TmYear()) +                    \
                  (end_tm.TmMon() - start_tm.TmMon());                            \
    if (end_tm.TmMday() > start_tm.TmMday()) {                                    \
      /* case a */                                                                \
      diff = MONTHS_TO_TIMEUNIT(months_diff, N_MONTHS);                           \
      return SIGN_ADJUST_DIFF(is_positive, diff);                                 \
    }                                                                             \
    if (end_tm.TmMday() < start_tm.TmMday()) {                                    \
      /* case b */                                                                \
      diff = MONTHS_TO_TIMEUNIT(months_diff - 1, N_MONTHS);                       \
      return SIGN_ADJUST_DIFF(is_positive, diff);                                 \
    }                                                                             \
    int32 end_day_millis = end_tm.TmHour() * MILLIS_IN_HOUR +                     \
                           end_tm.TmMin() * MILLIS_IN_MIN + end_tm.TmSec();       \
    int32 start_day_millis = start_tm.TmHour() * MILLIS_IN_HOUR +                 \
                             start_tm.TmMin() * MILLIS_IN_MIN + start_tm.TmSec(); \
    if (end_day_millis >= start_day_millis) {                                     \
      /* case c1 */                                                               \
      diff = MONTHS_TO_TIMEUNIT(months_diff, N_MONTHS);                           \
      return SIGN_ADJUST_DIFF(is_positive, diff);                                 \
    }                                                                             \
    /* case c2 */                                                                 \
    diff = MONTHS_TO_TIMEUNIT(months_diff - 1, N_MONTHS);                         \
    return SIGN_ADJUST_DIFF(is_positive, diff);                                   \
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

#define ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(TYPE, NAME, TO_MILLIS) \
  FORCE_INLINE                                                    \
  TYPE NAME##_##TYPE##_int32(TYPE millis, int32 count) {          \
    return millis + TO_MILLIS * (TYPE)count;                      \
  }

// Documentation of mktime suggests that it handles
// TmMon() being negative, and also TmMon() being >= 12 by
// adjusting TmYear() accordingly
//
// Using gmtime_r() and timegm() instead of localtime_r() and mktime()
// since the input millis are since epoch
#define ADD_INT32_TO_TIMESTAMP_MONTH_UNITS(TYPE, NAME, N_MONTHS)      \
  FORCE_INLINE                                                        \
  TYPE NAME##_##TYPE##_int32(TYPE millis, int32 count) {              \
    EpochTimePoint tp(millis);                                        \
    return (TYPE)(tp.AddMonths(count * N_MONTHS).MillisSinceEpoch()); \
  }

// TODO: Handle overflow while converting int64 to millis
#define ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(TYPE, NAME, TO_MILLIS) \
  FORCE_INLINE                                                    \
  TYPE NAME##_##TYPE##_int64(TYPE millis, int64 count) {          \
    return millis + TO_MILLIS * (TYPE)count;                      \
  }

#define ADD_INT64_TO_TIMESTAMP_MONTH_UNITS(TYPE, NAME, N_MONTHS)      \
  FORCE_INLINE                                                        \
  TYPE NAME##_##TYPE##_int64(TYPE millis, int64 count) {              \
    EpochTimePoint tp(millis);                                        \
    return (TYPE)(tp.AddMonths(count * N_MONTHS).MillisSinceEpoch()); \
  }

#define TIMESTAMP_ADD_INT32(TYPE)                                             \
  ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(TYPE, timestampaddSecond, MILLIS_IN_SEC) \
  ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(TYPE, timestampaddMinute, MILLIS_IN_MIN) \
  ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(TYPE, timestampaddHour, MILLIS_IN_HOUR)  \
  ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(TYPE, timestampaddDay, MILLIS_IN_DAY)    \
  ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(TYPE, timestampaddWeek, MILLIS_IN_WEEK)  \
  ADD_INT32_TO_TIMESTAMP_MONTH_UNITS(TYPE, timestampaddMonth, 1)              \
  ADD_INT32_TO_TIMESTAMP_MONTH_UNITS(TYPE, timestampaddQuarter, 3)            \
  ADD_INT32_TO_TIMESTAMP_MONTH_UNITS(TYPE, timestampaddYear, 12)

#define TIMESTAMP_ADD_INT64(TYPE)                                             \
  ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(TYPE, timestampaddSecond, MILLIS_IN_SEC) \
  ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(TYPE, timestampaddMinute, MILLIS_IN_MIN) \
  ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(TYPE, timestampaddHour, MILLIS_IN_HOUR)  \
  ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(TYPE, timestampaddDay, MILLIS_IN_DAY)    \
  ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(TYPE, timestampaddWeek, MILLIS_IN_WEEK)  \
  ADD_INT64_TO_TIMESTAMP_MONTH_UNITS(TYPE, timestampaddMonth, 1)              \
  ADD_INT64_TO_TIMESTAMP_MONTH_UNITS(TYPE, timestampaddQuarter, 3)            \
  ADD_INT64_TO_TIMESTAMP_MONTH_UNITS(TYPE, timestampaddYear, 12)

#define TIMESTAMP_ADD_INT(TYPE) \
  TIMESTAMP_ADD_INT32(TYPE)     \
  TIMESTAMP_ADD_INT64(TYPE)

TIMESTAMP_ADD_INT(date64)
TIMESTAMP_ADD_INT(timestamp)

// add int32 to timestamp
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(date64, date_add, MILLIS_IN_DAY)
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(date64, add, MILLIS_IN_DAY)
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(timestamp, date_add, MILLIS_IN_DAY)
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(timestamp, add, MILLIS_IN_DAY)

// add int64 to timestamp
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(date64, date_add, MILLIS_IN_DAY)
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(date64, add, MILLIS_IN_DAY)
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(timestamp, date_add, MILLIS_IN_DAY)
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(timestamp, add, MILLIS_IN_DAY)

// date_sub, subtract, date_diff on int32
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(date64, date_sub, -1 * MILLIS_IN_DAY)
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(date64, subtract, -1 * MILLIS_IN_DAY)
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(date64, date_diff, -1 * MILLIS_IN_DAY)
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(timestamp, date_sub, -1 * MILLIS_IN_DAY)
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(timestamp, subtract, -1 * MILLIS_IN_DAY)
ADD_INT32_TO_TIMESTAMP_FIXED_UNITS(timestamp, date_diff, -1 * MILLIS_IN_DAY)

// date_sub, subtract, date_diff on int64
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(date64, date_sub, -1 * MILLIS_IN_DAY)
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(date64, subtract, -1 * MILLIS_IN_DAY)
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(date64, date_diff, -1 * MILLIS_IN_DAY)
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(timestamp, date_sub, -1 * MILLIS_IN_DAY)
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(timestamp, subtract, -1 * MILLIS_IN_DAY)
ADD_INT64_TO_TIMESTAMP_FIXED_UNITS(timestamp, date_diff, -1 * MILLIS_IN_DAY)

#define ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(TYPE, NAME, TO_MILLIS) \
  FORCE_INLINE                                                    \
  TYPE NAME##_int32_##TYPE(int32 count, TYPE millis) {            \
    return millis + TO_MILLIS * (TYPE)count;                      \
  }

#define ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(TYPE, NAME, TO_MILLIS) \
  FORCE_INLINE                                                    \
  TYPE NAME##_int64_##TYPE(int64 count, TYPE millis) {            \
    return millis + TO_MILLIS * (TYPE)count;                      \
  }

// add timestamp to int32
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(date64, date_add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(date64, add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(timestamp, date_add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT32_FIXED_UNITS(timestamp, add, MILLIS_IN_DAY)

// add timestamp to int64
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(date64, date_add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(date64, add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(timestamp, date_add, MILLIS_IN_DAY)
ADD_TIMESTAMP_TO_INT64_FIXED_UNITS(timestamp, add, MILLIS_IN_DAY)

}  // extern "C"
