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

extern "C" {

#include <time.h>
#include "./types.h"

#define MILLIS_IN_SEC (1000)
#define MILLIS_IN_MIN (60 * MILLIS_IN_SEC)
#define MILLIS_IN_HOUR (60 * MILLIS_IN_MIN)
#define MILLIS_IN_DAY (24 * MILLIS_IN_HOUR)
#define MILLIS_IN_WEEK (7 * MILLIS_IN_DAY)

#define MILLIS_TO_SEC(millis) ((millis) / MILLIS_IN_SEC)
#define MILLIS_TO_MINS(millis) ((millis) / MILLIS_IN_MIN)
#define MILLIS_TO_HOUR(millis) ((millis) / MILLIS_IN_HOUR)
#define MILLIS_TO_DAY(millis) ((millis) / MILLIS_IN_DAY)
#define MILLIS_TO_WEEK(millis) ((millis) / MILLIS_IN_WEEK)

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
#define TIMESTAMP_DIFF_MONTH_UNITS(TYPE, NAME, N_MONTHS)                                 \
  FORCE_INLINE                                                                           \
  int32 NAME##_##TYPE##_##TYPE(TYPE start_millis, TYPE end_millis) {                     \
    int32 diff;                                                                          \
    bool is_positive = (end_millis > start_millis);                                      \
    if (!is_positive) {                                                                  \
      /* if end_millis < start_millis, swap and multiply by -1 at the end */             \
      TYPE tmp = start_millis;                                                           \
      start_millis = end_millis;                                                         \
      end_millis = tmp;                                                                  \
    }                                                                                    \
    time_t start_tsec = (time_t)MILLIS_TO_SEC(start_millis);                             \
    struct tm start_tm;                                                                  \
    gmtime_r(&start_tsec, &start_tm);                                                    \
    time_t end_tsec = (time_t)MILLIS_TO_SEC(end_millis);                                 \
    struct tm end_tm;                                                                    \
    gmtime_r(&end_tsec, &end_tm);                                                        \
    int32 months_diff;                                                                   \
    months_diff =                                                                        \
        12 * (end_tm.tm_year - start_tm.tm_year) + (end_tm.tm_mon - start_tm.tm_mon);    \
    if (end_tm.tm_mday > start_tm.tm_mday) {                                             \
      /* case a */                                                                       \
      diff = MONTHS_TO_TIMEUNIT(months_diff, N_MONTHS);                                  \
      return SIGN_ADJUST_DIFF(is_positive, diff);                                        \
    }                                                                                    \
    if (end_tm.tm_mday < start_tm.tm_mday) {                                             \
      /* case b */                                                                       \
      diff = MONTHS_TO_TIMEUNIT(months_diff - 1, N_MONTHS);                              \
      return SIGN_ADJUST_DIFF(is_positive, diff);                                        \
    }                                                                                    \
    int32 end_day_millis =                                                               \
        end_tm.tm_hour * MILLIS_IN_HOUR + end_tm.tm_min * MILLIS_IN_MIN + end_tm.tm_sec; \
    int32 start_day_millis = start_tm.tm_hour * MILLIS_IN_HOUR +                         \
                             start_tm.tm_min * MILLIS_IN_MIN + start_tm.tm_sec;          \
    if (end_day_millis >= start_day_millis) {                                            \
      /* case c1 */                                                                      \
      diff = MONTHS_TO_TIMEUNIT(months_diff, N_MONTHS);                                  \
      return SIGN_ADJUST_DIFF(is_positive, diff);                                        \
    }                                                                                    \
    /* case c2 */                                                                        \
    diff = MONTHS_TO_TIMEUNIT(months_diff - 1, N_MONTHS);                                \
    return SIGN_ADJUST_DIFF(is_positive, diff);                                          \
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

}  // extern "C"
