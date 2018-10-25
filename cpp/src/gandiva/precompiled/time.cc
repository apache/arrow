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

extern "C" {

#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "./time_constants.h"
#include "./types.h"

#define MINS_IN_HOUR 60
#define SECONDS_IN_MINUTE 60
#define SECONDS_IN_HOUR (SECONDS_IN_MINUTE) * (MINS_IN_HOUR)

#define HOURS_IN_DAY 24

// Expand inner macro for all date types.
#define DATE_TYPES(INNER) \
  INNER(date64)           \
  INNER(timestamp)

// Extract millennium
#define EXTRACT_MILLENNIUM(TYPE)                  \
  FORCE_INLINE                                    \
  int64 extractMillennium##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);                    \
    return (1900 + tp.TmYear() - 1) / 1000 + 1;   \
  }

DATE_TYPES(EXTRACT_MILLENNIUM)

// Extract century
#define EXTRACT_CENTURY(TYPE)                  \
  FORCE_INLINE                                 \
  int64 extractCentury##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);                 \
    return (1900 + tp.TmYear() - 1) / 100 + 1; \
  }

DATE_TYPES(EXTRACT_CENTURY)

// Extract  decade
#define EXTRACT_DECADE(TYPE)                  \
  FORCE_INLINE                                \
  int64 extractDecade##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);                \
    return (1900 + tp.TmYear()) / 10;         \
  }

DATE_TYPES(EXTRACT_DECADE)

// Extract  year.
#define EXTRACT_YEAR(TYPE)                  \
  FORCE_INLINE                              \
  int64 extractYear##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);              \
    return 1900 + tp.TmYear();              \
  }

DATE_TYPES(EXTRACT_YEAR)

#define EXTRACT_DOY(TYPE)                  \
  FORCE_INLINE                             \
  int64 extractDoy##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);             \
    return 1 + tp.TmYday();                \
  }

DATE_TYPES(EXTRACT_DOY)

#define EXTRACT_QUARTER(TYPE)                  \
  FORCE_INLINE                                 \
  int64 extractQuarter##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);                 \
    return tp.TmMon() / 3 + 1;                 \
  }

DATE_TYPES(EXTRACT_QUARTER)

#define EXTRACT_MONTH(TYPE)                  \
  FORCE_INLINE                               \
  int64 extractMonth##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);               \
    return 1 + tp.TmMon();                   \
  }

DATE_TYPES(EXTRACT_MONTH)

#define JAN1_WDAY(tp) ((tp.TmWday() - (tp.TmYday() % 7) + 7) % 7)

bool IsLeapYear(int yy) {
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

// Day belongs to current year
// Note that TmYday is 0 for Jan 1 (subtract 1 from day in the below examples)
//
// If Jan 1 is Mon, (TmYday) / 7 + 1 (Jan 1->WK1, Jan 8->WK2, etc)
// If Jan 1 is Tues, (TmYday + 1) / 7 + 1 (Jan 1->WK1, Jan 7->WK2, etc)
// If Jan 1 is Wed, (TmYday + 2) / 7 + 1
// If Jan 1 is Thu, (TmYday + 3) / 7 + 1
//
// If Jan 1 is Fri, Sat or Sun, the first few days belong to the previous year
// If Jan 1 is Fri, (TmYday - 3) / 7 + 1 (Jan 4->WK1, Jan 11->WK2)
// If Jan 1 is Sat, (TmYday - 2) / 7 + 1 (Jan 3->WK1, Jan 10->WK2)
// If Jan 1 is Sun, (TmYday - 1) / 7 + 1 (Jan 2->WK1, Jan 9->WK2)
int weekOfCurrentYear(const EpochTimePoint& tp) {
  int jan1_wday = JAN1_WDAY(tp);
  switch (jan1_wday) {
    // Monday
    case 1:
    // Tuesday
    case 2:
    // Wednesday
    case 3:
    // Thursday
    case 4: {
      return (tp.TmYday() + jan1_wday - 1) / 7 + 1;
    }
    // Friday
    case 5:
    // Saturday
    case 6: {
      return (tp.TmYday() - (8 - jan1_wday)) / 7 + 1;
    }
    // Sunday
    case 0: {
      return (tp.TmYday() - 1) / 7 + 1;
    }
  }

  // cannot reach here
  // keep compiler happy
  return 0;
}

// Jan 1-3
// If Jan 1 is one of Mon, Tue, Wed, Thu - belongs to week of current year
// If Jan 1 is Fri/Sat/Sun - belongs to previous year
int getJanWeekOfYear(const EpochTimePoint& tp) {
  int jan1_wday = JAN1_WDAY(tp);

  if ((jan1_wday >= 1) && (jan1_wday <= 4)) {
    // Jan 1-3 with the week belonging to this year
    return 1;
  }

  if (jan1_wday == 5) {
    // Jan 1 is a Fri
    // Jan 1-3 belong to previous year. Dec 31 of previous year same week # as Jan 1-3
    // previous year is a leap year:
    // Prev Jan 1 is a Wed. Jan 6th is Mon
    // Dec 31 - Jan 6 = 366 - 5 = 361
    // week from Jan 6 = (361 - 1) / 7 + 1 = 52
    // week # in previous year = 52 + 1 = 53
    //
    // previous year is not a leap year. Jan 1 is Thu. Jan 5th is Mon
    // Dec 31 - Jan 5 = 365 - 4 = 361
    // week from Jan 5 = (361 - 1) / 7 + 1 = 52
    // week # in previous year = 52 + 1 = 53
    return 53;
  }

  if (jan1_wday == 0) {
    // Jan 1 is a Sun
    if (tp.TmMday() > 1) {
      // Jan 2 and 3 belong to current year
      return 1;
    }

    // day belongs to previous year. Same as Dec 31
    // Same as the case where Jan 1 is a Fri, except that previous year
    // does not have an extra week
    // Hence, return 52
    return 52;
  }

  // Jan 1 is a Sat
  // Jan 1-2 belong to previous year
  if (tp.TmMday() == 3) {
    // Jan 3, return 1
    return 1;
  }

  // prev Jan 1 is leap year
  // prev Jan 1 is a Thu
  // return 53 (extra week)
  if (IsLeapYear(1900 + tp.TmYear() - 1)) {
    return 53;
  }

  // prev Jan 1 is not a leap year
  // prev Jan 1 is a Fri
  // return 52 (no extra week)
  return 52;
}

// Dec 29-31
int getDecWeekOfYear(const EpochTimePoint& tp) {
  int next_jan1_wday = (tp.TmWday() + (31 - tp.TmMday()) + 1) % 7;

  if (next_jan1_wday == 4) {
    // next Jan 1 is a Thu
    // day belongs to week 1 of next year
    return 1;
  }

  if (next_jan1_wday == 3) {
    // next Jan 1 is a Wed
    // Dec 31 and 30 belong to next year - return 1
    if (tp.TmMday() != 29) {
      return 1;
    }

    // Dec 29 belongs to current year
    return weekOfCurrentYear(tp);
  }

  if (next_jan1_wday == 2) {
    // next Jan 1 is a Tue
    // Dec 31 belongs to next year - return 1
    if (tp.TmMday() == 31) {
      return 1;
    }

    // Dec 29 and 30 belong to current year
    return weekOfCurrentYear(tp);
  }

  // next Jan 1 is a Fri/Sat/Sun. No day from this year belongs to that week
  // next Jan 1 is a Mon. No day from this year belongs to that week
  return weekOfCurrentYear(tp);
}

// Week of year is determined by ISO 8601 standard
// Take a look at: https://en.wikipedia.org/wiki/ISO_week_date
//
// Important points to note:
// Week starts with a Monday and ends with a Sunday
// A week can have some days in this year and some days in the previous/next year
// This is true for the first and last weeks
//
// The first week of the year should have at-least 4 days in the current year
// The last week of the year should have at-least 4 days in the current year
//
// A given day might belong to the first week of the next year - e.g Dec 29, 30 and 31
// A given day might belong to the last week of the previous year - e.g. Jan 1, 2 and 3
//
// Algorithm:
// If day belongs to week in current year, weekOfCurrentYear
//
// If day is Jan 1-3, see getJanWeekOfYear
// If day is Dec 29-21, see getDecWeekOfYear
//
int64 weekOfYear(const EpochTimePoint& tp) {
  if (tp.TmYday() < 3) {
    // Jan 1-3
    return getJanWeekOfYear(tp);
  }

  if ((tp.TmMon() == 11) && (tp.TmMday() >= 29)) {
    // Dec 29-31
    return getDecWeekOfYear(tp);
  }

  return weekOfCurrentYear(tp);
}

#define EXTRACT_WEEK(TYPE)                  \
  FORCE_INLINE                              \
  int64 extractWeek##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);              \
    return weekOfYear(tp);                  \
  }

DATE_TYPES(EXTRACT_WEEK)

#define EXTRACT_DOW(TYPE)                  \
  FORCE_INLINE                             \
  int64 extractDow##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);             \
    return 1 + tp.TmWday();                \
  }

DATE_TYPES(EXTRACT_DOW)

#define EXTRACT_DAY(TYPE)                  \
  FORCE_INLINE                             \
  int64 extractDay##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);             \
    return tp.TmMday();                    \
  }

DATE_TYPES(EXTRACT_DAY)

#define EXTRACT_HOUR(TYPE)                  \
  FORCE_INLINE                              \
  int64 extractHour##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);              \
    return tp.TmHour();                     \
  }

DATE_TYPES(EXTRACT_HOUR)

#define EXTRACT_MINUTE(TYPE)                  \
  FORCE_INLINE                                \
  int64 extractMinute##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);                \
    return tp.TmMin();                        \
  }

DATE_TYPES(EXTRACT_MINUTE)

#define EXTRACT_SECOND(TYPE)                  \
  FORCE_INLINE                                \
  int64 extractSecond##_##TYPE(TYPE millis) { \
    EpochTimePoint tp(millis);                \
    return tp.TmSec();                        \
  }

DATE_TYPES(EXTRACT_SECOND)

#define EXTRACT_EPOCH(TYPE) \
  FORCE_INLINE              \
  int64 extractEpoch##_##TYPE(TYPE millis) { return MILLIS_TO_SEC(millis); }

DATE_TYPES(EXTRACT_EPOCH)

// Functions that work on millis in a day
#define EXTRACT_SECOND_TIME(TYPE)                   \
  FORCE_INLINE                                      \
  int64 extractSecond##_##TYPE(TYPE millis) {       \
    int64 seconds_of_day = MILLIS_TO_SEC(millis);   \
    int64 sec = seconds_of_day % SECONDS_IN_MINUTE; \
    return sec;                                     \
  }

EXTRACT_SECOND_TIME(time32)

#define EXTRACT_MINUTE_TIME(TYPE)             \
  FORCE_INLINE                                \
  int64 extractMinute##_##TYPE(TYPE millis) { \
    TYPE mins = MILLIS_TO_MINS(millis);       \
    return (mins % (MINS_IN_HOUR));           \
  }

EXTRACT_MINUTE_TIME(time32)

#define EXTRACT_HOUR_TIME(TYPE) \
  FORCE_INLINE                  \
  int64 extractHour##_##TYPE(TYPE millis) { return MILLIS_TO_HOUR(millis); }

EXTRACT_HOUR_TIME(time32)

#define DATE_TRUNC_FIXED_UNIT(NAME, TYPE, NMILLIS_IN_UNIT) \
  FORCE_INLINE                                             \
  TYPE NAME##_##TYPE(TYPE millis) {                        \
    return ((millis / NMILLIS_IN_UNIT) * NMILLIS_IN_UNIT); \
  }

#define DATE_TRUNC_WEEK(TYPE)                                               \
  FORCE_INLINE                                                              \
  TYPE date_trunc_Week_##TYPE(TYPE millis) {                                \
    EpochTimePoint tp(millis);                                              \
    int ndays_to_trunc = 0;                                                 \
    if (tp.TmWday() == 0) {                                                 \
      /* Sunday */                                                          \
      ndays_to_trunc = 6;                                                   \
    } else {                                                                \
      /* All other days */                                                  \
      ndays_to_trunc = tp.TmWday() - 1;                                     \
    }                                                                       \
    return tp.AddDays(-ndays_to_trunc).ClearTimeOfDay().MillisSinceEpoch(); \
  }

#define DATE_TRUNC_MONTH_UNITS(NAME, TYPE, NMONTHS_IN_UNIT)              \
  FORCE_INLINE                                                           \
  TYPE NAME##_##TYPE(TYPE millis) {                                      \
    EpochTimePoint tp(millis);                                           \
    int ndays_to_trunc = tp.TmMday() - 1;                                \
    int nmonths_to_trunc =                                               \
        tp.TmMon() - ((tp.TmMon() / NMONTHS_IN_UNIT) * NMONTHS_IN_UNIT); \
    return tp.AddDays(-ndays_to_trunc)                                   \
        .AddMonths(-nmonths_to_trunc)                                    \
        .ClearTimeOfDay()                                                \
        .MillisSinceEpoch();                                             \
  }

#define DATE_TRUNC_YEAR_UNITS(NAME, TYPE, NYEARS_IN_UNIT, OFF_BY)        \
  FORCE_INLINE                                                           \
  TYPE NAME##_##TYPE(TYPE millis) {                                      \
    EpochTimePoint tp(millis);                                           \
    int ndays_to_trunc = tp.TmMday() - 1;                                \
    int nmonths_to_trunc = tp.TmMon();                                   \
    int year = 1900 + tp.TmYear();                                       \
    year = ((year - OFF_BY) / NYEARS_IN_UNIT) * NYEARS_IN_UNIT + OFF_BY; \
    int nyears_to_trunc = tp.TmYear() - (year - 1900);                   \
    return tp.AddDays(-ndays_to_trunc)                                   \
        .AddMonths(-nmonths_to_trunc)                                    \
        .AddYears(-nyears_to_trunc)                                      \
        .ClearTimeOfDay()                                                \
        .MillisSinceEpoch();                                             \
  }

#define DATE_TRUNC_FUNCTIONS(TYPE)                              \
  DATE_TRUNC_FIXED_UNIT(date_trunc_Second, TYPE, MILLIS_IN_SEC) \
  DATE_TRUNC_FIXED_UNIT(date_trunc_Minute, TYPE, MILLIS_IN_MIN) \
  DATE_TRUNC_FIXED_UNIT(date_trunc_Hour, TYPE, MILLIS_IN_HOUR)  \
  DATE_TRUNC_FIXED_UNIT(date_trunc_Day, TYPE, MILLIS_IN_DAY)    \
  DATE_TRUNC_WEEK(TYPE)                                         \
  DATE_TRUNC_MONTH_UNITS(date_trunc_Month, TYPE, 1)             \
  DATE_TRUNC_MONTH_UNITS(date_trunc_Quarter, TYPE, 3)           \
  DATE_TRUNC_MONTH_UNITS(date_trunc_Year, TYPE, 12)             \
  DATE_TRUNC_YEAR_UNITS(date_trunc_Decade, TYPE, 10, 0)         \
  DATE_TRUNC_YEAR_UNITS(date_trunc_Century, TYPE, 100, 1)       \
  DATE_TRUNC_YEAR_UNITS(date_trunc_Millennium, TYPE, 1000, 1)

DATE_TRUNC_FUNCTIONS(date64)
DATE_TRUNC_FUNCTIONS(timestamp)

FORCE_INLINE
date64 castDATE_int64(int64 in) { return in; }

static int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

bool IsLastDayOfMonth(const EpochTimePoint& tp) {
  if (tp.TmMon() != 1) {
    // not February. Dont worry about leap year
    return (tp.TmMday() == days_in_month[tp.TmMon()]);
  }

  // this is February, check if the day is 28 or 29
  if (tp.TmMday() < 28) {
    return false;
  }

  if (tp.TmMday() == 29) {
    // Feb 29th
    return true;
  }

  // check if year is non-leap year
  return !IsLeapYear(tp.TmYear());
}

// MONTHS_BETWEEN returns number of months between dates date1 and date2.
// If date1 is later than date2, then the result is positive.
// If date1 is earlier than date2, then the result is negative.
// If date1 and date2 are either the same days of the month or both last days of months,
// then the result is always an integer. Otherwise Oracle Database calculates the
// fractional portion of the result based on a 31-day month and considers the difference
// in time components date1 and date2
#define MONTHS_BETWEEN(TYPE)                                                        \
  FORCE_INLINE                                                                      \
  double months_between##_##TYPE##_##TYPE(uint64_t endEpoch, uint64_t startEpoch) { \
    EpochTimePoint endTime(endEpoch);                                               \
    EpochTimePoint startTime(startEpoch);                                           \
    int endYear = endTime.TmYear();                                                 \
    int endMonth = endTime.TmMon();                                                 \
    int startYear = startTime.TmYear();                                             \
    int startMonth = startTime.TmMon();                                             \
    int monthsDiff = (endYear - startYear) * 12 + (endMonth - startMonth);          \
    if ((endTime.TmMday() == startTime.TmMday()) ||                                 \
        (IsLastDayOfMonth(endTime) && IsLastDayOfMonth(startTime))) {               \
      return static_cast<double>(monthsDiff);                                       \
    }                                                                               \
    double diffDays = static_cast<double>(endTime.TmMday() - startTime.TmMday()) /  \
                      static_cast<double>(31);                                      \
    double diffHours = static_cast<double>(endTime.TmHour() - startTime.TmHour()) + \
                       static_cast<double>(endTime.TmMin() - startTime.TmMin()) /   \
                           static_cast<double>(MINS_IN_HOUR) +                      \
                       static_cast<double>(endTime.TmSec() - startTime.TmSec()) /   \
                           static_cast<double>(SECONDS_IN_HOUR);                    \
    return static_cast<double>(monthsDiff) + diffDays +                             \
           diffHours / static_cast<double>(HOURS_IN_DAY * 31);                      \
  }

DATE_TYPES(MONTHS_BETWEEN)

FORCE_INLINE
void set_error_for_date(int32 length, const char* input, const char* msg,
                        int64_t execution_context) {
  int size = length + static_cast<int>(strlen(msg)) + 1;
  char* error = reinterpret_cast<char*>(malloc(size));
  snprintf(error, size, "%s%s", msg, input);
  gdv_fn_context_set_error_msg(execution_context, error);
  free(error);
}

date64 castDATE_utf8(int64_t context, const char* input, int32 length) {
  // format : 0 is year, 1 is month and 2 is day.
  int dateFields[3];
  int dateIndex = 0, index = 0, value = 0;
  while (dateIndex < 3 && index < length) {
    if (!isdigit(input[index])) {
      dateFields[dateIndex++] = value;
      value = 0;
    } else {
      value = (value * 10) + (input[index] - '0');
    }
    index++;
  }

  if (dateIndex < 3) {
    // If we reached the end of input, we would have not encountered a separator
    // store the last value
    dateFields[dateIndex++] = value;
  }
  const char* msg = "Not a valid date value ";
  if (dateIndex != 3) {
    set_error_for_date(length, input, msg, context);
    return 0;
  }

  /* Handle two digit years
   * If range of two digits is between 70 - 99 then year = 1970 - 1999
   * Else if two digits is between 00 - 69 = 2000 - 2069
   */
  if (dateFields[0] < 100) {
    if (dateFields[0] < 70) {
      dateFields[0] += 2000;
    } else {
      dateFields[0] += 1900;
    }
  }
  date::year_month_day day =
      date::year(dateFields[0]) / date::month(dateFields[1]) / date::day(dateFields[2]);
  if (!day.ok()) {
    set_error_for_date(length, input, msg, context);
    return 0;
  }
  return std::chrono::time_point_cast<std::chrono::milliseconds>(date::sys_days(day))
      .time_since_epoch()
      .count();
}
}  // extern "C"
