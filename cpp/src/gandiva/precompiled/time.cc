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

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "./time_constants.h"
#include "./time_fields.h"
#include "./types.h"

#define MINS_IN_HOUR 60
#define SECONDS_IN_MINUTE 60
#define SECONDS_IN_HOUR (SECONDS_IN_MINUTE) * (MINS_IN_HOUR)

#define HOURS_IN_DAY 24

// Expand inner macro for all date types.
#define DATE_TYPES(INNER) \
  INNER(date64)           \
  INNER(timestamp)

#define INTEGER_NUMERIC_TYPES(INNER) \
  INNER(int8)                        \
  INNER(int16)                       \
  INNER(int32)                       \
  INNER(int64)                       \
  INNER(uint8)                       \
  INNER(uint16)                      \
  INNER(uint32)                      \
  INNER(uint64)

#define REAL_NUMERIC_TYPES(INNER) \
  INNER(float32)                  \
  INNER(float64)

// Expand inner macro for all base numeric types.
#define NUMERIC_TYPES(INNER)   \
  INTEGER_NUMERIC_TYPES(INNER) \
  REAL_NUMERIC_TYPES(INNER)

// Extract millennium
#define EXTRACT_MILLENNIUM(TYPE)                            \
  FORCE_INLINE                                              \
  gdv_int64 extractMillennium##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                              \
    return (1900 + tp.TmYear() - 1) / 1000 + 1;             \
  }

DATE_TYPES(EXTRACT_MILLENNIUM)

// Extract century
#define EXTRACT_CENTURY(TYPE)                            \
  FORCE_INLINE                                           \
  gdv_int64 extractCentury##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                           \
    return (1900 + tp.TmYear() - 1) / 100 + 1;           \
  }

DATE_TYPES(EXTRACT_CENTURY)

// Extract  decade
#define EXTRACT_DECADE(TYPE)                            \
  FORCE_INLINE                                          \
  gdv_int64 extractDecade##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                          \
    return (1900 + tp.TmYear()) / 10;                   \
  }

DATE_TYPES(EXTRACT_DECADE)

// Extract  year.
#define EXTRACT_YEAR(TYPE)                            \
  FORCE_INLINE                                        \
  gdv_int64 extractYear##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                        \
    return 1900 + tp.TmYear();                        \
  }

DATE_TYPES(EXTRACT_YEAR)

#define EXTRACT_DOY(TYPE)                            \
  FORCE_INLINE                                       \
  gdv_int64 extractDoy##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                       \
    return 1 + tp.TmYday();                          \
  }

DATE_TYPES(EXTRACT_DOY)

#define EXTRACT_QUARTER(TYPE)                            \
  FORCE_INLINE                                           \
  gdv_int64 extractQuarter##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                           \
    return tp.TmMon() / 3 + 1;                           \
  }

DATE_TYPES(EXTRACT_QUARTER)

#define EXTRACT_MONTH(TYPE)                            \
  FORCE_INLINE                                         \
  gdv_int64 extractMonth##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                         \
    return 1 + tp.TmMon();                             \
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

static const char* WEEK[] = {"SUNDAY",   "MONDAY", "TUESDAY", "WEDNESDAY",
                             "THURSDAY", "FRIDAY", "SATURDAY"};

static const int WEEK_LEN[] = {6, 6, 7, 9, 8, 6, 8};

#define NEXT_DAY_FUNC(TYPE)                                                              \
  FORCE_INLINE                                                                           \
  gdv_date64 next_day_from_##TYPE(gdv_int64 context, gdv_##TYPE millis, const char* in,  \
                                  int32_t in_len) {                                      \
    EpochTimePoint tp(millis);                                                           \
    const auto& dayWithoutHoursAndSec = tp.ClearTimeOfDay();                             \
    const auto& presentDate = extractDow_timestamp(tp.MillisSinceEpoch());               \
                                                                                         \
    int dateSearch = 0;                                                                  \
    for (int n = 0; n < 7; n++) {                                                        \
      if (is_substr_utf8_utf8(WEEK[n], WEEK_LEN[n], in, in_len)) {                       \
        dateSearch = n + 1;                                                              \
        break;                                                                           \
      }                                                                                  \
    }                                                                                    \
    if (dateSearch == 0) {                                                               \
      gdv_fn_context_set_error_msg(context, "The weekday in this entry is invalid");     \
      return 0;                                                                          \
    }                                                                                    \
                                                                                         \
    int64_t distanceDay = dateSearch - presentDate;                                      \
    if (distanceDay <= 0) {                                                              \
      distanceDay = 7 + distanceDay;                                                     \
    }                                                                                    \
                                                                                         \
    int64_t nextDate =                                                                   \
        date_add_int64_timestamp(distanceDay, dayWithoutHoursAndSec.MillisSinceEpoch()); \
                                                                                         \
    return nextDate;                                                                     \
  }

DATE_TYPES(NEXT_DAY_FUNC)

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
gdv_int64 weekOfYear(const EpochTimePoint& tp) {
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

#define EXTRACT_WEEK(TYPE)                            \
  FORCE_INLINE                                        \
  gdv_int64 extractWeek##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                        \
    return weekOfYear(tp);                            \
  }

DATE_TYPES(EXTRACT_WEEK)

#define EXTRACT_DOW(TYPE)                            \
  FORCE_INLINE                                       \
  gdv_int64 extractDow##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                       \
    return 1 + tp.TmWday();                          \
  }

DATE_TYPES(EXTRACT_DOW)

#define EXTRACT_DAY(TYPE)                            \
  FORCE_INLINE                                       \
  gdv_int64 extractDay##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                       \
    return tp.TmMday();                              \
  }

DATE_TYPES(EXTRACT_DAY)

#define EXTRACT_HOUR(TYPE)                            \
  FORCE_INLINE                                        \
  gdv_int64 extractHour##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                        \
    return tp.TmHour();                               \
  }

DATE_TYPES(EXTRACT_HOUR)

#define EXTRACT_MINUTE(TYPE)                            \
  FORCE_INLINE                                          \
  gdv_int64 extractMinute##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                          \
    return tp.TmMin();                                  \
  }

DATE_TYPES(EXTRACT_MINUTE)

#define EXTRACT_SECOND(TYPE)                            \
  FORCE_INLINE                                          \
  gdv_int64 extractSecond##_##TYPE(gdv_##TYPE millis) { \
    EpochTimePoint tp(millis);                          \
    return tp.TmSec();                                  \
  }

DATE_TYPES(EXTRACT_SECOND)

#define EXTRACT_EPOCH(TYPE) \
  FORCE_INLINE              \
  gdv_int64 extractEpoch##_##TYPE(gdv_##TYPE millis) { return MILLIS_TO_SEC(millis); }

DATE_TYPES(EXTRACT_EPOCH)

// Functions that work on millis in a day
#define EXTRACT_SECOND_TIME(TYPE)                       \
  FORCE_INLINE                                          \
  gdv_int64 extractSecond##_##TYPE(gdv_##TYPE millis) { \
    gdv_int64 seconds_of_day = MILLIS_TO_SEC(millis);   \
    gdv_int64 sec = seconds_of_day % SECONDS_IN_MINUTE; \
    return sec;                                         \
  }

EXTRACT_SECOND_TIME(time32)

#define EXTRACT_MINUTE_TIME(TYPE)                       \
  FORCE_INLINE                                          \
  gdv_int64 extractMinute##_##TYPE(gdv_##TYPE millis) { \
    gdv_##TYPE mins = MILLIS_TO_MINS(millis);           \
    return (mins % (MINS_IN_HOUR));                     \
  }

EXTRACT_MINUTE_TIME(time32)

#define EXTRACT_HOUR_TIME(TYPE) \
  FORCE_INLINE                  \
  gdv_int64 extractHour##_##TYPE(gdv_##TYPE millis) { return MILLIS_TO_HOUR(millis); }

EXTRACT_HOUR_TIME(time32)

#define DATE_TRUNC_FIXED_UNIT(NAME, TYPE, NMILLIS_IN_UNIT)                               \
  FORCE_INLINE                                                                           \
  gdv_##TYPE NAME##_##TYPE(gdv_##TYPE millis) {                                          \
    return millis >= 0                                                                   \
               ? ((millis / NMILLIS_IN_UNIT) * NMILLIS_IN_UNIT)                          \
               : (((millis - NMILLIS_IN_UNIT + 1) / NMILLIS_IN_UNIT) * NMILLIS_IN_UNIT); \
  }

#define DATE_TRUNC_WEEK(TYPE)                                               \
  FORCE_INLINE                                                              \
  gdv_##TYPE date_trunc_Week_##TYPE(gdv_##TYPE millis) {                    \
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
  gdv_##TYPE NAME##_##TYPE(gdv_##TYPE millis) {                          \
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
  gdv_##TYPE NAME##_##TYPE(gdv_##TYPE millis) {                          \
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

#define LAST_DAY_FUNC(TYPE)                                                   \
  FORCE_INLINE                                                                \
  gdv_date64 last_day_from_##TYPE(gdv_date64 millis) {                        \
    EpochTimePoint received_day(millis);                                      \
    const auto& day_without_hours_and_sec = received_day.ClearTimeOfDay();    \
                                                                              \
    int received_day_in_month = day_without_hours_and_sec.TmMday();           \
    const auto& first_day_in_month =                                          \
        day_without_hours_and_sec.AddDays(1 - received_day_in_month);         \
                                                                              \
    const auto& month_last_day = first_day_in_month.AddMonths(1).AddDays(-1); \
                                                                              \
    return month_last_day.MillisSinceEpoch();                                 \
  }

DATE_TYPES(LAST_DAY_FUNC)

FORCE_INLINE
gdv_date64 castDATE_int64(gdv_int64 in) { return in; }

FORCE_INLINE
gdv_date32 castDATE_int32(gdv_int32 in) { return in; }

FORCE_INLINE
gdv_date64 castDATE_date32(gdv_date32 days) {
  return days * static_cast<gdv_date64>(MILLIS_IN_DAY);
}

static int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

bool IsLastDayOfMonth(const EpochTimePoint& tp) {
  if (tp.TmMon() != 1) {
    // not February. Don't worry about leap year
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

FORCE_INLINE
bool is_valid_time(const int hours, const int minutes, const int seconds) {
  return hours >= 0 && hours < 24 && minutes >= 0 && minutes < 60 && seconds >= 0 &&
         seconds < 60;
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
void set_error_for_date(gdv_int32 length, const char* input, const char* msg,
                        int64_t execution_context) {
  int size = length + static_cast<int>(strlen(msg)) + 1;
  char* error = reinterpret_cast<char*>(malloc(size));
  snprintf(error, size, "%s%s", msg, input);
  gdv_fn_context_set_error_msg(execution_context, error);
  free(error);
}

gdv_date64 castDATE_utf8(int64_t context, const char* input, gdv_int32 length) {
  using arrow_vendored::date::day;
  using arrow_vendored::date::month;
  using arrow_vendored::date::sys_days;
  using arrow_vendored::date::year;
  using arrow_vendored::date::year_month_day;
  using gandiva::TimeFields;
  // format : 0 is year, 1 is month and 2 is day.
  int dateFields[3];
  int dateIndex = 0, index = 0, value = 0;
  int year_str_len = 0;
  while (dateIndex < 3 && index < length) {
    if (!isdigit(input[index])) {
      dateFields[dateIndex++] = value;
      value = 0;
    } else {
      value = (value * 10) + (input[index] - '0');
      if (dateIndex == TimeFields::kYear) {
        year_str_len++;
      }
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
  if (dateFields[TimeFields::kYear] < 100 && year_str_len < 4) {
    if (dateFields[TimeFields::kYear] < 70) {
      dateFields[TimeFields::kYear] += 2000;
    } else {
      dateFields[TimeFields::kYear] += 1900;
    }
  }
  year_month_day date = year(dateFields[TimeFields::kYear]) /
                        month(dateFields[TimeFields::kMonth]) /
                        day(dateFields[TimeFields::kDay]);
  if (!date.ok()) {
    set_error_for_date(length, input, msg, context);
    return 0;
  }
  return std::chrono::time_point_cast<std::chrono::milliseconds>(sys_days(date))
      .time_since_epoch()
      .count();
}

/*
 * Input consists of mandatory and optional fields.
 * Mandatory fields are year, month and day.
 * Optional fields are time, displacement and zone.
 * Format is <year-month-day>[ hours:minutes:seconds][.millis][ displacement|zone]
 */
gdv_timestamp castTIMESTAMP_utf8(int64_t context, const char* input, gdv_int32 length) {
  using arrow_vendored::date::day;
  using arrow_vendored::date::month;
  using arrow_vendored::date::sys_days;
  using arrow_vendored::date::year;
  using arrow_vendored::date::year_month_day;
  using gandiva::TimeFields;
  using std::chrono::hours;
  using std::chrono::milliseconds;
  using std::chrono::minutes;
  using std::chrono::seconds;

  int ts_fields[9] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
  gdv_boolean add_displacement = true;
  gdv_boolean encountered_zone = false;
  int year_str_len = 0, sub_seconds_len = 0;
  int ts_field_index = TimeFields::kYear, index = 0, value = 0;
  while (ts_field_index < TimeFields::kMax && index < length) {
    if (isdigit(input[index])) {
      value = (value * 10) + (input[index] - '0');
      if (ts_field_index == TimeFields::kYear) {
        year_str_len++;
      }
      if (ts_field_index == TimeFields::kSubSeconds) {
        sub_seconds_len++;
      }
    } else {
      ts_fields[ts_field_index] = value;
      value = 0;

      switch (input[index]) {
        case '.':
        case ':':
        case ' ':
          ts_field_index++;
          break;
        case '+':
          // +08:00, means time zone is 8 hours ahead. Need to subtract.
          add_displacement = false;
          ts_field_index = TimeFields::kDisplacementHours;
          break;
        case '-':
          // Overloaded as date separator and negative displacement.
          ts_field_index = (ts_field_index < 3) ? (ts_field_index + 1)
                                                : TimeFields::kDisplacementHours;
          break;
        default:
          encountered_zone = true;
          break;
      }
    }
    if (encountered_zone) {
      break;
    }
    index++;
  }

  // Store the last value
  if (ts_field_index < TimeFields::kMax) {
    ts_fields[ts_field_index++] = value;
  }

  // adjust the year
  if (ts_fields[TimeFields::kYear] < 100 && year_str_len < 4) {
    if (ts_fields[TimeFields::kYear] < 70) {
      ts_fields[TimeFields::kYear] += 2000;
    } else {
      ts_fields[TimeFields::kYear] += 1900;
    }
  }

  // adjust the milliseconds
  if (sub_seconds_len > 0) {
    if (sub_seconds_len > 3) {
      const char* msg = "Invalid millis for timestamp value ";
      set_error_for_date(length, input, msg, context);
      return 0;
    }
    while (sub_seconds_len < 3) {
      ts_fields[TimeFields::kSubSeconds] *= 10;
      sub_seconds_len++;
    }
  }
  // handle timezone
  if (encountered_zone) {
    int err = 0;
    gdv_timestamp ret_time = 0;
    err = gdv_fn_time_with_zone(&ts_fields[0], (input + index), (length - index),
                                &ret_time);
    if (err) {
      const char* msg = "Invalid timestamp or unknown zone for timestamp value ";
      set_error_for_date(length, input, msg, context);
      return 0;
    }
    return ret_time;
  }

  year_month_day date = year(ts_fields[TimeFields::kYear]) /
                        month(ts_fields[TimeFields::kMonth]) /
                        day(ts_fields[TimeFields::kDay]);
  if (!date.ok()) {
    const char* msg = "Not a valid day for timestamp value ";
    set_error_for_date(length, input, msg, context);
    return 0;
  }

  if (!is_valid_time(ts_fields[TimeFields::kHours], ts_fields[TimeFields::kMinutes],
                     ts_fields[TimeFields::kSeconds])) {
    const char* msg = "Not a valid time for timestamp value ";
    set_error_for_date(length, input, msg, context);
    return 0;
  }

  auto date_time = sys_days(date) + hours(ts_fields[TimeFields::kHours]) +
                   minutes(ts_fields[TimeFields::kMinutes]) +
                   seconds(ts_fields[TimeFields::kSeconds]) +
                   milliseconds(ts_fields[TimeFields::kSubSeconds]);
  if (ts_fields[TimeFields::kDisplacementHours] ||
      ts_fields[TimeFields::kDisplacementMinutes]) {
    auto displacement_time = hours(ts_fields[TimeFields::kDisplacementHours]) +
                             minutes(ts_fields[TimeFields::kDisplacementMinutes]);
    date_time = (add_displacement) ? (date_time + displacement_time)
                                   : (date_time - displacement_time);
  }
  return std::chrono::time_point_cast<milliseconds>(date_time).time_since_epoch().count();
}

gdv_timestamp castTIMESTAMP_date64(gdv_date64 date_in_millis) { return date_in_millis; }

gdv_timestamp castTIMESTAMP_int64(gdv_int64 in) { return in; }

gdv_date64 castDATE_timestamp(gdv_timestamp timestamp_in_millis) {
  EpochTimePoint tp(timestamp_in_millis);
  return tp.ClearTimeOfDay().MillisSinceEpoch();
}

/*
 * Input consists of mandatory and optional fields.
 * Mandatory fields are hours, minutes.
 * The seconds and subseconds are optional.
 * Format is hours:minutes[:seconds.millis]
 */
gdv_time32 castTIME_utf8(int64_t context, const char* input, int32_t length) {
  using gandiva::TimeFields;
  using std::chrono::hours;
  using std::chrono::milliseconds;
  using std::chrono::minutes;
  using std::chrono::seconds;

  const int32_t kDisplacementHours = 4;
  int32_t time_fields[kDisplacementHours] = {0, 0, 0, 0};
  int32_t sub_seconds_len = 0;
  int32_t time_field_idx = TimeFields::kHours, index = 0, value = 0;

  bool has_invalid_digit = false;
  while (time_field_idx < TimeFields::kDisplacementHours && index < length) {
    if (isdigit(input[index])) {
      value = (value * 10) + (input[index] - '0');

      if (time_field_idx == TimeFields::kSubSeconds) {
        sub_seconds_len++;
      }
    } else {
      time_fields[time_field_idx - TimeFields::kHours] = value;
      value = 0;

      switch (input[index]) {
        case '.':
        case ':':
          time_field_idx++;
          break;
        default:
          has_invalid_digit = true;
          break;
      }
    }

    index++;
  }

  if (has_invalid_digit) {
    const char* msg = "Invalid character in time ";
    set_error_for_date(length, input, msg, context);
    return 0;
  }

  // Check if the hours and minutes were defined and store the last value
  if (time_field_idx < TimeFields::kDisplacementHours) {
    time_fields[time_field_idx - TimeFields::kHours] = value;
  }

  // adjust the milliseconds
  if (sub_seconds_len > 0) {
    if (sub_seconds_len > 3) {
      const char* msg = "Invalid millis for time value ";
      set_error_for_date(length, input, msg, context);
      return 0;
    }

    while (sub_seconds_len < 3) {
      time_fields[TimeFields::kSubSeconds - TimeFields::kHours] *= 10;
      sub_seconds_len++;
    }
  }

  int32_t input_hours = time_fields[TimeFields::kHours - TimeFields::kHours];
  int32_t input_minutes = time_fields[TimeFields::kMinutes - TimeFields::kHours];
  int32_t input_seconds = time_fields[TimeFields::kSeconds - TimeFields::kHours];
  int32_t input_subseconds = time_fields[TimeFields::kSubSeconds - TimeFields::kHours];

  if (!is_valid_time(input_hours, input_minutes, input_seconds)) {
    const char* msg = "Not a valid time value ";
    set_error_for_date(length, input, msg, context);
    return 0;
  }

  auto time_info = hours(input_hours) + minutes(input_minutes) + seconds(input_seconds) +
                   milliseconds(input_subseconds);

  return static_cast<gdv_time32>(time_info.count());
}

gdv_time32 castTIME_timestamp(gdv_timestamp timestamp_in_millis) {
  // Retrieves a timestamp and returns the number of milliseconds since the midnight
  EpochTimePoint tp(timestamp_in_millis);
  auto tp_at_midnight = tp.ClearTimeOfDay();

  int64_t millis_since_midnight =
      tp.MillisSinceEpoch() - tp_at_midnight.MillisSinceEpoch();

  return static_cast<int32_t>(millis_since_midnight);
}

// Gets an arbitrary number and return the number of milliseconds since midnight
gdv_time32 castTIME_int32(int32_t int_val) {
  if (int_val < 0) {
    return 0;
  }

  auto millis_since_midnight = static_cast<gdv_time32>(int_val % MILLIS_IN_DAY);

  return millis_since_midnight;
}

const char* castVARCHAR_timestamp_int64(gdv_int64 context, gdv_timestamp in,
                                        gdv_int64 length, gdv_int32* out_len) {
  gdv_int64 year = extractYear_timestamp(in);
  gdv_int64 month = extractMonth_timestamp(in);
  gdv_int64 day = extractDay_timestamp(in);
  gdv_int64 hour = extractHour_timestamp(in);
  gdv_int64 minute = extractMinute_timestamp(in);
  gdv_int64 second = extractSecond_timestamp(in);
  gdv_int64 millis = in % MILLIS_IN_SEC;

  static const int kTimeStampStringLen = 23;
  const int char_buffer_length = kTimeStampStringLen + 1;  // snprintf adds \0
  char char_buffer[char_buffer_length];

  // yyyy-MM-dd hh:mm:ss.sss
  int res = snprintf(char_buffer, char_buffer_length,
                     "%04" PRId64 "-%02" PRId64 "-%02" PRId64 " %02" PRId64 ":%02" PRId64
                     ":%02" PRId64 ".%03" PRId64,
                     year, month, day, hour, minute, second, millis);
  if (res < 0) {
    gdv_fn_context_set_error_msg(context, "Could not format the timestamp");
    return "";
  }

  *out_len = static_cast<gdv_int32>(length);
  if (*out_len > kTimeStampStringLen) {
    *out_len = kTimeStampStringLen;
  }

  if (*out_len <= 0) {
    if (*out_len < 0) {
      gdv_fn_context_set_error_msg(context, "Length of output string cannot be negative");
    }
    *out_len = 0;
    return "";
  }

  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
  if (ret == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }

  memcpy(ret, char_buffer, *out_len);
  return ret;
}

#define IS_NULL(TYPE) \
  FORCE_INLINE        \
  bool isnull_##TYPE(gdv_##TYPE in, gdv_boolean is_valid) { return !is_valid; }
IS_NULL(day_time_interval)
IS_NULL(month_interval)

FORCE_INLINE
gdv_int64 extractDay_daytimeinterval(gdv_day_time_interval in) {
  gdv_int32 days = static_cast<gdv_int32>(in & 0x00000000FFFFFFFF);
  return static_cast<gdv_int64>(days);
}

FORCE_INLINE
gdv_int64 extractMillis_daytimeinterval(gdv_day_time_interval in) {
  gdv_int32 millis = static_cast<gdv_int32>((in & 0xFFFFFFFF00000000) >> 32);
  return static_cast<gdv_int64>(millis);
}

FORCE_INLINE
gdv_int64 castBIGINT_daytimeinterval(gdv_day_time_interval in) {
  return extractMillis_daytimeinterval(in) +
         extractDay_daytimeinterval(in) * MILLIS_IN_DAY;
}

// Convert the seconds since epoch argument to timestamp
#define TO_TIMESTAMP_INTEGER(TYPE)                              \
  FORCE_INLINE                                                  \
  gdv_timestamp to_timestamp##_##TYPE(gdv_##TYPE seconds) {     \
    return static_cast<gdv_timestamp>(seconds) * MILLIS_IN_SEC; \
  }

#define TO_TIMESTAMP_REAL(TYPE)                                 \
  FORCE_INLINE                                                  \
  gdv_timestamp to_timestamp##_##TYPE(gdv_##TYPE seconds) {     \
    return static_cast<gdv_timestamp>(seconds * MILLIS_IN_SEC); \
  }

INTEGER_NUMERIC_TYPES(TO_TIMESTAMP_INTEGER)
REAL_NUMERIC_TYPES(TO_TIMESTAMP_REAL)

#undef TO_TIMESTAMP_INTEGER
#undef TO_TIMESTAMP_REAL

// Convert the seconds since epoch argument to time
#define TO_TIME(TYPE)                                                     \
  FORCE_INLINE                                                            \
  gdv_time32 to_time##_##TYPE(gdv_##TYPE seconds) {                       \
    EpochTimePoint tp(static_cast<int64_t>(seconds * MILLIS_IN_SEC));     \
    return static_cast<gdv_time32>(tp.TimeOfDay().to_duration().count()); \
  }

NUMERIC_TYPES(TO_TIME)

#define CAST_INT_YEAR_INTERVAL(TYPE, OUT_TYPE)                 \
  FORCE_INLINE                                                 \
  gdv_##OUT_TYPE TYPE##_year_interval(gdv_month_interval in) { \
    return static_cast<gdv_##OUT_TYPE>(in / 12.0);             \
  }

CAST_INT_YEAR_INTERVAL(castBIGINT, int64)
CAST_INT_YEAR_INTERVAL(castINT, int32)

#define CAST_NULLABLE_INTERVAL_DAY(TYPE)                                \
  FORCE_INLINE                                                          \
  gdv_day_time_interval castNULLABLEINTERVALDAY_##TYPE(gdv_##TYPE in) { \
    return static_cast<gdv_day_time_interval>(in);                      \
  }

CAST_NULLABLE_INTERVAL_DAY(int32)
CAST_NULLABLE_INTERVAL_DAY(int64)

#define CAST_NULLABLE_INTERVAL_YEAR(TYPE)                                              \
  FORCE_INLINE                                                                         \
  gdv_month_interval castNULLABLEINTERVALYEAR_##TYPE(int64_t context, gdv_##TYPE in) { \
    gdv_month_interval value = static_cast<gdv_month_interval>(in);                    \
    if (value != in) {                                                                 \
      gdv_fn_context_set_error_msg(context, "Integer overflow");                       \
    }                                                                                  \
    return value;                                                                      \
  }

FORCE_INLINE
gdv_int32 datediff_timestamp_timestamp(gdv_timestamp start_millis,
                                       gdv_timestamp end_millis) {
  return static_cast<int32_t>(
      ((start_millis - end_millis) / (24 * (60 * (60 * (1000))))));
}

CAST_NULLABLE_INTERVAL_YEAR(int32)
CAST_NULLABLE_INTERVAL_YEAR(int64)

}  // extern "C"
