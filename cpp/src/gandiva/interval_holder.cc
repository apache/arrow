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

#include "gandiva/interval_holder.h"

#include <algorithm>

#include "gandiva/node.h"
#include "gandiva/regex_util.h"

namespace gandiva {

// pre-compiled pattern for matching period that only have numbers
static const RE2 period_only_contains_numbers(R"(\d+)");

// pre-compiled pattern for matching periods in 8601 formats that not contains weeks.
static const RE2 iso8601_complete_period(
    R"(P(-?[[:digit:]]+Y|-?[[:digit:]]+[,.][[:digit:]]+Y)?)"
    R"((-?[[:digit:]]+M|-?[[:digit:]]+[,.][[:digit:]]+M)?)"
    R"((-?[[:digit:]]+D|-?[[:digit:]]+[,.][[:digit:]]+D)?)"
    R"(T(-?[[:digit:]]+H|-?[[:digit:]]+[,.][[:digit:]]+H)?)"
    R"((-?[[:digit:]]+M|-?[[:digit:]]+[,.][[:digit:]]+M)?)"
    R"((-?[[:digit:]]+S|-?[[:digit:]]+[,.][[:digit:]]+S)?)");

// pre-compiled pattern for matching periods in 8601 formats that not contain time
// (hours, minutes and seconds) information.
static const RE2 iso8601_period_without_time(
    R"(P(-?[[:digit:]]+Y|-?[[:digit:]]+[,.][[:digit:]]+Y)?)"
    R"((-?[[:digit:]]+M|-?[[:digit:]]+[,.][[:digit:]]+M)?)"
    R"((-?[[:digit:]]+D|-?[[:digit:]]+[,.][[:digit:]]+D)?)");

// pre-compiled pattern for matching periods in 8601 formats that not contain time
// (hours, minutes and seconds) information.
static const std::regex period_not_contains_time(R"(^((?!T).)*$)");

// pre-compiled pattern for matching periods in 8601 formats that contains weeks inside
// them. The ISO8601 specification defines that if the string contains a week, it can not
// have other time granularities information, like day, years and months.
static const RE2 iso8601_period_with_weeks(
    R"(P(-?[[:digit:]]+W|-?[[:digit:]]+[,.][[:digit:]]+W){1})");

// It considers that a day has exactly 24 hours of duration
static const int64_t kMillisInDay = 86400000;
static const int64_t kMillisInAWeek = kMillisInDay * 7;
static const int64_t kMillisInAHour = 3600000;
static const int64_t kMillisInAMinute = 60000;
static const int64_t kMillisInASecond = 1000;

static void return_error_with_cause(ExecutionContext* context, std::string& data,
                                    int32_t supression_error) {
  if (supression_error != 0) {
    return;
  }

  context->set_error_msg(data.c_str());
}

int64_t IntervalDaysHolder::GetIntervalDayFromMillis(ExecutionContext* context,
                                                     std::string& number_as_string,
                                                     int32_t suppress_errors,
                                                     bool* out_valid) {
  int64_t period_in_millis = 0;
  try {
    period_in_millis = std::stol(number_as_string);
  } catch (...) {
    std::string cause("Error converting the number of millis");
    return_error_with_cause(context, cause, suppress_errors);
    *out_valid = false;
    return 0;
  }

  int64_t qty_days = period_in_millis / kMillisInDay;
  int64_t qty_millis = period_in_millis % kMillisInDay;

  // The response is a 64-bit integer where the lower half of the bytes represents the
  // number of the days and the other half represents the number of milliseconds.
  int64_t out = (qty_days & 0x00000000FFFFFFFF);
  out |= ((qty_millis << 32) & 0xFFFFFFFF00000000);

  *out_valid = true;
  return out;
}

int64_t IntervalDaysHolder::GetIntervalDayFromWeeks(ExecutionContext* context,
                                                    std::string& number_as_string,
                                                    int32_t suppress_errors,
                                                    bool* out_valid) {
  double qty_weeks = 0;

  try {
    qty_weeks = std::stod(number_as_string);
  } catch (...) {
    // Error converting double value
    std::string cause("Error converting the number of weeks");
    return_error_with_cause(context, cause, suppress_errors);
    *out_valid = false;
    return 0;
  }

  auto millis_in_all_weeks = static_cast<int64_t>(qty_weeks * kMillisInAWeek);

  int64_t qty_days = millis_in_all_weeks / kMillisInDay;
  int64_t qty_millis = millis_in_all_weeks % kMillisInDay;

  // The response is a 64-bit integer where the lower half of the bytes represents the
  // number of the days and the other half represents the number of milliseconds.
  int64_t out = (qty_days & 0x00000000FFFFFFFF);
  out |= ((qty_millis << 32) & 0xFFFFFFFF00000000);

  *out_valid = true;
  return out;
}

int64_t IntervalDaysHolder::GetIntervalDayFromCompletePeriod(
    ExecutionContext* context, std::string& days_in_period, std::string& hours_in_period,
    std::string& minutes_in_period, std::string& seconds_in_period,
    int32_t suppress_errors, bool* out_valid) {
  double qty_days = 0;
  double qty_hours = 0;
  double qty_minutes = 0;
  double qty_seconds = 0;

  if (!days_in_period.empty()) {
    try {
      std::replace(days_in_period.begin(), days_in_period.end(), ',', '.');
      qty_days = std::stod(days_in_period);
    } catch (...) {
      std::string cause("Invalid number of days");
      return_error_with_cause(context, cause, suppress_errors);
      *out_valid = false;
      return 0;
    }
  }

  if (!hours_in_period.empty()) {
    try {
      std::replace(hours_in_period.begin(), hours_in_period.end(), ',', '.');
      qty_hours = std::stod(hours_in_period);
    } catch (...) {
      std::string cause("Invalid number of hours");
      return_error_with_cause(context, cause, suppress_errors);
      *out_valid = false;
      return 0;
    }
  }

  if (!minutes_in_period.empty()) {
    try {
      std::replace(minutes_in_period.begin(), minutes_in_period.end(), ',', '.');
      qty_minutes = std::stod(minutes_in_period);
    } catch (...) {
      std::string cause("Invalid number of minutes");
      return_error_with_cause(context, cause, suppress_errors);
      *out_valid = false;
      return 0;
    }
  }

  if (!seconds_in_period.empty()) {
    try {
      std::replace(seconds_in_period.begin(), seconds_in_period.end(), ',', '.');
      qty_seconds = std::stod(seconds_in_period);
    } catch (...) {
      std::string cause("Invalid number of seconds");
      return_error_with_cause(context, cause, suppress_errors);
      *out_valid = false;
      return 0;
    }
  }

  auto millis_in_the_period =
      static_cast<int64_t>(qty_hours * kMillisInAHour +      // millis in a hour
                           qty_minutes * kMillisInAMinute +  // millis in a minute
                           qty_seconds * kMillisInASecond);

  int64_t total_days_in_millis = millis_in_the_period / kMillisInDay;
  auto total_days = static_cast<int64_t>(qty_days + total_days_in_millis);
  int64_t remainder_millis = millis_in_the_period % kMillisInDay;

  // The response is a 64-bit integer where the lower half of the bytes represents the
  // number of the days and the other half represents the number of milliseconds.
  int64_t out = (total_days & 0x00000000FFFFFFFF);
  out |= ((remainder_millis << 32) & 0xFFFFFFFF00000000);

  *out_valid = true;
  return out;
}

// The operator will cast a generic string defined by the user into an interval of days.
// There are two formats of strings that are acceptable:
//   - The period in millis: '238398430'
//   - The period using a ISO8601 compatible format: 'P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W'
int64_t IntervalDaysHolder::operator()(ExecutionContext* ctx, const char* data,
                                       int32_t data_len, bool in_valid, bool* out_valid) {
  *out_valid = false;

  if (!in_valid) {
    return 0;
  }

  if (data_len <= 0) {
    std::string empty;
    return_error(ctx, empty);
    return 0;
  }

  std::string data_as_str(data, data_len);

  if (RE2::FullMatch(data_as_str, period_only_contains_numbers)) {
    return GetIntervalDayFromMillis(ctx, data_as_str, suppress_errors_, out_valid);
  }

  std::string period_in_weeks;
  if (RE2::FullMatch(data_as_str, iso8601_period_with_weeks, &period_in_weeks)) {
    return GetIntervalDayFromWeeks(ctx, period_in_weeks, suppress_errors_, out_valid);
  }

  std::string days_in_period;
  std::string hours_in_period;
  std::string minutes_in_period;
  std::string seconds_in_period;
  std::string ignored_string;  // string to store unnecessary captured groups
  if (std::regex_match(data_as_str, period_not_contains_time)) {
    if (RE2::FullMatch(data_as_str, iso8601_period_without_time, &ignored_string,
                       &ignored_string, &days_in_period)) {
      return GetIntervalDayFromCompletePeriod(ctx, days_in_period, hours_in_period,
                                              minutes_in_period, seconds_in_period,
                                              suppress_errors_, out_valid);
    }

    return_error(ctx, data_as_str);
    return 0;
  }

  if (RE2::FullMatch(data_as_str, iso8601_complete_period, &ignored_string,
                     &ignored_string, &days_in_period, &hours_in_period,
                     &minutes_in_period, &seconds_in_period)) {
    return GetIntervalDayFromCompletePeriod(ctx, days_in_period, hours_in_period,
                                            minutes_in_period, seconds_in_period,
                                            suppress_errors_, out_valid);
  }

  return_error(ctx, data_as_str);
  return 0;
}

Status IntervalDaysHolder::Make(const FunctionNode& node,
                                std::shared_ptr<IntervalDaysHolder>* holder) {
  const std::string function_name("castINTERVALDAY");
  return IntervalHolder<IntervalDaysHolder>::Make(node, holder, function_name);
}

Status IntervalDaysHolder::Make(int32_t suppress_errors,
                                std::shared_ptr<IntervalDaysHolder>* holder) {
  return IntervalHolder<IntervalDaysHolder>::Make(suppress_errors, holder);
}

Status IntervalYearsHolder::Make(const FunctionNode& node,
                                 std::shared_ptr<IntervalYearsHolder>* holder) {
  const std::string function_name("castINTERVALYEAR");
  return IntervalHolder<IntervalYearsHolder>::Make(node, holder, function_name);
}

Status IntervalYearsHolder::Make(int32_t suppress_errors,
                                 std::shared_ptr<IntervalYearsHolder>* holder) {
  return IntervalHolder<IntervalYearsHolder>::Make(suppress_errors, holder);
}

// The operator will cast a generic string defined by the user into an interval of months.
// There are two formats of strings that are acceptable:
//   - The period in months: '238398430'
//   - The period using a ISO8601 compatible format: 'P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W'
int32_t IntervalYearsHolder::operator()(ExecutionContext* ctx, const char* data,
                                        int32_t data_len, bool in_valid,
                                        bool* out_valid) {
  *out_valid = false;

  if (!in_valid) {
    return 0;
  }

  if (data_len <= 0) {
    std::string empty;
    return_error(ctx, empty);
    return 0;
  }

  std::string data_as_str(data, data_len);

  if (RE2::FullMatch(data_as_str, period_only_contains_numbers)) {
    return GetIntervalYearFromNumber(ctx, data_as_str, suppress_errors_, out_valid);
  }

  std::string period_in_weeks;
  if (RE2::FullMatch(data_as_str, iso8601_period_with_weeks, &period_in_weeks)) {
    *out_valid = true;
    return 0;
  }

  std::string yrs_in_period;
  std::string months_in_period;
  std::string ignored_string;  // string to store unnecessary captured variables
  if (std::regex_match(data_as_str, period_not_contains_time)) {
    if (RE2::FullMatch(data_as_str, iso8601_period_without_time, &yrs_in_period,
                       &months_in_period, &ignored_string)) {
      return GetIntervalYearFromCompletePeriod(ctx, yrs_in_period, months_in_period,
                                               suppress_errors_, out_valid);
    }

    return_error(ctx, data_as_str);
    return 0;
  }

  if (RE2::FullMatch(data_as_str, iso8601_complete_period, &yrs_in_period,
                     &months_in_period, &ignored_string, &ignored_string, &ignored_string,
                     &ignored_string)) {
    return GetIntervalYearFromCompletePeriod(ctx, yrs_in_period, months_in_period,
                                             suppress_errors_, out_valid);
  }

  return_error(ctx, data_as_str);
  return 0;
}

int32_t IntervalYearsHolder::GetIntervalYearFromNumber(ExecutionContext* context,
                                                       std::string& number_as_string,
                                                       int32_t suppress_errors,
                                                       bool* out_valid) {
  int32_t number_of_months = 0;

  try {
    number_of_months = std::stoi(number_as_string);
  } catch (...) {
    // Error converting the value
    std::string error_cause("Error converting number to month");
    return_error_with_cause(context, error_cause, suppress_errors);
    *out_valid = false;
    return 0;
  }

  *out_valid = true;
  return number_of_months;
}

int32_t IntervalYearsHolder::GetIntervalYearFromCompletePeriod(
    ExecutionContext* context, std::string& yrs_in_period, std::string& months_in_period,
    int32_t suppress_errors, bool* out_valid) {
  float qty_yrs = 0;
  float qty_months = 0;

  if (!yrs_in_period.empty()) {
    std::replace(yrs_in_period.begin(), yrs_in_period.end(), ',', '.');

    try {
      qty_yrs = std::stof(yrs_in_period);
    } catch (...) {
      // Error converting the float value
      std::string cause("Error converting the number of months");
      return_error_with_cause(context, cause, suppress_errors);
      *out_valid = false;
      return 0;
    }
  }

  if (!months_in_period.empty()) {
    std::replace(months_in_period.begin(), months_in_period.end(), ',', '.');

    try {
      qty_months = std::stof(months_in_period);
    } catch (...) {
      // Error converting the float value
      *out_valid = false;
      return 0;
    }
  }

  auto total_months = static_cast<int32_t>(qty_yrs * 12 +  // qty months in an year
                                           qty_months);

  *out_valid = true;
  return total_months;
}
}  // namespace gandiva
