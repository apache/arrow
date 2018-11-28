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

#ifndef GANDIVA_EPOCH_TIME_POINT_H
#define GANDIVA_EPOCH_TIME_POINT_H

// TODO(wesm): IR compilation does not have any include directories set
#include "../../arrow/util/date.h"

// A point of time measured in millis since epoch.
class EpochTimePoint {
 public:
  explicit EpochTimePoint(std::chrono::milliseconds millis_since_epoch)
      : tp_(millis_since_epoch) {}

  explicit EpochTimePoint(int64_t millis_since_epoch)
      : EpochTimePoint(std::chrono::milliseconds(millis_since_epoch)) {}

  int TmYear() const { return static_cast<int>(YearMonthDay().year()) - 1900; }

  int TmMon() const { return static_cast<unsigned int>(YearMonthDay().month()) - 1; }

  int TmYday() const {
    auto to_days = date::floor<date::days>(tp_);
    auto first_day_in_year = date::sys_days{YearMonthDay().year() / date::jan / 1};
    return (to_days - first_day_in_year).count();
  }

  int TmMday() const { return static_cast<unsigned int>(YearMonthDay().day()); }

  int TmWday() const {
    auto to_days = date::floor<date::days>(tp_);
    return (date::weekday{to_days} - date::Sunday).count();  // NOLINT
  }

  int TmHour() const { return static_cast<int>(TimeOfDay().hours().count()); }

  int TmMin() const { return static_cast<int>(TimeOfDay().minutes().count()); }

  int TmSec() const {
    // TODO(wesm): UNIX y2k issue on int=int32 platforms
    return static_cast<int>(TimeOfDay().seconds().count());
  }

  EpochTimePoint AddYears(int num_years) const {
    auto ymd = YearMonthDay() + date::years(num_years);
    return EpochTimePoint((date::sys_days{ymd} +  // NOLINT
                           TimeOfDay().to_duration())
                              .time_since_epoch());
  }

  EpochTimePoint AddMonths(int num_months) const {
    auto ymd = YearMonthDay() + date::months(num_months);
    return EpochTimePoint((date::sys_days{ymd} +  // NOLINT
                           TimeOfDay().to_duration())
                              .time_since_epoch());
  }

  EpochTimePoint AddDays(int num_days) const {
    auto days_since_epoch = date::sys_days{YearMonthDay()}  // NOLINT
                            + date::days(num_days);
    return EpochTimePoint(
        (days_since_epoch + TimeOfDay().to_duration()).time_since_epoch());
  }

  EpochTimePoint ClearTimeOfDay() const {
    return EpochTimePoint((tp_ - TimeOfDay().to_duration()).time_since_epoch());
  }

  bool operator==(const EpochTimePoint& other) const { return tp_ == other.tp_; }

  int64_t MillisSinceEpoch() const { return tp_.time_since_epoch().count(); }

 private:
  date::year_month_day YearMonthDay() const {
    return date::year_month_day{date::floor<date::days>(tp_)};  // NOLINT
  }

  date::time_of_day<std::chrono::milliseconds> TimeOfDay() const {
    auto millis_since_midnight = tp_ - date::floor<date::days>(tp_);
    return date::time_of_day<std::chrono::milliseconds>(millis_since_midnight);
  }

  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp_;
};

#endif  // GANDIVA_EPOCH_TIME_POINT_H
