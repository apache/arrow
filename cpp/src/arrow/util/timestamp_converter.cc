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
#include "arrow/util/timestamp_converter.h"

#include <chrono>
#include <cstring>
#include <ctime>
#include <iomanip>
#include "arrow/vendored/datetime.h"
#include "arrow/util/make_unique.h"

namespace arrow {
StrptimeTimestampParser::StrptimeTimestampParser(const std::string& format) : format_(format) {}

int64_t ConvertTimePoint(const std::shared_ptr<DataType>& type, arrow_vendored::date::sys_time<std::chrono::seconds> tp) {
  auto duration = tp.time_since_epoch();
  switch (internal::checked_cast<TimestampType*>(type.get())->unit()) {
    case TimeUnit::SECOND:
      return std::chrono::duration_cast<std::chrono::seconds>(duration).count();
    case TimeUnit::MILLI:
      return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    case TimeUnit::MICRO:
      return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    case TimeUnit::NANO:
      return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
  }
  return 0;
}

bool StrptimeTimestampParser::operator()(const std::shared_ptr<DataType>& type, const char* s,
                                   size_t length, value_type* out) const{
  arrow_vendored::date::sys_time<std::chrono::seconds> time_point;
  if (std::stringstream({s, length}) >> arrow_vendored::date::parse(format_, time_point)) {
    *out = ConvertTimePoint(type, time_point);
    return true;
  }
  return false;
}

std::unique_ptr<TimestampConverter> StrptimeTimestampParser::Make(const std::string& format) {
  return internal::make_unique<StrptimeTimestampParser>(format);
}

bool ISO8601Parser::operator()(const std::shared_ptr<DataType>& type, const char* s, size_t length,
                               value_type* out) const {
    // We allow the following formats:
    // - "YYYY-MM-DD"
    // - "YYYY-MM-DD[ T]hh"
    // - "YYYY-MM-DD[ T]hhZ"
    // - "YYYY-MM-DD[ T]hh:mm"
    // - "YYYY-MM-DD[ T]hh:mmZ"
    // - "YYYY-MM-DD[ T]hh:mm:ss"
    // - "YYYY-MM-DD[ T]hh:mm:ssZ"
    // UTC is always assumed, and the DataType's timezone is ignored.
    arrow_vendored::date::year_month_day ymd;
    if (ARROW_PREDICT_FALSE(length < 10)) {
      return false;
    }
    if (length == 10) {
      if (ARROW_PREDICT_FALSE(!ParseYYYY_MM_DD(s, &ymd))) {
        return false;
      }
      return ConvertTimePoint(arrow_vendored::date::sys_days(ymd), out);
    }
    if (ARROW_PREDICT_FALSE(s[10] != ' ') && ARROW_PREDICT_FALSE(s[10] != 'T')) {
      return false;
    }
    if (s[length - 1] == 'Z') {
      --length;
    }
    if (length == 13) {
      if (ARROW_PREDICT_FALSE(!ParseYYYY_MM_DD(s, &ymd))) {
        return false;
      }
      std::chrono::duration<value_type> seconds;
      if (ARROW_PREDICT_FALSE(!ParseHH(s + 11, &seconds))) {
        return false;
      }
      return ConvertTimePoint(arrow_vendored::date::sys_days(ymd) + seconds, out);
    }
    if (length == 16) {
      if (ARROW_PREDICT_FALSE(!ParseYYYY_MM_DD(s, &ymd))) {
        return false;
      }
      std::chrono::duration<value_type> seconds;
      if (ARROW_PREDICT_FALSE(!ParseHH_MM(s + 11, &seconds))) {
        return false;
      }
      return ConvertTimePoint(arrow_vendored::date::sys_days(ymd) + seconds, out);
    }
    if (length == 19) {
      if (ARROW_PREDICT_FALSE(!ParseYYYY_MM_DD(s, &ymd))) {
        return false;
      }
      std::chrono::duration<value_type> seconds;
      if (ARROW_PREDICT_FALSE(!ParseHH_MM_SS(s + 11, &seconds))) {
        return false;
      }
      return ConvertTimePoint(arrow_vendored::date::sys_days(ymd) + seconds, out);
    }
    return false;
  }

  template <class TimePoint>
  bool ISO8601Parser::ConvertTimePoint(TimePoint tp, value_type* out) const {
    auto duration = tp.time_since_epoch();
    switch (unit_) {
      case TimeUnit::SECOND:
        *out = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
        return true;
      case TimeUnit::MILLI:
        *out = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        return true;
      case TimeUnit::MICRO:
        *out = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
        return true;
      case TimeUnit::NANO:
        *out = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        return true;
    }
    // Unreachable, but suppress compiler warning
    assert(0);
    *out = 0;
    return true;
  }

  bool ISO8601Parser::ParseYYYY_MM_DD(const char* s, arrow_vendored::date::year_month_day* out) const {
    uint16_t year;
    uint8_t month, day;
    if (ARROW_PREDICT_FALSE(s[4] != '-') || ARROW_PREDICT_FALSE(s[7] != '-')) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!internal::detail::ParseUnsigned(s + 0, 4, &year))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!internal::detail::ParseUnsigned(s + 5, 2, &month))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!internal::detail::ParseUnsigned(s + 8, 2, &day))) {
      return false;
    }
    *out = {arrow_vendored::date::year{year}, arrow_vendored::date::month{month},
            arrow_vendored::date::day{day}};
    return out->ok();
  }

  bool ISO8601Parser::ParseHH(const char* s, std::chrono::duration<value_type>* out) const {
    uint8_t hours;
    if (ARROW_PREDICT_FALSE(!internal::detail::ParseUnsigned(s + 0, 2, &hours))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(hours >= 24)) {
      return false;
    }
    *out = std::chrono::duration<value_type>(3600U * hours);
    return true;
  }

  bool ISO8601Parser::ParseHH_MM(const char* s, std::chrono::duration<value_type>* out) const {
    uint8_t hours, minutes;
    if (ARROW_PREDICT_FALSE(s[2] != ':')) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!internal::detail::ParseUnsigned(s + 0, 2, &hours))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!internal::detail::ParseUnsigned(s + 3, 2, &minutes))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(hours >= 24)) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(minutes >= 60)) {
      return false;
    }
    *out = std::chrono::duration<value_type>(3600U * hours + 60U * minutes);
    return true;
  }

  bool ISO8601Parser::ParseHH_MM_SS(const char* s, std::chrono::duration<value_type>* out) const {
    uint8_t hours, minutes, seconds;
    if (ARROW_PREDICT_FALSE(s[2] != ':') || ARROW_PREDICT_FALSE(s[5] != ':')) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!internal::detail::ParseUnsigned(s + 0, 2, &hours))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!internal::detail::ParseUnsigned(s + 3, 2, &minutes))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!internal::detail::ParseUnsigned(s + 6, 2, &seconds))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(hours >= 24)) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(minutes >= 60)) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(seconds >= 60)) {
      return false;
    }
    *out = std::chrono::duration<value_type>(3600U * hours + 60U * minutes + seconds);
    return true;
  }
}  // namespace arrow
