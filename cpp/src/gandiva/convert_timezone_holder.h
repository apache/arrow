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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "arrow/vendored/datetime/date.h"
#include "arrow/vendored/datetime/tz.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

using arrow_vendored::date::local_time;
using arrow_vendored::date::locate_zone;
using arrow_vendored::date::make_zoned;
using arrow_vendored::date::sys_time;
using arrow_vendored::date::time_zone;
using std::chrono::milliseconds;

namespace gandiva {

class OffsetZone {
  std::chrono::seconds offset_;

 public:
  explicit OffsetZone(std::chrono::seconds offset) : offset_(offset) {}

  template <class Duration>
  auto to_local(arrow_vendored::date::sys_time<Duration> tp) const {
    using namespace std::chrono;
    using LT = local_time<std::common_type_t<Duration, seconds>>;
    return LT((tp + offset_).time_since_epoch());
  }

  template <class Duration>
  auto to_sys(arrow_vendored::date::local_time<Duration> tp) const {
    using namespace std::chrono;
    using ST = sys_time<std::common_type_t<Duration, seconds>>;
    return ST((tp - offset_).time_since_epoch());
  }
};

/// Function Holder for SQL 'CONVERT_TIMEZONE'
class GANDIVA_EXPORT ConvertTimezoneHolder : public FunctionHolder {
 public:
  ~ConvertTimezoneHolder() override = default;

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<ConvertTimezoneHolder>* holder);

  static Status Make(const std::string& srcTz, const std::string& destTz,
                     std::shared_ptr<ConvertTimezoneHolder>* holder);

  /// Return the converted timestamp
  int64_t convert(const int64_t src_timestamp) {
    using namespace std::chrono;

    if (dest_timezone != nullptr && src_timezone == nullptr) {
      return dest_timezone
          ->to_local(src_offset_tz->to_sys<milliseconds>(
              local_time<milliseconds>(milliseconds(src_timestamp))))
          .time_since_epoch()
          .count();
    } else if (dest_timezone == nullptr && src_timezone != nullptr) {
      return dest_offset_tz
          ->to_local(src_timezone->to_sys<milliseconds>(
              local_time<milliseconds>(milliseconds(src_timestamp))))
          .time_since_epoch()
          .count();
    } else if (dest_timezone == nullptr && src_timezone == nullptr) {
      return dest_offset_tz
          ->to_local(src_offset_tz->to_sys<milliseconds>(
              local_time<milliseconds>(milliseconds(src_timestamp))))
          .time_since_epoch()
          .count();
    } else {
      return dest_timezone
          ->to_local(src_timezone->to_sys<milliseconds>(
              local_time<milliseconds>(milliseconds(src_timestamp))))
          .time_since_epoch()
          .count();
    }
  }

  // Tracks if the timezones given could be found in IANA Timezone DB.
  bool ok = true;

 private:
  explicit ConvertTimezoneHolder(const std::string& srcTz, const std::string& destTz) {
    auto srcTz_abbrv_offset = abbrv_tz.find(srcTz);
    auto destTz_abbrv_offset = abbrv_tz.find(destTz);

    try {
      if (srcTz_abbrv_offset != abbrv_tz.end()) {
        auto secs = convert_offset_to_seconds(srcTz_abbrv_offset->second);
        src_offset_tz = new OffsetZone(secs);
      } else {
        if (is_timezone_shift(srcTz) ||
            is_timezone_offset(const_cast<std::string&>(srcTz))) {
          auto secs = convert_offset_to_seconds(srcTz);
          src_offset_tz = new OffsetZone(secs);
        } else {
          src_timezone = locate_zone(srcTz);
        }
      }

      if (destTz_abbrv_offset != abbrv_tz.end()) {
        auto secs = convert_offset_to_seconds(destTz_abbrv_offset->second);
        dest_offset_tz = new OffsetZone(secs);
      } else {
        if (is_timezone_shift(destTz) ||
            is_timezone_offset(const_cast<std::string&>(destTz))) {
          auto secs = convert_offset_to_seconds(destTz);
          dest_offset_tz = new OffsetZone(secs);
        } else {
          dest_timezone = locate_zone(destTz);
        }
      }
    } catch (...) {
      ok = false;
    }
  }

  static bool is_timezone_offset(std::string& offset) {
    if ((offset.rfind("-") == 0 || offset.rfind("+") == 0) && offset.length() == 6) {
      return true;
    }
    return false;
  }

  std::chrono::seconds convert_offset_to_seconds(std::string string_offset) {
    int32_t prefix_offset_length = is_timezone_shift(string_offset);
    if (prefix_offset_length != 0) {
      auto abbrv_offset = abbrv_tz.find(string_offset.substr(0, prefix_offset_length));
      auto abbrv_seconds = off_set_to_seconds(abbrv_offset->second);
      auto shift_seconds = off_set_to_seconds(string_offset.substr(prefix_offset_length));
      return abbrv_seconds + shift_seconds;
    } else {
      return off_set_to_seconds(string_offset);
    }
  }

  static int32_t parse_number(std::string& string_offset, int32_t pos,
                              bool preceded_by_colon) {
    if (preceded_by_colon && string_offset[pos - 1] != ':') {
      throw "Invalid ID for ZoneOffset, colon not found when expected: " + string_offset;
    }
    char ch1 = string_offset[pos];
    char ch2 = string_offset[pos + 1];
    if (ch1 < '0' || ch1 > '9' || ch2 < '0' || ch2 > '9') {
      throw "Invalid ID for ZoneOffset, non numeric characters found: " + string_offset;
    }

    return (ch1 - 48) * 10 + (ch2 - 48);
  }

  std::chrono::seconds off_set_to_seconds(std::string string_offset) {
    // parse - +h, +hh, +hhmm, +hh:mm, +hhmmss, +hh:mm:ss
    int32_t hours, minutes, seconds;
    std::string sub_minus, sub_plus;
    switch (string_offset.length()) {
      case 2:
        sub_minus = string_offset.substr(string_offset.find("-")+1);
        if (sub_minus.length()==1){
          string_offset = "-0"+sub_minus; // fallthru
        }
        sub_plus = string_offset.substr(string_offset.find("+")+1);
        if (sub_plus.length()==1){
          string_offset = "+0"+sub_plus; // fallthru
        }
      case 3:
        hours = parse_number(string_offset, 1, false);
        minutes = 0;
        seconds = 0;
        break;
      case 5:
        hours = parse_number(string_offset, 1, false);
        minutes = parse_number(string_offset, 3, false);
        seconds = 0;
        break;
      case 6:
        hours = parse_number(string_offset, 1, false);
        minutes = parse_number(string_offset, 4, true);
        seconds = 0;
        break;
      case 7:
        hours = parse_number(string_offset, 1, false);
        minutes = parse_number(string_offset, 3, false);
        seconds = parse_number(string_offset, 5, false);
        break;
      case 9:
        hours = parse_number(string_offset, 1, false);
        minutes = parse_number(string_offset, 4, true);
        seconds = parse_number(string_offset, 7, true);
        break;
      default:
        break;
    }
    char first = string_offset.at(0);
    if (first != '+' && first != '-') {
      throw "Invalid ID for ZoneOffset, plus/minus not found when expected: " +
          string_offset;
    }
    if (first == '-') {
      return seconds_from_minutes_hours(-hours, -minutes, -seconds);
    } else {
      return seconds_from_minutes_hours(hours, minutes, seconds);
    }
  }

  void validate(int32_t hours, int32_t minutes, int32_t seconds) {
    if (hours < -18 || hours > 18) {
      throw "Zone offset hours not in valid range";
    }
    if (hours > 0) {
      if (minutes < 0 || seconds < 0) {
        throw "Zone offset minutes and seconds must be positive because hours is "
              "positive";
      }
    } else if (hours < 0) {
      if (minutes > 0 || seconds > 0) {
        throw "Zone offset minutes and seconds must be negative because hours is "
              "negative";
      }
    } else if ((minutes > 0 && seconds < 0) || (minutes < 0 && seconds > 0)) {
      throw "Zone offset minutes and seconds must have the same sign";
    }
    if (minutes < -59 || minutes > 59) {
      throw "Zone offset minutes not in valid range";
    }
    if (seconds < -59 || seconds > 59) {
      throw "Zone offset seconds not in valid range";
    }
    if (abs(hours) == 18 && (minutes | seconds) != 0) {
      throw "Zone offset not in valid range: -18:00 to +18:00";
    }
  }

  std::chrono::seconds seconds_from_minutes_hours(int32_t hours, int32_t minutes,
                                                  int32_t seconds) {
    validate(hours, minutes, seconds);
    int32_t total_seconds = hours * 3600 + minutes * 60 + seconds;
    return std::chrono::seconds(total_seconds);
  }

  int32_t is_timezone_shift(const std::string& string_offset) {
    if (string_offset.rfind("UTC") == 0 || string_offset.rfind("GMT") == 0) {
      return 3;
    } else if (string_offset.rfind("UT") == 0) {
      return 2;
    }
    return 0;
  }

  const OffsetZone* src_offset_tz = nullptr;
  const OffsetZone* dest_offset_tz = nullptr;

  const time_zone* src_timezone = nullptr;
  const time_zone* dest_timezone = nullptr;

  static std::unordered_map<std::string, std::string> abbrv_tz;
};

}  // namespace gandiva
