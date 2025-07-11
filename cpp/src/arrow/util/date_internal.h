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

#include "arrow/vendored/datetime.h"

namespace arrow::internal {

using arrow_vendored::date::choose;
using arrow_vendored::date::days;
using arrow_vendored::date::floor;
using arrow_vendored::date::format;
using arrow_vendored::date::local_days;
using arrow_vendored::date::local_time;
using arrow_vendored::date::locate_zone;
using arrow_vendored::date::sys_days;
using arrow_vendored::date::sys_info;
using arrow_vendored::date::sys_seconds;
using arrow_vendored::date::sys_time;
using arrow_vendored::date::time_zone;
using arrow_vendored::date::year_month_day;
using arrow_vendored::date::zoned_time;
using arrow_vendored::date::zoned_traits;
using std::chrono::minutes;

class OffsetZone {
  std::chrono::minutes offset_;

 public:
  explicit OffsetZone(std::chrono::minutes offset) : offset_{offset} {}

  template <class Duration>
  local_time<Duration> to_local(sys_time<Duration> tp) const {
    return local_time<Duration>{(tp + offset_).time_since_epoch()};
  }

  template <class Duration>
  sys_time<Duration> to_sys(local_time<Duration> tp, choose = choose::earliest) const {
    return sys_time<Duration>{(tp - offset_).time_since_epoch()};
  }

  template <class Duration>
  sys_info get_info(sys_time<Duration> st) const {
    return {sys_seconds::min(), sys_seconds::max(), offset_, minutes(0),
            offset_ >= minutes(0) ? "+" + format("%H%M", offset_)
                                  : "-" + format("%H%M", -offset_)};
  }

  const OffsetZone* operator->() const { return this; }
};

}  // namespace arrow::internal

namespace arrow_vendored::date {
using arrow::internal::OffsetZone;

template <>
struct zoned_traits<OffsetZone> {
  static OffsetZone default_zone() { return OffsetZone{std::chrono::minutes{0}}; }

  static OffsetZone locate_zone(const std::string& name) {
    if (name == "UTC") return OffsetZone{std::chrono::minutes{0}};
    throw std::runtime_error{"OffsetZone can't parse " + name};
  }
};
}  // namespace arrow_vendored::date
