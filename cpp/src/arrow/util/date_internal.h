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

namespace date = arrow_vendored::date;

// OffsetZone object is inspired by an example from date.h documentation:
// https://howardhinnant.github.io/date/tz.html#Examples

class OffsetZone {
  std::chrono::minutes offset_;

 public:
  explicit OffsetZone(std::chrono::minutes offset) : offset_{offset} {}

  template <class Duration>
  date::local_time<Duration> to_local(date::sys_time<Duration> tp) const {
    return date::local_time<Duration>{(tp + offset_).time_since_epoch()};
  }

  template <class Duration>
  date::sys_time<Duration> to_sys(
      date::local_time<Duration> tp,
      [[maybe_unused]] date::choose = date::choose::earliest) const {
    return date::sys_time<Duration>{(tp - offset_).time_since_epoch()};
  }

  template <class Duration>
  date::sys_info get_info(date::sys_time<Duration> st) const {
    return {date::sys_seconds::min(), date::sys_seconds::max(), offset_,
            std::chrono::minutes(0),
            offset_ >= std::chrono::minutes(0) ? "+" + date::format("%H%M", offset_)
                                               : "-" + date::format("%H%M", -offset_)};
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
    throw std::runtime_error{"OffsetZone can't parse " + name};
  }
};
}  // namespace arrow_vendored::date
