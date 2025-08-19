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

#include "odbcabstraction/calendar_utils.h"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>

namespace driver {
namespace odbcabstraction {
int64_t GetTodayTimeFromEpoch() {
  tm date{};
  int64_t t = std::time(0);

  GetTimeForSecondsSinceEpoch(t, date);

  date.tm_hour = 0;
  date.tm_min = 0;
  date.tm_sec = 0;

#if defined(_WIN32)
  return _mkgmtime(&date);
#else
  return timegm(&date);
#endif
}

void GetTimeForSecondsSinceEpoch(const int64_t seconds_since_epoch, std::tm& out_tm) {
  std::memset(&out_tm, 0, sizeof(std::tm));

  std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> timepoint{
      std::chrono::seconds{seconds_since_epoch}};
  auto tm_days = std::chrono::floor<std::chrono::days>(timepoint);

  std::chrono::year_month_day ymd(tm_days);
  std::chrono::hh_mm_ss<std::chrono::seconds> timeofday(timepoint - tm_days);

  out_tm.tm_year = static_cast<int>(ymd.year()) - 1900;
  out_tm.tm_mon = static_cast<unsigned>(ymd.month()) - 1;
  out_tm.tm_mday = static_cast<unsigned>(ymd.day());
  out_tm.tm_hour = static_cast<int>(timeofday.hours().count());
  out_tm.tm_min = static_cast<int>(timeofday.minutes().count());
  out_tm.tm_sec = static_cast<int>(timeofday.seconds().count());
}
}  // namespace odbcabstraction
}  // namespace driver
