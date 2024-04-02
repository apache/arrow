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

#include <cstdint>
#include <ctime>

namespace driver {
namespace odbcabstraction {
int64_t GetTodayTimeFromEpoch() {
  tm date{};
  int64_t t = std::time(0);

  GetTimeForSecondsSinceEpoch(date, t);

  date.tm_hour = 0;
  date.tm_min = 0;
  date.tm_sec = 0;

#if defined(_WIN32)
  return _mkgmtime(&date);
#else
  return timegm(&date);
#endif
}

void GetTimeForSecondsSinceEpoch(tm& date, int64_t value) {
#if defined(_WIN32)
  gmtime_s(&date, &value);
#else
  time_t time_value = static_cast<time_t>(value);
  gmtime_r(&time_value, &date);
#endif
}
}  // namespace odbcabstraction
}  // namespace driver
