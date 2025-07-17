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

#include <boost/date_time/posix_time/posix_time.hpp>
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
  try {
    boost::posix_time::ptime pt =
        boost::posix_time::from_time_t(0) + boost::posix_time::seconds(value);
    date = boost::posix_time::to_tm(pt);
  } catch (const std::exception&) {
    std::memset(&date, 0, sizeof(tm));
    date.tm_year = -1900;  // Represents year 0
    date.tm_mon = 0;
    date.tm_mday = 1;
  }
}
}  // namespace odbcabstraction
}  // namespace driver
