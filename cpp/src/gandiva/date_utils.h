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

#ifndef TO_DATE_HELPER_H
#define TO_DATE_HELPER_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#if defined(_MSC_VER)
#include <ctime>
#include <iomanip>
#include <sstream>
#endif

#include "arrow/util/macros.h"
#include "arrow/vendored/datetime.h"

#include "gandiva/arrow.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// \brief Utility class for converting sql date patterns to internal date patterns.
class GANDIVA_EXPORT DateUtils {
 public:
  static Status ToInternalFormat(const std::string& format,
                                 std::shared_ptr<std::string>* internal_format);

 private:
  using date_format_converter = std::unordered_map<std::string, std::string>;

  static date_format_converter sql_date_format_to_boost_map_;

  static date_format_converter InitMap();

  static std::vector<std::string> GetMatches(std::string pattern, bool exactMatch);

  static std::vector<std::string> GetPotentialMatches(const std::string& pattern);

  static std::vector<std::string> GetExactMatches(const std::string& pattern);
};

namespace internal {

/// \brief Returns seconds since the UNIX epoch
static inline bool ParseTimestamp(const char* buf, const char* format,
                                  bool ignoreTimeInDay, int64_t* out) {
#if defined(_MSC_VER)
  static std::locale lc_all(setlocale(LC_ALL, NULLPTR));
  std::istringstream stream(buf);
  stream.imbue(lc_all);

  // TODO: date::parse fails parsing when the hour value is 0.
  // eg.1886-12-01 00:00:00
  arrow::util::date::sys_seconds seconds;
  if (ignoreTimeInDay) {
    arrow::util::date::sys_days days;
    stream >> arrow::util::date::parse(format, days);
    if (stream.fail()) {
      return false;
    }
    seconds = days;
  } else {
    stream >> arrow::util::date::parse(format, seconds);
    if (stream.fail()) {
      return false;
    }
  }
  auto seconds_in_epoch = seconds.time_since_epoch().count();
  *out = seconds_in_epoch;
  return true;
#else
  struct tm result;
  char* ret = strptime(buf, format, &result);
  if (ret == NULLPTR) {
    return false;
  }
  // ignore the time part
  arrow::util::date::sys_seconds secs =
      arrow::util::date::sys_days(arrow::util::date::year(result.tm_year + 1900) /
                                  (result.tm_mon + 1) / result.tm_mday);
  if (!ignoreTimeInDay) {
    secs += (std::chrono::hours(result.tm_hour) + std::chrono::minutes(result.tm_min) +
             std::chrono::seconds(result.tm_sec));
  }
  *out = secs.time_since_epoch().count();
  return true;
#endif
}

}  // namespace internal
}  // namespace gandiva

#endif  // TO_DATE_HELPER_H
