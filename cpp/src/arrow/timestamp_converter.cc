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
#include "arrow/timestamp_converter.h"

#include <chrono>
#include <cstring>
#include <ctime>
#include <iomanip>

#include "arrow/util/make_unique.h"
#include "arrow/util/parsing.h"
namespace arrow {
StrptimeConverter::StrptimeConverter(const std::string& format) : format_(format) {}

template <class TimePoint>
bool ConvertTimePoint(const std::shared_ptr<DataType>& type, TimePoint tp,
                      TimestampType::c_type* out) {
  auto duration = tp.time_since_epoch();
  switch (internal::checked_cast<TimestampType*>(type.get())->unit()) {
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

bool StrptimeConverter::operator()(const std::shared_ptr<DataType>& type, const char* s,
                                   size_t length, value_type* out) const{
  std::tm tm;
  std::stringstream ss;
  ss << s;
  ss >> std::get_time(&tm, format_.c_str());
  if (ss.fail()) {
    return false;
  }
  std::time_t t = std::mktime(&tm);
  auto tp = std::chrono::system_clock::from_time_t(t);

  return ConvertTimePoint(type, tp, out);
}

std::unique_ptr<TimestampConverter> StrptimeConverter::Make(const std::string& format) {
  return internal::make_unique<StrptimeConverter>(format);
}
}  // namespace arrow
