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
#include "arrow/vendored/datetime.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/parsing.h"
namespace arrow {
FormattedTimestampConverter::FormattedTimestampConverter(const std::string& format) : format_(format) {}

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

bool FormattedTimestampConverter::operator()(const std::shared_ptr<DataType>& type, const char* s,
                                   size_t length, value_type* out) const{
  arrow_vendored::date::sys_time<std::chrono::seconds> time_point;
  if (std::stringstream({s, length}) >> arrow_vendored::date::parse(format_, time_point)) {
    *out = ConvertTimePoint(type, time_point);
    return true;
  }
  return false;
}

std::unique_ptr<TimestampConverter> FormattedTimestampConverter::Make(const std::string& format) {
  return internal::make_unique<FormattedTimestampConverter>(format);
}
}  // namespace arrow
