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

#include "arrow/type.h"
#include "arrow/util/parsing.h"

namespace arrow {
class TimestampConverter {
 public:
  using value_type = TimestampType::c_type;
  virtual bool operator()(const std::shared_ptr<DataType>& type, const char* s,
                          size_t length, value_type* out) const = 0;
};

class StrptimeTimestampParser : public TimestampConverter {
 private:
  std::string format_;

 public:
  explicit StrptimeTimestampParser(const std::string& format);
  bool operator()(const std::shared_ptr<DataType>& type, const char* s, size_t length,
                  value_type* out) const override;
  static std::unique_ptr<TimestampConverter> Make(const std::string& format);
};

class ISO8601Parser : public TimestampConverter {
  public:
  explicit ISO8601Parser(const std::shared_ptr<DataType>& type)
      : unit_(internal::checked_cast<TimestampType*>(type.get())->unit()) {}

  bool operator()(const std::shared_ptr<DataType>& type, const char* s, size_t length,
                  value_type* out) const override;

 protected:
  template <class TimePoint>
  bool ConvertTimePoint(TimePoint tp, value_type* out) const;

  bool ParseYYYY_MM_DD(const char* s, arrow_vendored::date::year_month_day* out) const;

  bool ParseHH(const char* s, std::chrono::duration<value_type>* out) const;

  bool ParseHH_MM(const char* s, std::chrono::duration<value_type>* out) const;

  bool ParseHH_MM_SS(const char* s, std::chrono::duration<value_type>* out) const;

  const TimeUnit::type unit_;
};

}  // namespace arrow
