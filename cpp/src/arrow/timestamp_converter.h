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

namespace arrow {
class TimestampConverter {
 public:
  using value_type = TimestampType::c_type;
  virtual bool operator()(const std::shared_ptr<DataType>& type, const char* s,
                          size_t length, value_type* out) const = 0;
};

class FormattedTimestampConverter : public TimestampConverter {
 private:
  std::string format_;

 public:
  explicit FormattedTimestampConverter(const std::string& format);
  bool operator()(const std::shared_ptr<DataType>& type, const char* s, size_t length,
                  value_type* out) const override;
  static std::unique_ptr<TimestampConverter> Make(const std::string& format);
};
}  // namespace arrow
