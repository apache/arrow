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

#include "arrow/util/value_parsing.h"

#include <string>
#include <utility>

#include "arrow/vendored/fast_float/fast_float.h"

namespace arrow {
namespace internal {

bool StringToFloat(const char* s, size_t length, char decimal_point, float* out) {
  ::arrow_vendored::fast_float::parse_options options{
      ::arrow_vendored::fast_float::chars_format::general, decimal_point};
  const auto res =
      ::arrow_vendored::fast_float::from_chars_advanced(s, s + length, *out, options);
  return res.ec == std::errc() && res.ptr == s + length;
}

bool StringToFloat(const char* s, size_t length, char decimal_point, double* out) {
  ::arrow_vendored::fast_float::parse_options options{
      ::arrow_vendored::fast_float::chars_format::general, decimal_point};
  const auto res =
      ::arrow_vendored::fast_float::from_chars_advanced(s, s + length, *out, options);
  return res.ec == std::errc() && res.ptr == s + length;
}

// ----------------------------------------------------------------------
// strptime-like parsing

namespace {

class StrptimeTimestampParser : public TimestampParser {
 public:
  explicit StrptimeTimestampParser(std::string format)
      : format_(std::move(format)), have_zone_offset_(false) {
    // Check for use of %z
    size_t cur = 0;
    while (cur < format_.size()) {
      if (format_[cur] == '%') {
        if (cur + 1 < format_.size() && format_[cur + 1] == 'z') {
          have_zone_offset_ = true;
          break;
        }
        cur++;
      }
      cur++;
    }
  }

  bool operator()(const char* s, size_t length, TimeUnit::type out_unit, int64_t* out,
                  bool* out_zone_offset_present = NULLPTR) const override {
    if (out_zone_offset_present) {
      *out_zone_offset_present = have_zone_offset_;
    }
    return ParseTimestampStrptime(s, length, format_.c_str(),
                                  /*ignore_time_in_day=*/false,
                                  /*allow_trailing_chars=*/false, out_unit, out);
  }

  const char* kind() const override { return "strptime"; }

  const char* format() const override { return format_.c_str(); }

 private:
  std::string format_;
  bool have_zone_offset_;
};

class ISO8601Parser : public TimestampParser {
 public:
  ISO8601Parser() {}

  bool operator()(const char* s, size_t length, TimeUnit::type out_unit, int64_t* out,
                  bool* out_zone_offset_present = NULLPTR) const override {
    return ParseTimestampISO8601(s, length, out_unit, out, out_zone_offset_present);
  }

  const char* kind() const override { return "iso8601"; }
};

}  // namespace
}  // namespace internal

const char* TimestampParser::format() const { return ""; }

std::shared_ptr<TimestampParser> TimestampParser::MakeStrptime(std::string format) {
  return std::make_shared<internal::StrptimeTimestampParser>(std::move(format));
}

std::shared_ptr<TimestampParser> TimestampParser::MakeISO8601() {
  return std::make_shared<internal::ISO8601Parser>();
}

}  // namespace arrow
