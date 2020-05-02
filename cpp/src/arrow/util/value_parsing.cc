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

#include <chrono>
#include <istream>
#include <streambuf>
#include <string>
#include <utility>

#include "arrow/util/double_conversion.h"

namespace arrow {
namespace internal {

struct StringToFloatConverter::Impl {
  Impl()
      : main_converter_(flags_, main_junk_value_, main_junk_value_, "inf", "nan"),
        fallback_converter_(flags_, fallback_junk_value_, fallback_junk_value_, "inf",
                            "nan") {}

  // NOTE: This is only supported in double-conversion 3.1+
  static constexpr int flags_ =
      util::double_conversion::StringToDoubleConverter::ALLOW_CASE_INSENSIBILITY;

  // Two unlikely values to signal a parsing error
  static constexpr double main_junk_value_ = 0.7066424364107089;
  static constexpr double fallback_junk_value_ = 0.40088499148279166;

  util::double_conversion::StringToDoubleConverter main_converter_;
  util::double_conversion::StringToDoubleConverter fallback_converter_;
};

constexpr int StringToFloatConverter::Impl::flags_;
constexpr double StringToFloatConverter::Impl::main_junk_value_;
constexpr double StringToFloatConverter::Impl::fallback_junk_value_;

StringToFloatConverter::StringToFloatConverter() : impl_(new Impl()) {}

StringToFloatConverter::~StringToFloatConverter() {}

bool StringToFloatConverter::StringToFloat(const char* s, size_t length, float* out) {
  int processed_length;
  float v;
  v = impl_->main_converter_.StringToFloat(s, static_cast<int>(length),
                                           &processed_length);
  if (ARROW_PREDICT_FALSE(v == static_cast<float>(impl_->main_junk_value_))) {
    v = impl_->fallback_converter_.StringToFloat(s, static_cast<int>(length),
                                                 &processed_length);
    if (ARROW_PREDICT_FALSE(v == static_cast<float>(impl_->fallback_junk_value_))) {
      return false;
    }
  }
  *out = v;
  return true;
}

bool StringToFloatConverter::StringToFloat(const char* s, size_t length, double* out) {
  int processed_length;
  double v;
  v = impl_->main_converter_.StringToDouble(s, static_cast<int>(length),
                                            &processed_length);
  if (ARROW_PREDICT_FALSE(v == impl_->main_junk_value_)) {
    v = impl_->fallback_converter_.StringToDouble(s, static_cast<int>(length),
                                                  &processed_length);
    if (ARROW_PREDICT_FALSE(v == impl_->fallback_junk_value_)) {
      return false;
    }
  }
  *out = v;
  return true;
}

// ----------------------------------------------------------------------
// strptime-like parsing

// To avoid copying data into a std::istringstream
struct StreambufView : std::streambuf {
  StreambufView(const char* s, size_t length) {
    char* start = const_cast<char*>(s);
    this->setg(start, start, start + length);
  }
};

class StrptimeTimestampParser : public TimestampParser {
 public:
  explicit StrptimeTimestampParser(std::string format) : format_(std::move(format)) {}

  bool operator()(const char* s, size_t length, TimeUnit::type out_unit,
                  value_type* out) const override {
    arrow_vendored::date::sys_time<std::chrono::seconds> time_point;
    StreambufView view(s, length);
    if (std::istream(&view) >> arrow_vendored::date::parse(format_, time_point)) {
      *out = detail::ConvertTimePoint(time_point, out_unit);
      return true;
    }
    return false;
  }

 private:
  std::string format_;
};

class ISO8601Parser : public TimestampParser {
 public:
  ISO8601Parser() {}

  bool operator()(const char* s, size_t length, TimeUnit::type out_unit,
                  value_type* out) const override {
    return ParseISO8601(s, length, out_unit, out);
  }
};

}  // namespace internal

std::shared_ptr<TimestampParser> TimestampParser::MakeStrptime(std::string format) {
  return std::make_shared<internal::StrptimeTimestampParser>(std::move(format));
}

std::shared_ptr<TimestampParser> TimestampParser::MakeISO8601() {
  return std::make_shared<internal::ISO8601Parser>();
}

}  // namespace arrow
