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

#include "arrow/util/double_conversion.h"

namespace arrow {
namespace internal {

namespace {

struct StringToFloatConverterImpl {
  StringToFloatConverterImpl()
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

static const StringToFloatConverterImpl g_string_to_float;

}  // namespace

bool StringToFloat(const char* s, size_t length, float* out) {
  int processed_length;
  float v;
  v = g_string_to_float.main_converter_.StringToFloat(s, static_cast<int>(length),
                                                      &processed_length);
  if (ARROW_PREDICT_FALSE(v == static_cast<float>(g_string_to_float.main_junk_value_))) {
    v = g_string_to_float.fallback_converter_.StringToFloat(s, static_cast<int>(length),
                                                            &processed_length);
    if (ARROW_PREDICT_FALSE(v ==
                            static_cast<float>(g_string_to_float.fallback_junk_value_))) {
      return false;
    }
  }
  *out = v;
  return true;
}

bool StringToFloat(const char* s, size_t length, double* out) {
  int processed_length;
  double v;
  v = g_string_to_float.main_converter_.StringToDouble(s, static_cast<int>(length),
                                                       &processed_length);
  if (ARROW_PREDICT_FALSE(v == g_string_to_float.main_junk_value_)) {
    v = g_string_to_float.fallback_converter_.StringToDouble(s, static_cast<int>(length),
                                                             &processed_length);
    if (ARROW_PREDICT_FALSE(v == g_string_to_float.fallback_junk_value_)) {
      return false;
    }
  }
  *out = v;
  return true;
}

// ----------------------------------------------------------------------
// strptime-like parsing

class StrptimeTimestampParser : public TimestampParser {
 public:
  explicit StrptimeTimestampParser(std::string format) : format_(std::move(format)) {}

  bool operator()(const char* s, size_t length, TimeUnit::type out_unit,
                  int64_t* out) const override {
    return ParseTimestampStrptime(s, length, format_.c_str(),
                                  /*ignore_time_in_day=*/false,
                                  /*allow_trailing_chars=*/false, out_unit, out);
  }

 private:
  std::string format_;
};

class ISO8601Parser : public TimestampParser {
 public:
  ISO8601Parser() {}

  bool operator()(const char* s, size_t length, TimeUnit::type out_unit,
                  int64_t* out) const override {
    return ParseTimestampISO8601(s, length, out_unit, out);
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
