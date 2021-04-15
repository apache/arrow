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

#include "arrow/util/formatting.h"
#include "arrow/util/config.h"
#include "arrow/util/double_conversion.h"
#include "arrow/util/logging.h"

namespace arrow {

using util::double_conversion::DoubleToStringConverter;

static constexpr int kMinBufferSize = DoubleToStringConverter::kBase10MaximalLength + 1;

namespace internal {
namespace detail {

const char digit_pairs[] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

}  // namespace detail

struct FloatToStringFormatter::Impl {
  Impl()
      : converter_(DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN, "inf", "nan",
                   'e', -6, 10, 6, 0) {}

  Impl(int flags, const char* inf_symbol, const char* nan_symbol, char exp_character,
       int decimal_in_shortest_low, int decimal_in_shortest_high,
       int max_leading_padding_zeroes_in_precision_mode,
       int max_trailing_padding_zeroes_in_precision_mode)
      : converter_(flags, inf_symbol, nan_symbol, exp_character, decimal_in_shortest_low,
                   decimal_in_shortest_high, max_leading_padding_zeroes_in_precision_mode,
                   max_trailing_padding_zeroes_in_precision_mode) {}

  DoubleToStringConverter converter_;
};

FloatToStringFormatter::FloatToStringFormatter() : impl_(new Impl()) {}

FloatToStringFormatter::FloatToStringFormatter(
    int flags, const char* inf_symbol, const char* nan_symbol, char exp_character,
    int decimal_in_shortest_low, int decimal_in_shortest_high,
    int max_leading_padding_zeroes_in_precision_mode,
    int max_trailing_padding_zeroes_in_precision_mode)
    : impl_(new Impl(flags, inf_symbol, nan_symbol, exp_character,
                     decimal_in_shortest_low, decimal_in_shortest_high,
                     max_leading_padding_zeroes_in_precision_mode,
                     max_trailing_padding_zeroes_in_precision_mode)) {}

FloatToStringFormatter::~FloatToStringFormatter() {}

int FloatToStringFormatter::FormatFloat(float v, char* out_buffer, int out_size) {
  DCHECK_GE(out_size, kMinBufferSize);
  // StringBuilder checks bounds in debug mode for us
  util::double_conversion::StringBuilder builder(out_buffer, out_size);
  bool result = impl_->converter_.ToShortestSingle(v, &builder);
  DCHECK(result);
  ARROW_UNUSED(result);
  return builder.position();
}

int FloatToStringFormatter::FormatFloat(double v, char* out_buffer, int out_size) {
  DCHECK_GE(out_size, kMinBufferSize);
  util::double_conversion::StringBuilder builder(out_buffer, out_size);
  bool result = impl_->converter_.ToShortest(v, &builder);
  DCHECK(result);
  ARROW_UNUSED(result);
  return builder.position();
}

}  // namespace internal
}  // namespace arrow
