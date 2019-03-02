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

// Operations that can deal with very large values (256-bit).
//
// The intermediate results with decimal can be larger than what can fit into 128-bit,
// but the final results can fit in 128-bit after scaling down. These functions deal
// with operations on the intermediate values.
//

#include "gandiva/decimal_xlarge.h"

#include <limits>
#include <vector>
#include "boost/multiprecision/cpp_int.hpp"

#include "arrow/util/basic_decimal.h"
#include "gandiva/decimal_type_util.h"
#include "gandiva/logging.h"

#ifndef GANDIVA_UNIT_TEST
#include "gandiva/engine.h"
#include "gandiva/exported_funcs.h"

namespace gandiva {

void ExportedDecimalFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

  // gdv_multiply_and_scale_down
  args = {types->i64_type(),      // int64_t x_high
          types->i64_type(),      // uint64_t x_low
          types->i64_type(),      // int64_t y_high
          types->i64_type(),      // uint64_t x_low
          types->i32_type(),      // int32_t reduce_scale_by
          types->i64_ptr_type(),  // int64_t* out_high
          types->i64_ptr_type(),  // uint64_t* out_low
          types->i8_ptr_type()};  // bool* overflow

  engine->AddGlobalMappingForFunc(
      "gdv_xlarge_multiply_and_scale_down", types->void_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_xlarge_multiply_and_scale_down));
}
}  // namespace gandiva

#endif  // !GANDIVA_UNIT_TEST

using arrow::BasicDecimal128;
using boost::multiprecision::int256_t;

namespace gandiva {
namespace internal {

// Convert to 256-bit integer from 128-bit decimal.
static int256_t ConvertToInt256(BasicDecimal128 in) {
  int256_t v = in.high_bits();
  v <<= 64;
  v |= in.low_bits();
  return v;
}

// Convert to 128-bit decimal from 256-bit integer.
// If there is an overflow, the output is undefined.
static BasicDecimal128 ConvertToDecimal128(int256_t in, bool* overflow) {
  BasicDecimal128 result;
  constexpr int256_t UINT64_MASK = std::numeric_limits<uint64_t>::max();

  int256_t in_abs = abs(in);
  bool is_negative = in < 0;

  uint64_t low = (in_abs & UINT64_MASK).convert_to<uint64_t>();
  in_abs >>= 64;
  uint64_t high = (in_abs & UINT64_MASK).convert_to<uint64_t>();
  in_abs >>= 64;

  if (in_abs > 0) {
    // we've shifted in by 128-bit, so nothing should be left.
    *overflow = true;
  } else if (high > INT64_MAX) {
    // the high-bit must not be set (signed 128-bit).
    *overflow = true;
  } else {
    result = BasicDecimal128(static_cast<int64_t>(high), low);
    if (result > BasicDecimal128::GetMaxValue()) {
      *overflow = true;
    }
  }
  return is_negative ? -result : result;
}

// divide input by 10^reduce_by, and round up the fractional part.
static int256_t ReduceScaleBy(int256_t in, int32_t reduce_by) {
  DCHECK_GE(reduce_by, 0);
  DCHECK_LE(reduce_by, 2 * DecimalTypeUtil::kMaxPrecision);

  if (reduce_by == 0) {
    // nothing to do.
    return in;
  }

  int256_t divisor;
  if (reduce_by <= DecimalTypeUtil::kMaxPrecision) {
    divisor = ConvertToInt256(BasicDecimal128::GetScaleMultiplier(reduce_by));
  } else {
    divisor = ConvertToInt256(
        BasicDecimal128::GetScaleMultiplier(DecimalTypeUtil::kMaxPrecision));
    for (auto i = DecimalTypeUtil::kMaxPrecision; i < reduce_by; i++) {
      divisor *= 10;
    }
  }

  DCHECK_GT(divisor, 0);
  DCHECK_EQ(divisor % 2, 0);  // multiple of 10.
  auto result = in / divisor;
  auto remainder = in % divisor;
  // round up (same as BasicDecimal128::ReduceScaleBy)
  if (abs(remainder) >= (divisor >> 1)) {
    result += (in > 0 ? 1 : -1);
  }
  return result;
}

}  // namespace internal
}  // namespace gandiva

extern "C" {

void gdv_xlarge_multiply_and_scale_down(int64_t x_high, uint64_t x_low, int64_t y_high,
                                        uint64_t y_low, int32_t reduce_scale_by,
                                        int64_t* out_high, uint64_t* out_low,
                                        bool* overflow) {
  BasicDecimal128 x{x_high, x_low};
  BasicDecimal128 y{y_high, y_low};
  auto intermediate_result =
      gandiva::internal::ConvertToInt256(x) * gandiva::internal::ConvertToInt256(y);
  intermediate_result =
      gandiva::internal::ReduceScaleBy(intermediate_result, reduce_scale_by);
  auto result = gandiva::internal::ConvertToDecimal128(intermediate_result, overflow);
  *out_high = result.high_bits();
  *out_low = result.low_bits();
}

}  // extern "C"
