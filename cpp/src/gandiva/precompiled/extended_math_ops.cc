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

#include "gandiva/precompiled/decimal_ops.h"

extern "C" {

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "./types.h"

// Expand the inner fn for types that support extended math.
#define ENUMERIC_TYPES_UNARY(INNER, OUT_TYPE) \
  INNER(int32, OUT_TYPE)                      \
  INNER(uint32, OUT_TYPE)                     \
  INNER(int64, OUT_TYPE)                      \
  INNER(uint64, OUT_TYPE)                     \
  INNER(float32, OUT_TYPE)                    \
  INNER(float64, OUT_TYPE)

// Cubic root
#define CBRT(IN_TYPE, OUT_TYPE)                                       \
  FORCE_INLINE                                                        \
  OUT_TYPE cbrt_##IN_TYPE(IN_TYPE in) {                               \
    return static_cast<float64>(cbrtl(static_cast<long double>(in))); \
  }

ENUMERIC_TYPES_UNARY(CBRT, float64)

// Exponent
#define EXP(IN_TYPE, OUT_TYPE)                                       \
  FORCE_INLINE                                                       \
  OUT_TYPE exp_##IN_TYPE(IN_TYPE in) {                               \
    return static_cast<float64>(expl(static_cast<long double>(in))); \
  }

ENUMERIC_TYPES_UNARY(EXP, float64)

// log
#define LOG(IN_TYPE, OUT_TYPE)                                       \
  FORCE_INLINE                                                       \
  OUT_TYPE log_##IN_TYPE(IN_TYPE in) {                               \
    return static_cast<float64>(logl(static_cast<long double>(in))); \
  }

ENUMERIC_TYPES_UNARY(LOG, float64)

// log base 10
#define LOG10(IN_TYPE, OUT_TYPE)                                       \
  FORCE_INLINE                                                         \
  OUT_TYPE log10_##IN_TYPE(IN_TYPE in) {                               \
    return static_cast<float64>(log10l(static_cast<long double>(in))); \
  }

#define LOGL(VALUE) static_cast<float64>(logl(static_cast<long double>(VALUE)))

ENUMERIC_TYPES_UNARY(LOG10, float64)

FORCE_INLINE
void set_error_for_logbase(int64_t execution_context, double base) {
  char const* prefix = "divide by zero error with log of base";
  int size = static_cast<int>(strlen(prefix)) + 64;
  char* error = reinterpret_cast<char*>(malloc(size));
  snprintf(error, size, "%s %f", prefix, base);
  gdv_fn_context_set_error_msg(execution_context, error);
  free(static_cast<char*>(error));
}

// log with base
#define LOG_WITH_BASE(IN_TYPE1, IN_TYPE2, OUT_TYPE)                                    \
  FORCE_INLINE                                                                         \
  OUT_TYPE log_##IN_TYPE1##_##IN_TYPE2(int64 context, IN_TYPE1 base, IN_TYPE2 value) { \
    OUT_TYPE log_of_base = LOGL(base);                                                 \
    if (log_of_base == 0) {                                                            \
      set_error_for_logbase(context, static_cast<float64>(base));                      \
      return 0;                                                                        \
    }                                                                                  \
    return LOGL(value) / LOGL(base);                                                   \
  }

LOG_WITH_BASE(int32, int32, float64)
LOG_WITH_BASE(uint32, uint32, float64)
LOG_WITH_BASE(int64, int64, float64)
LOG_WITH_BASE(uint64, uint64, float64)
LOG_WITH_BASE(float32, float32, float64)
LOG_WITH_BASE(float64, float64, float64)

// power
#define POWER(IN_TYPE1, IN_TYPE2, OUT_TYPE)                            \
  FORCE_INLINE                                                         \
  OUT_TYPE power_##IN_TYPE1##_##IN_TYPE2(IN_TYPE1 in1, IN_TYPE2 in2) { \
    return static_cast<float64>(powl(in1, in2));                       \
  }

POWER(float64, float64, float64)

FORCE_INLINE
int64 truncate_int64_int32(int64 in, int32 out_scale) {
  bool overflow = false;
  arrow::BasicDecimal128 decimal = gandiva::decimalops::FromInt64(in, 38, 0, &overflow);
  arrow::BasicDecimal128 decimal_with_outscale = gandiva::decimalops::Truncate(
      gandiva::BasicDecimalScalar128(decimal, 38, 0), out_scale, &overflow);
  if (out_scale < 0) {
    out_scale = 0;
  }
  return gandiva::decimalops::ToInt64(
      gandiva::BasicDecimalScalar128(decimal_with_outscale, 38, out_scale), &overflow);
}

}  // extern "C"
