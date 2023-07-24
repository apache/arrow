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

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

#include "arrow/util/logging.h"
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
#define CBRT(IN_TYPE, OUT_TYPE)                                           \
  FORCE_INLINE                                                            \
  gdv_##OUT_TYPE cbrt_##IN_TYPE(gdv_##IN_TYPE in) {                       \
    return static_cast<gdv_float64>(cbrtl(static_cast<long double>(in))); \
  }

ENUMERIC_TYPES_UNARY(CBRT, float64)

// Exponent
#define EXP(IN_TYPE, OUT_TYPE)                                           \
  FORCE_INLINE                                                           \
  gdv_##OUT_TYPE exp_##IN_TYPE(gdv_##IN_TYPE in) {                       \
    return static_cast<gdv_float64>(expl(static_cast<long double>(in))); \
  }

ENUMERIC_TYPES_UNARY(EXP, float64)

// log
#define LOG(IN_TYPE, OUT_TYPE)                                           \
  FORCE_INLINE                                                           \
  gdv_##OUT_TYPE log_##IN_TYPE(gdv_##IN_TYPE in) {                       \
    return static_cast<gdv_float64>(logl(static_cast<long double>(in))); \
  }

ENUMERIC_TYPES_UNARY(LOG, float64)

// log base 10
#define LOG10(IN_TYPE, OUT_TYPE)                                           \
  FORCE_INLINE                                                             \
  gdv_##OUT_TYPE log10_##IN_TYPE(gdv_##IN_TYPE in) {                       \
    return static_cast<gdv_float64>(log10l(static_cast<long double>(in))); \
  }

#define LOGL(VALUE) static_cast<gdv_float64>(logl(static_cast<long double>(VALUE)))

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
#define LOG_WITH_BASE(IN_TYPE1, IN_TYPE2, OUT_TYPE)                                  \
  FORCE_INLINE                                                                       \
  gdv_##OUT_TYPE log_##IN_TYPE1##_##IN_TYPE2(gdv_int64 context, gdv_##IN_TYPE1 base, \
                                             gdv_##IN_TYPE2 value) {                 \
    gdv_##OUT_TYPE log_of_base = LOGL(base);                                         \
    if (log_of_base == 0) {                                                          \
      set_error_for_logbase(context, static_cast<gdv_float64>(base));                \
      return 0;                                                                      \
    }                                                                                \
    return LOGL(value) / log_of_base;                                                \
  }

LOG_WITH_BASE(int32, int32, float64)
LOG_WITH_BASE(uint32, uint32, float64)
LOG_WITH_BASE(int64, int64, float64)
LOG_WITH_BASE(uint64, uint64, float64)
LOG_WITH_BASE(float32, float32, float64)
LOG_WITH_BASE(float64, float64, float64)

// Sin
#define SIN(IN_TYPE, OUT_TYPE)                                             \
  FORCE_INLINE                                                             \
  gdv_##OUT_TYPE sin_##IN_TYPE(gdv_##IN_TYPE in) {                         \
    return static_cast<gdv_##OUT_TYPE>(sin(static_cast<long double>(in))); \
  }
ENUMERIC_TYPES_UNARY(SIN, float64)

// Asin
#define ASIN(IN_TYPE, OUT_TYPE)                                             \
  FORCE_INLINE                                                              \
  gdv_##OUT_TYPE asin_##IN_TYPE(gdv_##IN_TYPE in) {                         \
    return static_cast<gdv_##OUT_TYPE>(asin(static_cast<long double>(in))); \
  }
ENUMERIC_TYPES_UNARY(ASIN, float64)

// Cos
#define COS(IN_TYPE, OUT_TYPE)                                             \
  FORCE_INLINE                                                             \
  gdv_##OUT_TYPE cos_##IN_TYPE(gdv_##IN_TYPE in) {                         \
    return static_cast<gdv_##OUT_TYPE>(cos(static_cast<long double>(in))); \
  }
ENUMERIC_TYPES_UNARY(COS, float64)

// Acos
#define ACOS(IN_TYPE, OUT_TYPE)                                             \
  FORCE_INLINE                                                              \
  gdv_##OUT_TYPE acos_##IN_TYPE(gdv_##IN_TYPE in) {                         \
    return static_cast<gdv_##OUT_TYPE>(acos(static_cast<long double>(in))); \
  }
ENUMERIC_TYPES_UNARY(ACOS, float64)

// Tan
#define TAN(IN_TYPE, OUT_TYPE)                                             \
  FORCE_INLINE                                                             \
  gdv_##OUT_TYPE tan_##IN_TYPE(gdv_##IN_TYPE in) {                         \
    return static_cast<gdv_##OUT_TYPE>(tan(static_cast<long double>(in))); \
  }
ENUMERIC_TYPES_UNARY(TAN, float64)

// Atan
#define ATAN(IN_TYPE, OUT_TYPE)                                             \
  FORCE_INLINE                                                              \
  gdv_##OUT_TYPE atan_##IN_TYPE(gdv_##IN_TYPE in) {                         \
    return static_cast<gdv_##OUT_TYPE>(atan(static_cast<long double>(in))); \
  }
ENUMERIC_TYPES_UNARY(ATAN, float64)

// Sinh
#define SINH(IN_TYPE, OUT_TYPE)                                             \
  FORCE_INLINE                                                              \
  gdv_##OUT_TYPE sinh_##IN_TYPE(gdv_##IN_TYPE in) {                         \
    return static_cast<gdv_##OUT_TYPE>(sinh(static_cast<long double>(in))); \
  }
ENUMERIC_TYPES_UNARY(SINH, float64)

// Cosh
#define COSH(IN_TYPE, OUT_TYPE)                                             \
  FORCE_INLINE                                                              \
  gdv_##OUT_TYPE cosh_##IN_TYPE(gdv_##IN_TYPE in) {                         \
    return static_cast<gdv_##OUT_TYPE>(cosh(static_cast<long double>(in))); \
  }
ENUMERIC_TYPES_UNARY(COSH, float64)

// Tanh
#define TANH(IN_TYPE, OUT_TYPE)                                             \
  FORCE_INLINE                                                              \
  gdv_##OUT_TYPE tanh_##IN_TYPE(gdv_##IN_TYPE in) {                         \
    return static_cast<gdv_##OUT_TYPE>(tanh(static_cast<long double>(in))); \
  }
ENUMERIC_TYPES_UNARY(TANH, float64)

// Atan2
#define ATAN2(IN_TYPE, OUT_TYPE)                                                     \
  FORCE_INLINE                                                                       \
  gdv_##OUT_TYPE atan2_##IN_TYPE##_##IN_TYPE(gdv_##IN_TYPE in1, gdv_##IN_TYPE in2) { \
    return static_cast<gdv_##OUT_TYPE>(                                              \
        atan2(static_cast<long double>(in1), static_cast<long double>(in2)));        \
  }
ENUMERIC_TYPES_UNARY(ATAN2, float64)

// Cot
#define COT(IN_TYPE, OUT_TYPE)                                                        \
  FORCE_INLINE                                                                        \
  gdv_##OUT_TYPE cot_##IN_TYPE(gdv_##IN_TYPE in) {                                    \
    return static_cast<gdv_##OUT_TYPE>(tan(M_PI / 2 - static_cast<long double>(in))); \
  }
ENUMERIC_TYPES_UNARY(COT, float64)

// Radians
#define RADIANS(IN_TYPE, OUT_TYPE)                                                   \
  FORCE_INLINE                                                                       \
  gdv_##OUT_TYPE radians_##IN_TYPE(gdv_##IN_TYPE in) {                               \
    return static_cast<gdv_##OUT_TYPE>(static_cast<long double>(in) * M_PI / 180.0); \
  }
ENUMERIC_TYPES_UNARY(RADIANS, float64)

// Degrees
#define DEGREES(IN_TYPE, OUT_TYPE)                                                   \
  FORCE_INLINE                                                                       \
  gdv_##OUT_TYPE degrees_##IN_TYPE(gdv_##IN_TYPE in) {                               \
    return static_cast<gdv_##OUT_TYPE>(static_cast<long double>(in) * 180.0 / M_PI); \
  }
ENUMERIC_TYPES_UNARY(DEGREES, float64)

// power
#define POWER(IN_TYPE1, IN_TYPE2, OUT_TYPE)                                              \
  FORCE_INLINE                                                                           \
  gdv_##OUT_TYPE power_##IN_TYPE1##_##IN_TYPE2(gdv_##IN_TYPE1 in1, gdv_##IN_TYPE2 in2) { \
    return static_cast<gdv_float64>(powl(in1, in2));                                     \
  }
POWER(float64, float64, float64)

FORCE_INLINE
gdv_int32 round_int32(gdv_int32 num) { return num; }

FORCE_INLINE
gdv_int64 round_int64(gdv_int64 num) { return num; }

// Lookup table to make the factorial function to execute faster
// It is used because the range of values for the function is limited to [0!-20!]
static const int64_t kFactorialLookupTable[] = {1,
                                                1,
                                                2,
                                                6,
                                                24,
                                                120,
                                                720,
                                                5040,
                                                40320,
                                                362880,
                                                3628800,
                                                39916800,
                                                479001600,
                                                6227020800,
                                                87178291200,
                                                1307674368000,
                                                20922789888000,
                                                355687428096000,
                                                6402373705728000,
                                                121645100408832000,
                                                2432902008176640000};

#define FACTORIAL(IN_TYPE)                                                          \
  FORCE_INLINE                                                                      \
  gdv_int64 factorial_##IN_TYPE(gdv_int64 ctx, gdv_##IN_TYPE value) {               \
    if (value < 0) {                                                                \
      gdv_fn_context_set_error_msg(ctx, "Factorial of negative number not exist!"); \
      return 0;                                                                     \
    }                                                                               \
    /* For numbers greater than 20 causes an overflow. */                           \
    if (value > 20) {                                                               \
      gdv_fn_context_set_error_msg(ctx, "Numbers greater than 20 cause overflow!"); \
      return 0;                                                                     \
    }                                                                               \
                                                                                    \
    return kFactorialLookupTable[static_cast<int32_t>(value)];                      \
  }

FACTORIAL(int32)
FACTORIAL(int64)

// rounds the number to the nearest integer
#define ROUND_DECIMAL(TYPE)                                                 \
  FORCE_INLINE                                                              \
  gdv_##TYPE round_##TYPE(gdv_##TYPE num) {                                 \
    return static_cast<gdv_##TYPE>(trunc(num + ((num >= 0) ? 0.5 : -0.5))); \
  }

ROUND_DECIMAL(float32)
ROUND_DECIMAL(float64)

// rounds the number to the nearest integer
FORCE_INLINE
gdv_float64 bround_float64(gdv_float64 num) {
  gdv_float64 round_num = round(num);
  gdv_float64 diff_num = round_num - num;
  if ((diff_num != 0.5) && (diff_num != -0.5)) {
    return round_num;
  }
  if (fmod(round_num, 2.0) == 0.0) {
    return round_num;
  }

  return num - diff_num;
}

// rounds the number to the given scale
#define ROUND_DECIMAL_TO_SCALE(TYPE)                                        \
  FORCE_INLINE                                                              \
  gdv_##TYPE round_##TYPE##_int32(gdv_##TYPE number, gdv_int32 out_scale) { \
    gdv_float64 scale_multiplier = get_scale_multiplier(out_scale);         \
    return static_cast<gdv_##TYPE>(                                         \
        trunc(number * scale_multiplier + ((number >= 0) ? 0.5 : -0.5)) /   \
        scale_multiplier);                                                  \
  }

ROUND_DECIMAL_TO_SCALE(float32)
ROUND_DECIMAL_TO_SCALE(float64)

FORCE_INLINE
gdv_int32 round_int32_int32(gdv_int32 number, gdv_int32 precision) {
  // for integers, there is nothing following the decimal point,
  // so round() always returns the same number if precision >= 0
  if (precision >= 0) {
    return number;
  }
  gdv_int32 abs_precision = -precision;
  // This is to ensure that there is no overflow while calculating 10^precision, 9 is
  // the smallest N for which 10^N does not fit into 32 bits, so we can safely return 0
  if (abs_precision > 9) {
    return 0;
  }
  gdv_int32 num_sign = (number > 0) ? 1 : -1;
  gdv_int32 abs_number = number * num_sign;
  gdv_int32 power_of_10 = static_cast<gdv_int32>(get_power_of_10(abs_precision));
  gdv_int32 remainder = abs_number % power_of_10;
  abs_number -= remainder;
  // if the fractional part of the quotient >= 0.5, round to next higher integer
  if (remainder >= power_of_10 / 2) {
    abs_number += power_of_10;
  }
  return abs_number * num_sign;
}

FORCE_INLINE
gdv_int64 round_int64_int32(gdv_int64 number, gdv_int32 precision) {
  // for long integers, there is nothing following the decimal point,
  // so round() always returns the same number if precision >= 0
  if (precision >= 0) {
    return number;
  }
  gdv_int32 abs_precision = -precision;
  // This is to ensure that there is no overflow while calculating 10^precision, 19 is
  // the smallest N for which 10^N does not fit into 64 bits, so we can safely return 0
  if (abs_precision > 18) {
    return 0;
  }
  gdv_int32 num_sign = (number > 0) ? 1 : -1;
  gdv_int64 abs_number = number * num_sign;
  gdv_int64 power_of_10 = get_power_of_10(abs_precision);
  gdv_int64 remainder = abs_number % power_of_10;
  abs_number -= remainder;
  // if the fractional part of the quotient >= 0.5, round to next higher integer
  if (remainder >= power_of_10 / 2) {
    abs_number += power_of_10;
  }
  return abs_number * num_sign;
}

FORCE_INLINE
gdv_int64 get_power_of_10(gdv_int32 exp) {
  DCHECK_GE(exp, 0);
  DCHECK_LE(exp, 18);
  static const gdv_int64 power_of_10[] = {1,
                                          10,
                                          100,
                                          1000,
                                          10000,
                                          100000,
                                          1000000,
                                          10000000,
                                          100000000,
                                          1000000000,
                                          10000000000,
                                          100000000000,
                                          1000000000000,
                                          10000000000000,
                                          100000000000000,
                                          1000000000000000,
                                          10000000000000000,
                                          100000000000000000,
                                          1000000000000000000};
  return power_of_10[exp];
}

FORCE_INLINE
gdv_int64 truncate_int64_int32(gdv_int64 in, gdv_int32 out_scale) {
  bool overflow = false;
  arrow::BasicDecimal128 decimal = gandiva::decimalops::FromInt64(in, 38, 0, &overflow);
  arrow::BasicDecimal128 decimal_with_outscale =
      gandiva::decimalops::Truncate(gandiva::BasicDecimalScalar128(decimal, 38, 0), 38,
                                    out_scale, out_scale, &overflow);
  if (out_scale < 0) {
    out_scale = 0;
  }
  return gandiva::decimalops::ToInt64(
      gandiva::BasicDecimalScalar128(decimal_with_outscale, 38, out_scale), &overflow);
}

FORCE_INLINE
gdv_float64 get_scale_multiplier(gdv_int32 scale) {
  static const gdv_float64 values[] = {1.0,
                                       10.0,
                                       100.0,
                                       1000.0,
                                       10000.0,
                                       100000.0,
                                       1000000.0,
                                       10000000.0,
                                       100000000.0,
                                       1000000000.0,
                                       10000000000.0,
                                       100000000000.0,
                                       1000000000000.0,
                                       10000000000000.0,
                                       100000000000000.0,
                                       1000000000000000.0,
                                       10000000000000000.0,
                                       100000000000000000.0,
                                       1000000000000000000.0,
                                       10000000000000000000.0};
  if (scale >= 0 && scale < 20) {
    return values[scale];
  }
  return power_float64_float64(10.0, scale);
}

// returns the binary representation of a given integer (e.g. 928 -> 1110100000)
#define BIN_INTEGER(IN_TYPE)                                                          \
  FORCE_INLINE                                                                        \
  const char* bin_##IN_TYPE(int64_t context, gdv_##IN_TYPE value, int32_t* out_len) { \
    *out_len = 0;                                                                     \
    int32_t len = 8 * sizeof(value);                                                  \
    char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, len));   \
    if (ret == nullptr) {                                                             \
      gdv_fn_context_set_error_msg(context, "Could not allocate memory for output");  \
      return "";                                                                      \
    }                                                                                 \
    /* handle case when value is zero */                                              \
    if (value == 0) {                                                                 \
      *out_len = 1;                                                                   \
      ret[0] = '0';                                                                   \
      return ret;                                                                     \
    }                                                                                 \
    /* generate binary representation iteratively */                                  \
    gdv_u##IN_TYPE i;                                                                 \
    int8_t count = 0;                                                                 \
    bool first = false; /* flag for not printing left zeros in positive numbers */    \
    for (i = static_cast<gdv_u##IN_TYPE>(1) << (len - 1); i > 0; i = i / 2) {         \
      if ((value & i) != 0) {                                                         \
        ret[count] = '1';                                                             \
        if (!first) first = true;                                                     \
      } else {                                                                        \
        if (!first) continue;                                                         \
        ret[count] = '0';                                                             \
      }                                                                               \
      count += 1;                                                                     \
    }                                                                                 \
    *out_len = count;                                                                 \
    return ret;                                                                       \
  }

BIN_INTEGER(int32)
BIN_INTEGER(int64)

#undef BIN_INTEGER

}  // extern "C"
