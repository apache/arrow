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

#include <cmath>
#include <cstdint>
#include "arrow/util/basic_decimal.h"

extern "C" {

#include "./types.h"

// Expand inner macro for all numeric types.
#define NUMERIC_TYPES(INNER, NAME, OP) \
  INNER(NAME, int8, OP)                \
  INNER(NAME, int16, OP)               \
  INNER(NAME, int32, OP)               \
  INNER(NAME, int64, OP)               \
  INNER(NAME, uint8, OP)               \
  INNER(NAME, uint16, OP)              \
  INNER(NAME, uint32, OP)              \
  INNER(NAME, uint64, OP)              \
  INNER(NAME, float32, OP)             \
  INNER(NAME, float64, OP)

// Expand inner macros for all date/time types.
#define DATE_TYPES(INNER, NAME, OP) \
  INNER(NAME, date64, OP)           \
  INNER(NAME, date32, OP)           \
  INNER(NAME, timestamp, OP)        \
  INNER(NAME, time32, OP)

#define NUMERIC_DATE_TYPES(INNER, NAME, OP) \
  NUMERIC_TYPES(INNER, NAME, OP)            \
  DATE_TYPES(INNER, NAME, OP)

#define NUMERIC_BOOL_DATE_TYPES(INNER, NAME, OP) \
  NUMERIC_TYPES(INNER, NAME, OP)                 \
  DATE_TYPES(INNER, NAME, OP)                    \
  INNER(NAME, boolean, OP)

#define MOD_OP(NAME, IN_TYPE1, IN_TYPE2, OUT_TYPE)                      \
  FORCE_INLINE                                                          \
  gdv_##OUT_TYPE NAME##_##IN_TYPE1##_##IN_TYPE2(gdv_##IN_TYPE1 left,    \
                                                gdv_##IN_TYPE2 right) { \
    return (right == 0 ? static_cast<gdv_##OUT_TYPE>(left)              \
                       : static_cast<gdv_##OUT_TYPE>(left % right));    \
  }

#define PMOD_OP(NAME, IN_TYPE1, IN_TYPE2, OUT_TYPE)                                   \
  FORCE_INLINE                                                                        \
  gdv_##OUT_TYPE NAME##_##IN_TYPE1##_##IN_TYPE2(int64_t context, gdv_##IN_TYPE1 left, \
                                                gdv_##IN_TYPE2 right) {               \
    if (right == static_cast<gdv_##IN_TYPE2>(0)) {                                    \
      gdv_fn_context_set_error_msg(context, "divide by zero error");                  \
      return static_cast<gdv_##IN_TYPE1>(0);                                          \
    }                                                                                 \
    double mod = fmod(static_cast<double>(left), static_cast<double>(right));         \
    if (mod < 0 || right < 0) {                                                       \
      mod += static_cast<double>(right);                                              \
    }                                                                                 \
    return static_cast<gdv_##IN_TYPE1>(mod);                                          \
  }

// Symmetric binary fns : left, right params and return type are same.
#define BINARY_SYMMETRIC(NAME, TYPE, OP)                                 \
  FORCE_INLINE                                                           \
  gdv_##TYPE NAME##_##TYPE##_##TYPE(gdv_##TYPE left, gdv_##TYPE right) { \
    return static_cast<gdv_##TYPE>(left OP right);                       \
  }

NUMERIC_TYPES(BINARY_SYMMETRIC, add, +)
NUMERIC_TYPES(BINARY_SYMMETRIC, subtract, -)
NUMERIC_TYPES(BINARY_SYMMETRIC, multiply, *)
BINARY_SYMMETRIC(bitwise_and, int32, &)
BINARY_SYMMETRIC(bitwise_and, int64, &)
BINARY_SYMMETRIC(bitwise_or, int32, |)
BINARY_SYMMETRIC(bitwise_or, int64, |)
BINARY_SYMMETRIC(bitwise_xor, int32, ^)
BINARY_SYMMETRIC(bitwise_xor, int64, ^)

#undef BINARY_SYMMETRIC

MOD_OP(mod, int64, int32, int32)
MOD_OP(mod, int64, int64, int64)
MOD_OP(mod, uint32, uint32, uint32)
MOD_OP(mod, uint64, uint64, uint64)

PMOD_OP(pmod, int32, int32, int32)
PMOD_OP(pmod, int64, int64, int64)
PMOD_OP(pmod, float32, float32, float32)
PMOD_OP(pmod, float64, float64, float64)

#undef MOD_OP
#undef PMOD_OP

gdv_float64 mod_float64_float64(int64_t context, gdv_float64 x, gdv_float64 y) {
  if (y == 0.0) {
    char const* err_msg = "divide by zero error";
    gdv_fn_context_set_error_msg(context, err_msg);
    return 0.0;
  }
  return fmod(x, y);
}

// Relational binary fns : left, right params are same, return is bool.
#define BINARY_RELATIONAL(NAME, TYPE, OP) \
  FORCE_INLINE                            \
  bool NAME##_##TYPE##_##TYPE(gdv_##TYPE left, gdv_##TYPE right) { return left OP right; }

NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL, equal, ==)
NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL, not_equal, !=)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, less_than, <)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, less_than_or_equal_to, <=)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, greater_than, >)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, greater_than_or_equal_to, >=)

#undef BINARY_RELATIONAL

// Returns the greatest or least value from a list of values
#define COMPARE_TWO_VALUES(NAME, TYPE, OP)                            \
  FORCE_INLINE                                                        \
  gdv_##TYPE NAME##_##TYPE##_##TYPE(gdv_##TYPE in1, gdv_##TYPE in2) { \
    return (in1 OP in2 ? in1 : in2);                                  \
  }

#define COMPARE_THREE_VALUES(NAME, TYPE, OP)                                 \
  FORCE_INLINE                                                               \
  gdv_##TYPE NAME##_##TYPE##_##TYPE##_##TYPE(gdv_##TYPE in1, gdv_##TYPE in2, \
                                             gdv_##TYPE in3) {               \
    gdv_##TYPE compared = (in1 OP in2 ? in1 : in2);                          \
    return (compared OP in3 ? compared : in3);                               \
  }

#define COMPARE_FOUR_VALUES(NAME, TYPE, OP)                                             \
  FORCE_INLINE                                                                          \
  gdv_##TYPE NAME##_##TYPE##_##TYPE##_##TYPE##_##TYPE(gdv_##TYPE in1, gdv_##TYPE in2,   \
                                                      gdv_##TYPE in3, gdv_##TYPE in4) { \
    gdv_##TYPE compared = (in1 OP in2 ? in1 : in2);                                     \
    compared = (compared OP in3 ? compared : in3);                                      \
    return (compared OP in4 ? compared : in4);                                          \
  }

#define COMPARE_FIVE_VALUES(NAME, TYPE, OP)                                             \
  FORCE_INLINE                                                                          \
  gdv_##TYPE NAME##_##TYPE##_##TYPE##_##TYPE##_##TYPE##_##TYPE(                         \
      gdv_##TYPE in1, gdv_##TYPE in2, gdv_##TYPE in3, gdv_##TYPE in4, gdv_##TYPE in5) { \
    gdv_##TYPE compared = (in1 OP in2 ? in1 : in2);                                     \
    compared = (compared OP in3 ? compared : in3);                                      \
    compared = (compared OP in4 ? compared : in4);                                      \
    return (compared OP in5 ? compared : in5);                                          \
  }

#define COMPARE_SIX_VALUES(NAME, TYPE, OP)                                            \
  FORCE_INLINE                                                                        \
  gdv_##TYPE NAME##_##TYPE##_##TYPE##_##TYPE##_##TYPE##_##TYPE##_##TYPE(              \
      gdv_##TYPE in1, gdv_##TYPE in2, gdv_##TYPE in3, gdv_##TYPE in4, gdv_##TYPE in5, \
      gdv_##TYPE in6) {                                                               \
    gdv_##TYPE compared = (in1 OP in2 ? in1 : in2);                                   \
    compared = (compared OP in3 ? compared : in3);                                    \
    compared = (compared OP in4 ? compared : in4);                                    \
    compared = (compared OP in5 ? compared : in5);                                    \
    return (compared OP in6 ? compared : in6);                                        \
  }

NUMERIC_DATE_TYPES(COMPARE_TWO_VALUES, greatest, >)
NUMERIC_DATE_TYPES(COMPARE_TWO_VALUES, least, <)
NUMERIC_DATE_TYPES(COMPARE_THREE_VALUES, greatest, >)
NUMERIC_DATE_TYPES(COMPARE_THREE_VALUES, least, <)
NUMERIC_DATE_TYPES(COMPARE_FOUR_VALUES, greatest, >)
NUMERIC_DATE_TYPES(COMPARE_FOUR_VALUES, least, <)
NUMERIC_DATE_TYPES(COMPARE_FIVE_VALUES, greatest, >)
NUMERIC_DATE_TYPES(COMPARE_FIVE_VALUES, least, <)
NUMERIC_DATE_TYPES(COMPARE_SIX_VALUES, greatest, >)
NUMERIC_DATE_TYPES(COMPARE_SIX_VALUES, least, <)

#undef COMPARE_TWO_VALUES
#undef COMPARE_THREE_VALUES
#undef COMPARE_FOUR_VALUES
#undef COMPARE_FIVE_VALUES
#undef COMPARE_SIX_VALUES

// cast fns : takes one param type, returns another type.
#define CAST_UNARY(NAME, IN_TYPE, OUT_TYPE)           \
  FORCE_INLINE                                        \
  gdv_##OUT_TYPE NAME##_##IN_TYPE(gdv_##IN_TYPE in) { \
    return static_cast<gdv_##OUT_TYPE>(in);           \
  }

CAST_UNARY(castBIGINT, int32, int64)
CAST_UNARY(castINT, int64, int32)
CAST_UNARY(castFLOAT4, int32, float32)
CAST_UNARY(castFLOAT4, int64, float32)
CAST_UNARY(castFLOAT8, int32, float64)
CAST_UNARY(castFLOAT8, int64, float64)
CAST_UNARY(castFLOAT8, float32, float64)
CAST_UNARY(castFLOAT4, float64, float32)

#undef CAST_UNARY

// cast float types to int types.
#define CAST_INT_FLOAT(NAME, IN_TYPE, OUT_TYPE)                  \
  FORCE_INLINE                                                   \
  gdv_##OUT_TYPE NAME##_##IN_TYPE(gdv_##IN_TYPE in) {            \
    gdv_##OUT_TYPE out = static_cast<gdv_##OUT_TYPE>(round(in)); \
    return out;                                                  \
  }

CAST_INT_FLOAT(castBIGINT, float32, int64)
CAST_INT_FLOAT(castBIGINT, float64, int64)
CAST_INT_FLOAT(castINT, float32, int32)
CAST_INT_FLOAT(castINT, float64, int32)

#undef CAST_INT_FLOAT

// simple nullable functions, result value = fn(input validity)
#define VALIDITY_OP(NAME, TYPE, OP) \
  FORCE_INLINE                      \
  bool NAME##_##TYPE(gdv_##TYPE in, gdv_boolean is_valid) { return OP is_valid; }

NUMERIC_BOOL_DATE_TYPES(VALIDITY_OP, isnull, !)
NUMERIC_BOOL_DATE_TYPES(VALIDITY_OP, isnotnull, +)
NUMERIC_TYPES(VALIDITY_OP, isnumeric, +)

#undef VALIDITY_OP

#define IS_TRUE_OR_FALSE_BOOL(NAME, TYPE, OP)                      \
  FORCE_INLINE                                                     \
  gdv_##TYPE NAME##_boolean(gdv_##TYPE in, gdv_boolean is_valid) { \
    return is_valid && OP in;                                      \
  }

IS_TRUE_OR_FALSE_BOOL(istrue, boolean, +)
IS_TRUE_OR_FALSE_BOOL(isfalse, boolean, !)

#define IS_NOT_TRUE_OR_IS_NOT_FALSE_BOOL(NAME, TYPE, OP)           \
  FORCE_INLINE                                                     \
  gdv_##TYPE NAME##_boolean(gdv_##TYPE in, gdv_boolean is_valid) { \
    return !is_valid || OP in;                                     \
  }

IS_NOT_TRUE_OR_IS_NOT_FALSE_BOOL(isnottrue, boolean, !)
IS_NOT_TRUE_OR_IS_NOT_FALSE_BOOL(isnotfalse, boolean, +)

#define IS_TRUE_OR_FALSE_NUMERIC(NAME, TYPE, OP)                   \
  FORCE_INLINE                                                     \
  gdv_boolean NAME##_##TYPE(gdv_##TYPE in, gdv_boolean is_valid) { \
    return is_valid && OP(in != 0);                                \
  }

NUMERIC_TYPES(IS_TRUE_OR_FALSE_NUMERIC, istrue, +)
NUMERIC_TYPES(IS_TRUE_OR_FALSE_NUMERIC, isfalse, !)

#define IS_NOT_TRUE_OR_IS_NOT_FALSE_NUMERIC(NAME, TYPE, OP)        \
  FORCE_INLINE                                                     \
  gdv_boolean NAME##_##TYPE(gdv_##TYPE in, gdv_boolean is_valid) { \
    return !is_valid || OP(in != 0);                               \
  }

NUMERIC_TYPES(IS_NOT_TRUE_OR_IS_NOT_FALSE_NUMERIC, isnottrue, !)
NUMERIC_TYPES(IS_NOT_TRUE_OR_IS_NOT_FALSE_NUMERIC, isnotfalse, +)

#define NUMERIC_FUNCTION_FOR_REAL(INNER) \
  INNER(float32)                         \
  INNER(float64)

#define NUMERIC_FUNCTION(INNER) \
  INNER(int8)                   \
  INNER(int16)                  \
  INNER(int32)                  \
  INNER(int64)                  \
  INNER(uint8)                  \
  INNER(uint16)                 \
  INNER(uint32)                 \
  INNER(uint64)                 \
  NUMERIC_FUNCTION_FOR_REAL(INNER)

#define DATE_FUNCTION(INNER) \
  INNER(date32)              \
  INNER(date64)              \
  INNER(timestamp)           \
  INNER(time32)

#define NUMERIC_BOOL_DATE_FUNCTION(INNER) \
  NUMERIC_FUNCTION(INNER)                 \
  DATE_FUNCTION(INNER)                    \
  INNER(boolean)

#define NVL(TYPE)                                                                  \
  FORCE_INLINE                                                                     \
  gdv_##TYPE nvl_##TYPE##_##TYPE(gdv_##TYPE in, gdv_boolean is_valid_in,           \
                                 gdv_##TYPE replace, gdv_boolean is_valid_value) { \
    return (is_valid_in ? in : replace);                                           \
  }

NUMERIC_BOOL_DATE_FUNCTION(NVL)

#undef NVL

FORCE_INLINE
gdv_boolean not_boolean(gdv_boolean in) { return !in; }

// is_distinct_from
#define IS_DISTINCT_FROM(TYPE)                                                   \
  FORCE_INLINE                                                                   \
  bool is_distinct_from_##TYPE##_##TYPE(gdv_##TYPE in1, gdv_boolean is_valid1,   \
                                        gdv_##TYPE in2, gdv_boolean is_valid2) { \
    if (is_valid1 != is_valid2) {                                                \
      return true;                                                               \
    }                                                                            \
    if (!is_valid1) {                                                            \
      return false;                                                              \
    }                                                                            \
    return in1 != in2;                                                           \
  }

// is_not_distinct_from
#define IS_NOT_DISTINCT_FROM(TYPE)                                                   \
  FORCE_INLINE                                                                       \
  bool is_not_distinct_from_##TYPE##_##TYPE(gdv_##TYPE in1, gdv_boolean is_valid1,   \
                                            gdv_##TYPE in2, gdv_boolean is_valid2) { \
    if (is_valid1 != is_valid2) {                                                    \
      return false;                                                                  \
    }                                                                                \
    if (!is_valid1) {                                                                \
      return true;                                                                   \
    }                                                                                \
    return in1 == in2;                                                               \
  }

NUMERIC_BOOL_DATE_FUNCTION(IS_DISTINCT_FROM)
NUMERIC_BOOL_DATE_FUNCTION(IS_NOT_DISTINCT_FROM)

#undef IS_DISTINCT_FROM
#undef IS_NOT_DISTINCT_FROM

#define DIVIDE(TYPE)                                                                     \
  FORCE_INLINE                                                                           \
  gdv_##TYPE divide_##TYPE##_##TYPE(gdv_int64 context, gdv_##TYPE in1, gdv_##TYPE in2) { \
    if (in2 == 0) {                                                                      \
      char const* err_msg = "divide by zero error";                                      \
      gdv_fn_context_set_error_msg(context, err_msg);                                    \
      return 0;                                                                          \
    }                                                                                    \
    return static_cast<gdv_##TYPE>(in1 / in2);                                           \
  }

NUMERIC_FUNCTION(DIVIDE)

#undef DIVIDE

#define POSITIVE(TYPE) \
  FORCE_INLINE         \
  gdv_##TYPE positive_##TYPE(gdv_##TYPE in) { return in; }

NUMERIC_FUNCTION(POSITIVE)

#undef POSITIVE

#define NEGATIVE(TYPE) \
  FORCE_INLINE         \
  gdv_##TYPE negative_##TYPE(gdv_##TYPE in) { return static_cast<gdv_##TYPE>(-1 * in); }

NUMERIC_FUNCTION_FOR_REAL(NEGATIVE)

#define NEGATIVE_INTEGER(TYPE, SIZE)                                           \
  FORCE_INLINE                                                                 \
  gdv_##TYPE negative_##TYPE(gdv_int64 context, gdv_##TYPE in) {               \
    if (in <= INT##SIZE##_MIN) {                                               \
      gdv_fn_context_set_error_msg(context, "Overflow in negative execution"); \
      return 0;                                                                \
    }                                                                          \
    return -1 * in;                                                            \
  }

NEGATIVE_INTEGER(int32, 32)
NEGATIVE_INTEGER(int64, 64)
NEGATIVE_INTEGER(month_interval, 32)

const int64_t INT_MAX_TO_NEGATIVE_INTERVAL_DAY_TIME = 9223372034707292159;
const int64_t INT_MIN_TO_NEGATIVE_INTERVAL_DAY_TIME = -9223372030412324863;

gdv_int64 negative_daytimeinterval(gdv_int64 context, gdv_day_time_interval interval) {
  if (interval > INT_MAX_TO_NEGATIVE_INTERVAL_DAY_TIME ||
      interval < INT_MIN_TO_NEGATIVE_INTERVAL_DAY_TIME) {
    gdv_fn_context_set_error_msg(
        context, "Interval day time is out of boundaries for the negative function");
    return 0;
  }

  int64_t left = interval >> 32;
  int64_t right = interval & 0x00000000FFFFFFFF;

  left = -1 * left;
  right = -1 * right;

  gdv_int64 out = (left & 0x00000000FFFFFFFF) << 32;
  out |= (right & 0x00000000FFFFFFFF);

  return out;
}

#undef NEGATIVE
#undef NEGATIVE_INTEGER

void negative_decimal(gdv_int64 context, int64_t high_bits, uint64_t low_bits,
                      int32_t /*precision*/, int32_t /*scale*/, int32_t /*out_precision*/,
                      int32_t /*out_scale*/, int64_t* out_high_bits,
                      uint64_t* out_low_bits) {
  arrow::BasicDecimal128 res = arrow::BasicDecimal128(high_bits, low_bits).Negate();

  *out_high_bits = res.high_bits();
  *out_low_bits = res.low_bits();
}

#define DIV(TYPE)                                                                     \
  FORCE_INLINE                                                                        \
  gdv_##TYPE div_##TYPE##_##TYPE(gdv_int64 context, gdv_##TYPE in1, gdv_##TYPE in2) { \
    if (in2 == 0) {                                                                   \
      char const* err_msg = "divide by zero error";                                   \
      gdv_fn_context_set_error_msg(context, err_msg);                                 \
      return 0;                                                                       \
    }                                                                                 \
    return static_cast<gdv_##TYPE>(in1 / in2);                                        \
  }

DIV(int32)
DIV(int64)
DIV(uint32)
DIV(uint64)

#undef DIV

#define DIV_FLOAT(TYPE)                                                               \
  FORCE_INLINE                                                                        \
  gdv_##TYPE div_##TYPE##_##TYPE(gdv_int64 context, gdv_##TYPE in1, gdv_##TYPE in2) { \
    if (in2 == 0) {                                                                   \
      char const* err_msg = "divide by zero error";                                   \
      gdv_fn_context_set_error_msg(context, err_msg);                                 \
      return 0;                                                                       \
    }                                                                                 \
    return static_cast<gdv_##TYPE>(::trunc(in1 / in2));                               \
  }

DIV_FLOAT(float32)
DIV_FLOAT(float64)

#undef DIV_FLOAT

#define BITWISE_NOT(TYPE) \
  FORCE_INLINE            \
  gdv_##TYPE bitwise_not_##TYPE(gdv_##TYPE in) { return static_cast<gdv_##TYPE>(~in); }

BITWISE_NOT(int32)
BITWISE_NOT(int64)

#undef BITWISE_NOT

#undef DATE_FUNCTION
#undef DATE_TYPES
#undef NUMERIC_BOOL_DATE_TYPES
#undef NUMERIC_DATE_TYPES

#define SIGN(TYPE)                         \
  FORCE_INLINE                             \
  gdv_##TYPE sign_##TYPE(gdv_##TYPE in1) { \
    gdv_##TYPE out;                        \
    if (in1 > 0) {                         \
      out = static_cast<gdv_##TYPE>(1);    \
    } else if (in1 < 0) {                  \
      out = static_cast<gdv_##TYPE>(-1);   \
    } else {                               \
      out = in1;                           \
    }                                      \
    return out;                            \
  }

SIGN(int32)
SIGN(int64)
SIGN(float32)
SIGN(float64)

#undef SIGN

#define ABS(TYPE) \
  FORCE_INLINE    \
  gdv_##TYPE abs_##TYPE(gdv_##TYPE in1) { return static_cast<gdv_##TYPE>(std::abs(in1)); }

ABS(int32)
ABS(int64)
ABS(float32)
ABS(float64)

#undef ABS

#define CEILING(TYPE) \
  FORCE_INLINE        \
  gdv_##TYPE ceiling_##TYPE(gdv_##TYPE in1) { return static_cast<gdv_##TYPE>(ceil(in1)); }

CEILING(float32)
CEILING(float64)

#undef CEILING

#define FLOOR(TYPE) \
  FORCE_INLINE      \
  gdv_##TYPE floor_##TYPE(gdv_##TYPE in1) { return static_cast<gdv_##TYPE>(floor(in1)); }

FLOOR(float32)
FLOOR(float64)

#undef FLOOR
#define SQRT(TYPE)                              \
  FORCE_INLINE                                  \
  gdv_float64 sqrt_##TYPE(gdv_##TYPE in1) {     \
    if (in1 < 0) {                              \
      return NAN;                               \
    }                                           \
    return static_cast<gdv_float64>(sqrt(in1)); \
  }

SQRT(int32)
SQRT(int64)
SQRT(float32)
SQRT(float64)

#undef SQRT

#undef NUMERIC_FUNCTION
#undef NUMERIC_TYPES

}  // extern "C"
