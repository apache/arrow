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

extern "C" {

#include <math.h>
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
  INNER(NAME, timestamp, OP)        \
  INNER(NAME, time32, OP)

#define NUMERIC_DATE_TYPES(INNER, NAME, OP) \
  NUMERIC_TYPES(INNER, NAME, OP)            \
  DATE_TYPES(INNER, NAME, OP)

#define NUMERIC_BOOL_DATE_TYPES(INNER, NAME, OP) \
  NUMERIC_TYPES(INNER, NAME, OP)                 \
  DATE_TYPES(INNER, NAME, OP)                    \
  INNER(NAME, boolean, OP)

#define MOD_OP(NAME, IN_TYPE1, IN_TYPE2, OUT_TYPE)                         \
  FORCE_INLINE                                                             \
  OUT_TYPE NAME##_##IN_TYPE1##_##IN_TYPE2(IN_TYPE1 left, IN_TYPE2 right) { \
    return (right == 0 ? static_cast<OUT_TYPE>(left)                       \
                       : static_cast<OUT_TYPE>(left % right));             \
  }

// Symmetric binary fns : left, right params and return type are same.
#define BINARY_SYMMETRIC(NAME, TYPE, OP)               \
  FORCE_INLINE                                         \
  TYPE NAME##_##TYPE##_##TYPE(TYPE left, TYPE right) { \
    return static_cast<TYPE>(left OP right);           \
  }

NUMERIC_TYPES(BINARY_SYMMETRIC, add, +)
NUMERIC_TYPES(BINARY_SYMMETRIC, subtract, -)
NUMERIC_TYPES(BINARY_SYMMETRIC, multiply, *)

MOD_OP(mod, int64, int32, int32)
MOD_OP(mod, int64, int64, int64)

float64 mod_float64_float64(int64_t context, float64 x, float64 y) {
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
  bool NAME##_##TYPE##_##TYPE(TYPE left, TYPE right) { return left OP right; }

NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL, equal, ==)
NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL, not_equal, !=)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, less_than, <)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, less_than_or_equal_to, <=)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, greater_than, >)
NUMERIC_DATE_TYPES(BINARY_RELATIONAL, greater_than_or_equal_to, >=)

// cast fns : takes one param type, returns another type.
#define CAST_UNARY(NAME, IN_TYPE, OUT_TYPE) \
  FORCE_INLINE                              \
  OUT_TYPE NAME##_##IN_TYPE(IN_TYPE in) { return (OUT_TYPE)in; }

CAST_UNARY(castBIGINT, int32, int64)
CAST_UNARY(castFLOAT4, int32, float32)
CAST_UNARY(castFLOAT4, int64, float32)
CAST_UNARY(castFLOAT8, int32, float64)
CAST_UNARY(castFLOAT8, int64, float64)
CAST_UNARY(castFLOAT8, float32, float64)

// simple nullable functions, result value = fn(input validity)
#define VALIDITY_OP(NAME, TYPE, OP) \
  FORCE_INLINE                      \
  bool NAME##_##TYPE(TYPE in, boolean is_valid) { return OP is_valid; }

NUMERIC_BOOL_DATE_TYPES(VALIDITY_OP, isnull, !)
NUMERIC_BOOL_DATE_TYPES(VALIDITY_OP, isnotnull, +)
NUMERIC_TYPES(VALIDITY_OP, isnumeric, +)

#define NUMERIC_FUNCTION(INNER) \
  INNER(int8)                   \
  INNER(int16)                  \
  INNER(int32)                  \
  INNER(int64)                  \
  INNER(uint8)                  \
  INNER(uint16)                 \
  INNER(uint32)                 \
  INNER(uint64)                 \
  INNER(float32)                \
  INNER(float64)

#define DATE_FUNCTION(INNER) \
  INNER(date64)              \
  INNER(timestamp)           \
  INNER(time32)

#define NUMERIC_BOOL_DATE_FUNCTION(INNER) \
  NUMERIC_FUNCTION(INNER)                 \
  DATE_FUNCTION(INNER)                    \
  INNER(boolean)

FORCE_INLINE
boolean not_boolean(boolean in) { return !in; }

// is_distinct_from
#define IS_DISTINCT_FROM(TYPE)                                                 \
  FORCE_INLINE                                                                 \
  bool is_distinct_from_##TYPE##_##TYPE(TYPE in1, boolean is_valid1, TYPE in2, \
                                        boolean is_valid2) {                   \
    if (is_valid1 != is_valid2) {                                              \
      return true;                                                             \
    }                                                                          \
    if (!is_valid1) {                                                          \
      return false;                                                            \
    }                                                                          \
    return in1 != in2;                                                         \
  }

// is_not_distinct_from
#define IS_NOT_DISTINCT_FROM(TYPE)                                                 \
  FORCE_INLINE                                                                     \
  bool is_not_distinct_from_##TYPE##_##TYPE(TYPE in1, boolean is_valid1, TYPE in2, \
                                            boolean is_valid2) {                   \
    if (is_valid1 != is_valid2) {                                                  \
      return false;                                                                \
    }                                                                              \
    if (!is_valid1) {                                                              \
      return true;                                                                 \
    }                                                                              \
    return in1 == in2;                                                             \
  }

NUMERIC_BOOL_DATE_FUNCTION(IS_DISTINCT_FROM)
NUMERIC_BOOL_DATE_FUNCTION(IS_NOT_DISTINCT_FROM)

#define DIVIDE(TYPE)                                               \
  FORCE_INLINE                                                     \
  TYPE divide_##TYPE##_##TYPE(int64 context, TYPE in1, TYPE in2) { \
    if (in2 == 0) {                                                \
      char const* err_msg = "divide by zero error";                \
      gdv_fn_context_set_error_msg(context, err_msg);              \
      return 0;                                                    \
    }                                                              \
    return static_cast<TYPE>(in1 / in2);                           \
  }

NUMERIC_FUNCTION(DIVIDE)

#define DIV(TYPE)                                               \
  FORCE_INLINE                                                  \
  TYPE div_##TYPE##_##TYPE(int64 context, TYPE in1, TYPE in2) { \
    if (in2 == 0) {                                             \
      char const* err_msg = "divide by zero error";             \
      gdv_fn_context_set_error_msg(context, err_msg);           \
      return 0;                                                 \
    }                                                           \
    return static_cast<TYPE>(in1 / in2);                        \
  }

DIV(int32)
DIV(int64)

#define DIV_FLOAT(TYPE)                                         \
  FORCE_INLINE                                                  \
  TYPE div_##TYPE##_##TYPE(int64 context, TYPE in1, TYPE in2) { \
    if (in2 == 0) {                                             \
      char const* err_msg = "divide by zero error";             \
      gdv_fn_context_set_error_msg(context, err_msg);           \
      return 0;                                                 \
    }                                                           \
    return static_cast<TYPE>(::trunc(in1 / in2));               \
  }

DIV_FLOAT(float32)
DIV_FLOAT(float64)

}  // extern "C"
