/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

extern "C" {

#include "./types.h"

/*
 * Expand inner macro for all numeric types.
 */
#define NUMERIC_TYPES(INNER, NAME, OP) \
  INNER(NAME, int32, OP) \
  INNER(NAME, int64, OP) \
  INNER(NAME, float32, OP) \
  INNER(NAME, float64, OP)

#define NUMERIC_AND_BOOL_TYPES(INNER, NAME, OP) \
  NUMERIC_TYPES(INNER, NAME, OP) \
  INNER(NAME, boolean, OP)

#define BINARY_GENERIC_OP(NAME, IN_TYPE1, IN_TYPE2, OUT_TYPE, OP) \
  __attribute__((always_inline)) \
  OUT_TYPE NAME##_##IN_TYPE1##_##IN_TYPE2(IN_TYPE1 left, IN_TYPE2 right) { \
    return left OP right; \
  }

/*
 * Symmetric binary fns : left, right params and return type are same.
 */
#define BINARY_SYMMETRIC(NAME, TYPE, OP) \
  __attribute__((always_inline)) \
  TYPE NAME##_##TYPE##_##TYPE(TYPE left, TYPE right) { \
    return left OP right; \
  }

NUMERIC_TYPES(BINARY_SYMMETRIC, add, +)
NUMERIC_TYPES(BINARY_SYMMETRIC, subtract, -)
NUMERIC_TYPES(BINARY_SYMMETRIC, multiply, *)
NUMERIC_TYPES(BINARY_SYMMETRIC, divide, /)

BINARY_GENERIC_OP(mod, int64, int32, int32, %)
BINARY_GENERIC_OP(mod, int64, int64, int64, %)

/*
 * Relational binary fns : left, right params are same, return is bool.
 */
#define BINARY_RELATIONAL(NAME, TYPE, OP) \
  __attribute__((always_inline)) \
  bool NAME##_##TYPE##_##TYPE(TYPE left, TYPE right) { \
    return left OP right; \
  }

NUMERIC_TYPES(BINARY_RELATIONAL, equal, ==)
NUMERIC_TYPES(BINARY_RELATIONAL, not_equal, !=)
NUMERIC_TYPES(BINARY_RELATIONAL, less_than, <)
NUMERIC_TYPES(BINARY_RELATIONAL, less_than_or_equal_to, <=)
NUMERIC_TYPES(BINARY_RELATIONAL, greater_than, >)
NUMERIC_TYPES(BINARY_RELATIONAL, greater_than_or_equal_to, >=)

/*
 * cast fns : takes one param type, returns another type.
 */
#define CAST_UNARY(NAME, IN_TYPE, OUT_TYPE) \
  __attribute__((always_inline)) \
  OUT_TYPE NAME##_##IN_TYPE(IN_TYPE in) { \
    return (OUT_TYPE)in; \
  }

CAST_UNARY(castBIGINT, int32, int64)
CAST_UNARY(castFLOAT4, int32, float32)
CAST_UNARY(castFLOAT4, int64, float32)
CAST_UNARY(castFLOAT8, int32, float64)
CAST_UNARY(castFLOAT8, int64, float64)
CAST_UNARY(castFLOAT8, float32, float64)

/*
 * simple nullable functions, result value = fn(input validity)
 */
#define VALIDITY_OP(NAME, TYPE, OP) \
  __attribute__((always_inline)) \
  bool NAME##_##TYPE(TYPE in, boolean is_valid) { \
    return OP is_valid; \
  }

NUMERIC_AND_BOOL_TYPES(VALIDITY_OP, isnull, !)
NUMERIC_AND_BOOL_TYPES(VALIDITY_OP, isnotnull, +)
NUMERIC_TYPES(VALIDITY_OP, isnumeric, +)

} // extern "C"
