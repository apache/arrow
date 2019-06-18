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

#ifndef GANDIVA_FUNCTION_REGISTRY_COMMON_H
#define GANDIVA_FUNCTION_REGISTRY_COMMON_H

#include <memory>
#include <unordered_map>
#include <vector>

#include "gandiva/arrow.h"
#include "gandiva/function_signature.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/native_function.h"

/* This is a private file, intended for internal use by gandiva & must not be included
 * directly.
 */
namespace gandiva {

using arrow::binary;
using arrow::boolean;
using arrow::date64;
using arrow::float32;
using arrow::float64;
using arrow::int16;
using arrow::int32;
using arrow::int64;
using arrow::int8;
using arrow::uint16;
using arrow::uint32;
using arrow::uint64;
using arrow::uint8;
using arrow::utf8;

inline DataTypePtr time32() { return arrow::time32(arrow::TimeUnit::MILLI); }

inline DataTypePtr time64() { return arrow::time64(arrow::TimeUnit::MICRO); }

inline DataTypePtr timestamp() { return arrow::timestamp(arrow::TimeUnit::MILLI); }
inline DataTypePtr decimal128() { return arrow::decimal(38, 0); }

struct KeyHash {
  std::size_t operator()(const FunctionSignature* k) const { return k->Hash(); }
};

struct KeyEquals {
  bool operator()(const FunctionSignature* s1, const FunctionSignature* s2) const {
    return *s1 == *s2;
  }
};

typedef std::unordered_map<const FunctionSignature*, const NativeFunction*, KeyHash,
                           KeyEquals>
    SignatureMap;

// Binary functions that :
// - have the same input type for both params
// - output type is same as the input type
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type names. eg. add_int32_int32
#define BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(NAME, TYPE)                             \
  NativeFunction(#NAME, DataTypeVector{TYPE(), TYPE()}, TYPE(), kResultNullIfNull, \
                 ARROW_STRINGIFY(NAME##_##TYPE##_##TYPE))

// Binary functions that :
// - have the same input type for both params
// - NULL handling is of type NULL_IINTERNAL
// - can return error.
//
// The pre-compiled fn name includes the base name & input type names. eg. add_int32_int32
#define BINARY_UNSAFE_NULL_IF_NULL(NAME, IN_TYPE, OUT_TYPE)                        \
  NativeFunction(#NAME, DataTypeVector{IN_TYPE(), IN_TYPE()}, OUT_TYPE(),          \
                 kResultNullIfNull, ARROW_STRINGIFY(NAME##_##IN_TYPE##_##IN_TYPE), \
                 NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors)

#define BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(NAME, TYPE) \
  BINARY_UNSAFE_NULL_IF_NULL(NAME, TYPE, TYPE)

// Binary functions that :
// - have different input types, or output type
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type names. eg. mod_int64_int32
#define BINARY_GENERIC_SAFE_NULL_IF_NULL(NAME, IN_TYPE1, IN_TYPE2, OUT_TYPE) \
  NativeFunction(#NAME, DataTypeVector{IN_TYPE1(), IN_TYPE2()}, OUT_TYPE(),  \
                 kResultNullIfNull, ARROW_STRINGIFY(NAME##_##IN_TYPE1##_##IN_TYPE2))

// Binary functions that :
// - have the same input type
// - output type is boolean
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type names.
// eg. equal_int32_int32
#define BINARY_RELATIONAL_SAFE_NULL_IF_NULL(NAME, TYPE)                               \
  NativeFunction(#NAME, DataTypeVector{TYPE(), TYPE()}, boolean(), kResultNullIfNull, \
                 ARROW_STRINGIFY(NAME##_##TYPE##_##TYPE))

// Unary functions that :
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type name. eg. castFloat_int32
#define UNARY_SAFE_NULL_IF_NULL(NAME, IN_TYPE, OUT_TYPE)                          \
  NativeFunction(#NAME, DataTypeVector{IN_TYPE()}, OUT_TYPE(), kResultNullIfNull, \
                 ARROW_STRINGIFY(NAME##_##IN_TYPE))

// Unary functions that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type name. eg. isnull_int32
#define UNARY_SAFE_NULL_NEVER_BOOL(NAME, TYPE)                               \
  NativeFunction(#NAME, DataTypeVector{TYPE()}, boolean(), kResultNullNever, \
                 ARROW_STRINGIFY(NAME##_##TYPE))

// Unary functions that :
// - NULL handling is of type NULL_INTERNAL
//
// The pre-compiled fn name includes the base name & input type name. eg. castFloat_int32
#define UNARY_UNSAFE_NULL_IF_NULL(NAME, IN_TYPE, OUT_TYPE)                        \
  NativeFunction(#NAME, DataTypeVector{IN_TYPE()}, OUT_TYPE(), kResultNullIfNull, \
                 ARROW_STRINGIFY(NAME##_##IN_TYPE),                               \
                 NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors)

// Binary functions that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type names,
// eg. is_distinct_from_int32_int32
#define BINARY_SAFE_NULL_NEVER_BOOL(NAME, TYPE)                                      \
  NativeFunction(#NAME, DataTypeVector{TYPE(), TYPE()}, boolean(), kResultNullNever, \
                 ARROW_STRINGIFY(NAME##_##TYPE##_##TYPE))

// Extract functions (used with data/time types) that :
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type name. eg. extractYear_date
#define EXTRACT_SAFE_NULL_IF_NULL(NAME, TYPE)                               \
  NativeFunction(#NAME, DataTypeVector{TYPE()}, int64(), kResultNullIfNull, \
                 ARROW_STRINGIFY(NAME##_##TYPE))

// Hash32 functions that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type name. hash32_int8
#define HASH32_SAFE_NULL_NEVER(NAME, TYPE)                                 \
  NativeFunction(#NAME, DataTypeVector{TYPE()}, int32(), kResultNullNever, \
                 ARROW_STRINGIFY(NAME##_##TYPE))

// Hash32 functions that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type name. hash32_int8
#define HASH64_SAFE_NULL_NEVER(NAME, TYPE)                                 \
  NativeFunction(#NAME, DataTypeVector{TYPE()}, int64(), kResultNullNever, \
                 ARROW_STRINGIFY(NAME##_##TYPE))

// Hash32 functions with seed that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type name. hash32WithSeed_int8
#define HASH32_SEED_SAFE_NULL_NEVER(NAME, TYPE)                                     \
  NativeFunction(#NAME, DataTypeVector{TYPE(), int32()}, int32(), kResultNullNever, \
                 ARROW_STRINGIFY(NAME##WithSeed_##TYPE))

// Hash64 functions with seed that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type name. hash32WithSeed_int8
#define HASH64_SEED_SAFE_NULL_NEVER(NAME, TYPE)                                     \
  NativeFunction(#NAME, DataTypeVector{TYPE(), int64()}, int64(), kResultNullNever, \
                 ARROW_STRINGIFY(NAME##WithSeed_##TYPE))

// Iterate the inner macro over all numeric types
#define NUMERIC_TYPES(INNER, NAME)                                                       \
  INNER(NAME, int8), INNER(NAME, int16), INNER(NAME, int32), INNER(NAME, int64),         \
      INNER(NAME, uint8), INNER(NAME, uint16), INNER(NAME, uint32), INNER(NAME, uint64), \
      INNER(NAME, float32), INNER(NAME, float64)

// Iterate the inner macro over numeric and date/time types
#define NUMERIC_DATE_TYPES(INNER, NAME) \
  NUMERIC_TYPES(INNER, NAME), DATE_TYPES(INNER, NAME), TIME_TYPES(INNER, NAME)

// Iterate the inner macro over all date types
#define DATE_TYPES(INNER, NAME) INNER(NAME, date64), INNER(NAME, timestamp)

// Iterate the inner macro over all time types
#define TIME_TYPES(INNER, NAME) INNER(NAME, time32)

// Iterate the inner macro over all data types
#define VAR_LEN_TYPES(INNER, NAME) INNER(NAME, utf8), INNER(NAME, binary)

// Iterate the inner macro over all numeric types, date types and bool type
#define NUMERIC_BOOL_DATE_TYPES(INNER, NAME) \
  NUMERIC_DATE_TYPES(INNER, NAME), INNER(NAME, boolean)

// Iterate the inner macro over all numeric types, date types, bool and varlen types
#define NUMERIC_BOOL_DATE_VAR_LEN_TYPES(INNER, NAME) \
  NUMERIC_BOOL_DATE_TYPES(INNER, NAME), VAR_LEN_TYPES(INNER, NAME)

}  // namespace gandiva

#endif
