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

#include "gandiva/function_registry_binaryfn.h"

#include <memory>
#include <vector>

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
using std::vector;

#define STRINGIFY(a) #a

// Binary functions that :
// - have the same input type for both params
// - output type is same as the input type
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type names. eg. add_int32_int32
#define BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(NAME, TYPE)                             \
  NativeFunction(#NAME, DataTypeVector{TYPE(), TYPE()}, TYPE(), kResultNullIfNull, \
                 STRINGIFY(NAME##_##TYPE##_##TYPE))

// Binary functions that :
// - have the same input type for both params
// - NULL handling is of type NULL_IINTERNAL
// - can return error.
//
// The pre-compiled fn name includes the base name & input type names. eg. add_int32_int32
#define BINARY_UNSAFE_NULL_IF_NULL(NAME, IN_TYPE, OUT_TYPE)                  \
  NativeFunction(#NAME, DataTypeVector{IN_TYPE(), IN_TYPE()}, OUT_TYPE(),    \
                 kResultNullIfNull, STRINGIFY(NAME##_##IN_TYPE##_##IN_TYPE), \
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
                 kResultNullIfNull, STRINGIFY(NAME##_##IN_TYPE1##_##IN_TYPE2))

// Binary functions that :
// - have the same input type
// - output type is boolean
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type names.
// eg. equal_int32_int32
#define BINARY_RELATIONAL_SAFE_NULL_IF_NULL(NAME, TYPE)                               \
  NativeFunction(#NAME, DataTypeVector{TYPE(), TYPE()}, boolean(), kResultNullIfNull, \
                 STRINGIFY(NAME##_##TYPE##_##TYPE))

// Unary functions that :
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type name. eg. castFloat_int32
#define UNARY_SAFE_NULL_IF_NULL(NAME, IN_TYPE, OUT_TYPE)                          \
  NativeFunction(#NAME, DataTypeVector{IN_TYPE()}, OUT_TYPE(), kResultNullIfNull, \
                 STRINGIFY(NAME##_##IN_TYPE))

// Unary functions that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type name. eg. isnull_int32
#define UNARY_SAFE_NULL_NEVER_BOOL(NAME, TYPE)                               \
  NativeFunction(#NAME, DataTypeVector{TYPE()}, boolean(), kResultNullNever, \
                 STRINGIFY(NAME##_##TYPE))

// Unary functions that :
// - NULL handling is of type NULL_INTERNAL
//
// The pre-compiled fn name includes the base name & input type name. eg. castFloat_int32
#define UNARY_UNSAFE_NULL_IF_NULL(NAME, IN_TYPE, OUT_TYPE)                        \
  NativeFunction(#NAME, DataTypeVector{IN_TYPE()}, OUT_TYPE(), kResultNullIfNull, \
                 STRINGIFY(NAME##_##IN_TYPE),                                     \
                 NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors)

// Binary functions that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type names,
// eg. is_distinct_from_int32_int32
#define BINARY_SAFE_NULL_NEVER_BOOL(NAME, TYPE)                                      \
  NativeFunction(#NAME, DataTypeVector{TYPE(), TYPE()}, boolean(), kResultNullNever, \
                 STRINGIFY(NAME##_##TYPE##_##TYPE))

// Extract functions (used with data/time types) that :
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type name. eg. extractYear_date
#define EXTRACT_SAFE_NULL_IF_NULL(NAME, TYPE)                               \
  NativeFunction(#NAME, DataTypeVector{TYPE()}, int64(), kResultNullIfNull, \
                 STRINGIFY(NAME##_##TYPE))

// Hash32 functions that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type name. hash32_int8
#define HASH32_SAFE_NULL_NEVER(NAME, TYPE)                                 \
  NativeFunction(#NAME, DataTypeVector{TYPE()}, int32(), kResultNullNever, \
                 STRINGIFY(NAME##_##TYPE))

// Hash32 functions that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type name. hash32_int8
#define HASH64_SAFE_NULL_NEVER(NAME, TYPE)                                 \
  NativeFunction(#NAME, DataTypeVector{TYPE()}, int64(), kResultNullNever, \
                 STRINGIFY(NAME##_##TYPE))

// Hash32 functions with seed that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type name. hash32WithSeed_int8
#define HASH32_SEED_SAFE_NULL_NEVER(NAME, TYPE)                                     \
  NativeFunction(#NAME, DataTypeVector{TYPE(), int32()}, int32(), kResultNullNever, \
                 STRINGIFY(NAME##WithSeed_##TYPE))

// Hash64 functions with seed that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type name. hash32WithSeed_int8
#define HASH64_SEED_SAFE_NULL_NEVER(NAME, TYPE)                                     \
  NativeFunction(#NAME, DataTypeVector{TYPE(), int64()}, int64(), kResultNullNever, \
                 STRINGIFY(NAME##WithSeed_##TYPE))

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



FunctionRegistryBin::SignatureMap& FunctionRegistryBin::GetBinaryFnSignature() {
  static NativeFunction binary_fn_registry_[] = {
    BINARY_UNSAFE_NULL_IF_NULL(log, int32, float64),
    BINARY_UNSAFE_NULL_IF_NULL(log, int64, float64),
    BINARY_UNSAFE_NULL_IF_NULL(log, uint32, float64),
    BINARY_UNSAFE_NULL_IF_NULL(log, uint64, float64),
    BINARY_UNSAFE_NULL_IF_NULL(log, float32, float64),
    BINARY_UNSAFE_NULL_IF_NULL(log, float64, float64),

    BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(power, float64),

    BINARY_GENERIC_SAFE_NULL_IF_NULL(months_between, date64, date64, float64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(months_between, timestamp, timestamp, float64),

    // timestamp diff operations
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffSecond, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffMinute, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffHour, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffDay, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffWeek, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffMonth, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffQuarter, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffYear, timestamp, timestamp, int32),

    // timestamp add int32 operations
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddSecond, timestamp, int32, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMinute, timestamp, int32, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddHour, timestamp, int32, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddDay, timestamp, int32, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddWeek, timestamp, int32, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMonth, timestamp, int32, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddQuarter, timestamp, int32, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddYear, timestamp, int32, timestamp),
    // date add int32 operations
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddSecond, date64, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMinute, date64, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddHour, date64, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddDay, date64, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddWeek, date64, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMonth, date64, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddQuarter, date64, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddYear, date64, int32, date64),

    // timestamp add int64 operations
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddSecond, timestamp, int64, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMinute, timestamp, int64, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddHour, timestamp, int64, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddDay, timestamp, int64, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddWeek, timestamp, int64, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMonth, timestamp, int64, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddQuarter, timestamp, int64, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddYear, timestamp, int64, timestamp),
    // date add int64 operations
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddSecond, date64, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMinute, date64, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddHour, date64, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddDay, date64, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddWeek, date64, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMonth, date64, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddQuarter, date64, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddYear, date64, int64, date64),

    // date_add(date64, int32), date_add(timestamp, int32)
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, date64, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(add, date64, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, timestamp, int32, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(add, timestamp, int32, timestamp),

    // date_add(date64, int64), date_add(timestamp, int64)
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, date64, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(add, date64, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, timestamp, int64, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(add, timestamp, int64, timestamp),

    // date_add(int32, date64), date_add(int32, timestamp)
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, int32, date64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(add, int32, date64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, int32, timestamp, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(add, int32, timestamp, timestamp),

    // date_add(int64, date64), date_add(int64, timestamp)
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, int64, date64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(add, int64, date64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, int64, timestamp, timestamp),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(add, int64, timestamp, timestamp),

    // date_sub(date64, int32), subtract and date_diff
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_sub, date64, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(subtract, date64, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_diff, date64, int32, date64),
    // date_sub(timestamp, int32), subtract and date_diff
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_sub, timestamp, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(subtract, timestamp, int32, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_diff, timestamp, int32, date64),

    // date_sub(date64, int64), subtract and date_diff
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_sub, date64, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(subtract, date64, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_diff, date64, int64, date64),
    // date_sub(timestamp, int64), subtract and date_diff
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_sub, timestamp, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(subtract, timestamp, int64, date64),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(date_diff, timestamp, int64, date64),


    BINARY_RELATIONAL_SAFE_NULL_IF_NULL(starts_with, utf8),
    BINARY_RELATIONAL_SAFE_NULL_IF_NULL(ends_with, utf8)
  };

  static FunctionRegistryBin::SignatureMap map;

  const int num_entries =
      static_cast<int>(sizeof(binary_fn_registry_) / sizeof(NativeFunction));
  for (int i = 0; i < num_entries; i++) {
    const NativeFunction* entry = &binary_fn_registry_[i];

    DCHECK(map.find(&entry->signature()) == map.end());
    map[&entry->signature()] = entry;
  }

  return map;
}

}  // namespace gandiva
