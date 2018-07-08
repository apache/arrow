// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "codegen/function_registry.h"

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
#define BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(NAME, TYPE)                \
  NativeFunction(#NAME, DataTypeVector{TYPE(), TYPE()}, TYPE(), true, \
                 RESULT_NULL_IF_NULL, STRINGIFY(NAME##_##TYPE##_##TYPE))

// Binary functions that :
// - have different input types, or output type
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type names. eg. mod_int64_int32
#define BINARY_GENERIC_SAFE_NULL_IF_NULL(NAME, IN_TYPE1, IN_TYPE2, OUT_TYPE)      \
  NativeFunction(#NAME, DataTypeVector{IN_TYPE1(), IN_TYPE2()}, OUT_TYPE(), true, \
                 RESULT_NULL_IF_NULL, STRINGIFY(NAME##_##IN_TYPE1##_##IN_TYPE2))

// Binary functions that :
// - have the same input type
// - output type is boolean
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type names.
// eg. equal_int32_int32
#define BINARY_RELATIONAL_SAFE_NULL_IF_NULL(NAME, TYPE)                  \
  NativeFunction(#NAME, DataTypeVector{TYPE(), TYPE()}, boolean(), true, \
                 RESULT_NULL_IF_NULL, STRINGIFY(NAME##_##TYPE##_##TYPE))

// Unary functions that :
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type name. eg. castFloat_int32
#define UNARY_SAFE_NULL_IF_NULL(NAME, IN_TYPE, OUT_TYPE)             \
  NativeFunction(#NAME, DataTypeVector{IN_TYPE()}, OUT_TYPE(), true, \
                 RESULT_NULL_IF_NULL, STRINGIFY(NAME##_##IN_TYPE))

// Unary functions that :
// - NULL handling is of type NULL_NEVER
//
// The pre-compiled fn name includes the base name & input type name. eg. isnull_int32
#define UNARY_SAFE_NULL_NEVER_BOOL(NAME, TYPE)                                      \
  NativeFunction(#NAME, DataTypeVector{TYPE()}, boolean(), true, RESULT_NULL_NEVER, \
                 STRINGIFY(NAME##_##TYPE))

// Extract functions (used with data/time types) that :
// - NULL handling is of type NULL_IF_NULL
//
// The pre-compiled fn name includes the base name & input type name. eg. extractYear_date
#define EXTRACT_SAFE_NULL_IF_NULL(NAME, TYPE)                                       \
  NativeFunction(#NAME, DataTypeVector{TYPE()}, int64(), true, RESULT_NULL_IF_NULL, \
                 STRINGIFY(NAME##_##TYPE))

// Iterate the inner macro over all numeric types
#define NUMERIC_TYPES(INNER, NAME)                                                       \
  INNER(NAME, int8), INNER(NAME, int16), INNER(NAME, int32), INNER(NAME, int64),         \
      INNER(NAME, uint8), INNER(NAME, uint16), INNER(NAME, uint32), INNER(NAME, uint64), \
      INNER(NAME, float32), INNER(NAME, float64)

// Iterate the inner macro over all numeric types and bool type
#define NUMERIC_AND_BOOL_TYPES(INNER, NAME) \
  NUMERIC_TYPES(INNER, NAME), INNER(NAME, boolean)

// Iterate the inner macro over all data types
#define DATE_TYPES(INNER, NAME) INNER(NAME, date64), INNER(NAME, timestamp)

#define TIME_TYPES(INNER, NAME) INNER(NAME, time32)

// Iterate the inner macro over all data types
#define VAR_LEN_TYPES(INNER, NAME) INNER(NAME, utf8), INNER(NAME, binary)

// list of registered native functions.
NativeFunction FunctionRegistry::pc_registry_[] = {
    // Arithmetic operations
    NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, add),
    NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, subtract),
    NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, multiply),
    NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, divide),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, int64, int32, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, int64, int64, int64),
    NUMERIC_AND_BOOL_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, equal),
    NUMERIC_AND_BOOL_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, not_equal),
    NUMERIC_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, less_than),
    NUMERIC_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, less_than_or_equal_to),
    NUMERIC_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, greater_than),
    NUMERIC_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, greater_than_or_equal_to),

    // cast operations
    UNARY_SAFE_NULL_IF_NULL(castBIGINT, int32, int64),
    UNARY_SAFE_NULL_IF_NULL(castFLOAT4, int32, float32),
    UNARY_SAFE_NULL_IF_NULL(castFLOAT4, int64, float32),
    UNARY_SAFE_NULL_IF_NULL(castFLOAT8, int32, float64),
    UNARY_SAFE_NULL_IF_NULL(castFLOAT8, int64, float64),
    UNARY_SAFE_NULL_IF_NULL(castFLOAT8, float32, float64),

    // nullable never operations
    NUMERIC_AND_BOOL_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, isnull),
    NUMERIC_AND_BOOL_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, isnotnull),
    NUMERIC_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, isnumeric),

    // date/timestamp operations
    DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractYear),
    DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractMonth),
    DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractDay),
    DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractHour),
    DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractMinute),

    // time operations
    TIME_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractHour),
    TIME_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractMinute),

    // timestamp diff operations
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffSecond, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffMinute, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffHour, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffDay, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffWeek, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffMonth, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffQuarter, timestamp, timestamp, int32),
    BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffYear, timestamp, timestamp, int32),

    // utf8/binary operations
    UNARY_SAFE_NULL_IF_NULL(octet_length, utf8, int32),
    UNARY_SAFE_NULL_IF_NULL(octet_length, binary, int32),
    UNARY_SAFE_NULL_IF_NULL(bit_length, utf8, int32),
    UNARY_SAFE_NULL_IF_NULL(bit_length, binary, int32),
    VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, equal),
    VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, not_equal),
    VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, less_than),
    VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, less_than_or_equal_to),
    VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, greater_than),
    VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, greater_than_or_equal_to),

    // Null internal (sample)
    NativeFunction("half_or_null", DataTypeVector{int32()}, int32(), true,
                   RESULT_NULL_INTERNAL, "half_or_null_int32"),
};

FunctionRegistry::iterator FunctionRegistry::begin() const {
  return std::begin(pc_registry_);
}

FunctionRegistry::iterator FunctionRegistry::end() const {
  return std::end(pc_registry_);
}

FunctionRegistry::SignatureMap FunctionRegistry::pc_registry_map_ = InitPCMap();

FunctionRegistry::SignatureMap FunctionRegistry::InitPCMap() {
  SignatureMap map;

  int num_entries = sizeof(pc_registry_) / sizeof(NativeFunction);
  printf("Registry has %d pre-compiled functions\n", num_entries);

  for (int i = 0; i < num_entries; i++) {
    const NativeFunction *entry = &pc_registry_[i];

    DCHECK(map.find(&entry->signature()) == map.end());
    map[&entry->signature()] = entry;
    // printf("%s -> %s\n", entry->signature().ToString().c_str(),
    //      entry->pc_name().c_str());
  }
  return map;
}

const NativeFunction *FunctionRegistry::LookupSignature(
    const FunctionSignature &signature) const {
  auto got = pc_registry_map_.find(&signature);
  return got == pc_registry_map_.end() ? NULL : got->second;
}

}  // namespace gandiva
