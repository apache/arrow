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

#include "gandiva/function_registry_arithmetic.h"

#include "gandiva/function_registry_common.h"

namespace gandiva {

#define BINARY_SYMMETRIC_FN(name, ALIASES) \
  NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, name, ALIASES)

#define BINARY_RELATIONAL_BOOL_FN(name, ALIASES) \
  NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, name, ALIASES)

#define BINARY_RELATIONAL_BOOL_DATE_FN(name, ALIASES) \
  NUMERIC_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, name, ALIASES)

#define UNARY_CAST_TO_FLOAT64(type) UNARY_SAFE_NULL_IF_NULL(castFLOAT8, {}, type, float64)

#define UNARY_CAST_TO_FLOAT32(type) UNARY_SAFE_NULL_IF_NULL(castFLOAT4, {}, type, float32)

#define UNARY_CAST_TO_INT32(type) UNARY_SAFE_NULL_IF_NULL(castINT, {}, type, int32)

#define UNARY_CAST_TO_INT64(type) UNARY_SAFE_NULL_IF_NULL(castBIGINT, {}, type, int64)

#define MULTIPLE_SAFE_NULL_IF_NULL(NAME, ALIASES, TYPE)                                 \
  NativeFunction(#NAME, std::vector<std::string> ALIASES,                               \
                 DataTypeVector{TYPE(), TYPE()}, TYPE(), kResultNullIfNull,             \
                 ARROW_STRINGIFY(NAME##_##TYPE##_##TYPE)),                              \
      NativeFunction(#NAME, std::vector<std::string> ALIASES,                           \
                     DataTypeVector{TYPE(), TYPE(), TYPE()}, TYPE(), kResultNullIfNull, \
                     ARROW_STRINGIFY(NAME##_##TYPE##_##TYPE##_##TYPE)),                 \
      NativeFunction(#NAME, std::vector<std::string> ALIASES,                           \
                     DataTypeVector{TYPE(), TYPE(), TYPE(), TYPE()}, TYPE(),            \
                     kResultNullIfNull,                                                 \
                     ARROW_STRINGIFY(NAME##_##TYPE##_##TYPE##_##TYPE##_##TYPE)),        \
      NativeFunction(                                                                   \
          #NAME, std::vector<std::string> ALIASES,                                      \
          DataTypeVector{TYPE(), TYPE(), TYPE(), TYPE(), TYPE()}, TYPE(),               \
          kResultNullIfNull,                                                            \
          ARROW_STRINGIFY(NAME##_##TYPE##_##TYPE##_##TYPE##_##TYPE##_##TYPE)),          \
      NativeFunction(                                                                   \
          #NAME, std::vector<std::string> ALIASES,                                      \
          DataTypeVector{TYPE(), TYPE(), TYPE(), TYPE(), TYPE(), TYPE()}, TYPE(),       \
          kResultNullIfNull,                                                            \
          ARROW_STRINGIFY(NAME##_##TYPE##_##TYPE##_##TYPE##_##TYPE##_##TYPE##_##TYPE))

std::vector<NativeFunction> GetArithmeticFunctionRegistry() {
  static std::vector<NativeFunction> arithmetic_fn_registry_ = {
      UNARY_SAFE_NULL_IF_NULL(not, {}, boolean, boolean),
      UNARY_SAFE_NULL_IF_NULL(castBIGINT, {}, int32, int64),
      UNARY_SAFE_NULL_IF_NULL(castINT, {}, int64, int32),
      UNARY_SAFE_NULL_IF_NULL(castBIGINT, {}, decimal128, int64),

      // cast to float32
      UNARY_CAST_TO_FLOAT32(int32), UNARY_CAST_TO_FLOAT32(int64),
      UNARY_CAST_TO_FLOAT32(float64),

      // cast to int32
      UNARY_CAST_TO_INT32(float32), UNARY_CAST_TO_INT32(float64),

      // cast to int64
      UNARY_CAST_TO_INT64(float32), UNARY_CAST_TO_INT64(float64),

      // cast to float64
      UNARY_CAST_TO_FLOAT64(int32), UNARY_CAST_TO_FLOAT64(int64),
      UNARY_CAST_TO_FLOAT64(float32), UNARY_CAST_TO_FLOAT64(decimal128),

      // cast to decimal
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, {"decimal"}, int32, decimal128),
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, {"decimal"}, int64, decimal128),
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, {"decimal"}, float32, decimal128),
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, {"decimal"}, float64, decimal128),
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, {"decimal"}, decimal128, decimal128),
      UNARY_UNSAFE_NULL_IF_NULL(castDECIMAL, {"decimal"}, utf8, decimal128),

      NativeFunction("castDECIMALNullOnOverflow", {}, DataTypeVector{decimal128()},
                     decimal128(), kResultNullInternal,
                     "castDECIMALNullOnOverflow_decimal128"),

      UNARY_SAFE_NULL_IF_NULL(castDATE, {}, int64, date64),
      UNARY_SAFE_NULL_IF_NULL(castDATE, {}, int32, date32),
      UNARY_SAFE_NULL_IF_NULL(castDATE, {}, date32, date64),

      // add/sub/multiply/divide/mod
      BINARY_SYMMETRIC_FN(add, {}), BINARY_SYMMETRIC_FN(subtract, {}),
      BINARY_SYMMETRIC_FN(multiply, {}),
      NUMERIC_TYPES(BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL, divide, {}),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, {"modulo"}, int64, int32, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, {"modulo"}, int64, int64, int64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, {"modulo"}, uint32, uint32, uint32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, {"modulo"}, uint64, uint64, uint64),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(mod, {"modulo"}, decimal128),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(mod, {"modulo"}, float64),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(pmod, {}, int32),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(pmod, {}, int64),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(pmod, {}, float32),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(pmod, {}, float64),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(div, {}, int32),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(div, {}, int64),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(div, {}, uint32),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(div, {}, uint64),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(div, {}, float32),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(div, {}, float64),

      // bitwise operators
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_and, {}, int32),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_and, {}, int64),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_or, {}, int32),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_or, {}, int64),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_xor, {"xor"}, int32),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(bitwise_xor, {"xor"}, int64),
      UNARY_SAFE_NULL_IF_NULL(bitwise_not, {}, int32, int32),
      UNARY_SAFE_NULL_IF_NULL(bitwise_not, {}, int64, int64),

      UNARY_SAFE_NULL_NEVER_BOOL(isnotfalse, ({"is not false"}), boolean),
      UNARY_SAFE_NULL_NEVER_BOOL(isnottrue, ({"is not true"}), boolean),
      UNARY_SAFE_NULL_NEVER_BOOL(isnotfalse, ({"is not false"}), int32),
      UNARY_SAFE_NULL_NEVER_BOOL(isnottrue, ({"is not true"}), int32),
      UNARY_SAFE_NULL_NEVER_BOOL(isnotfalse, ({"is not false"}), int64),
      UNARY_SAFE_NULL_NEVER_BOOL(isnottrue, ({"is not true"}), int64),
      UNARY_SAFE_NULL_NEVER_BOOL(isnotfalse, ({"is not false"}), float32),
      UNARY_SAFE_NULL_NEVER_BOOL(isnottrue, ({"is not true"}), float32),
      UNARY_SAFE_NULL_NEVER_BOOL(isnotfalse, ({"is not false"}), float64),
      UNARY_SAFE_NULL_NEVER_BOOL(isnottrue, ({"is not true"}), float64),

      UNARY_SAFE_NULL_NEVER_BOOL(istrue, ({"is true"}), boolean),
      UNARY_SAFE_NULL_NEVER_BOOL(isfalse, ({"is false"}), boolean),
      UNARY_SAFE_NULL_NEVER_BOOL(istrue, ({"is true"}), int32),
      UNARY_SAFE_NULL_NEVER_BOOL(isfalse, ({"is false"}), int32),
      UNARY_SAFE_NULL_NEVER_BOOL(istrue, ({"is true"}), int64),
      UNARY_SAFE_NULL_NEVER_BOOL(isfalse, ({"is false"}), int64),
      UNARY_SAFE_NULL_NEVER_BOOL(istrue, ({"is true"}), float32),
      UNARY_SAFE_NULL_NEVER_BOOL(isfalse, ({"is false"}), float32),
      UNARY_SAFE_NULL_NEVER_BOOL(istrue, ({"is true"}), float64),
      UNARY_SAFE_NULL_NEVER_BOOL(isfalse, ({"is false"}), float64),

      // round functions
      UNARY_SAFE_NULL_IF_NULL(round, {}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(round, {}, float64, float64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(round, {}, float32, int32, float32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(round, {}, float64, int32, float64),
      UNARY_SAFE_NULL_IF_NULL(round, {}, int32, int32),
      UNARY_SAFE_NULL_IF_NULL(round, {}, int64, int64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(round, {}, int32, int32, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(round, {}, int64, int32, int64),

      // bround functions
      NativeFunction("bround", {}, DataTypeVector{float64()}, float64(),
                     kResultNullIfNull, "bround_float64"),

      // positive and negative functions
      UNARY_SAFE_NULL_IF_NULL(positive, {}, int32, int32),
      UNARY_SAFE_NULL_IF_NULL(positive, {}, int64, int64),
      UNARY_SAFE_NULL_IF_NULL(positive, {}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(positive, {}, float64, float64),
      UNARY_SAFE_NULL_IF_NULL(negative, {}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(negative, {}, float64, float64),

      NativeFunction("negative", {}, DataTypeVector{int32()}, int32(), kResultNullIfNull,
                     "negative_int32",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("negative", {}, DataTypeVector{int64()}, int64(), kResultNullIfNull,
                     "negative_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("negative", {}, DataTypeVector{decimal128()}, decimal128(),
                     kResultNullIfNull, "negative_decimal",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("negative", {}, DataTypeVector{month_interval()}, month_interval(),
                     kResultNullIfNull, "negative_month_interval",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("negative", {}, DataTypeVector{day_time_interval()},
                     day_time_interval(), kResultNullIfNull, "negative_daytimeinterval",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      // sign functions
      UNARY_SAFE_NULL_IF_NULL(sign, {}, int32, int32),
      UNARY_SAFE_NULL_IF_NULL(sign, {}, int64, int64),
      UNARY_SAFE_NULL_IF_NULL(sign, {}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(sign, {}, float64, float64),
      // abs functions
      UNARY_SAFE_NULL_IF_NULL(abs, {}, int32, int32),
      UNARY_SAFE_NULL_IF_NULL(abs, {}, int64, int64),
      UNARY_SAFE_NULL_IF_NULL(abs, {}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(abs, {}, float64, float64),
      // ceil functions
      UNARY_SAFE_NULL_IF_NULL(ceiling, {"ceil"}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(ceiling, {"ceil"}, float64, float64),
      // floor functions
      UNARY_SAFE_NULL_IF_NULL(floor, {}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(floor, {}, float64, float64),
      // sqrt functions
      UNARY_SAFE_NULL_IF_NULL(sqrt, {}, int32, float64),
      UNARY_SAFE_NULL_IF_NULL(sqrt, {}, int64, float64),
      UNARY_SAFE_NULL_IF_NULL(sqrt, {}, float32, float64),
      UNARY_SAFE_NULL_IF_NULL(sqrt, {}, float64, float64),

      // compare functions
      BINARY_RELATIONAL_BOOL_FN(equal, ({"eq", "same"})),
      BINARY_RELATIONAL_BOOL_FN(not_equal, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(less_than, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(less_than_or_equal_to, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(greater_than, {}),
      BINARY_RELATIONAL_BOOL_DATE_FN(greater_than_or_equal_to, {}),
      BASE_NUMERIC_TYPES(MULTIPLE_SAFE_NULL_IF_NULL, greatest, {}),
      BASE_NUMERIC_TYPES(MULTIPLE_SAFE_NULL_IF_NULL, least, {}),

      // binary representation of integer values
      UNARY_UNSAFE_NULL_IF_NULL(bin, {}, int32, utf8),
      UNARY_UNSAFE_NULL_IF_NULL(bin, {}, int64, utf8)};

  return arithmetic_fn_registry_;
}

}  // namespace gandiva
