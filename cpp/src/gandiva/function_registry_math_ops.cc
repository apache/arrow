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

#include "gandiva/function_registry_math_ops.h"
#include "gandiva/function_registry_common.h"

namespace gandiva {

#define MATH_UNARY_OPS(name, ALIASES)                           \
  UNARY_SAFE_NULL_IF_NULL(name, ALIASES, int32, float64),       \
      UNARY_SAFE_NULL_IF_NULL(name, ALIASES, int64, float64),   \
      UNARY_SAFE_NULL_IF_NULL(name, ALIASES, uint32, float64),  \
      UNARY_SAFE_NULL_IF_NULL(name, ALIASES, uint64, float64),  \
      UNARY_SAFE_NULL_IF_NULL(name, ALIASES, float32, float64), \
      UNARY_SAFE_NULL_IF_NULL(name, ALIASES, float64, float64)

#define MATH_UNARY_OPS_SAME_TYPE_RETURN(name, ALIASES)          \
  UNARY_SAFE_NULL_IF_NULL(name, ALIASES, int32, int32),         \
      UNARY_SAFE_NULL_IF_NULL(name, ALIASES, int64, int64),     \
      UNARY_SAFE_NULL_IF_NULL(name, ALIASES, uint32, uint32),   \
      UNARY_SAFE_NULL_IF_NULL(name, ALIASES, uint64, uint64),   \
      UNARY_SAFE_NULL_IF_NULL(name, ALIASES, float32, float32), \
      UNARY_SAFE_NULL_IF_NULL(name, ALIASES, float64, float64)

#define STUBS_MATH_UNARY_OPS_SAME_TYPE_RETURN(name, ALIASES)          \
  STUBS_UNARY_SAFE_NULL_IF_NULL(name, ALIASES, int32, int32),         \
      STUBS_UNARY_SAFE_NULL_IF_NULL(name, ALIASES, int64, int64),     \
      STUBS_UNARY_SAFE_NULL_IF_NULL(name, ALIASES, uint32, uint32),   \
      STUBS_UNARY_SAFE_NULL_IF_NULL(name, ALIASES, uint64, uint64),   \
      STUBS_UNARY_SAFE_NULL_IF_NULL(name, ALIASES, float32, float32), \
      STUBS_UNARY_SAFE_NULL_IF_NULL(name, ALIASES, float64, float64)

#define MATH_BINARY_UNSAFE(name, ALIASES)                          \
  BINARY_UNSAFE_NULL_IF_NULL(name, ALIASES, int32, float64),       \
      BINARY_UNSAFE_NULL_IF_NULL(name, ALIASES, int64, float64),   \
      BINARY_UNSAFE_NULL_IF_NULL(name, ALIASES, uint32, float64),  \
      BINARY_UNSAFE_NULL_IF_NULL(name, ALIASES, uint64, float64),  \
      BINARY_UNSAFE_NULL_IF_NULL(name, ALIASES, float32, float64), \
      BINARY_UNSAFE_NULL_IF_NULL(name, ALIASES, float64, float64)

#define MATH_BINARY_SAFE(name, ALIASES)                                           \
  BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int32, int32, float64),         \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int64, int64, float64),     \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, uint32, uint32, float64),   \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, uint64, uint64, float64),   \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, float32, float32, float64), \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, float64, float64, float64)

#define UNARY_SAFE_NULL_NEVER_BOOL_FN(name, ALIASES) \
  NUMERIC_BOOL_DATE_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, name, ALIASES)

#define BINARY_SAFE_NULL_NEVER_BOOL_FN(name, ALIASES) \
  NUMERIC_BOOL_DATE_TYPES(BINARY_SAFE_NULL_NEVER_BOOL, name, ALIASES)

#define BINARY_SYMMETRIC_SAFE_NULL_NEVER_FN(name, ALIASES) \
  BINARY_SAFE_NULL_NEVER(name, ALIASES, int32),            \
      BINARY_SAFE_NULL_NEVER(name, ALIASES, int64),        \
      BINARY_SAFE_NULL_NEVER(name, ALIASES, uint32),       \
      BINARY_SAFE_NULL_NEVER(name, ALIASES, uint64),       \
      BINARY_SAFE_NULL_NEVER(name, ALIASES, float32),      \
      BINARY_SAFE_NULL_NEVER(name, ALIASES, float64),      \
      BINARY_SAFE_NULL_NEVER(name, ALIASES, boolean)

std::vector<NativeFunction> GetMathOpsFunctionRegistry() {
  static std::vector<NativeFunction> math_fn_registry_ = {
      MATH_UNARY_OPS(cbrt, {}), MATH_UNARY_OPS(exp, {}), MATH_UNARY_OPS(log, {"ln"}),
      MATH_UNARY_OPS(log2, {}), MATH_UNARY_OPS(log10, {}),

      MATH_BINARY_UNSAFE(log, {}),

      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(power, {"pow"}, float64),

      UNARY_SAFE_NULL_NEVER_BOOL_FN(isnull, {}),
      UNARY_SAFE_NULL_NEVER_BOOL_FN(isnotnull, {}),

      NUMERIC_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, isnumeric, {}),

      BINARY_SAFE_NULL_NEVER_BOOL_FN(is_distinct_from, {}),
      BINARY_SAFE_NULL_NEVER_BOOL_FN(is_not_distinct_from, {}),

      UNARY_UNSAFE_NULL_IF_NULL(factorial, {}, int32, int64),
      UNARY_UNSAFE_NULL_IF_NULL(factorial, {}, int64, int64),

      // trigonometry functions
      MATH_UNARY_OPS(sin, {}), MATH_UNARY_OPS(cos, {}), MATH_UNARY_OPS(asin, {}),
      MATH_UNARY_OPS(acos, {}), MATH_UNARY_OPS(tan, {}), MATH_UNARY_OPS(atan, {}),
      MATH_UNARY_OPS(sinh, {}), MATH_UNARY_OPS(cosh, {}), MATH_UNARY_OPS(tanh, {}),
      MATH_UNARY_OPS(cot, {}), MATH_UNARY_OPS(radians, {}), MATH_UNARY_OPS(degrees, {}),
      MATH_BINARY_SAFE(atan2, {}),

      // extended functions
      MATH_UNARY_OPS(sqrt, {}), STUBS_MATH_UNARY_OPS_SAME_TYPE_RETURN(abs, {}),
      MATH_UNARY_OPS_SAME_TYPE_RETURN(sign, {}),
      UNARY_SAFE_NULL_IF_NULL(ceil, {}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(ceil, {}, float64, float64),
      UNARY_SAFE_NULL_IF_NULL(floor, {}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(floor, {}, float64, float64),
      UNARY_SAFE_NULL_IF_NULL(lshift, {"shiftleft"}, int32, int32),
      UNARY_SAFE_NULL_IF_NULL(lshift, {"shiftleft"}, int64, int64),
      UNARY_SAFE_NULL_IF_NULL(rshift, {"shiftright"}, int32, int32),
      UNARY_SAFE_NULL_IF_NULL(rshift, {"shiftright"}, int64, int64),
      UNARY_SAFE_NULL_IF_NULL(rshift, {"shiftrightunsigned"}, uint32, uint32),
      UNARY_SAFE_NULL_IF_NULL(rshift, {"shiftrightunsigned"}, uint64, uint64),
      UNARY_SAFE_NULL_IF_NULL(truncate, {"trunc"}, int32, int32),
      UNARY_SAFE_NULL_IF_NULL(truncate, {"trunc"}, int64, int64),
      UNARY_SAFE_NULL_IF_NULL(truncate, {"trunc"}, float32, float32),
      UNARY_SAFE_NULL_IF_NULL(truncate, {"trunc"}, float64, float64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(truncate, {"trunc"}, int32, int32, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(truncate, {"trunc"}, int64, int32, int64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(truncate, {"trunc"}, float32, int32, float32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(truncate, {"trunc"}, float64, int32, float64),

      // decimal functions
      UNARY_SAFE_NULL_IF_NULL(abs, {}, decimal128, decimal128),
      UNARY_SAFE_NULL_IF_NULL(ceil, {}, decimal128, decimal128),
      UNARY_SAFE_NULL_IF_NULL(floor, {}, decimal128, decimal128),
      UNARY_SAFE_NULL_IF_NULL(round, {}, decimal128, decimal128),
      UNARY_SAFE_NULL_IF_NULL(truncate, {"trunc"}, decimal128, decimal128),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(round, {}, decimal128, int32, decimal128),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(truncate, {"trunc"}, decimal128, int32,
                                       decimal128),
      BINARY_SYMMETRIC_SAFE_NULL_NEVER_FN(nvl, {}),

      NativeFunction("pi", {}, {}, float64(), kResultNullIfNull, "pi"),
      NativeFunction("e", {}, {}, float64(), kResultNullIfNull, "e"),
      NativeFunction("random", {"rand"}, DataTypeVector{}, float64(), kResultNullNever,
                     "gdv_fn_random", NativeFunction::kNeedsFunctionHolder),
      NativeFunction("random", {"rand"}, DataTypeVector{int32()}, float64(),
                     kResultNullNever, "gdv_fn_random_with_seed",
                     NativeFunction::kNeedsFunctionHolder)};

  return math_fn_registry_;
}

#undef MATH_UNARY_OPS

#undef MATH_BINARY_UNSAFE

#undef UNARY_SAFE_NULL_NEVER_BOOL_FN

#undef BINARY_SAFE_NULL_NEVER_BOOL_FN

}  // namespace gandiva
