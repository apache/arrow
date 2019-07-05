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

#define BINARY_SYMMETRIC_FN(name) NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, name)

#define BINARY_RELATIONAL_BOOL_FN(name) \
  NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, name)

#define BINARY_RELATIONAL_BOOL_DATE_FN(name) \
  NUMERIC_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, name)

#define UNARY_CAST_TO_FLOAT64(name) UNARY_SAFE_NULL_IF_NULL(castFLOAT8, name, float64)

#define UNARY_CAST_TO_FLOAT32(name) UNARY_SAFE_NULL_IF_NULL(castFLOAT4, name, float32)

std::vector<NativeFunction> GetArithmeticFunctionRegistry() {
  static std::vector<NativeFunction> arithmetic_fn_registry_ = {
      UNARY_SAFE_NULL_IF_NULL(not, boolean, boolean),
      UNARY_SAFE_NULL_IF_NULL(castBIGINT, int32, int64),
      UNARY_SAFE_NULL_IF_NULL(castBIGINT, decimal128, int64),

      // cast to float32
      UNARY_CAST_TO_FLOAT32(int32), UNARY_CAST_TO_FLOAT32(int64),

      // cast to float64
      UNARY_CAST_TO_FLOAT64(int32), UNARY_CAST_TO_FLOAT64(int64),
      UNARY_CAST_TO_FLOAT64(float32), UNARY_CAST_TO_FLOAT64(decimal128),

      // cast to decimal
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, int64, decimal128),
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, float64, decimal128),
      UNARY_SAFE_NULL_IF_NULL(castDECIMAL, decimal128, decimal128),
      UNARY_UNSAFE_NULL_IF_NULL(castDECIMAL, utf8, decimal128),

      UNARY_SAFE_NULL_IF_NULL(castDATE, int64, date64),

      // add/sub/multiply/divide/mod
      BINARY_SYMMETRIC_FN(add), BINARY_SYMMETRIC_FN(subtract),
      BINARY_SYMMETRIC_FN(multiply),
      NUMERIC_TYPES(BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL, divide),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, int64, int32, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, int64, int64, int64),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(add, decimal128),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(subtract, decimal128),
      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(multiply, decimal128),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(divide, decimal128),
      BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(mod, decimal128),

      // compare functions
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL(equal, decimal128),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL(not_equal, decimal128),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL(less_than, decimal128),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL(less_than_or_equal_to, decimal128),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL(greater_than, decimal128),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL(greater_than_or_equal_to, decimal128),
      BINARY_RELATIONAL_BOOL_FN(equal), BINARY_RELATIONAL_BOOL_FN(not_equal),
      BINARY_RELATIONAL_BOOL_DATE_FN(less_than),
      BINARY_RELATIONAL_BOOL_DATE_FN(less_than_or_equal_to),
      BINARY_RELATIONAL_BOOL_DATE_FN(greater_than),
      BINARY_RELATIONAL_BOOL_DATE_FN(greater_than_or_equal_to)};

  return arithmetic_fn_registry_;
}

}  // namespace gandiva
