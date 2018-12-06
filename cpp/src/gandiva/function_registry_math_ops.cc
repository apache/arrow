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
#include <utility>
#include <vector>

namespace gandiva {

void FunctionRegistryMathOps::GetMathOpsFnSignature(SignatureMap* map) {
  // list of registered native functions.

  static NativeFunction math_fn_registry_[] = {
      // extended math ops
      UNARY_SAFE_NULL_IF_NULL(cbrt, int32, float64),
      UNARY_SAFE_NULL_IF_NULL(cbrt, int64, float64),
      UNARY_SAFE_NULL_IF_NULL(cbrt, uint32, float64),
      UNARY_SAFE_NULL_IF_NULL(cbrt, uint64, float64),
      UNARY_SAFE_NULL_IF_NULL(cbrt, float32, float64),
      UNARY_SAFE_NULL_IF_NULL(cbrt, float64, float64),

      UNARY_SAFE_NULL_IF_NULL(exp, int32, float64),
      UNARY_SAFE_NULL_IF_NULL(exp, int64, float64),
      UNARY_SAFE_NULL_IF_NULL(exp, uint32, float64),
      UNARY_SAFE_NULL_IF_NULL(exp, uint64, float64),
      UNARY_SAFE_NULL_IF_NULL(exp, float32, float64),
      UNARY_SAFE_NULL_IF_NULL(exp, float64, float64),

      UNARY_SAFE_NULL_IF_NULL(log, int32, float64),
      UNARY_SAFE_NULL_IF_NULL(log, int64, float64),
      UNARY_SAFE_NULL_IF_NULL(log, uint32, float64),
      UNARY_SAFE_NULL_IF_NULL(log, uint64, float64),
      UNARY_SAFE_NULL_IF_NULL(log, float32, float64),
      UNARY_SAFE_NULL_IF_NULL(log, float64, float64),

      UNARY_SAFE_NULL_IF_NULL(log10, int32, float64),
      UNARY_SAFE_NULL_IF_NULL(log10, int64, float64),
      UNARY_SAFE_NULL_IF_NULL(log10, uint32, float64),
      UNARY_SAFE_NULL_IF_NULL(log10, uint64, float64),
      UNARY_SAFE_NULL_IF_NULL(log10, float32, float64),
      UNARY_SAFE_NULL_IF_NULL(log10, float64, float64),

      BINARY_UNSAFE_NULL_IF_NULL(log, int32, float64),
      BINARY_UNSAFE_NULL_IF_NULL(log, int64, float64),
      BINARY_UNSAFE_NULL_IF_NULL(log, uint32, float64),
      BINARY_UNSAFE_NULL_IF_NULL(log, uint64, float64),
      BINARY_UNSAFE_NULL_IF_NULL(log, float32, float64),
      BINARY_UNSAFE_NULL_IF_NULL(log, float64, float64),

      BINARY_SYMMETRIC_SAFE_NULL_IF_NULL(power, float64),

      // nullable never operations
      NUMERIC_BOOL_DATE_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, isnull),
      NUMERIC_BOOL_DATE_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, isnotnull),
      NUMERIC_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, isnumeric),

      // nullable never binary operations
      NUMERIC_BOOL_DATE_TYPES(BINARY_SAFE_NULL_NEVER_BOOL, is_distinct_from),
      NUMERIC_BOOL_DATE_TYPES(BINARY_SAFE_NULL_NEVER_BOOL, is_not_distinct_from)};

  const int num_entries =
      static_cast<int>(sizeof(math_fn_registry_) / sizeof(NativeFunction));
  for (int i = 0; i < num_entries; i++) {
    const NativeFunction* entry = &math_fn_registry_[i];

    DCHECK(map->find(&entry->signature()) == map->end());
    map->insert(std::pair<const FunctionSignature*, const NativeFunction*>(
        &entry->signature(), entry));
  }
}

}  // namespace gandiva
