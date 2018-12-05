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
#include <vector>
#include <utility>

namespace gandiva {


void FunctionRegistryArithmetic::GetArithmeticFnSignature(SignatureMap* map) {
  // list of registered native functions.

  static NativeFunction arithmetic_fn_registry_[] = {
      // Arithmetic operations
      UNARY_SAFE_NULL_IF_NULL(not, boolean, boolean),

      // cast operations
      UNARY_SAFE_NULL_IF_NULL(castBIGINT, int32, int64),
      UNARY_SAFE_NULL_IF_NULL(castFLOAT4, int32, float32),
      UNARY_SAFE_NULL_IF_NULL(castFLOAT4, int64, float32),
      UNARY_SAFE_NULL_IF_NULL(castFLOAT8, int32, float64),
      UNARY_SAFE_NULL_IF_NULL(castFLOAT8, int64, float64),
      UNARY_SAFE_NULL_IF_NULL(castFLOAT8, float32, float64),
      UNARY_SAFE_NULL_IF_NULL(castDATE, int64, date64),

      NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, add),
      NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, subtract),
      NUMERIC_TYPES(BINARY_SYMMETRIC_SAFE_NULL_IF_NULL, multiply),
      NUMERIC_TYPES(BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL, divide),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, int64, int32, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(mod, int64, int64, int64),
      NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, equal),
      NUMERIC_BOOL_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, not_equal),
      NUMERIC_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, less_than),
      NUMERIC_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, less_than_or_equal_to),
      NUMERIC_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, greater_than),
      NUMERIC_DATE_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, greater_than_or_equal_to),

      // utf8/binary operations
      UNARY_SAFE_NULL_IF_NULL(octet_length, utf8, int32),
      UNARY_SAFE_NULL_IF_NULL(octet_length, binary, int32),
      UNARY_SAFE_NULL_IF_NULL(bit_length, utf8, int32),
      UNARY_SAFE_NULL_IF_NULL(bit_length, binary, int32),
      UNARY_UNSAFE_NULL_IF_NULL(char_length, utf8, int32),
      UNARY_UNSAFE_NULL_IF_NULL(length, utf8, int32),
      UNARY_UNSAFE_NULL_IF_NULL(lengthUtf8, binary, int32)};

  const int num_entries =
      static_cast<int>(sizeof(arithmetic_fn_registry_) / sizeof(NativeFunction));
  for (int i = 0; i < num_entries; i++) {
    const NativeFunction* entry = &arithmetic_fn_registry_[i];

    DCHECK(map->find(&entry->signature()) == map->end());
    map->insert(std::pair<const FunctionSignature*, const NativeFunction*>(
        &entry->signature(), entry));
  }
}

}  // namespace gandiva
