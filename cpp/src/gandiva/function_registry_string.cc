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

#include "gandiva/function_registry_string.h"
#include <utility>
#include <vector>

namespace gandiva {

void FunctionRegistryString::GetStringFnSignature(SignatureMap* map) {
  // list of registered native functions.

  static NativeFunction string_fn_registry_[] = {
      VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, equal),
      VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, not_equal),
      VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, less_than),
      VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, less_than_or_equal_to),
      VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, greater_than),
      VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, greater_than_or_equal_to),

      BINARY_RELATIONAL_SAFE_NULL_IF_NULL(starts_with, utf8),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL(ends_with, utf8),

      NativeFunction("upper", DataTypeVector{utf8()}, utf8(), kResultNullIfNull,
                     "upper_utf8", NativeFunction::kNeedsContext),

      NativeFunction("like", DataTypeVector{utf8(), utf8()}, boolean(), kResultNullIfNull,
                     "gdv_fn_like_utf8_utf8", NativeFunction::kNeedsFunctionHolder)};

  const int num_entries =
      static_cast<int>(sizeof(string_fn_registry_) / sizeof(NativeFunction));
  for (int i = 0; i < num_entries; i++) {
    const NativeFunction* entry = &string_fn_registry_[i];

    DCHECK(map->find(&entry->signature()) == map->end());
    map->insert(std::pair<const FunctionSignature*, const NativeFunction*>(
        &entry->signature(), entry));
  }
}

}  // namespace gandiva
