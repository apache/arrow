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

#include "gandiva/function_registry_hash.h"
#include <utility>
#include <vector>

namespace gandiva {

void FunctionRegistryHash::GetHashFnSignature(SignatureMap* map) {
  // list of registered native functions.

  static NativeFunction hash_fn_registry_[] = {
      // hash functions
      NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH32_SAFE_NULL_NEVER, hash),
      NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH32_SAFE_NULL_NEVER, hash32),
      NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH32_SAFE_NULL_NEVER, hash32AsDouble),
      NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH32_SEED_SAFE_NULL_NEVER, hash32),
      NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH32_SEED_SAFE_NULL_NEVER, hash32AsDouble),

      NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH64_SAFE_NULL_NEVER, hash64),
      NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH64_SAFE_NULL_NEVER, hash64AsDouble),
      NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH64_SEED_SAFE_NULL_NEVER, hash64),
      NUMERIC_BOOL_DATE_VAR_LEN_TYPES(HASH64_SEED_SAFE_NULL_NEVER, hash64AsDouble)};

  const int num_entries =
      static_cast<int>(sizeof(hash_fn_registry_) / sizeof(NativeFunction));
  for (int i = 0; i < num_entries; i++) {
    const NativeFunction* entry = &hash_fn_registry_[i];

    DCHECK(map->find(&entry->signature()) == map->end());
    map->insert(std::pair<const FunctionSignature*, const NativeFunction*>(
        &entry->signature(), entry));
  }
}

}  // namespace gandiva
