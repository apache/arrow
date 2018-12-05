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

#include "gandiva/function_registry.h"
#include "gandiva/function_registry_arithmetic.h"
#include "gandiva/function_registry_datetime.h"
#include "gandiva/function_registry_hash.h"
#include "gandiva/function_registry_math_ops.h"
#include "gandiva/function_registry_string.h"
#include "gandiva/function_registry_timestamp_arithmetic.h"

#include <iterator>
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
using std::iterator;
using std::vector;

#define STRINGIFY(a) #a

FunctionRegistry::iterator FunctionRegistry::begin() const {
  return &(*pc_registry_.begin());
}

FunctionRegistry::iterator FunctionRegistry::end() const {
  return &(*pc_registry_.end());
}

std::vector<NativeFunction> FunctionRegistry::pc_registry_;

SignatureMap FunctionRegistry::pc_registry_map_ = InitPCMap();

SignatureMap FunctionRegistry::InitPCMap() {
  SignatureMap map;

#if 0
  int num_entries = static_cast<int>(sizeof(pc_registry_) / sizeof(NativeFunction));
  for (int i = 0; i < num_entries; i++) {
    const NativeFunction* entry = &pc_registry_[i];

    DCHECK(map.find(&entry->signature()) == map.end());
    map[&entry->signature()] = entry;
    // printf("%s -> %s\n", entry->signature().ToString().c_str(),
    //      entry->pc_name().c_str());
  }
#endif

  FunctionRegistryArithmetic::GetArithmeticFnSignature(&map);
  FunctionRegistryDateTime::GetDateTimeFnSignature(&map);
  FunctionRegistryHash::GetHashFnSignature(&map);
  FunctionRegistryMathOps::GetMathOpsFnSignature(&map);
  FunctionRegistryString::GetStringFnSignature(&map);
  FunctionRegistryDateTimeArithmetic::GetDateTimeArithmeticFnSignature(&map);

  for (auto elem : map) pc_registry_.push_back(*elem.second);

  return map;
}

const NativeFunction* FunctionRegistry::LookupSignature(
    const FunctionSignature& signature) const {
  auto got = pc_registry_map_.find(&signature);
  return got == pc_registry_map_.end() ? NULL : got->second;
}

}  // namespace gandiva
