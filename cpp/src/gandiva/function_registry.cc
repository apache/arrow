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
#include <utility>
#include <vector>

namespace gandiva {

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

  auto v1 = GetArithmeticFunctionRegistry();
  pc_registry_.insert(std::end(pc_registry_), v1.begin(), v1.end());
  auto v2 = GetDateTimeFunctionRegistry();
  pc_registry_.insert(std::end(pc_registry_), v2.begin(), v2.end());

  auto v3 = GetHashFunctionRegistry();
  pc_registry_.insert(std::end(pc_registry_), v3.begin(), v3.end());

  auto v4 = GetMathOpsFunctionRegistry();
  pc_registry_.insert(std::end(pc_registry_), v4.begin(), v4.end());

  auto v5 = GetStringFunctionRegistry();
  pc_registry_.insert(std::end(pc_registry_), v5.begin(), v5.end());

  auto v6 = GetDateTimeArithmeticFunctionRegistry();
  pc_registry_.insert(std::end(pc_registry_), v6.begin(), v6.end());

  for (auto& elem : pc_registry_) {
    map.insert(std::make_pair(&(elem.signature()), &elem));
  }

  return map;
}

const NativeFunction* FunctionRegistry::LookupSignature(
    const FunctionSignature& signature) const {
  auto got = pc_registry_map_.find(&signature);
  return got == pc_registry_map_.end() ? nullptr : got->second;
}

}  // namespace gandiva
