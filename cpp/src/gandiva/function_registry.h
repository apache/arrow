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

#ifndef GANDIVA_FUNCTION_REGISTRY_H
#define GANDIVA_FUNCTION_REGISTRY_H

#include <vector>
#include "gandiva/function_registry_common.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/native_function.h"
#include "gandiva/visibility.h"

namespace gandiva {

///\brief Registry of pre-compiled IR functions.
class GANDIVA_EXPORT FunctionRegistry {
 public:
  using iterator = const NativeFunction*;

  /// Lookup a pre-compiled function by its signature.
  const NativeFunction* LookupSignature(const FunctionSignature& signature) const;

  iterator begin() const;
  iterator end() const;

 private:
  static SignatureMap InitPCMap();

  static std::vector<NativeFunction> pc_registry_;
  static SignatureMap pc_registry_map_;
};

}  // namespace gandiva

#endif  // GANDIVA_FUNCTION_REGISTRY_H
