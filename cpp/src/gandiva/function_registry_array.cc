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

#include "gandiva/function_registry_array.h"

#include "gandiva/function_registry_common.h"

namespace gandiva {
std::vector<NativeFunction> GetArrayFunctionRegistry() {
  static std::vector<NativeFunction> array_fn_registry_ = {
      NativeFunction("array_contains", {}, DataTypeVector{list(utf8()), utf8()},
                     boolean(), kResultNullIfNull, "array_utf8_contains_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),
      NativeFunction("array_length", {}, DataTypeVector{list(utf8())}, int64(),
                     kResultNullIfNull, "array_utf8_length",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),
  };
  return array_fn_registry_;
}

}  // namespace gandiva
