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

#include "function_registry_encrypt.h"
#include "gandiva/function_registry_common.h"

namespace gandiva {

std::vector<NativeFunction> GetEncryptFunctionRegistry() {
  static std::vector<NativeFunction> encrypt_fn_registry_ = {
      NativeFunction("aes_encrypt", {}, DataTypeVector{utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "gdv_fn_aes_encrypt",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),
      NativeFunction("aes_decrypt", {}, DataTypeVector{utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "gdv_fn_aes_decrypt",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors)};

  return encrypt_fn_registry_;
}

}  // namespace gandiva
