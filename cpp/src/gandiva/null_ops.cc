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
#include "gandiva/null_ops.h"

#include "gandiva/engine.h"
#include "gandiva/exported_funcs.h"
#include "gandiva/gdv_function_stubs.h"

/// Stub functions that can be accessed from LLVM or the pre-compiled library.

extern "C" {

GANDIVA_EXPORT
void compare_null_null(bool in1_valid, bool in2_valid) {}

GANDIVA_EXPORT
bool isnull_null(bool in_valid) { return true; }

GANDIVA_EXPORT
bool isnotnull_null(bool in_valid) { return false; }
}

namespace gandiva {
void ExportedNullFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

  args = {types->i1_type(), types->i1_type()};
  engine->AddGlobalMappingForFunc("compare_null_null", types->void_type() /*return_type*/,
                                  args, reinterpret_cast<void*>(compare_null_null));

  args = {types->i1_type()};
  engine->AddGlobalMappingForFunc("isnull_null", types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(isnull_null));

  args = {types->i1_type()};
  engine->AddGlobalMappingForFunc("isnotnull_null", types->i1_type() /*return_type*/,
                                  args, reinterpret_cast<void*>(isnotnull_null));
}
}  // namespace gandiva
