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

// This file is also used in the pre-compiled unit tests, which do include
// llvm/engine/..
#ifndef GANDIVA_UNIT_TEST
#include "gandiva/exported_funcs.h"
#include "gandiva/gdv_function_stubs.h"

#include "gandiva/engine.h"

namespace gandiva {

arrow::Status ExportedContextFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

  // gdv_fn_context_set_error_msg
  args = {types->i64_type(),      // int64_t context_ptr
          types->i8_ptr_type()};  // char const* err_msg

  engine->AddGlobalMappingForFunc("gdv_fn_context_set_error_msg", types->void_type(),
                                  args,
                                  reinterpret_cast<void*>(gdv_fn_context_set_error_msg));

  // gdv_fn_context_arena_malloc
  args = {types->i64_type(),   // int64_t context_ptr
          types->i32_type()};  // int32_t size

  engine->AddGlobalMappingForFunc("gdv_fn_context_arena_malloc", types->i8_ptr_type(),
                                  args,
                                  reinterpret_cast<void*>(gdv_fn_context_arena_malloc));

  // gdv_fn_context_arena_reset
  args = {types->i64_type()};  // int64_t context_ptr

  engine->AddGlobalMappingForFunc("gdv_fn_context_arena_reset", types->void_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_context_arena_reset));
  return arrow::Status::OK();
}

}  // namespace gandiva
#endif  // !GANDIVA_UNIT_TEST

#include "gandiva/execution_context.h"

extern "C" {

void gdv_fn_context_set_error_msg(int64_t context_ptr, char const* err_msg) {
  auto context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  context->set_error_msg(err_msg);
}

uint8_t* gdv_fn_context_arena_malloc(int64_t context_ptr, int32_t size) {
  auto context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  return context->arena()->Allocate(size);
}

void gdv_fn_context_arena_reset(int64_t context_ptr) {
  auto context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  return context->arena()->Reset();
}
}
