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

#include "gandiva/gdv_function_stubs.h"

#include <string>
#include <vector>

#include "gandiva/engine.h"
#include "gandiva/exported_funcs.h"
#include "gandiva/like_holder.h"
#include "gandiva/to_date_holder.h"

/// Stub functions that can be accessed from LLVM or the pre-compiled library.

extern "C" {

bool gdv_fn_like_utf8_utf8(int64_t ptr, const char* data, int data_len,
                           const char* pattern, int pattern_len) {
  gandiva::LikeHolder* holder = reinterpret_cast<gandiva::LikeHolder*>(ptr);
  return (*holder)(std::string(data, data_len));
}

int64_t gdv_fn_to_date_utf8_utf8_int32(int64_t ptr, const char* data, int data_len,
                                       bool in1_validity, const char* pattern,
                                       int pattern_len, bool in2_validity,
                                       int32_t suppress_errors, bool in3_validity,
                                       int64_t execution_context, bool* out_valid) {
  gandiva::ToDateHolder* holder = reinterpret_cast<gandiva::ToDateHolder*>(ptr);
  return (*holder)(std::string(data, data_len), in1_validity, execution_context,
                   out_valid);
}
}

namespace gandiva {

void ExportedStubFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

  // gdv_fn_like_utf8_utf8
  args = {types->i64_type(),     // int64_t ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),     // int data_len
          types->i8_ptr_type(),  // const char* pattern
          types->i32_type()};    // int pattern_len

  engine->AddGlobalMappingForFunc("gdv_fn_like_utf8_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_like_utf8_utf8));

  // gdv_fn_to_date_utf8_utf8_int32
  args = {types->i64_type(),                   // int64_t ptr
          types->i8_ptr_type(),                // const char* data
          types->i32_type(),                   // int data_len
          types->i1_type(),                    // bool in1_validity
          types->i8_ptr_type(),                // const char* pattern
          types->i32_type(),                   // int pattern_len
          types->i1_type(),                    // bool in2_validity
          types->i32_type(),                   // int32_t suppress_errors
          types->i1_type(),                    // bool in3_validity
          types->i64_type(),                   // int64_t execution_context
          types->ptr_type(types->i8_type())};  // bool* out_valid

  engine->AddGlobalMappingForFunc(
      "gdv_fn_to_date_utf8_utf8_int32", types->i64_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_to_date_utf8_utf8_int32));
}

}  // namespace gandiva
