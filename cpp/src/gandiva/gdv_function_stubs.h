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

#ifndef GDV_FUNCTION_STUBS_H
#define GDV_FUNCTION_STUBS_H

#include <cstdint>

/// Stub functions that can be accessed from LLVM.
extern "C" {

bool gdv_fn_like_utf8_utf8(int64_t ptr, const char* data, int data_len,
                           const char* pattern, int pattern_len);

int64_t gdv_fn_to_date_utf8_utf8_int32(int64_t ptr, const char* data, int data_len,
                                       bool in1_validity, const char* pattern,
                                       int pattern_len, bool in2_validity,
                                       int32_t suppress_errors, bool in3_validity,
                                       int64_t execution_context, bool* out_valid);

void gdv_fn_context_set_error_msg(int64_t context_ptr, const char* err_msg);

bool in_expr_lookup_int32(int64_t ptr, int32_t value, bool in_validity);

bool in_expr_lookup_int64(int64_t ptr, int64_t value, bool in_validity);

bool in_expr_lookup_utf8(int64_t ptr, const char* data, int data_len, bool in_validity);
}

#endif  // GDV_FUNCTION_STUBS_H
