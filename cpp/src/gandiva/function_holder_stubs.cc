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

#include "gandiva/like_holder.h"
#include "gandiva/to_date_holder.h"

// Wrapper C functions for "like" to be invoked from LLVM.
extern "C" bool like_utf8_utf8(int64_t ptr, const char* data, int data_len,
                               const char* pattern, int pattern_len) {
  gandiva::helpers::LikeHolder* holder =
      reinterpret_cast<gandiva::helpers::LikeHolder*>(ptr);
  return (*holder)(std::string(data, data_len));
}

extern "C" int64_t to_date_utf8_utf8_int32(int64_t ptr, const char* data, int data_len,
                                           bool in1_validity, const char* pattern,
                                           int pattern_len, bool in2_validity,
                                           int32_t suppress_errors, bool in3_validity,
                                           int64_t execution_context, bool* out_valid) {
  gandiva::helpers::ToDateHolder* holder =
      reinterpret_cast<gandiva::helpers::ToDateHolder*>(ptr);
  return (*holder)(std::string(data, data_len), in1_validity, execution_context,
                   out_valid);
}
