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

#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <string.h>

extern "C" {

#include "./types.h"

FORCE_INLINE
const char* capture_utf8_utf8(gdv_int64 context, const char* source_string,
                              gdv_int32 source_len, bool source_validity,
                              const char* pattern_string, gdv_int32 pattern_len,
                              bool pattern_validity, gdv_int32* out_len) {
  if (!source_validity || !pattern_validity) {
    *out_len = 0;
    return "";
  } else {
    std::string cap;
    RE2::Extract(std::string(source_string, source_len),
                 std::string(pattern_string, pattern_len), "\\1", &cap);
    *out_len = (gdv_int32)cap.size();
    char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *out_len));
    for (gdv_int32 i = 0; i < *out_len; ++i) {
      ret[i] = cap[i];
    }
    return ret;
  }
}

}  // extern "C"
