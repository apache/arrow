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

#include "gandiva/function_registry_datetime.h"
#include "gandiva/function_registry_common.h"

namespace gandiva {

#define DATE_EXTRACTION_FNS(name)                           \
  DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Millennium),  \
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Century), \
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Decade),  \
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Year),    \
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Quarter), \
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Month),   \
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Week),    \
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Day),     \
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Hour),    \
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Minute),  \
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Second)

#define TIME_EXTRACTION_FNS(name)                          \
  TIME_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Hour),       \
      TIME_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Minute), \
      TIME_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Second)

std::vector<NativeFunction> GetDateTimeFunctionRegistry() {
  static std::vector<NativeFunction> date_time_fn_registry_ = {
      DATE_EXTRACTION_FNS(extract),
      DATE_EXTRACTION_FNS(date_trunc_),

      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractDoy),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractDow),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractEpoch),

      TIME_EXTRACTION_FNS(extract),

      NativeFunction("castDATE", DataTypeVector{utf8()}, date64(), kResultNullIfNull,
                     "castDATE_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castTIMESTAMP", DataTypeVector{utf8()}, timestamp(),
                     kResultNullIfNull, "castTIMESTAMP_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("to_date", DataTypeVector{utf8(), utf8(), int32()}, date64(),
                     kResultNullInternal, "gdv_fn_to_date_utf8_utf8_int32",
                     NativeFunction::kNeedsContext |
                         NativeFunction::kNeedsFunctionHolder |
                         NativeFunction::kCanReturnErrors)};

  return date_time_fn_registry_;
}

}  // namespace gandiva
