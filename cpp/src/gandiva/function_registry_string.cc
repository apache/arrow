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

#include "gandiva/function_registry_string.h"
#include "gandiva/function_registry_common.h"

namespace gandiva {

#define BINARY_RELATIONAL_SAFE_NULL_IF_NULL_FN(name, ALIASES) \
  VAR_LEN_TYPES(BINARY_RELATIONAL_SAFE_NULL_IF_NULL, name, ALIASES)

#define BINARY_RELATIONAL_SAFE_NULL_IF_NULL_UTF8_FN(name, ALIASES) \
  BINARY_RELATIONAL_SAFE_NULL_IF_NULL(name, ALIASES, utf8)

#define UNARY_OCTET_LEN_FN(name, ALIASES)              \
  UNARY_SAFE_NULL_IF_NULL(name, ALIASES, utf8, int32), \
      UNARY_SAFE_NULL_IF_NULL(name, ALIASES, binary, int32)

#define UNARY_SAFE_NULL_NEVER_BOOL_FN(name, ALIASES) \
  VAR_LEN_TYPES(UNARY_SAFE_NULL_NEVER_BOOL, name, ALIASES)

std::vector<NativeFunction> GetStringFunctionRegistry() {
  static std::vector<NativeFunction> string_fn_registry_ = {
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL_FN(equal, {}),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL_FN(not_equal, {}),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL_FN(less_than, {}),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL_FN(less_than_or_equal_to, {}),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL_FN(greater_than, {}),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL_FN(greater_than_or_equal_to, {}),

      BINARY_RELATIONAL_SAFE_NULL_IF_NULL_UTF8_FN(starts_with, {}),
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL_UTF8_FN(ends_with, {}),

      UNARY_OCTET_LEN_FN(octet_length, {}),
      UNARY_OCTET_LEN_FN(bit_length, {}),

      UNARY_UNSAFE_NULL_IF_NULL(char_length, {}, utf8, int32),
      UNARY_UNSAFE_NULL_IF_NULL(length, {}, utf8, int32),
      UNARY_UNSAFE_NULL_IF_NULL(lengthUtf8, {}, binary, int32),

      UNARY_SAFE_NULL_NEVER_BOOL_FN(isnull, {}),
      UNARY_SAFE_NULL_NEVER_BOOL_FN(isnotnull, {}),

      NativeFunction("upper", {}, DataTypeVector{utf8()}, utf8(), kResultNullIfNull,
                     "upper_utf8", NativeFunction::kNeedsContext),

      NativeFunction("castVARCHAR", {}, DataTypeVector{utf8(), int64()}, utf8(),
                     kResultNullIfNull, "castVARCHAR_utf8_int64",
                     NativeFunction::kNeedsContext),

      NativeFunction("castVARCHAR", {}, DataTypeVector{decimal128(), int64()}, utf8(),
                     kResultNullIfNull, "castVARCHAR_decimal128_int64",
                     NativeFunction::kNeedsContext),

      NativeFunction("like", {}, DataTypeVector{utf8(), utf8()}, boolean(),
                     kResultNullIfNull, "gdv_fn_like_utf8_utf8",
                     NativeFunction::kNeedsFunctionHolder),

      NativeFunction("substr", {"substring"},
                     DataTypeVector{utf8(), int64() /*offset*/, int64() /*length*/},
                     utf8(), kResultNullIfNull, "substr_utf8_int64_int64",
                     NativeFunction::kNeedsContext),

      NativeFunction("substr", {"substring"}, DataTypeVector{utf8(), int64() /*offset*/},
                     utf8(), kResultNullIfNull, "substr_utf8_int64",
                     NativeFunction::kNeedsContext),

      NativeFunction("concatOperator", {}, DataTypeVector{utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "concatOperator_utf8_utf8",
                     NativeFunction::kNeedsContext),

      // concat treats null inputs as empty strings whereas concatOperator returns null if
      // one of the inputs is null
      NativeFunction("concat", {}, DataTypeVector{utf8(), utf8()}, utf8(),
                     kResultNullNever, "concat_utf8_utf8", NativeFunction::kNeedsContext),

      NativeFunction("convert_fromUTF8", {"convert_fromutf8"}, DataTypeVector{binary()},
                     utf8(), kResultNullIfNull, "convert_fromUTF8_binary",
                     NativeFunction::kNeedsContext),
  };

  return string_fn_registry_;
}

}  // namespace gandiva
