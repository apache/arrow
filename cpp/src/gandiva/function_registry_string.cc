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
      BINARY_RELATIONAL_SAFE_NULL_IF_NULL_UTF8_FN(is_substr, {}),

      BINARY_UNSAFE_NULL_IF_NULL(locate, {"position"}, utf8, int32),
      BINARY_UNSAFE_NULL_IF_NULL(strpos, {}, utf8, int32),

      UNARY_OCTET_LEN_FN(octet_length, {}), UNARY_OCTET_LEN_FN(bit_length, {}),

      UNARY_UNSAFE_NULL_IF_NULL(char_length, {}, utf8, int32),
      UNARY_UNSAFE_NULL_IF_NULL(length, {}, utf8, int32),
      UNARY_UNSAFE_NULL_IF_NULL(lengthUtf8, {}, binary, int32),
      UNARY_UNSAFE_NULL_IF_NULL(reverse, {}, utf8, utf8),
      UNARY_UNSAFE_NULL_IF_NULL(ltrim, {}, utf8, utf8),
      UNARY_UNSAFE_NULL_IF_NULL(rtrim, {}, utf8, utf8),
      UNARY_UNSAFE_NULL_IF_NULL(btrim, {}, utf8, utf8),
      UNARY_UNSAFE_NULL_IF_NULL(space, {}, int32, utf8),
      UNARY_UNSAFE_NULL_IF_NULL(space, {}, int64, utf8),

      UNARY_SAFE_NULL_NEVER_BOOL_FN(isnull, {}),
      UNARY_SAFE_NULL_NEVER_BOOL_FN(isnotnull, {}),

      NativeFunction("chr", {}, DataTypeVector{int32()}, utf8(), kResultNullIfNull,
                     "chr_int32", NativeFunction::kNeedsContext),

      NativeFunction("chr", {}, DataTypeVector{int64()}, utf8(), kResultNullIfNull,
                     "chr_int64", NativeFunction::kNeedsContext),

      NativeFunction("ascii", {}, DataTypeVector{utf8()}, int32(), kResultNullIfNull,
                     "ascii_utf8"),

      NativeFunction("base64", {}, DataTypeVector{binary()}, utf8(), kResultNullIfNull,
                     "gdv_fn_base64_encode_binary", NativeFunction::kNeedsContext),

      NativeFunction("unbase64", {}, DataTypeVector{utf8()}, binary(), kResultNullIfNull,
                     "gdv_fn_base64_decode_utf8", NativeFunction::kNeedsContext),

      NativeFunction("repeat", {}, DataTypeVector{utf8(), int32()}, utf8(),
                     kResultNullIfNull, "repeat_utf8_int32",
                     NativeFunction::kNeedsContext),

      NativeFunction("soundex", {}, DataTypeVector{utf8()}, utf8(), kResultNullIfNull,
                     "soundex_utf8", NativeFunction::kNeedsContext),

      NativeFunction("upper", {}, DataTypeVector{utf8()}, utf8(), kResultNullIfNull,
                     "gdv_fn_upper_utf8", NativeFunction::kNeedsContext),

      NativeFunction("lower", {}, DataTypeVector{utf8()}, utf8(), kResultNullIfNull,
                     "gdv_fn_lower_utf8", NativeFunction::kNeedsContext),

      NativeFunction("initcap", {}, DataTypeVector{utf8()}, utf8(), kResultNullIfNull,
                     "gdv_fn_initcap_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("concat_ws", {}, DataTypeVector{utf8(), utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "concat_ws_utf8_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("concat_ws", {}, DataTypeVector{utf8(), utf8(), utf8(), utf8()},
                     utf8(), kResultNullIfNull, "concat_ws_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("concat_ws", {},
                     DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "concat_ws_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("concat_ws", {},
                     DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8(), utf8()},
                     utf8(), kResultNullIfNull, "concat_ws_utf8_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("elt", {}, DataTypeVector{int32(), utf8(), utf8()}, utf8(),
                     kResultNullInternal, "elt_int32_utf8_utf8"),

      NativeFunction("elt", {}, DataTypeVector{int32(), utf8(), utf8(), utf8()}, utf8(),
                     kResultNullInternal, "elt_int32_utf8_utf8_utf8"),

      NativeFunction("elt", {}, DataTypeVector{int32(), utf8(), utf8(), utf8(), utf8()},
                     utf8(), kResultNullInternal, "elt_int32_utf8_utf8_utf8_utf8"),

      NativeFunction("elt", {},
                     DataTypeVector{int32(), utf8(), utf8(), utf8(), utf8(), utf8()},
                     utf8(), kResultNullInternal, "elt_int32_utf8_utf8_utf8_utf8_utf8"),

      NativeFunction("castBIT", {"castBOOLEAN"}, DataTypeVector{utf8()}, boolean(),
                     kResultNullIfNull, "castBIT_utf8", NativeFunction::kNeedsContext),

      NativeFunction("castINT", {}, DataTypeVector{utf8()}, int32(), kResultNullIfNull,
                     "gdv_fn_castINT_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castBIGINT", {}, DataTypeVector{utf8()}, int64(), kResultNullIfNull,
                     "gdv_fn_castBIGINT_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castFLOAT4", {}, DataTypeVector{utf8()}, float32(),
                     kResultNullIfNull, "gdv_fn_castFLOAT4_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castFLOAT8", {}, DataTypeVector{utf8()}, float64(),
                     kResultNullIfNull, "gdv_fn_castFLOAT8_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castINT", {}, DataTypeVector{binary()}, int32(), kResultNullIfNull,
                     "gdv_fn_castINT_varbinary",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castBIGINT", {}, DataTypeVector{binary()}, int64(),
                     kResultNullIfNull, "gdv_fn_castBIGINT_varbinary",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castFLOAT4", {}, DataTypeVector{binary()}, float32(),
                     kResultNullIfNull, "gdv_fn_castFLOAT4_varbinary",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castFLOAT8", {}, DataTypeVector{binary()}, float64(),
                     kResultNullIfNull, "gdv_fn_castFLOAT8_varbinary",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARCHAR", {}, DataTypeVector{boolean(), int64()}, utf8(),
                     kResultNullIfNull, "castVARCHAR_bool_int64",
                     NativeFunction::kNeedsContext),

      NativeFunction("castVARCHAR", {}, DataTypeVector{utf8(), int64()}, utf8(),
                     kResultNullIfNull, "castVARCHAR_utf8_int64",
                     NativeFunction::kNeedsContext),

      NativeFunction("castVARCHAR", {}, DataTypeVector{binary(), int64()}, utf8(),
                     kResultNullIfNull, "castVARCHAR_binary_int64",
                     NativeFunction::kNeedsContext),

      NativeFunction("castVARCHAR", {}, DataTypeVector{int32(), int64()}, utf8(),
                     kResultNullIfNull, "gdv_fn_castVARCHAR_int32_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARCHAR", {}, DataTypeVector{int64(), int64()}, utf8(),
                     kResultNullIfNull, "gdv_fn_castVARCHAR_int64_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARCHAR", {}, DataTypeVector{date64(), int64()}, utf8(),
                     kResultNullIfNull, "gdv_fn_castVARCHAR_date64_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARCHAR", {}, DataTypeVector{float32(), int64()}, utf8(),
                     kResultNullIfNull, "gdv_fn_castVARCHAR_float32_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARCHAR", {}, DataTypeVector{float64(), int64()}, utf8(),
                     kResultNullIfNull, "gdv_fn_castVARCHAR_float64_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARCHAR", {}, DataTypeVector{decimal128(), int64()}, utf8(),
                     kResultNullIfNull, "castVARCHAR_decimal128_int64",
                     NativeFunction::kNeedsContext),

      NativeFunction("crc32", {}, DataTypeVector{utf8()}, int64(), kResultNullIfNull,
                     "gdv_fn_crc_32_utf8", NativeFunction::kNeedsContext),

      NativeFunction("crc32", {}, DataTypeVector{binary()}, int64(), kResultNullIfNull,
                     "gdv_fn_crc_32_binary", NativeFunction::kNeedsContext),

      NativeFunction("like", {}, DataTypeVector{utf8(), utf8()}, boolean(),
                     kResultNullIfNull, "gdv_fn_like_utf8_utf8",
                     NativeFunction::kNeedsFunctionHolder),

      NativeFunction("like", {}, DataTypeVector{utf8(), utf8(), utf8()}, boolean(),
                     kResultNullIfNull, "gdv_fn_like_utf8_utf8_utf8",
                     NativeFunction::kNeedsFunctionHolder),

      NativeFunction("ilike", {}, DataTypeVector{utf8(), utf8()}, boolean(),
                     kResultNullIfNull, "gdv_fn_ilike_utf8_utf8",
                     NativeFunction::kNeedsFunctionHolder),

      NativeFunction("ltrim", {}, DataTypeVector{utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "ltrim_utf8_utf8", NativeFunction::kNeedsContext),

      NativeFunction("rtrim", {}, DataTypeVector{utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "rtrim_utf8_utf8", NativeFunction::kNeedsContext),

      NativeFunction("btrim", {}, DataTypeVector{utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "btrim_utf8_utf8", NativeFunction::kNeedsContext),

      NativeFunction("substr", {"substring"},
                     DataTypeVector{utf8(), int64() /*offset*/, int64() /*length*/},
                     utf8(), kResultNullIfNull, "substr_utf8_int64_int64",
                     NativeFunction::kNeedsContext),

      NativeFunction("substr", {"substring"}, DataTypeVector{utf8(), int64() /*offset*/},
                     utf8(), kResultNullIfNull, "substr_utf8_int64",
                     NativeFunction::kNeedsContext),

      NativeFunction("lpad", {}, DataTypeVector{utf8(), int32(), utf8()}, utf8(),
                     kResultNullIfNull, "lpad_utf8_int32_utf8",
                     NativeFunction::kNeedsContext),

      NativeFunction("lpad", {}, DataTypeVector{utf8(), int32()}, utf8(),
                     kResultNullIfNull, "lpad_utf8_int32", NativeFunction::kNeedsContext),

      NativeFunction("rpad", {}, DataTypeVector{utf8(), int32(), utf8()}, utf8(),
                     kResultNullIfNull, "rpad_utf8_int32_utf8",
                     NativeFunction::kNeedsContext),

      NativeFunction("rpad", {}, DataTypeVector{utf8(), int32()}, utf8(),
                     kResultNullIfNull, "rpad_utf8_int32", NativeFunction::kNeedsContext),

      NativeFunction("regexp_replace", {}, DataTypeVector{utf8(), utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "gdv_fn_regexp_replace_utf8_utf8",
                     NativeFunction::kNeedsContext |
                         NativeFunction::kNeedsFunctionHolder |
                         NativeFunction::kCanReturnErrors),

      NativeFunction("concatOperator", {}, DataTypeVector{utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "concatOperator_utf8_utf8",
                     NativeFunction::kNeedsContext),
      NativeFunction("concatOperator", {}, DataTypeVector{utf8(), utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "concatOperator_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),
      NativeFunction("concatOperator", {}, DataTypeVector{utf8(), utf8(), utf8(), utf8()},
                     utf8(), kResultNullIfNull, "concatOperator_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),
      NativeFunction("concatOperator", {},
                     DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "concatOperator_utf8_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),
      NativeFunction("concatOperator", {},
                     DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8(), utf8()},
                     utf8(), kResultNullIfNull,
                     "concatOperator_utf8_utf8_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),
      NativeFunction(
          "concatOperator", {},
          DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8(), utf8(), utf8()}, utf8(),
          kResultNullIfNull, "concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8",
          NativeFunction::kNeedsContext),
      NativeFunction(
          "concatOperator", {},
          DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8(), utf8(), utf8(), utf8()},
          utf8(), kResultNullIfNull,
          "concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8",
          NativeFunction::kNeedsContext),
      NativeFunction("concatOperator", {},
                     DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8(), utf8(),
                                    utf8(), utf8(), utf8()},
                     utf8(), kResultNullIfNull,
                     "concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),
      NativeFunction("concatOperator", {},
                     DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8(), utf8(),
                                    utf8(), utf8(), utf8(), utf8()},
                     utf8(), kResultNullIfNull,
                     "concatOperator_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),

      // concat treats null inputs as empty strings whereas concatOperator returns null if
      // one of the inputs is null
      NativeFunction("concat", {}, DataTypeVector{utf8(), utf8()}, utf8(),
                     kResultNullNever, "concat_utf8_utf8", NativeFunction::kNeedsContext),
      NativeFunction("concat", {}, DataTypeVector{utf8(), utf8(), utf8()}, utf8(),
                     kResultNullNever, "concat_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),
      NativeFunction("concat", {}, DataTypeVector{utf8(), utf8(), utf8(), utf8()}, utf8(),
                     kResultNullNever, "concat_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),
      NativeFunction("concat", {}, DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8()},
                     utf8(), kResultNullNever, "concat_utf8_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),
      NativeFunction("concat", {},
                     DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8(), utf8()},
                     utf8(), kResultNullNever, "concat_utf8_utf8_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),
      NativeFunction(
          "concat", {},
          DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8(), utf8(), utf8()}, utf8(),
          kResultNullNever, "concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8",
          NativeFunction::kNeedsContext),
      NativeFunction(
          "concat", {},
          DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8(), utf8(), utf8(), utf8()},
          utf8(), kResultNullNever, "concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8",
          NativeFunction::kNeedsContext),
      NativeFunction("concat", {},
                     DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8(), utf8(),
                                    utf8(), utf8(), utf8()},
                     utf8(), kResultNullNever,
                     "concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),
      NativeFunction("concat", {},
                     DataTypeVector{utf8(), utf8(), utf8(), utf8(), utf8(), utf8(),
                                    utf8(), utf8(), utf8(), utf8()},
                     utf8(), kResultNullNever,
                     "concat_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext),

      NativeFunction("quote", {}, DataTypeVector{utf8()}, utf8(), kResultNullIfNull,
                     "quote_utf8", NativeFunction::kNeedsContext),

      NativeFunction("byte_substr", {"bytesubstring"},
                     DataTypeVector{binary(), int32(), int32()}, binary(),
                     kResultNullIfNull, "byte_substr_binary_int32_int32",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_fromUTF8", {"convert_fromutf8"}, DataTypeVector{binary()},
                     utf8(), kResultNullIfNull, "convert_fromUTF8_binary",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_replaceUTF8", {"convert_replaceutf8"},
                     DataTypeVector{binary(), utf8()}, utf8(), kResultNullIfNull,
                     "convert_replace_invalid_fromUTF8_binary",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toDOUBLE", {}, DataTypeVector{float64()}, binary(),
                     kResultNullIfNull, "convert_toDOUBLE",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toDOUBLE_be", {}, DataTypeVector{float64()}, binary(),
                     kResultNullIfNull, "convert_toDOUBLE_be",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toFLOAT", {}, DataTypeVector{float32()}, binary(),
                     kResultNullIfNull, "convert_toFLOAT", NativeFunction::kNeedsContext),

      NativeFunction("convert_toFLOAT_be", {}, DataTypeVector{float32()}, binary(),
                     kResultNullIfNull, "convert_toFLOAT_be",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toINT", {}, DataTypeVector{int32()}, binary(),
                     kResultNullIfNull, "convert_toINT", NativeFunction::kNeedsContext),

      NativeFunction("convert_toINT_be", {}, DataTypeVector{int32()}, binary(),
                     kResultNullIfNull, "convert_toINT_be",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toBIGINT", {}, DataTypeVector{int64()}, binary(),
                     kResultNullIfNull, "convert_toBIGINT",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toBIGINT_be", {}, DataTypeVector{int64()}, binary(),
                     kResultNullIfNull, "convert_toBIGINT_be",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toBOOLEAN_BYTE", {}, DataTypeVector{boolean()}, binary(),
                     kResultNullIfNull, "convert_toBOOLEAN",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toTIME_EPOCH", {}, DataTypeVector{time32()}, binary(),
                     kResultNullIfNull, "convert_toTIME_EPOCH",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toTIME_EPOCH_be", {}, DataTypeVector{time32()}, binary(),
                     kResultNullIfNull, "convert_toTIME_EPOCH_be",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toTIMESTAMP_EPOCH", {}, DataTypeVector{timestamp()},
                     binary(), kResultNullIfNull, "convert_toTIMESTAMP_EPOCH",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toTIMESTAMP_EPOCH_be", {}, DataTypeVector{timestamp()},
                     binary(), kResultNullIfNull, "convert_toTIMESTAMP_EPOCH_be",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toDATE_EPOCH", {}, DataTypeVector{date64()}, binary(),
                     kResultNullIfNull, "convert_toDATE_EPOCH",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toDATE_EPOCH_be", {}, DataTypeVector{date64()}, binary(),
                     kResultNullIfNull, "convert_toDATE_EPOCH_be",
                     NativeFunction::kNeedsContext),

      NativeFunction("convert_toUTF8", {}, DataTypeVector{utf8()}, binary(),
                     kResultNullIfNull, "convert_toUTF8", NativeFunction::kNeedsContext),

      NativeFunction("locate", {"position"}, DataTypeVector{utf8(), utf8(), int32()},
                     int32(), kResultNullIfNull, "locate_utf8_utf8_int32",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("replace", {}, DataTypeVector{utf8(), utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "replace_utf8_utf8_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("binary_string", {}, DataTypeVector{utf8()}, binary(),
                     kResultNullIfNull, "binary_string", NativeFunction::kNeedsContext),

      NativeFunction("left", {}, DataTypeVector{utf8(), int32()}, utf8(),
                     kResultNullIfNull, "left_utf8_int32", NativeFunction::kNeedsContext),

      NativeFunction("right", {}, DataTypeVector{utf8(), int32()}, utf8(),
                     kResultNullIfNull, "right_utf8_int32",
                     NativeFunction::kNeedsContext),

      NativeFunction("castVARBINARY", {}, DataTypeVector{binary(), int64()}, binary(),
                     kResultNullIfNull, "castVARBINARY_binary_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARBINARY", {}, DataTypeVector{utf8(), int64()}, binary(),
                     kResultNullIfNull, "castVARBINARY_utf8_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARBINARY", {}, DataTypeVector{int32(), int64()}, binary(),
                     kResultNullIfNull, "gdv_fn_castVARBINARY_int32_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARBINARY", {}, DataTypeVector{int64(), int64()}, binary(),
                     kResultNullIfNull, "gdv_fn_castVARBINARY_int64_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARBINARY", {}, DataTypeVector{float32(), int64()}, binary(),
                     kResultNullIfNull, "gdv_fn_castVARBINARY_float32_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARBINARY", {}, DataTypeVector{float64(), int64()}, binary(),
                     kResultNullIfNull, "gdv_fn_castVARBINARY_float64_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("levenshtein", {}, DataTypeVector{utf8(), utf8()}, int32(),
                     kResultNullIfNull, "levenshtein",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("to_hex", {"hex"}, DataTypeVector{binary()}, utf8(),
                     kResultNullIfNull, "to_hex_binary", NativeFunction::kNeedsContext),

      NativeFunction("to_hex", {"hex"}, DataTypeVector{utf8()}, utf8(), kResultNullIfNull,
                     "to_hex_binary", NativeFunction::kNeedsContext),

      NativeFunction("to_hex", {"hex"}, DataTypeVector{int64()}, utf8(),
                     kResultNullIfNull, "to_hex_int64", NativeFunction::kNeedsContext),

      NativeFunction("to_hex", {"hex"}, DataTypeVector{int32()}, utf8(),
                     kResultNullIfNull, "to_hex_int32", NativeFunction::kNeedsContext),

      NativeFunction("from_hex", {"unhex"}, DataTypeVector{utf8()}, binary(),
                     kResultNullInternal, "from_hex_utf8", NativeFunction::kNeedsContext),

      NativeFunction("split_part", {}, DataTypeVector{utf8(), utf8(), int32()}, utf8(),
                     kResultNullIfNull, "split_part",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("aes_encrypt", {}, DataTypeVector{utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "gdv_fn_aes_encrypt",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("aes_decrypt", {}, DataTypeVector{utf8(), utf8()}, utf8(),
                     kResultNullIfNull, "gdv_fn_aes_decrypt",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("mask_first_n", {}, DataTypeVector{utf8(), int32()}, utf8(),
                     kResultNullIfNull, "gdv_mask_first_n_utf8_int32",
                     NativeFunction::kNeedsContext),

      NativeFunction("mask_last_n", {}, DataTypeVector{utf8(), int32()}, utf8(),
                     kResultNullIfNull, "gdv_mask_last_n_utf8_int32",
                     NativeFunction::kNeedsContext),

      NativeFunction("instr", {}, DataTypeVector{utf8(), utf8()}, int32(),
                     kResultNullIfNull, "instr_utf8")};

  return string_fn_registry_;
}

#undef BINARY_RELATIONAL_SAFE_NULL_IF_NULL_FN

#undef BINARY_RELATIONAL_SAFE_NULL_IF_NULL_UTF8_FN

#undef UNARY_OCTET_LEN_FN

#undef UNARY_SAFE_NULL_NEVER_BOOL_FN

}  // namespace gandiva
