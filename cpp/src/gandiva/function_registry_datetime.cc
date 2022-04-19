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

#define DATE_EXTRACTION_TRUNCATION_FNS(INNER, name)                                 \
  DATE_TYPES(INNER, name##Millennium, {}), DATE_TYPES(INNER, name##Century, {}),    \
      DATE_TYPES(INNER, name##Decade, {}), DATE_TYPES(INNER, name##Year, {"year"}), \
      DATE_TYPES(INNER, name##Quarter, ({"quarter"})),                              \
      DATE_TYPES(INNER, name##Month, {"month"}),                                    \
      DATE_TYPES(INNER, name##Week, ({"weekofyear", "yearweek"})),                  \
      DATE_TYPES(INNER, name##Day, ({"day", "dayofmonth"})),                        \
      DATE_TYPES(INNER, name##Hour, {"hour"}),                                      \
      DATE_TYPES(INNER, name##Minute, {"minute"}),                                  \
      DATE_TYPES(INNER, name##Second, {"second"})

#define TO_TIMESTAMP_SAFE_NULL_IF_NULL(NAME, ALIASES, TYPE)                       \
  NativeFunction(#NAME, std::vector<std::string> ALIASES, DataTypeVector{TYPE()}, \
                 timestamp(), kResultNullIfNull, ARROW_STRINGIFY(NAME##_##TYPE))

#define TO_TIME_SAFE_NULL_IF_NULL(NAME, ALIASES, TYPE)                            \
  NativeFunction(#NAME, std::vector<std::string> ALIASES, DataTypeVector{TYPE()}, \
                 time32(), kResultNullIfNull, ARROW_STRINGIFY(NAME##_##TYPE))

#define TIME_EXTRACTION_FNS(name)                                      \
  TIME_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Hour, {"hour"}),         \
      TIME_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Minute, {"minute"}), \
      TIME_TYPES(EXTRACT_SAFE_NULL_IF_NULL, name##Second, {"second"})

std::vector<NativeFunction> GetDateTimeFunctionRegistry() {
  static std::vector<NativeFunction> date_time_fn_registry_ = {
      UNARY_SAFE_NULL_NEVER_BOOL(isnull, {}, day_time_interval),
      UNARY_SAFE_NULL_NEVER_BOOL(isnull, {}, month_interval),
      DATE_EXTRACTION_TRUNCATION_FNS(EXTRACT_SAFE_NULL_IF_NULL, extract),
      DATE_EXTRACTION_TRUNCATION_FNS(TRUNCATE_SAFE_NULL_IF_NULL, date_trunc_),

      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractDoy, {}),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractDow, {}),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractEpoch, {}),

      TIME_EXTRACTION_FNS(extract),

      NativeFunction("castDATE", {}, DataTypeVector{utf8()}, date64(), kResultNullIfNull,
                     "castDATE_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castTIMESTAMP", {}, DataTypeVector{utf8()}, timestamp(),
                     kResultNullIfNull, "castTIMESTAMP_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castVARCHAR", {"varchar"}, DataTypeVector{timestamp(), int64()},
                     utf8(), kResultNullIfNull, "castVARCHAR_timestamp_int64",
                     NativeFunction::kNeedsContext),

      NativeFunction("to_date", {}, DataTypeVector{utf8(), utf8()}, date64(),
                     kResultNullInternal, "gdv_fn_to_date_utf8_utf8",
                     NativeFunction::kNeedsContext |
                         NativeFunction::kNeedsFunctionHolder |
                         NativeFunction::kCanReturnErrors),

      NativeFunction("to_date", {}, DataTypeVector{utf8(), utf8(), int32()}, date64(),
                     kResultNullInternal, "gdv_fn_to_date_utf8_utf8_int32",
                     NativeFunction::kNeedsContext |
                         NativeFunction::kNeedsFunctionHolder |
                         NativeFunction::kCanReturnErrors),
      NativeFunction("castTIMESTAMP", {}, DataTypeVector{date64()}, timestamp(),
                     kResultNullIfNull, "castTIMESTAMP_date64"),

      NativeFunction("castTIMESTAMP", {}, DataTypeVector{int64()}, timestamp(),
                     kResultNullIfNull, "castTIMESTAMP_int64"),

      NativeFunction("castDATE", {"to_date"}, DataTypeVector{timestamp()}, date64(),
                     kResultNullIfNull, "castDATE_timestamp"),

      NativeFunction("castTIME", {}, DataTypeVector{utf8()}, time32(), kResultNullIfNull,
                     "castTIME_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castTIME", {}, DataTypeVector{timestamp()}, time32(),
                     kResultNullIfNull, "castTIME_timestamp"),

      NativeFunction("castTIME", {}, DataTypeVector{int32()}, time32(), kResultNullIfNull,
                     "castTIME_int32"),

      NativeFunction("castBIGINT", {}, DataTypeVector{day_time_interval()}, int64(),
                     kResultNullIfNull, "castBIGINT_daytimeinterval"),

      NativeFunction("castINT", {"castNULLABLEINT"}, DataTypeVector{month_interval()},
                     int32(), kResultNullIfNull, "castINT_year_interval",
                     NativeFunction::kCanReturnErrors),

      NativeFunction("castBIGINT", {"castNULLABLEBIGINT"},
                     DataTypeVector{month_interval()}, int64(), kResultNullIfNull,
                     "castBIGINT_year_interval", NativeFunction::kCanReturnErrors),

      NativeFunction("castNULLABLEINTERVALYEAR", {"castINTERVALYEAR"},
                     DataTypeVector{int32()}, month_interval(), kResultNullIfNull,
                     "castNULLABLEINTERVALYEAR_int32",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castNULLABLEINTERVALYEAR", {"castINTERVALYEAR"},
                     DataTypeVector{int64()}, month_interval(), kResultNullIfNull,
                     "castNULLABLEINTERVALYEAR_int64",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("castNULLABLEINTERVALDAY", {"castINTERVALDAY"},
                     DataTypeVector{int32()}, day_time_interval(), kResultNullIfNull,
                     "castNULLABLEINTERVALDAY_int32"),

      NativeFunction("castNULLABLEINTERVALDAY", {"castINTERVALDAY"},
                     DataTypeVector{int64()}, day_time_interval(), kResultNullIfNull,
                     "castNULLABLEINTERVALDAY_int64"),

      NativeFunction("extractDay", {}, DataTypeVector{day_time_interval()}, int64(),
                     kResultNullIfNull, "extractDay_daytimeinterval"),

      NativeFunction("castINTERVALDAY", {}, DataTypeVector{utf8()}, day_time_interval(),
                     kResultNullInternal, "gdv_fn_cast_intervalday_utf8",
                     NativeFunction::kNeedsContext |
                         NativeFunction::kNeedsFunctionHolder |
                         NativeFunction::kCanReturnErrors),

      NativeFunction(
          "castintervalday", {}, DataTypeVector{utf8(), int32()}, day_time_interval(),
          kResultNullInternal, "gdv_fn_cast_intervalday_utf8_int32",
          NativeFunction::kNeedsContext | NativeFunction::kNeedsFunctionHolder |
              NativeFunction::kCanReturnErrors),

      NativeFunction("castintervalyear", {}, DataTypeVector{utf8()}, month_interval(),
                     kResultNullInternal, "gdv_fn_cast_intervalyear_utf8",
                     NativeFunction::kNeedsContext |
                         NativeFunction::kNeedsFunctionHolder |
                         NativeFunction::kCanReturnErrors),

      NativeFunction(
          "castintervalyear", {}, DataTypeVector{utf8(), int32()}, month_interval(),
          kResultNullInternal, "gdv_fn_cast_intervalyear_utf8_int32",
          NativeFunction::kNeedsContext | NativeFunction::kNeedsFunctionHolder |
              NativeFunction::kCanReturnErrors),

      DATE_TYPES(LAST_DAY_SAFE_NULL_IF_NULL, last_day, {}),
      BASE_NUMERIC_TYPES(TO_TIME_SAFE_NULL_IF_NULL, to_time, {}),
      BASE_NUMERIC_TYPES(TO_TIMESTAMP_SAFE_NULL_IF_NULL, to_timestamp, {})};

  return date_time_fn_registry_;
}

}  // namespace gandiva
