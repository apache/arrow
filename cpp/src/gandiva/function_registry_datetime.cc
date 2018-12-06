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
#include <utility>
#include <vector>

namespace gandiva {

void FunctionRegistryDateTime::GetDateTimeFnSignature(SignatureMap* map) {
  // list of registered native functions.

  static NativeFunction date_time_fn_registry_[] = {
      // date/timestamp operations
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractMillennium),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractCentury),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractDecade),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractYear),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractDoy),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractQuarter),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractMonth),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractWeek),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractDow),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractDay),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractHour),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractMinute),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractSecond),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractEpoch),

      // date_trunc operations on date/timestamp
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, date_trunc_Millennium),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, date_trunc_Century),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, date_trunc_Decade),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, date_trunc_Year),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, date_trunc_Quarter),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, date_trunc_Month),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, date_trunc_Week),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, date_trunc_Day),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, date_trunc_Hour),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, date_trunc_Minute),
      DATE_TYPES(EXTRACT_SAFE_NULL_IF_NULL, date_trunc_Second),

      // time operations
      TIME_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractHour),
      TIME_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractMinute),
      TIME_TYPES(EXTRACT_SAFE_NULL_IF_NULL, extractSecond),

      NativeFunction("castDATE", DataTypeVector{utf8()}, date64(), kResultNullIfNull,
                     "castDATE_utf8",
                     NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),

      NativeFunction("to_date", DataTypeVector{utf8(), utf8(), int32()}, date64(),
                     kResultNullInternal, "gdv_fn_to_date_utf8_utf8_int32",
                     NativeFunction::kNeedsContext |
                         NativeFunction::kNeedsFunctionHolder |
                         NativeFunction::kCanReturnErrors)};

  const int num_entries =
      static_cast<int>(sizeof(date_time_fn_registry_) / sizeof(NativeFunction));
  for (int i = 0; i < num_entries; i++) {
    const NativeFunction* entry = &date_time_fn_registry_[i];

    DCHECK(map->find(&entry->signature()) == map->end());
    map->insert(std::pair<const FunctionSignature*, const NativeFunction*>(
        &entry->signature(), entry));
  }
}

}  // namespace gandiva
