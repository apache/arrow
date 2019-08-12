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

#include "gandiva/function_registry_timestamp_arithmetic.h"
#include "gandiva/function_registry_common.h"

namespace gandiva {

#define TIMESTAMP_ADD_FNS(name, ALIASES)                                            \
  BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int32, timestamp, timestamp),     \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int32, date64, date64),       \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int64, timestamp, timestamp), \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int64, date64, date64)

#define TIMESTAMP_DIFF_FN(name, ALIASES) \
  BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, timestamp, timestamp, int32)

#define DATE_ADD_FNS(name, ALIASES)                                                 \
  BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, date64, int32, date64),           \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, timestamp, int32, timestamp), \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, date64, int64, date64),       \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, timestamp, int64, timestamp), \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int32, date64, date64),       \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int32, timestamp, timestamp), \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int64, date64, date64),       \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int64, timestamp, timestamp)

#define DATE_DIFF_FNS(name, ALIASES)                                             \
  BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int32, date64, date64),        \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int32, timestamp, date64), \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int64, date64, date64),    \
      BINARY_GENERIC_SAFE_NULL_IF_NULL(name, ALIASES, int64, timestamp, date64)

std::vector<NativeFunction> GetDateTimeArithmeticFunctionRegistry() {
  static std::vector<NativeFunction> datetime_fn_registry_ = {
      BINARY_GENERIC_SAFE_NULL_IF_NULL(months_between, {}, date64, date64, float64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(months_between, {}, timestamp, timestamp, float64),

      TIMESTAMP_DIFF_FN(timestampdiffSecond, {}),
      TIMESTAMP_DIFF_FN(timestampdiffMinute, {}),
      TIMESTAMP_DIFF_FN(timestampdiffHour, {}),
      TIMESTAMP_DIFF_FN(timestampdiffDay, {}),
      TIMESTAMP_DIFF_FN(timestampdiffWeek, {}),
      TIMESTAMP_DIFF_FN(timestampdiffMonth, {}),
      TIMESTAMP_DIFF_FN(timestampdiffQuarter, {}),
      TIMESTAMP_DIFF_FN(timestampdiffYear, {}),

      TIMESTAMP_ADD_FNS(timestampaddSecond, {}),
      TIMESTAMP_ADD_FNS(timestampaddMinute, {}),
      TIMESTAMP_ADD_FNS(timestampaddHour, {}),
      TIMESTAMP_ADD_FNS(timestampaddDay, {}),
      TIMESTAMP_ADD_FNS(timestampaddWeek, {}),
      TIMESTAMP_ADD_FNS(timestampaddMonth, {}),
      TIMESTAMP_ADD_FNS(timestampaddQuarter, {}),
      TIMESTAMP_ADD_FNS(timestampaddYear, {}),

      DATE_ADD_FNS(date_add, {}),
      DATE_ADD_FNS(add, {}),

      NativeFunction("add", {}, DataTypeVector{date64(), int64()}, timestamp(),
                     kResultNullIfNull, "add_date64_int64"),

      DATE_DIFF_FNS(date_sub, {}),
      DATE_DIFF_FNS(subtract, {}),
      DATE_DIFF_FNS(date_diff, {})};

  return datetime_fn_registry_;
}

}  // namespace gandiva
