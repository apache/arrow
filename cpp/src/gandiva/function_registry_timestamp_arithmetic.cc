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

std::vector<NativeFunction> FunctionRegistryDateTimeArithmetic::GetFunctionRegistry() {
  // list of registered native functions.
  static std::vector<NativeFunction> datetime_fn_registry_ = {
      BINARY_GENERIC_SAFE_NULL_IF_NULL(months_between, date64, date64, float64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(months_between, timestamp, timestamp, float64),

      // timestamp diff operations
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffSecond, timestamp, timestamp, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffMinute, timestamp, timestamp, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffHour, timestamp, timestamp, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffDay, timestamp, timestamp, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffWeek, timestamp, timestamp, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffMonth, timestamp, timestamp, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffQuarter, timestamp, timestamp, int32),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampdiffYear, timestamp, timestamp, int32),

      // timestamp add int32 operations
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddSecond, timestamp, int32, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMinute, timestamp, int32, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddHour, timestamp, int32, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddDay, timestamp, int32, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddWeek, timestamp, int32, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMonth, timestamp, int32, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddQuarter, timestamp, int32, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddYear, timestamp, int32, timestamp),
      // date add int32 operations
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddSecond, date64, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMinute, date64, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddHour, date64, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddDay, date64, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddWeek, date64, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMonth, date64, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddQuarter, date64, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddYear, date64, int32, date64),

      // timestamp add int64 operations
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddSecond, timestamp, int64, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMinute, timestamp, int64, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddHour, timestamp, int64, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddDay, timestamp, int64, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddWeek, timestamp, int64, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMonth, timestamp, int64, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddQuarter, timestamp, int64, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddYear, timestamp, int64, timestamp),
      // date add int64 operations
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddSecond, date64, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMinute, date64, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddHour, date64, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddDay, date64, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddWeek, date64, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddMonth, date64, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddQuarter, date64, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(timestampaddYear, date64, int64, date64),

      // date_add(date64, int32), date_add(timestamp, int32)
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, date64, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(add, date64, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, timestamp, int32, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(add, timestamp, int32, timestamp),

      // date_add(date64, int64), date_add(timestamp, int64)
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, date64, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(add, date64, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, timestamp, int64, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(add, timestamp, int64, timestamp),

      // date_add(int32, date64), date_add(int32, timestamp)
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, int32, date64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(add, int32, date64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, int32, timestamp, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(add, int32, timestamp, timestamp),

      // date_add(int64, date64), date_add(int64, timestamp)
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, int64, date64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(add, int64, date64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_add, int64, timestamp, timestamp),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(add, int64, timestamp, timestamp),

      // date_sub(date64, int32), subtract and date_diff
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_sub, date64, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(subtract, date64, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_diff, date64, int32, date64),
      // date_sub(timestamp, int32), subtract and date_diff
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_sub, timestamp, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(subtract, timestamp, int32, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_diff, timestamp, int32, date64),

      // date_sub(date64, int64), subtract and date_diff
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_sub, date64, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(subtract, date64, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_diff, date64, int64, date64),
      // date_sub(timestamp, int64), subtract and date_diff
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_sub, timestamp, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(subtract, timestamp, int64, date64),
      BINARY_GENERIC_SAFE_NULL_IF_NULL(date_diff, timestamp, int64, date64)};

  return datetime_fn_registry_;
}

}  // namespace gandiva
