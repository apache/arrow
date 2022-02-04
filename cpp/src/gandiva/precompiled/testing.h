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

#pragma once

#include <ctime>
#include <string>

#include <gtest/gtest.h>

#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"

#include "gandiva/date_utils.h"
#include "gandiva/precompiled/time_constants.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

static inline gdv_timestamp StringToTimestamp(const std::string& s) {
  int64_t out = 0;
  bool success = ::arrow::internal::ParseTimestampStrptime(
      s.c_str(), s.length(), "%Y-%m-%d %H:%M:%S", /*ignore_time_in_day=*/false,
      /*allow_trailing_chars=*/false, ::arrow::TimeUnit::SECOND, &out);
  DCHECK(success);
  ARROW_UNUSED(success);
  return out * 1000;
}

static inline gdv_int32 ExtractTime(int32_t hour, int32_t minute, int32_t seconds,
                                    int32_t millis) {
  DCHECK(hour < 24 && hour >= 0);
  DCHECK(minute < 60 && minute >= 0);
  DCHECK(seconds < 60 && seconds >= 0);
  DCHECK(millis < 1000 && millis >= 0);

  int32_t total_millis = (hour * MILLIS_IN_HOUR) + (minute * MILLIS_IN_MIN) +
                         (seconds * MILLIS_IN_SEC) + millis;

  return total_millis;
}

static inline gdv_int64 ExtractIntervalDay(int64_t days, int64_t hours, int64_t minutes,
                                           int64_t seconds, int64_t millis) {
  int64_t total_millis = (hours * MILLIS_IN_HOUR) + (minutes * MILLIS_IN_MIN) +
                         (seconds * MILLIS_IN_SEC) + millis;

  return ((total_millis & 0x00000000FFFFFFFF) << 32) | (days & 0x00000000FFFFFFFF);
}

static inline gdv_int64 ExtractIntervalYear(int64_t years, int64_t months) {
  return ((months & 0x00000000FFFFFFFF) << 32) | (years & 0x00000000FFFFFFFF);
}

}  // namespace gandiva
