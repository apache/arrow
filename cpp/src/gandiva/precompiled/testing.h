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
#include "gandiva/precompiled/types.h"

namespace gandiva {

static inline gdv_timestamp StringToTimestamp(const std::string& s) {
  int64_t out = 0;
  bool success = ::arrow::internal::ParseTimestampStrptime(
      s.c_str(), s.length(), "%Y-%m-%d %H:%M:%S", /*ignore_time_in_day=*/false,
      /*allow_trailing_chars=*/false, ::arrow::TimeUnit::SECOND, &out);
  ARROW_DCHECK(success);
  ARROW_UNUSED(success);
  return out * 1000;
}

}  // namespace gandiva
