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

#include <string_view>

#include <simdjson.h>

#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace internal {

template <typename T>
Result<T> GetSimdjsonResult(simdjson::simdjson_result<T> result, std::string_view error) {
  T value;
  if (auto error_code = std::move(result).get(value); error_code != simdjson::SUCCESS) {
    return Status::Invalid(error, simdjson::error_message(error_code));
  }
  return value;
}

}  // namespace internal
}  // namespace arrow
