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

template <typename ObjectFn, typename ArrayFn, typename StringFn, typename BoolFn,
          typename NullFn, typename NumberFn>
Status VisitJsonValue(simdjson::ondemand::value value, ObjectFn&& object_fn,
                      ArrayFn&& array_fn, StringFn&& string_fn, BoolFn&& bool_fn,
                      NullFn&& null_fn, NumberFn&& number_fn) {
  ARROW_ASSIGN_OR_RAISE(
      auto type, GetSimdjsonResult(value.type(), "Failed to determine JSON type: "));

  switch (type) {
    case simdjson::ondemand::json_type::object: {
      ARROW_ASSIGN_OR_RAISE(
          auto object,
          GetSimdjsonResult(value.get_object(), "Failed to get JSON object: "));
      return object_fn(object);
    }

    case simdjson::ondemand::json_type::array: {
      ARROW_ASSIGN_OR_RAISE(
          auto array, GetSimdjsonResult(value.get_array(), "Failed to get JSON array: "));
      return array_fn(array);
    }

    case simdjson::ondemand::json_type::string: {
      ARROW_ASSIGN_OR_RAISE(
          auto string,
          GetSimdjsonResult(value.get_string(), "Failed to get JSON string: "));
      return string_fn(string);
    }

    case simdjson::ondemand::json_type::boolean: {
      ARROW_ASSIGN_OR_RAISE(
          auto boolean,
          GetSimdjsonResult(value.get_bool(), "Failed to get JSON boolean: "));
      return bool_fn(boolean);
    }

    case simdjson::ondemand::json_type::null:
      return null_fn();

    case simdjson::ondemand::json_type::number:
      return number_fn(value);

    case simdjson::ondemand::json_type::unknown:
      return Status::Invalid("Unknown JSON type");
  }

  return Status::Invalid("Unreachable");
}

}  // namespace internal
}  // namespace arrow
