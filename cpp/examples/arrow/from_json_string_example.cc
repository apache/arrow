// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// This example shows how to use some of the *FromJSONString helpers.

#include <iostream>
#include <memory>

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/json/from_string.h>
#include <arrow/status.h>

using arrow::json::ArrayFromJSONString;
using arrow::json::ChunkedArrayFromJSONString;
using arrow::json::DictArrayFromJSONString;

/**
 * \brief Run Example
 *
 * ./debug/from-json-string-example
 */
arrow::Status RunExample() {
  // Simple types
  ARROW_ASSIGN_OR_RAISE(auto int32_array,
                        ArrayFromJSONString(arrow::int32(), "[1, 2, 3]"));
  ARROW_ASSIGN_OR_RAISE(auto float64_array,
                        ArrayFromJSONString(arrow::float64(), "[4.0, 5.0, 6.0]"));
  ARROW_ASSIGN_OR_RAISE(auto bool_array,
                        ArrayFromJSONString(arrow::boolean(), "[true, false, true]"));
  ARROW_ASSIGN_OR_RAISE(
      auto string_array,
      ArrayFromJSONString(arrow::utf8(), R"(["Hello", "World", null])"));

  // Timestamps can be created from string representations
  ARROW_ASSIGN_OR_RAISE(
      auto ts_array,
      ArrayFromJSONString(timestamp(arrow::TimeUnit::SECOND),
                          R"(["1970-01-01", "2000-02-29","3989-07-14","1900-02-28"])"));

  // List, Map, Struct
  ARROW_ASSIGN_OR_RAISE(
      auto list_array,
      ArrayFromJSONString(list(arrow::int64()),
                          "[[null], [], null, [4, 5, 6, 7, 8], [2, 3]]"));
  ARROW_ASSIGN_OR_RAISE(
      auto map_array,
      ArrayFromJSONString(map(arrow::utf8(), arrow::int32()),
                          R"([[["joe", 0], ["mark", null]], null, [["cap", 8]], []])"));
  ARROW_ASSIGN_OR_RAISE(
      auto struct_array,
      ArrayFromJSONString(
          arrow::struct_({field("one", arrow::int32()), field("two", arrow::int32())}),
          "[[11, 22], null, [null, 33]]"));

  // ChunkedArrayFromJSONString
  ARROW_ASSIGN_OR_RAISE(
      auto chunked_array,
      ChunkedArrayFromJSONString(arrow::int32(), {"[5, 10]", "[null]", "[16]"}));

  // DictArrayFromJSONString
  ARROW_ASSIGN_OR_RAISE(
      auto dict_array,
      DictArrayFromJSONString(dictionary(arrow::int32(), arrow::utf8()),
                              "[0, 1, 0, 2, 0, 3]", R"(["k1", "k2", "k3", "k4"])"));

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  auto status = RunExample();
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
