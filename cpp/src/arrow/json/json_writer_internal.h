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

#include <simdjson.h>

#include <cstdint>
#include <string_view>

#include "arrow/util/visibility.h"

namespace arrow::json {

class ARROW_EXPORT JsonWriter {
 public:
  JsonWriter() = default;

  void StartObject();
  void EndObject();

  void StartArray();
  void EndArray();

  void Key(std::string_view key);

  void String(std::string_view value);
  void RawValue(std::string_view value);
  void Bool(bool value);

  void Int(int32_t value);
  void Int64(int64_t value);

  void Uint(uint32_t value);
  void Uint64(uint64_t value);

  void Double(double value);

  void Null();

  std::string_view GetString() const;

  void Clear();

 private:
  void MaybeComma();

  simdjson::builder::string_builder builder_;
  bool needs_comma_ = false;
};

}  // namespace arrow::json
