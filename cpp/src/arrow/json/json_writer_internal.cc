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

#include "arrow/json/json_writer_internal.h"

namespace arrow::json {

void JsonWriter::StartObject() {
  MaybeComma();
  builder_.start_object();
  needs_comma_ = false;
}

void JsonWriter::EndObject() {
  builder_.end_object();
  needs_comma_ = true;
}

void JsonWriter::StartArray() {
  MaybeComma();
  builder_.start_array();
  needs_comma_ = false;
}

void JsonWriter::EndArray() {
  builder_.end_array();
  needs_comma_ = true;
}

void JsonWriter::Key(std::string_view key) {
  MaybeComma();
  builder_.escape_and_append_with_quotes(key);
  builder_.append_colon();
  needs_comma_ = false;
}

void JsonWriter::String(std::string_view value) {
  MaybeComma();
  builder_.escape_and_append_with_quotes(value);
  needs_comma_ = true;
}

void JsonWriter::RawValue(std::string_view value) {
  MaybeComma();
  builder_.append_raw(value);
  needs_comma_ = true;
}

void JsonWriter::Bool(bool value) {
  MaybeComma();
  builder_.append(value);
  needs_comma_ = true;
}

void JsonWriter::Int(int32_t value) {
  MaybeComma();
  builder_.append(value);
  needs_comma_ = true;
}

void JsonWriter::Int64(int64_t value) {
  MaybeComma();
  builder_.append(value);
  needs_comma_ = true;
}

void JsonWriter::Uint(uint32_t value) {
  MaybeComma();
  builder_.append(value);
  needs_comma_ = true;
}

void JsonWriter::Uint64(uint64_t value) {
  MaybeComma();
  builder_.append(value);
  needs_comma_ = true;
}

void JsonWriter::Double(double value) {
  MaybeComma();
  builder_.append(value);
  needs_comma_ = true;
}

void JsonWriter::Null() {
  MaybeComma();
  builder_.append_null();
  needs_comma_ = true;
}

std::string_view JsonWriter::GetString() const { return builder_.view().value(); }

void JsonWriter::Clear() {
  builder_.clear();
  needs_comma_ = false;
}

void JsonWriter::MaybeComma() {
  if (needs_comma_) {
    builder_.append_comma();
  }
}

}  // namespace arrow::json
