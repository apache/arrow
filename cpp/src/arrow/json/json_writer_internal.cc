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
#include "arrow/util/simdjson_internal.h"

namespace arrow::json {

namespace sj = simdjson::ondemand;

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

Status JsonWriter::WriteValue(sj::value value) {
  ARROW_ASSIGN_OR_RAISE(auto type, internal::GetSimdjsonResult(
                                       value.type(), "Failed to determine JSON type: "));

  switch (type) {
    case sj::json_type::object: {
      StartObject();

      ARROW_ASSIGN_OR_RAISE(
          auto object,
          internal::GetSimdjsonResult(value.get_object(), "Failed to get JSON object: "));

      for (auto field : object) {
        ARROW_ASSIGN_OR_RAISE(auto key,
                              internal::GetSimdjsonResult(field.unescaped_key(),
                                                          "Failed to get object key: "));

        Key(key);

        ARROW_ASSIGN_OR_RAISE(
            auto field_value,
            internal::GetSimdjsonResult(field.value(), "Failed to get object value: "));

        RETURN_NOT_OK(WriteValue(field_value));
      }

      EndObject();
      break;
    }

    case sj::json_type::array: {
      StartArray();

      ARROW_ASSIGN_OR_RAISE(
          auto array,
          internal::GetSimdjsonResult(value.get_array(), "Failed to get JSON array: "));

      for (auto element : array) {
        ARROW_ASSIGN_OR_RAISE(
            auto element_value,
            internal::GetSimdjsonResult(element, "Failed to iterate JSON array: "));

        RETURN_NOT_OK(WriteValue(element_value));
      }

      EndArray();
      break;
    }

    case sj::json_type::string: {
      ARROW_ASSIGN_OR_RAISE(
          auto string_value,
          internal::GetSimdjsonResult(value.get_string(), "Failed to get JSON string: "));

      String(string_value);
      break;
    }

    case sj::json_type::boolean: {
      ARROW_ASSIGN_OR_RAISE(
          auto bool_value,
          internal::GetSimdjsonResult(value.get_bool(), "Failed to get JSON boolean: "));

      Bool(bool_value);
      break;
    }

    case sj::json_type::null: {
      Null();
      break;
    }

    case sj::json_type::number: {
      ARROW_ASSIGN_OR_RAISE(
          auto number_type,
          internal::GetSimdjsonResult(value.get_number_type(),
                                      "Failed to determine JSON number type: "));

      if (number_type == sj::number_type::big_integer) {
        ARROW_ASSIGN_OR_RAISE(auto raw_json,
                              internal::GetSimdjsonResult(simdjson::to_json_string(value),
                                                          "Failed to get raw JSON: "));
        RawValue(raw_json);
        break;
      }

      ARROW_ASSIGN_OR_RAISE(
          auto number, internal::GetSimdjsonResult(value.get_number(),
                                                   "Failed to convert JSON number: "));
      switch (number_type) {
        case sj::number_type::signed_integer:
          Int64(number.get_int64());
          break;

        case sj::number_type::unsigned_integer:
          Uint64(number.get_uint64());
          break;

        case sj::number_type::floating_point_number:
          Double(number.get_double());
          break;

        case sj::number_type::big_integer:
          // Big integers are handled before calling get_number()
          break;
      }

      break;
    }

    case sj::json_type::unknown: {
      return Status::Invalid("Unknown JSON type");
    }
  }

  return Status::OK();
}

void JsonWriter::Null() {
  MaybeComma();
  builder_.append_null();
  needs_comma_ = true;
}

Result<std::string_view> JsonWriter::GetString() const {
  std::string_view view;
  if (auto error = builder_.view().get(view); error != simdjson::SUCCESS) {
    if (error == simdjson::OUT_OF_CAPACITY) {
      return Status::OutOfMemory(
          "OutOfMemory when allocating buffer to serialize json to string");
    }
    return Status::Invalid("Failed to retrieve json from string builder: ",
                           simdjson::error_message(error));
  }
  return view;
}

void JsonWriter::Clear() {
  builder_.clear();
  needs_comma_ = false;
}

void JsonWriter::MaybeComma() {
  if (needs_comma_) {
    builder_.append_comma();
  }
}

void JsonWriter::StringField(std::string_view key, std::string_view value) {
  Key(key);
  String(value);
}

void JsonWriter::BoolField(std::string_view key, bool value) {
  Key(key);
  Bool(value);
}

}  // namespace arrow::json
