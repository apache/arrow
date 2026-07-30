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
  sj::json_type type;
  if (auto error = value.type().get(type); error != simdjson::SUCCESS) {
    return Status::Invalid(simdjson::error_message(error));
  }

  switch (type) {
    case sj::json_type::object: {
      StartObject();

      sj::object object;
      if (auto error = value.get_object().get(object); error != simdjson::SUCCESS) {
        return Status::Invalid(simdjson::error_message(error));
      }

      for (auto field : object) {
        std::string_view key;
        if (auto error = field.unescaped_key().get(key); error != simdjson::SUCCESS) {
          return Status::Invalid(simdjson::error_message(error));
        }

        Key(key);

        sj::value field_value;
        if (auto error = field.value().get(field_value); error != simdjson::SUCCESS) {
          return Status::Invalid(simdjson::error_message(error));
        }

        RETURN_NOT_OK(WriteValue(field_value));
      }

      EndObject();
      break;
    }

    case sj::json_type::array: {
      StartArray();

      sj::array array;
      if (auto error = value.get_array().get(array); error != simdjson::SUCCESS) {
        return Status::Invalid(simdjson::error_message(error));
      }

      for (auto element : array) {
        sj::value element_value;
        if (auto error = element.get(element_value); error != simdjson::SUCCESS) {
          return Status::Invalid(simdjson::error_message(error));
        }

        RETURN_NOT_OK(WriteValue(element_value));
      }

      EndArray();
      break;
    }

    case sj::json_type::string: {
      std::string_view string_value;
      if (auto error = value.get_string().get(string_value); error != simdjson::SUCCESS) {
        return Status::Invalid(simdjson::error_message(error));
      }

      String(string_value);
      break;
    }

    case sj::json_type::boolean: {
      bool bool_value;
      if (auto error = value.get_bool().get(bool_value); error != simdjson::SUCCESS) {
        return Status::Invalid(simdjson::error_message(error));
      }

      Bool(bool_value);
      break;
    }

    case sj::json_type::null: {
      Null();
      break;
    }

    case sj::json_type::number: {
      sj::number number;
      if (auto error = value.get_number().get(number); error != simdjson::SUCCESS) {
        return Status::Invalid("Failed to convert JSON number: ",
                               simdjson::error_message(error));
      }

      switch (number.get_number_type()) {
        case sj::number_type::signed_integer:
          Int64(number.get_int64());
          break;

        case sj::number_type::unsigned_integer:
          Uint64(number.get_uint64());
          break;

        case sj::number_type::floating_point_number:
          Double(number.get_double());
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
