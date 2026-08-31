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

#include "arrow/util/simdjson_internal.h"

namespace arrow::internal {

class ObjectParser::Impl {
 public:
  Status Parse(std::string_view json) {
    // Copy into padded buffer
    padded_json_ = simdjson::padded_string(json);

    // Store parsed document
    if (auto error = parser_.iterate(padded_json_).get(document_)) {
      return Status::Invalid("JSON parse error: ", simdjson::error_message(error));
    }

    // Validate root is an object
    auto object = document_.get_object();
    if (object.error()) {
      if (object.error() == simdjson::INCORRECT_TYPE) {
        return Status::TypeError("Not a JSON object");
      }
      return Status::Invalid("JSON parse error: ",
                             simdjson::error_message(object.error()));
    }

    return Status::OK();
  }

  Result<std::string> GetString(const char* key) {
    document_.rewind();

    auto object = document_.get_object();

    auto field = object.find_field(key);

    if (field.error() == simdjson::NO_SUCH_FIELD) {
      return Status::KeyError("Key '", key, "' does not exist");
    }
    if (field.error()) {
      return Status::Invalid("Error accessing key '", key,
                             "': ", simdjson::error_message(field.error()));
    }

    auto str_result = field.get_string();
    if (str_result.error() == simdjson::INCORRECT_TYPE) {
      return Status::TypeError("Key '", key, "' is not a string");
    }
    if (str_result.error()) {
      return Status::Invalid("Error getting string for key '", key,
                             "': ", simdjson::error_message(str_result.error()));
    }

    std::string_view str;
    if (auto error = std::move(str_result).get(str)) {
      return Status::Invalid("Error getting string for key '", key,
                             "': ", simdjson::error_message(error));
    }
    return std::string(str);
  }

  Result<std::unordered_map<std::string, std::string>> GetStringMap() {
    std::unordered_map<std::string, std::string> map;

    document_.rewind();

    auto object = document_.get_object();

    for (auto field : object) {
      std::string_view key;
      if (auto error = field.unescaped_key().get(key)) {
        return Status::Invalid("Error getting object key: ",
                               simdjson::error_message(error));
      }

      auto value = field.value();

      auto str_result = value.get_string();

      if (str_result.error() == simdjson::INCORRECT_TYPE) {
        return Status::TypeError("Key '", std::string(key),
                                 "' does not have a string value");
      }
      if (str_result.error()) {
        return Status::Invalid("Error getting value for key '", std::string(key),
                               "': (code=", static_cast<int>(str_result.error()), ")");
      }

      std::string_view str;
      if (auto error = std::move(str_result).get(str)) {
        return Status::Invalid("Error getting value for key '", std::string(key),
                               "': ", simdjson::error_message(error));
      }

      map.emplace(std::string(key), std::string(str));
    }

    return map;
  }

  Result<bool> GetBool(const char* key) {
    document_.rewind();

    auto object = document_.get_object();

    auto field = object.find_field(key);

    if (field.error() == simdjson::NO_SUCH_FIELD) {
      return Status::KeyError("Key '", key, "' does not exist");
    }
    if (field.error()) {
      return Status::Invalid("Error accessing key '", key,
                             "': ", simdjson::error_message(field.error()));
    }

    auto bool_result = field.get_bool();
    if (bool_result.error() == simdjson::INCORRECT_TYPE) {
      return Status::TypeError("Key '", key, "' is not a boolean");
    }
    if (bool_result.error()) {
      return Status::Invalid("Error getting bool for key '", key,
                             "': ", simdjson::error_message(bool_result.error()));
    }

    bool value;
    if (auto error = std::move(bool_result).get(value)) {
      return Status::Invalid("Error getting bool for key '", key,
                             "': ", simdjson::error_message(error));
    }

    return value;
  }

 private:
  simdjson::ondemand::parser parser_;
  simdjson::padded_string padded_json_;
  simdjson::ondemand::document document_;
};

ObjectParser::ObjectParser() : impl_(new ObjectParser::Impl()) {}

ObjectParser::~ObjectParser() = default;

Status ObjectParser::Parse(std::string_view json) { return impl_->Parse(json); }

Result<std::string> ObjectParser::GetString(const char* key) const {
  return impl_->GetString(key);
}

Result<bool> ObjectParser::GetBool(const char* key) const { return impl_->GetBool(key); }

Result<std::unordered_map<std::string, std::string>> ObjectParser::GetStringMap() const {
  return impl_->GetStringMap();
}

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
  return VisitJsonValue(
      value,

      [&](sj::object object) -> Status {
        StartObject();

        for (auto field : object) {
          ARROW_ASSIGN_OR_RAISE(
              auto key,
              ResolveSimdjsonResult(field.unescaped_key(), "Failed to get object key"));

          Key(key);

          ARROW_ASSIGN_OR_RAISE(
              auto field_value,
              ResolveSimdjsonResult(field.value(), "Failed to get object value"));

          RETURN_NOT_OK(WriteValue(field_value));
        }

        EndObject();
        return Status::OK();
      },

      [&](sj::array array) -> Status {
        StartArray();

        for (auto element : array) {
          ARROW_ASSIGN_OR_RAISE(
              auto element_value,
              ResolveSimdjsonResult(element, "Failed to iterate JSON array"));

          RETURN_NOT_OK(WriteValue(element_value));
        }

        EndArray();
        return Status::OK();
      },

      [&](std::string_view string_value) -> Status {
        String(string_value);
        return Status::OK();
      },

      [&](bool bool_value) -> Status {
        Bool(bool_value);
        return Status::OK();
      },

      [&]() -> Status {
        Null();
        return Status::OK();
      },

      [&](int64_t value) -> Status {
        Int64(value);
        return Status::OK();
      },

      [&](uint64_t value) -> Status {
        Uint64(value);
        return Status::OK();
      },

      [&](double value) -> Status {
        Double(value);
        return Status::OK();
      },

      [&](sj::value value) -> Status {
        ARROW_ASSIGN_OR_RAISE(auto raw_json,
                              ResolveSimdjsonResult(simdjson::to_json_string(value),
                                                    "Failed to get raw JSON"));
        RawValue(raw_json);
        return Status::OK();
      });
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

Result<std::string> JsonWriter::GetPrettyString(
    const simdjson::fractured_json_options& options) const {
  ARROW_ASSIGN_OR_RAISE(std::string_view json, GetString());
  return simdjson::fractured_json_string(json, options);
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

void JsonWriter::IntField(std::string_view key, int32_t value) {
  Key(key);
  Int(value);
}

const char* JsonTypeName(simdjson::dom::element_type type) {
  switch (type) {
    case simdjson::dom::element_type::ARRAY:
      return "array";
    case simdjson::dom::element_type::OBJECT:
      return "object";
    case simdjson::dom::element_type::INT64:
    case simdjson::dom::element_type::UINT64:
    case simdjson::dom::element_type::DOUBLE:
      return "number";
    case simdjson::dom::element_type::STRING:
      return "string";
    case simdjson::dom::element_type::BOOL:
      return "boolean";
    case simdjson::dom::element_type::NULL_VALUE:
      return "null";
    default:
      return "unknown";
  }
}

Result<simdjson::dom::array> GetJsonArray(simdjson::dom::element value,
                                          std::string_view name) {
  if (!value.is_array()) {
    return Status::Invalid(name, " must be an array, got ", JsonTypeName(value.type()));
  }
  return ResolveSimdjsonResult(value.get_array(), "Failed to get JSON array");
}

Result<int64_t> GetJsonInt(simdjson::dom::element value, std::string_view name,
                           std::string_view expected) {
  if (!value.is_int64()) {
    return Status::Invalid(name, " must contain ", expected, ", got ",
                           JsonTypeName(value.type()));
  }
  return ResolveSimdjsonResult(value.get_int64(), "Failed to get JSON integer");
}

Result<simdjson::dom::object> ParseJsonObject(simdjson::dom::parser& parser,
                                              const std::string& json) {
  return ResolveSimdjsonResult(parser.parse(json).get_object(),
                               "Invalid serialized JSON data");
}

Result<std::optional<simdjson::dom::element>> GetOptionalJsonField(
    const simdjson::dom::object& object, std::string_view key) {
  auto field = object.at_key(key);
  if (field.error() == simdjson::NO_SUCH_FIELD) {
    return std::nullopt;
  }

  ARROW_ASSIGN_OR_RAISE(
      auto value,
      ResolveSimdjsonResult(std::move(field), "Failed to get JSON object field"));

  return std::optional<simdjson::dom::element>(std::move(value));
}

Result<std::vector<int64_t>> GetJsonIntArray(simdjson::dom::element value,
                                             std::string_view name) {
  ARROW_ASSIGN_OR_RAISE(auto array, GetJsonArray(value, name));

  std::vector<int64_t> result;
  result.reserve(array.size());

  for (auto element : array) {
    ARROW_ASSIGN_OR_RAISE(auto number, GetJsonInt(element, name, "integers"));
    result.push_back(number);
  }

  return result;
}

Result<std::vector<std::optional<int64_t>>> GetJsonNullableIntArray(
    simdjson::dom::element value, std::string_view name) {
  ARROW_ASSIGN_OR_RAISE(auto array, GetJsonArray(value, name));

  std::vector<std::optional<int64_t>> result;
  result.reserve(array.size());

  for (auto element : array) {
    if (element.is_null()) {
      result.emplace_back(std::nullopt);
    } else {
      ARROW_ASSIGN_OR_RAISE(auto number, GetJsonInt(element, name, "integers or nulls"));
      result.emplace_back(number);
    }
  }

  return result;
}

Result<std::vector<std::string>> GetJsonStringArray(simdjson::dom::element value,
                                                    std::string_view name) {
  ARROW_ASSIGN_OR_RAISE(auto array, GetJsonArray(value, name));

  std::vector<std::string> result;
  result.reserve(array.size());

  for (auto element : array) {
    if (!element.is_string()) {
      return Status::Invalid(name, " must contain strings, got ",
                             JsonTypeName(element.type()));
    }

    ARROW_ASSIGN_OR_RAISE(
        auto string,
        ResolveSimdjsonResult(element.get_string(), "Failed to get JSON string"));
    result.emplace_back(string);
  }

  return result;
}

const char* JsonTypeName(simdjson::ondemand::json_type type) {
  switch (type) {
    case simdjson::ondemand::json_type::array:
      return "array";
    case simdjson::ondemand::json_type::object:
      return "object";
    case simdjson::ondemand::json_type::number:
      return "number";
    case simdjson::ondemand::json_type::string:
      return "string";
    case simdjson::ondemand::json_type::boolean:
      return "boolean";
    case simdjson::ondemand::json_type::null:
      return "null";
    default:
      return "unknown";
  }
}

Result<bool> IsJsonNull(simdjson::ondemand::value& value) {
  bool is_null;
  auto error_code = value.is_null().get(is_null);
  if (error_code != simdjson::SUCCESS) {
    return Status::Invalid("Error checking for JSON null: ",
                           simdjson::error_message(error_code));
  }
  return is_null;
}

Result<std::string> MinifyJson(std::string_view json) {
  std::string minified(json.size(), '\0');
  size_t minified_len = 0;

  if (auto error =
          simdjson::minify(json.data(), json.size(), minified.data(), minified_len);
      error != simdjson::SUCCESS) {
    return Status::Invalid("Failed to minify JSON: ", simdjson::error_message(error));
  }

  minified.resize(minified_len);
  return minified;
}

Status ConsumeJsonValue(simdjson::ondemand::value value) {
  return VisitJsonValue(
      value, ValidateJsonObject, ValidateJsonArray,
      [](std::string_view) { return Status::OK(); }, [](bool) { return Status::OK(); },
      []() { return Status::OK(); }, [](int64_t) { return Status::OK(); },
      [](uint64_t) { return Status::OK(); }, [](double) { return Status::OK(); },
      [](simdjson::ondemand::value) { return Status::OK(); });
}

Status ValidateJsonObject(simdjson::ondemand::object object) {
  for (auto field_result : object) {
    ARROW_ASSIGN_OR_RAISE(
        auto field, ResolveSimdjsonResult(field_result, "Failed to iterate JSON object"));

    RETURN_NOT_OK(ConsumeJsonValue(field.value()));
  }

  return Status::OK();
}

Status ValidateJsonArray(simdjson::ondemand::array array) {
  for (auto element_result : array) {
    ARROW_ASSIGN_OR_RAISE(
        auto value,
        ResolveSimdjsonResult(element_result, "Failed to iterate JSON array"));

    RETURN_NOT_OK(ConsumeJsonValue(value));
  }

  return Status::OK();
}

Status ValidateJsonDocument(simdjson::ondemand::parser& parser,
                            simdjson::padded_string& json) {
  ARROW_ASSIGN_OR_RAISE(
      auto document, ResolveSimdjsonResult(parser.iterate(json), "Failed to parse JSON"));

  ARROW_ASSIGN_OR_RAISE(auto value, ResolveSimdjsonResult(document.get_value(),
                                                          "Failed to get JSON value"));

  return ConsumeJsonValue(value);
}

}  // namespace arrow::internal
