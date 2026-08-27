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
