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

#include <concepts>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <simdjson.h>

#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace internal {

// Empty struct to represent the type of a simdjson null value
struct SimdjsonNull {};

template <typename T>
struct JsonTypeNameOf;

template <>
struct JsonTypeNameOf<simdjson::ondemand::array> {
  static constexpr const char* kValue = "array";
};

template <>
struct JsonTypeNameOf<simdjson::ondemand::object> {
  static constexpr const char* kValue = "object";
};

template <>
struct JsonTypeNameOf<std::string_view> {
  static constexpr const char* kValue = "string";
};

template <>
struct JsonTypeNameOf<bool> {
  static constexpr const char* kValue = "boolean";
};

template <>
struct JsonTypeNameOf<SimdjsonNull> {
  static constexpr const char* kValue = "null";
};

template <>
struct JsonTypeNameOf<int64_t> {
  static constexpr const char* kValue = "number";
};

template <>
struct JsonTypeNameOf<uint64_t> {
  static constexpr const char* kValue = "number";
};

template <>
struct JsonTypeNameOf<double> {
  static constexpr const char* kValue = "number";
};

template <typename T>
constexpr const char* JsonTypeName() {
  return JsonTypeNameOf<T>::kValue;
}

template <typename T>
Result<T> ResolveSimdjsonResult(simdjson::simdjson_result<T> result,
                                std::string_view error) {
  T value;
  if (auto error_code = std::move(result).get(value); error_code != simdjson::SUCCESS) {
    return Status::Invalid(error, ": ", simdjson::error_message(error_code));
  }
  return value;
}

inline const char* JsonTypeName(simdjson::dom::element_type type) {
  switch (type) {
    case simdjson::dom::element_type::ARRAY:
      return "array";
    case simdjson::dom::element_type::OBJECT:
      return "object";
    case simdjson::dom::element_type::INT64:
    case simdjson::dom::element_type::UINT64:
    case simdjson::dom::element_type::DOUBLE:
    case simdjson::dom::element_type::BIGINT:
      return "number";
    case simdjson::dom::element_type::STRING:
      return "string";
    case simdjson::dom::element_type::BOOL:
      return "boolean";
    case simdjson::dom::element_type::NULL_VALUE:
      return "null";
  }
  return "unknown";
}

inline Result<simdjson::dom::array> GetJsonArray(simdjson::dom::element value,
                                                 std::string_view name) {
  if (!value.is_array()) {
    return Status::Invalid(name, " must be an array, got ", JsonTypeName(value.type()));
  }
  return ResolveSimdjsonResult(value.get_array(), "Failed to get JSON array");
}

inline Result<int64_t> GetJsonInt(simdjson::dom::element value, std::string_view name,
                                  std::string_view expected) {
  if (!value.is_int64()) {
    return Status::Invalid(name, " must contain ", expected, ", got ",
                           JsonTypeName(value.type()));
  }
  return ResolveSimdjsonResult(value.get_int64(), "Failed to get JSON integer");
}

inline Result<simdjson::dom::object> ParseJsonObject(simdjson::dom::parser& parser,
                                                     const std::string& json) {
  return ResolveSimdjsonResult(parser.parse(json).get_object(),
                               "Invalid serialized JSON data");
}

// object.at_key() performs a linear search. This is acceptable here
// since these objects are expected to contain only a small number of fields.
inline Result<std::optional<simdjson::dom::element>> GetOptionalJsonField(
    const simdjson::dom::object& object, std::string_view key) {
  auto field = object.at_key(key);
  if (field.error() == simdjson::NO_SUCH_FIELD) {
    return std::nullopt;
  }

  ARROW_ASSIGN_OR_RAISE(
      auto value,
      ResolveSimdjsonResult(std::move(field), "Failed to get JSON object field"));

  return std::optional<simdjson::dom::element>(value);
}

inline Result<std::vector<int64_t>> GetJsonIntArray(simdjson::dom::element value,
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

inline Result<std::vector<std::optional<int64_t>>> GetJsonNullableIntArray(
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

inline Result<std::vector<std::string>> GetJsonStringArray(simdjson::dom::element value,
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

template <typename ObjectFn, typename ArrayFn, typename StringFn, typename BoolFn,
          typename NullFn, typename Int64Fn, typename Uint64Fn, typename DoubleFn,
          typename BigIntegerFn>
Status VisitJsonValue(simdjson::ondemand::value value, ObjectFn&& object_fn,
                      ArrayFn&& array_fn, StringFn&& string_fn, BoolFn&& bool_fn,
                      NullFn&& null_fn, Int64Fn&& int64_fn, Uint64Fn&& uint64_fn,
                      DoubleFn&& double_fn, BigIntegerFn&& big_integer_fn) {
  ARROW_ASSIGN_OR_RAISE(
      auto type, ResolveSimdjsonResult(value.type(), "Failed to determine JSON type"));

  switch (type) {
    case simdjson::ondemand::json_type::object: {
      ARROW_ASSIGN_OR_RAISE(
          auto object,
          ResolveSimdjsonResult(value.get_object(), "Failed to get JSON object"));
      return object_fn(object);
    }

    case simdjson::ondemand::json_type::array: {
      ARROW_ASSIGN_OR_RAISE(
          auto array,
          ResolveSimdjsonResult(value.get_array(), "Failed to get JSON array"));
      return array_fn(array);
    }

    case simdjson::ondemand::json_type::string: {
      ARROW_ASSIGN_OR_RAISE(
          auto string,
          ResolveSimdjsonResult(value.get_string(), "Failed to get JSON string"));
      return string_fn(string);
    }

    case simdjson::ondemand::json_type::boolean: {
      ARROW_ASSIGN_OR_RAISE(
          auto boolean,
          ResolveSimdjsonResult(value.get_bool(), "Failed to get JSON boolean"));
      return bool_fn(boolean);
    }

    case simdjson::ondemand::json_type::null:
      return null_fn();

    case simdjson::ondemand::json_type::number: {
      ARROW_ASSIGN_OR_RAISE(
          auto number_type,
          ResolveSimdjsonResult(value.get_number_type(),
                                "Failed to determine JSON number type"));

      switch (number_type) {
        case simdjson::ondemand::number_type::signed_integer: {
          ARROW_ASSIGN_OR_RAISE(
              auto number,
              ResolveSimdjsonResult(value.get_int64(), "Failed to get signed integer"));
          return int64_fn(number);
        }

        case simdjson::ondemand::number_type::unsigned_integer: {
          ARROW_ASSIGN_OR_RAISE(auto number,
                                ResolveSimdjsonResult(value.get_uint64(),
                                                      "Failed to get unsigned integer"));
          return uint64_fn(number);
        }

        case simdjson::ondemand::number_type::floating_point_number: {
          ARROW_ASSIGN_OR_RAISE(
              auto number, ResolveSimdjsonResult(value.get_double(),
                                                 "Failed to get floating-point number"));
          return double_fn(number);
        }

        case simdjson::ondemand::number_type::big_integer:
          return big_integer_fn(value);
      }

      return Status::Invalid("Unknown JSON number type");
    }

    case simdjson::ondemand::json_type::unknown:
      return Status::Invalid("Unknown JSON type");
  }

  return Status::Invalid("Unreachable");
}

inline const char* JsonTypeName(simdjson::ondemand::json_type type) {
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

// Result<bool> because peeking the nonRootScalar can fail (parsed lazily)
inline Result<bool> IsJsonNull(simdjson::ondemand::value& value) {
  bool is_null;
  auto error_code = value.is_null().get(is_null);
  if (error_code != simdjson::SUCCESS) {
    return Status::Invalid("Error checking for JSON null: ",
                           simdjson::error_message(error_code));
  }
  return is_null;
}

template <typename SimdjsonValueType>
Result<SimdjsonValueType> GetJsonAs(simdjson::ondemand::value& value) {
  SimdjsonValueType typed_value{};
  simdjson::error_code error_code;

  if constexpr (std::same_as<SimdjsonValueType, SimdjsonNull>) {
    // simdjson has no get<>() for null; probe it explicitly
    bool is_null;
    error_code = value.is_null().get(is_null);
    if (error_code == simdjson::SUCCESS && !is_null) {
      error_code = simdjson::INCORRECT_TYPE;
    }
  } else {
    error_code = value.get(typed_value);
  }

  if (error_code != simdjson::SUCCESS) {
    simdjson::ondemand::json_type json_type;
    if (value.type().get(json_type) != simdjson::SUCCESS) {
      if constexpr (std::same_as<SimdjsonValueType, SimdjsonNull>) {
        return Status::Invalid("Expected null, got malformed JSON value");
      } else {
        return Status::Invalid("Expected ", JsonTypeName<SimdjsonValueType>(),
                               ", got malformed JSON value");
      }
    }

    if constexpr (std::same_as<SimdjsonValueType, SimdjsonNull>) {
      return Status::Invalid("Expected null, got JSON type ", JsonTypeName(json_type));
    } else {
      return Status::Invalid("Expected ", JsonTypeName<SimdjsonValueType>(),
                             ", got JSON type ", JsonTypeName(json_type));
    }
  }

  return typed_value;
}

template <typename T>
Result<T> GetJsonField(simdjson::ondemand::object& object, std::string_view key) {
  for (auto field_result : object) {
    ARROW_ASSIGN_OR_RAISE(
        auto field, ResolveSimdjsonResult(field_result, "Failed to iterate JSON object"));

    ARROW_ASSIGN_OR_RAISE(
        auto field_key,
        ResolveSimdjsonResult(field.unescaped_key(), "Failed to get JSON object key"));

    if (field_key == key) {
      auto value = field.value();

      if constexpr (std::is_same_v<T, simdjson::ondemand::value>) {
        return value;
      } else {
        return GetJsonAs<T>(value);
      }
    }
  }

  return Status::KeyError("Missing JSON field: ", key);
}

inline Result<std::string> MinifyJson(std::string_view json) {
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

inline Status ValidateJsonObject(simdjson::ondemand::object object);

inline Status ValidateJsonArray(simdjson::ondemand::array array);

inline Status ConsumeJsonValue(simdjson::ondemand::value value) {
  return VisitJsonValue(
      value, ValidateJsonObject, ValidateJsonArray,
      [](std::string_view) { return Status::OK(); }, [](bool) { return Status::OK(); },
      []() { return Status::OK(); }, [](int64_t) { return Status::OK(); },
      [](uint64_t) { return Status::OK(); }, [](double) { return Status::OK(); },
      [](simdjson::ondemand::value) { return Status::OK(); });
}

inline Status ValidateJsonObject(simdjson::ondemand::object object) {
  for (auto field_result : object) {
    ARROW_ASSIGN_OR_RAISE(
        auto field, ResolveSimdjsonResult(field_result, "Failed to iterate JSON object"));

    RETURN_NOT_OK(ConsumeJsonValue(field.value()));
  }

  return Status::OK();
}

inline Status ValidateJsonArray(simdjson::ondemand::array array) {
  for (auto element_result : array) {
    ARROW_ASSIGN_OR_RAISE(
        auto value,
        ResolveSimdjsonResult(element_result, "Failed to iterate JSON array"));

    RETURN_NOT_OK(ConsumeJsonValue(value));
  }

  return Status::OK();
}

inline Status ValidateJsonDocument(simdjson::ondemand::parser& parser,
                                   simdjson::padded_string& json) {
  ARROW_ASSIGN_OR_RAISE(
      auto document, ResolveSimdjsonResult(parser.iterate(json), "Failed to parse JSON"));

  ARROW_ASSIGN_OR_RAISE(auto value, ResolveSimdjsonResult(document.get_value(),
                                                          "Failed to get JSON value"));

  return ConsumeJsonValue(value);
}

}  // namespace internal
}  // namespace arrow
