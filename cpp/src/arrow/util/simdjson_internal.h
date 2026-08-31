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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <simdjson.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow::internal {

/// This class is a helper to parse a JSON object from a string.
/// It uses simdjson in the implementation.
class ARROW_EXPORT ObjectParser {
 public:
  ObjectParser();
  ~ObjectParser();

  Status Parse(std::string_view json);

  Result<std::string> GetString(const char* key) const;

  Result<bool> GetBool(const char* key) const;

  // Get all members of the object as a map from string keys to string values
  Result<std::unordered_map<std::string, std::string>> GetStringMap() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

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

  Status WriteValue(simdjson::ondemand::value value);

  void Null();

  void StringField(std::string_view key, std::string_view value);
  void BoolField(std::string_view key, bool value);
  void IntField(std::string_view key, int32_t value);

  Result<std::string_view> GetString() const;

  Result<std::string> GetPrettyString(
      const simdjson::fractured_json_options& options = {}) const;

  void Clear();

 private:
  void MaybeComma();

  simdjson::builder::string_builder builder_;
  bool needs_comma_ = false;
};

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

ARROW_EXPORT const char* JsonTypeName(simdjson::dom::element_type type);

ARROW_EXPORT Result<simdjson::dom::array> GetJsonArray(simdjson::dom::element value,
                                                       std::string_view name);

ARROW_EXPORT Result<int64_t> GetJsonInt(simdjson::dom::element value,
                                        std::string_view name, std::string_view expected);

ARROW_EXPORT Result<simdjson::dom::object> ParseJsonObject(simdjson::dom::parser& parser,
                                                           const std::string& json);

// object.at_key() performs a linear search. This is acceptable here
// since these objects are expected to contain only a small number of fields.
ARROW_EXPORT Result<std::optional<simdjson::dom::element>> GetOptionalJsonField(
    const simdjson::dom::object& object, std::string_view key);

ARROW_EXPORT Result<std::vector<int64_t>> GetJsonIntArray(simdjson::dom::element value,
                                                          std::string_view name);

ARROW_EXPORT Result<std::vector<std::optional<int64_t>>> GetJsonNullableIntArray(
    simdjson::dom::element value, std::string_view name);

ARROW_EXPORT Result<std::vector<std::string>> GetJsonStringArray(
    simdjson::dom::element value, std::string_view name);

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

ARROW_EXPORT const char* JsonTypeName(simdjson::ondemand::json_type type);

// Result<bool> because peeking the nonRootScalar can fail (parsed lazily)
ARROW_EXPORT Result<bool> IsJsonNull(simdjson::ondemand::value& value);

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

ARROW_EXPORT Result<std::string> MinifyJson(std::string_view json);

ARROW_EXPORT Status ValidateJsonObject(simdjson::ondemand::object object);

ARROW_EXPORT Status ValidateJsonArray(simdjson::ondemand::array array);

ARROW_EXPORT Status ConsumeJsonValue(simdjson::ondemand::value value);

ARROW_EXPORT Status ValidateJsonDocument(simdjson::ondemand::parser& parser,
                                         simdjson::padded_string& json);

}  // namespace arrow::internal
