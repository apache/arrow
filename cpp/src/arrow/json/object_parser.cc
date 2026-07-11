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

#include "arrow/json/object_parser.h"

#include <simdjson.h>

namespace arrow {
namespace json {
namespace internal {

class ObjectParser::Impl {
 public:
  Status Parse(std::string_view json) {
    // Copy into padded buffer
    padded_json_ = simdjson::padded_string(json);

    // Parse
    auto result = parser_.parse(padded_json_);

    // Handle parse errors
    if (result.error()) {
      return Status::Invalid("Json parse error: ",
                             simdjson::error_message(result.error()));
    }

    // Store parsed document
    document_ = std::move(result).value();

    // Validate root is an object
    if (!document_.is_object()) {
      return Status::TypeError("Not a json object");
    }

    return Status::OK();
  }

  Result<std::string> GetString(const char* key) const {
    auto field = document_[key];
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

    return std::string(str_result.value());
  }

  Result<std::unordered_map<std::string, std::string>> GetStringMap() const {
    std::unordered_map<std::string, std::string> map;

    auto obj_result = document_.get_object();
    if (obj_result.error()) {
      return Status::TypeError("Document is not an object");
    }

    simdjson::dom::object obj = obj_result.value();

    for (auto [key, value] : obj) {
      auto str_result = value.get_string();

      if (str_result.error() == simdjson::INCORRECT_TYPE) {
        return Status::TypeError("Key '", std::string(key),
                                 "' does not have a string value");
      }
      if (str_result.error()) {
        return Status::Invalid("Error getting value for key '", std::string(key),
                               "': ", simdjson::error_message(str_result.error()));
      }

      map.emplace(std::string(key), std::string(str_result.value()));
    }

    return map;
  }

  Result<bool> GetBool(const char* key) const {
    auto field = document_[key];
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

    return bool_result.value();
  }

 private:
  simdjson::dom::parser parser_;
  simdjson::padded_string padded_json_;
  simdjson::dom::element document_;
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

}  // namespace internal
}  // namespace json
}  // namespace arrow
