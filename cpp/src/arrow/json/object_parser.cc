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

    // Store parsed document
    document_ = parser_.iterate(padded_json_);

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

    return std::string(str_result.value());
  }

  Result<std::unordered_map<std::string, std::string>> GetStringMap() {
    std::unordered_map<std::string, std::string> map;

    document_.rewind();

    auto object = document_.get_object();

    for (auto field : object) {
      auto key_result = field.unescaped_key();

      auto key = key_result.value();

      if (key_result.error()) {
        return Status::Invalid("Error getting value for key '", std::string(key),
                               "': ", simdjson::error_message(key_result.error()));
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

      map.emplace(std::string(key), std::string(str_result.value()));
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

    return bool_result.value();
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

}  // namespace internal
}  // namespace json
}  // namespace arrow
