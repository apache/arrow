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
#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep

#include <rapidjson/document.h>

namespace arrow {
namespace json {
namespace internal {

namespace rj = arrow::rapidjson;

class ObjectParser::Impl {
 public:
  Status Parse(arrow::util::string_view json) {
    document_.Parse(reinterpret_cast<const rj::Document::Ch*>(json.data()),
                    static_cast<size_t>(json.size()));

    if (document_.HasParseError()) {
      return Status::Invalid("Json parse error (offset ", document_.GetErrorOffset(),
                             "): ", document_.GetParseError());
    }
    if (!document_.IsObject()) {
      return Status::TypeError("Not a json object");
    }
    return Status::OK();
  }

  Result<std::string> GetString(const char* key) const {
    if (!document_.HasMember(key)) {
      return Status::KeyError("Key '", key, "' does not exist");
    }
    if (!document_[key].IsString()) {
      return Status::TypeError("Key '", key, "' is not a string");
    }
    return document_[key].GetString();
  }

  Result<bool> GetBool(const char* key) const {
    if (!document_.HasMember(key)) {
      return Status::KeyError("Key '", key, "' does not exist");
    }
    if (!document_[key].IsBool()) {
      return Status::TypeError("Key '", key, "' is not a boolean");
    }
    return document_[key].GetBool();
  }

 private:
  rj::Document document_;
};

ObjectParser::ObjectParser() : impl_(new ObjectParser::Impl()) {}

ObjectParser::~ObjectParser() = default;

Status ObjectParser::Parse(arrow::util::string_view json) { return impl_->Parse(json); }

Result<std::string> ObjectParser::GetString(const char* key) const {
  return impl_->GetString(key);
}

Result<bool> ObjectParser::GetBool(const char* key) const { return impl_->GetBool(key); }

}  // namespace internal
}  // namespace json
}  // namespace arrow
