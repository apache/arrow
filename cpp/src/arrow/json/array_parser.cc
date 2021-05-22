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

#include "arrow/json/array_parser.h"
#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep

#include <rapidjson/document.h>

namespace arrow {
namespace json {
namespace internal {

namespace rj = arrow::rapidjson;

class ArrayParser::Impl {
 public:
  Status Parse(arrow::util::string_view json) {
    document_.Parse(reinterpret_cast<const rj::Document::Ch*>(json.data()),
                    static_cast<size_t>(json.size()));

    if (document_.HasParseError()) {
      return Status::Invalid("Json parse error (offset ", document_.GetErrorOffset(),
                             "): ", document_.GetParseError());
    }
    if (!document_.IsArray()) {
      return Status::TypeError("Not a json array");
    }
    return Status::OK();
  }

  Result<int64_t> GetInt64(int32_t ordinal) const {
    if (!document_[ordinal].IsInt64()) {
      return Status::TypeError("Value at ordinal '", ordinal, "' is not a int64");
    }
    return document_[ordinal].GetInt64();
  }

  Result<uint32_t> Length() const { return document_.GetArray().Size(); }

 private:
  rj::Document document_;
};

ArrayParser::ArrayParser() : impl_(new ArrayParser::Impl()) {}

ArrayParser::~ArrayParser() = default;

Status ArrayParser::Parse(arrow::util::string_view json) { return impl_->Parse(json); }

Result<uint32_t> ArrayParser::Length() const { return impl_->Length(); }

Result<int64_t> ArrayParser::GetInt64(int32_t ordinal) const {
  return impl_->GetInt64(ordinal);
}

}  // namespace internal
}  // namespace json
}  // namespace arrow
