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

#include "arrow/json/object_writer.h"
#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace rj = arrow::rapidjson;

namespace arrow {
namespace json {
namespace internal {

class ObjectWriter::Impl {
 public:
  Impl() : root_(rj::kObjectType) {}

  void SetString(arrow::util::string_view key, arrow::util::string_view value) {
    rj::Document::AllocatorType& allocator = document_.GetAllocator();

    rj::Value str_key(key.data(), allocator);
    rj::Value str_value(value.data(), allocator);

    root_.AddMember(str_key, str_value, allocator);
  }

  void SetBool(arrow::util::string_view key, bool value) {
    rj::Document::AllocatorType& allocator = document_.GetAllocator();

    rj::Value str_key(key.data(), allocator);

    root_.AddMember(str_key, value, allocator);
  }

  std::string Serialize() {
    rj::StringBuffer buffer;
    rj::Writer<rj::StringBuffer> writer(buffer);
    root_.Accept(writer);

    return buffer.GetString();
  }

 private:
  rj::Document document_;
  rj::Value root_;
};

ObjectWriter::ObjectWriter() : impl_(new ObjectWriter::Impl()) {}

ObjectWriter::~ObjectWriter() = default;

void ObjectWriter::SetString(arrow::util::string_view key,
                             arrow::util::string_view value) {
  impl_->SetString(key, value);
}

void ObjectWriter::SetBool(arrow::util::string_view key, bool value) {
  impl_->SetBool(key, value);
}

std::string ObjectWriter::Serialize() { return impl_->Serialize(); }

}  // namespace internal
}  // namespace json
}  // namespace arrow
