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

#include "arrow/json/array_writer.h"
#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace rj = arrow::rapidjson;

namespace arrow {
namespace json {
namespace internal {

class ArrayWriter::Impl {
 public:
  Impl() : root_(rj::kArrayType) {}

  void AppendInt64(int64_t value) {
    rj::Document::AllocatorType& allocator = document_.GetAllocator();

    root_.PushBack(value, allocator);
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

ArrayWriter::ArrayWriter() : impl_(new ArrayWriter::Impl()) {}

ArrayWriter::~ArrayWriter() = default;

void ArrayWriter::AppendInt64(int64_t value) { impl_->AppendInt64(value); }

std::string ArrayWriter::Serialize() { return impl_->Serialize(); }

}  // namespace internal
}  // namespace json
}  // namespace arrow
