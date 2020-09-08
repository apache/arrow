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

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "arrow/json/object_writer.h"

namespace arrow {

namespace json {

ObjectWriter::ObjectWriter() : _root(rapidjson::kObjectType) {}

void ObjectWriter::SetString(arrow::util::string_view key,
                             arrow::util::string_view value) {
  rapidjson::Document::AllocatorType& allocator = _document.GetAllocator();

  rapidjson::Value str_key(key.data(), allocator);
  rapidjson::Value str_value(value.data(), allocator);

  _root.AddMember(str_key, str_value, allocator);
}

void ObjectWriter::SetBool(arrow::util::string_view key, bool value) {
  rapidjson::Document::AllocatorType& allocator = _document.GetAllocator();

  rapidjson::Value str_key(key.data(), allocator);

  _root.AddMember(str_key, value, allocator);
}

std::string ObjectWriter::Serialize() {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  _root.Accept(writer);

  return buffer.GetString();
}

}  // namespace json

}  // namespace arrow
