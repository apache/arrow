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

// Implement Arrow JSON serialization format

#ifndef ARROW_IPC_JSON_H
#define ARROW_IPC_JSON_H

#define RAPIDJSON_HAS_STDSTRING 1
#define RAPIDJSON_HAS_CXX11_RVALUE_REFS 1
#define RAPIDJSON_HAS_CXX11_RANGE_FOR 1

#include <memory>
#include <string>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace rj = rapidjson;
using RjWriter = rj::Writer<rj::StringBuffer>;

namespace arrow {
namespace ipc {

Status ARROW_EXPORT WriteJsonSchema(const Schema& schema, RjWriter* json_writer);
Status ARROW_EXPORT WriteJsonArray(
    const std::string& name, const Array& array, RjWriter* json_writer);

Status ARROW_EXPORT ReadJsonSchema(
    const rj::Value& json_arr, std::shared_ptr<Schema>* schema);
Status ARROW_EXPORT ReadJsonArray(MemoryPool* pool, const rj::Value& json_obj,
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* array);

Status ARROW_EXPORT ReadJsonArray(MemoryPool* pool, const rj::Value& json_obj,
    const Schema& schema, std::shared_ptr<Array>* array);

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_FILE_H
