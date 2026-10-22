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

#include <cstdint>
#include <memory>
#include <string>

#include <simdjson.h>

#include "arrow/ipc/type_fwd.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

using JsonValue = simdjson::dom::element;
using JsonObject = simdjson::dom::object;
using JsonArray = simdjson::dom::array;

namespace arrow::internal {
class JsonWriter;
}  // namespace arrow::internal

namespace arrow::internal::integration::json {

/// \brief Append integration test Schema format to JSON writer
ARROW_EXPORT
Status WriteSchema(const Schema& schema, const ipc::DictionaryFieldMapper& mapper,
                   JsonWriter*);

ARROW_EXPORT
Status WriteDictionary(int64_t id, const std::shared_ptr<Array>& dictionary, JsonWriter*);

ARROW_EXPORT
Status WriteRecordBatch(const RecordBatch& batch, JsonWriter*);

ARROW_EXPORT
Status WriteArray(const std::string& name, const Array& array, JsonWriter*);

ARROW_EXPORT
Result<std::shared_ptr<Schema>> ReadSchema(const JsonValue& json_obj, MemoryPool* pool,
                                           ipc::DictionaryMemo* dictionary_memo);

ARROW_EXPORT
Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const JsonValue& json_obj, const std::shared_ptr<Schema>& schema,
    ipc::DictionaryMemo* dict_memo, MemoryPool* pool);

// NOTE: Doesn't work with dictionary arrays, use ReadRecordBatch instead.
ARROW_EXPORT
Result<std::shared_ptr<Array>> ReadArray(MemoryPool* pool, const JsonValue& json_obj,
                                         const std::shared_ptr<Field>& field);

}  // namespace arrow::internal::integration::json
