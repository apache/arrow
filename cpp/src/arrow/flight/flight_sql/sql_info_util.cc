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

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/flight/flight_sql/sql_info_util.h>

#define PROPERTY_TO_OPTIONAL(COMMAND, PROPERTY) \
  COMMAND.has_##PROPERTY() ? util::make_optional(COMMAND.PROPERTY()) : util::nullopt;

#define REGISTER_FIELD(name, type, nullable) \
  const auto& name = field(ARROW_STRINGIFY(name), type, nullable);

#define REGISTER_FIELD_NOT_NULL(name, type) REGISTER_FIELD(name, type, false)

namespace arrow {
namespace flight {
namespace sql {
namespace internal {

Status SqlInfoResultAppender::operator()(const std::string& value) {
  const int8_t type_index = STRING_VALUE_INDEX;
  ARROW_RETURN_NOT_OK(value_builder_.Append(type_index));
  auto* string_value_builder =
      reinterpret_cast<StringBuilder*>(value_builder_.child(type_index));
  ARROW_RETURN_NOT_OK(string_value_builder->Append(value));
  return Status::OK();
}

Status SqlInfoResultAppender::operator()(const bool value) {
  const int8_t type_index = BOOL_VALUE_INDEX;
  ARROW_RETURN_NOT_OK(value_builder_.Append(type_index));
  auto* bool_value_builder =
      reinterpret_cast<BooleanBuilder*>(value_builder_.child(type_index));
  ARROW_RETURN_NOT_OK(bool_value_builder->Append(value));
  return Status::OK();
}

Status SqlInfoResultAppender::operator()(const int64_t value) {
  const int8_t type_index = BIGINT_VALUE_INDEX;
  ARROW_RETURN_NOT_OK(value_builder_.Append(type_index));
  auto* bigint_value_builder =
      reinterpret_cast<Int64Builder*>(value_builder_.child(type_index));
  ARROW_RETURN_NOT_OK(bigint_value_builder->Append(value));
  return Status::OK();
}

Status SqlInfoResultAppender::operator()(const int32_t value) {
  const int8_t type_index = INT32_BITMASK_INDEX;
  ARROW_RETURN_NOT_OK(value_builder_.Append(type_index));
  auto* int32_bitmask_builder =
      reinterpret_cast<Int32Builder*>(value_builder_.child(type_index));
  ARROW_RETURN_NOT_OK(int32_bitmask_builder->Append(value));
  return Status::OK();
}

Status SqlInfoResultAppender::operator()(const string_list_t& value) {
  const int8_t type_index = STRING_LIST_INDEX;
  ARROW_RETURN_NOT_OK(value_builder_.Append(type_index));
  auto* string_list_builder =
      reinterpret_cast<ListBuilder*>(value_builder_.child(type_index));
  ARROW_RETURN_NOT_OK(string_list_builder->Append());
  auto* string_list_child =
      reinterpret_cast<StringBuilder*>(string_list_builder->value_builder());
  for (const auto& string : value) {
    ARROW_RETURN_NOT_OK(string_list_child->Append(string));
  }
  return Status::OK();
}

Status SqlInfoResultAppender::operator()(const int32_to_int32_list_t& value) {
  const int8_t type_index = INT32_TO_INT32_LIST_INDEX;
  ARROW_RETURN_NOT_OK(value_builder_.Append(type_index));
  auto* int32_to_int32_list_builder =
      reinterpret_cast<MapBuilder*>(value_builder_.child(type_index));
  ARROW_RETURN_NOT_OK(int32_to_int32_list_builder->Append());
  for (const auto& pair : value) {
    ARROW_RETURN_NOT_OK(
        reinterpret_cast<Int32Builder*>(int32_to_int32_list_builder->key_builder())
            ->Append(pair.first));
    auto* int32_list_builder =
        reinterpret_cast<ListBuilder*>(int32_to_int32_list_builder->item_builder());
    ARROW_RETURN_NOT_OK(int32_list_builder->Append());
    auto* int32_list_child =
        reinterpret_cast<Int32Builder*>(int32_list_builder->value_builder());
    for (const auto& int32 : pair.second) {
      ARROW_RETURN_NOT_OK(int32_list_child->Append(int32));
    }
  }
  return Status::OK();
}

SqlInfoResultAppender::SqlInfoResultAppender(DenseUnionBuilder& value_builder)
    : value_builder_(value_builder) {}

}  // namespace internal
}  // namespace sql
}  // namespace flight
}  // namespace arrow
