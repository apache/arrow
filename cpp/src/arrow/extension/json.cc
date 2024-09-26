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

#include "arrow/extension/json.h"

#include <string>

#include "arrow/extension_type.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"

namespace arrow::extension {

bool JsonExtensionType::ExtensionEquals(const ExtensionType& other) const {
  return other.extension_name() == this->extension_name() &&
         other.storage_type()->Equals(storage_type_);
}

Result<std::shared_ptr<DataType>> JsonExtensionType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized) const {
  return JsonExtensionType::Make(std::move(storage_type));
}

std::string JsonExtensionType::Serialize() const { return ""; }

std::shared_ptr<Array> JsonExtensionType::MakeArray(
    std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("arrow.json",
            internal::checked_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<ExtensionArray>(data);
}

bool JsonExtensionType::IsSupportedStorageType(Type::type type_id) {
  return type_id == Type::STRING || type_id == Type::STRING_VIEW ||
         type_id == Type::LARGE_STRING;
}

Result<std::shared_ptr<DataType>> JsonExtensionType::Make(
    std::shared_ptr<DataType> storage_type) {
  if (!IsSupportedStorageType(storage_type->id())) {
    return Status::Invalid("Invalid storage type for JsonExtensionType: ",
                           storage_type->ToString());
  }
  return std::make_shared<JsonExtensionType>(std::move(storage_type));
}

std::shared_ptr<DataType> json(std::shared_ptr<DataType> storage_type) {
  return JsonExtensionType::Make(std::move(storage_type)).ValueOrDie();
}

}  // namespace arrow::extension
