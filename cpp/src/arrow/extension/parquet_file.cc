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

#include "arrow/extension/parquet_file.h"

#include "arrow/extension_type.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging_internal.h"

namespace arrow::extension {

namespace {

bool IsSupportedField(const std::shared_ptr<Field>& field) {
  if (!field->nullable()) {
    return false;
  }
  if (field->name() == "uri" || field->name() == "content_type" ||
      field->name() == "checksum") {
    return ::arrow::is_string_or_string_view(field->type()->id());
  }
  if (field->name() == "offset" || field->name() == "size") {
    return field->type()->id() == Type::INT64;
  }
  if (field->name() == "inline") {
    return ::arrow::is_binary_or_binary_view(field->type()->id());
  }
  return false;
}

}  // namespace

FileExtensionType::FileExtensionType(const std::shared_ptr<DataType>& storage_type)
    : ExtensionType(storage_type) {
  for (const auto& field : storage_type->fields()) {
    if (field->name() == "uri") {
      uri_ = field;
    } else if (field->name() == "offset") {
      offset_ = field;
    } else if (field->name() == "size") {
      size_ = field;
    } else if (field->name() == "content_type") {
      content_type_ = field;
    } else if (field->name() == "checksum") {
      checksum_ = field;
    } else if (field->name() == "inline") {
      inline_bytes_ = field;
    }
  }
}

bool FileExtensionType::ExtensionEquals(const ExtensionType& other) const {
  return other.extension_name() == extension_name() &&
         other.storage_type()->Equals(*storage_type());
}

Result<std::shared_ptr<DataType>> FileExtensionType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized) const {
  if (!serialized.empty()) {
    return Status::Invalid("Unexpected serialized metadata: '", serialized, "'");
  }
  return FileExtensionType::Make(std::move(storage_type));
}

std::string FileExtensionType::Serialize() const { return ""; }

std::shared_ptr<Array> FileExtensionType::MakeArray(
    std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ(kFileExtensionName,
            internal::checked_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<FileArray>(std::move(data));
}

bool FileExtensionType::IsSupportedStorageType(
    const std::shared_ptr<DataType>& storage_type) {
  if (!storage_type || storage_type->id() != Type::STRUCT ||
      storage_type->fields().empty()) {
    return false;
  }

  for (const auto& field : storage_type->fields()) {
    if (!IsSupportedField(field)) {
      return false;
    }
  }

  for (int i = 0; i < storage_type->num_fields(); ++i) {
    for (int j = i + 1; j < storage_type->num_fields(); ++j) {
      if (storage_type->field(i)->name() == storage_type->field(j)->name()) {
        return false;
      }
    }
  }
  return true;
}

Result<std::shared_ptr<DataType>> FileExtensionType::Make(
    std::shared_ptr<DataType> storage_type) {
  if (!IsSupportedStorageType(storage_type)) {
    return Status::Invalid("Invalid storage type for FileExtensionType: ",
                           storage_type ? storage_type->ToString() : "null");
  }
  return std::make_shared<FileExtensionType>(std::move(storage_type));
}

std::shared_ptr<DataType> file(std::shared_ptr<DataType> storage_type) {
  return FileExtensionType::Make(std::move(storage_type)).ValueOrDie();
}

}  // namespace arrow::extension
