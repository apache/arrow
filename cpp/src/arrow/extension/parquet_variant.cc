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

#include "arrow/extension/parquet_variant.h"

#include <algorithm>
#include <string>

#include "arrow/extension_type.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging_internal.h"

namespace arrow::extension {

VariantExtensionType::VariantExtensionType(const std::shared_ptr<DataType>& storage_type)
    : ExtensionType(storage_type) {
  // IsSupportedStorageType should have been called already, asserting that
  // metadata is present and at least one of value / typed_value is present.
  for (const auto& field : storage_type->fields()) {
    if (field->name() == "metadata") {
      metadata_ = field;
    } else if (field->name() == "value") {
      value_ = field;
    } else if (field->name() == "typed_value") {
      typed_value_ = field;
    }
  }
}

bool VariantExtensionType::ExtensionEquals(const ExtensionType& other) const {
  return other.extension_name() == this->extension_name() &&
         other.storage_type()->Equals(this->storage_type());
}

Result<std::shared_ptr<DataType>> VariantExtensionType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized) const {
  if (!serialized.empty()) {
    return Status::Invalid("Unexpected serialized metadata: '", serialized, "'");
  }
  return VariantExtensionType::Make(std::move(storage_type));
}

std::string VariantExtensionType::Serialize() const { return ""; }

std::shared_ptr<Array> VariantExtensionType::MakeArray(
    std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ(kVariantExtensionName,
            internal::checked_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<VariantArray>(data);
}

namespace {

bool IsSupportedPrimitiveTypedValue(const std::shared_ptr<DataType>& type) {
  switch (type->id()) {
    case Type::BOOL:
    case Type::INT8:
    case Type::INT16:
    case Type::INT32:
    case Type::INT64:
    case Type::FLOAT:
    case Type::DOUBLE:
    case Type::DATE32:
    case Type::BINARY:
    case Type::LARGE_BINARY:
    case Type::BINARY_VIEW:
    case Type::STRING:
    case Type::LARGE_STRING:
    case Type::STRING_VIEW:
      return true;
    case Type::DECIMAL32:
    case Type::DECIMAL64:
    case Type::DECIMAL128: {
      const auto& decimal = internal::checked_cast<const DecimalType&>(*type);
      return decimal.scale() >= 0 && decimal.scale() <= decimal.precision();
    }
    case Type::TIME64:
      return internal::checked_cast<const Time64Type&>(*type).unit() == TimeUnit::MICRO;
    case Type::TIMESTAMP: {
      const auto unit = internal::checked_cast<const TimestampType&>(*type).unit();
      return unit == TimeUnit::MICRO || unit == TimeUnit::NANO;
    }
    case Type::FIXED_SIZE_BINARY:
      return internal::checked_cast<const FixedSizeBinaryType&>(*type).byte_width() == 16;
    case Type::EXTENSION: {
      const auto& ext_type = internal::checked_cast<const ExtensionType&>(*type);
      return ext_type.extension_name() == "arrow.uuid";
    }
    default:
      return false;
  }
}

bool IsSupportedTypedValue(const std::shared_ptr<Field>& field);

bool IsVariantFieldGroup(const std::shared_ptr<DataType>& type) {
  if (type->id() != Type::STRUCT) {
    return false;
  }

  std::shared_ptr<Field> value;
  std::shared_ptr<Field> typed_value;
  for (const auto& field : type->fields()) {
    if (field->name() == "value") {
      if (value != nullptr || !field->nullable() ||
          !is_binary_or_binary_view(field->type()->storage_id())) {
        return false;
      }
      value = field;
    } else if (field->name() == "typed_value") {
      if (typed_value != nullptr || !IsSupportedTypedValue(field)) {
        return false;
      }
      typed_value = field;
    } else {
      return false;
    }
  }
  return value != nullptr || typed_value != nullptr;
}

bool IsSupportedTypedValue(const std::shared_ptr<Field>& field) {
  if (!field->nullable()) {
    return false;
  }
  auto is_variant_field_group = [](const auto& field) {
    return !field->nullable() && IsVariantFieldGroup(field->type());
  };

  switch (field->type()->id()) {
    case Type::STRUCT:
      return field->type()->num_fields() > 0 &&
             std::ranges::all_of(field->type()->fields(), is_variant_field_group);
    case Type::LIST:
    case Type::LARGE_LIST:
    case Type::LIST_VIEW:
    case Type::LARGE_LIST_VIEW:
    case Type::FIXED_SIZE_LIST:
      return is_variant_field_group(field->type()->field(0));
    default:
      return IsSupportedPrimitiveTypedValue(field->type());
  }
}

}  // namespace

bool VariantExtensionType::IsSupportedStorageType(
    const std::shared_ptr<DataType>& storage_type) {
  if (storage_type->id() != Type::STRUCT) {
    return false;
  }

  std::shared_ptr<Field> metadata;
  std::shared_ptr<Field> value;
  std::shared_ptr<Field> typed_value;

  for (const auto& field : storage_type->fields()) {
    if (field->name() == "metadata") {
      if (metadata != nullptr || !is_binary_or_binary_view(field->type()->storage_id()) ||
          field->nullable()) {
        return false;
      }
      metadata = field;
    } else if (field->name() == "value") {
      if (value != nullptr || !is_binary_or_binary_view(field->type()->storage_id())) {
        return false;
      }
      value = field;
    } else if (field->name() == "typed_value") {
      if (typed_value != nullptr || !IsSupportedTypedValue(field)) {
        return false;
      }
      typed_value = field;
    } else {
      return false;
    }
  }

  if (metadata == nullptr || (value == nullptr && typed_value == nullptr)) {
    return false;
  }
  if (value == nullptr) {
    return true;
  }
  if (typed_value == nullptr) {
    return !value->nullable();
  }
  return value->nullable();
}

Result<std::shared_ptr<DataType>> VariantExtensionType::Make(
    std::shared_ptr<DataType> storage_type) {
  if (!IsSupportedStorageType(storage_type)) {
    return Status::Invalid("Invalid storage type for VariantExtensionType: ",
                           storage_type->ToString());
  }

  return std::make_shared<VariantExtensionType>(std::move(storage_type));
}

std::shared_ptr<DataType> variant(std::shared_ptr<DataType> storage_type) {
  return VariantExtensionType::Make(std::move(storage_type)).ValueOrDie();
}

}  // namespace arrow::extension
