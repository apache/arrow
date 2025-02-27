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

#include "parquet/arrow/variant.h"

#include <string>

#include "arrow/extension_type.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"

namespace parquet::arrow {

using ::arrow::Type;

bool VariantExtensionType::ExtensionEquals(const ExtensionType& other) const {
  return other.extension_name() == this->extension_name() &&
         other.storage_type()->Equals(this->storage_type_);
}

Result<std::shared_ptr<DataType>> VariantExtensionType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized) const {
  return VariantExtensionType::Make(std::move(storage_type));
}

std::string VariantExtensionType::Serialize() const { return ""; }

std::shared_ptr<Array> VariantExtensionType::MakeArray(
    std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("parquet.variant",
            ::arrow::internal::checked_cast<const ExtensionType&>(*data->type)
                .extension_name());
  return std::make_shared<::arrow::ExtensionArray>(data);
}

bool VariantExtensionType::IsSupportedStorageType(
    std::shared_ptr<DataType> storage_type) {
  // For now, we only supported unshredded variants. Unshredded variant storage
  // type should be a struct with a binary metadata and binary value.
  //
  // TODO(neilechao) In shredded variants, the binary value field can be replaced
  // with one or more of the following: object, array, typed_value, and
  // variant_value.
  if (storage_type->id() == Type::STRUCT) {
    if (storage_type->num_fields() == 2) {
      return storage_type->field(0)->type()->storage_id() == Type::BINARY &&
             storage_type->field(1)->type()->storage_id() == Type::BINARY;
    }
  }

  return false;
}

Result<std::shared_ptr<DataType>> VariantExtensionType::Make(
    std::shared_ptr<DataType> storage_type) {
  if (!IsSupportedStorageType(storage_type)) {
    return ::arrow::Status::Invalid("Invalid storage type for VariantExtensionType: ",
                                    storage_type->ToString());
  }
  return std::make_shared<VariantExtensionType>(std::move(storage_type));
}

std::shared_ptr<DataType> variant(std::shared_ptr<DataType> storage_type) {
  return VariantExtensionType::Make(std::move(storage_type)).ValueOrDie();
}

}  // namespace parquet::arrow
