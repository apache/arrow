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

#include "parquet/arrow/variant_internal.h"

#include <string>

#include "arrow/extension_type.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging_internal.h"

namespace parquet::arrow {

using ::arrow::Array;
using ::arrow::ArrayData;
using ::arrow::DataType;
using ::arrow::ExtensionType;
using ::arrow::Result;
using ::arrow::Type;

VariantExtensionType::VariantExtensionType(
    const std::shared_ptr<::arrow::DataType>& storage_type)
    : ::arrow::ExtensionType(storage_type) {
  // GH-45948: Shredded variants will need to handle an optional shredded_value as
  // well as value_ becoming optional.

  // IsSupportedStorageType should have been called already, asserting that both
  // metadata and value are present.
  if (storage_type->field(0)->name() == "metadata") {
    metadata_ = storage_type->field(0);
    value_ = storage_type->field(1);
  } else {
    value_ = storage_type->field(0);
    metadata_ = storage_type->field(1);
  }
}

bool VariantExtensionType::ExtensionEquals(const ExtensionType& other) const {
  return other.extension_name() == this->extension_name() &&
         other.storage_type()->Equals(this->storage_type());
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
  return std::make_shared<VariantArray>(data);
}

namespace {
bool IsBinaryField(const std::shared_ptr<::arrow::Field> field) {
  return field->type()->storage_id() == Type::BINARY ||
         field->type()->storage_id() == Type::LARGE_BINARY;
}
}  // namespace

bool VariantExtensionType::IsSupportedStorageType(
    const std::shared_ptr<DataType>& storage_type) {
  // For now we only supported unshredded variants. Unshredded variant storage
  // type should be a struct with a binary metadata and binary value.
  //
  // GH-45948: In shredded variants, the binary value field can be replaced
  // with one or more of the following: object, array, typed_value, and
  // variant_value.
  if (storage_type->id() == Type::STRUCT) {
    if (storage_type->num_fields() == 2) {
      // Ordering of metadata and value fields does not matter, as we will assign
      // these to the VariantExtensionType's member shared_ptrs in the constructor.
      // Here we just need to check that they are both present.

      const auto& field0 = storage_type->field(0);
      const auto& field1 = storage_type->field(1);

      bool metadata_and_value_present =
          (field0->name() == "metadata" && field1->name() == "value") ||
          (field1->name() == "metadata" && field0->name() == "value");

      if (metadata_and_value_present) {
        // Both metadata and value must be non-nullable binary types for unshredded
        // variants. This will change in GH-46948, when we will require a Visitor
        // to traverse the structure of the variant.
        return IsBinaryField(field0) && IsBinaryField(field1) && !field0->nullable() &&
               !field1->nullable();
      }
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

/// NOTE: this is still experimental. GH-45948 will add shredding support, at which point
/// we need to separate this into unshredded_variant and shredded_variant helper
/// functions.
std::shared_ptr<DataType> variant(std::shared_ptr<DataType> storage_type) {
  return VariantExtensionType::Make(std::move(storage_type)).ValueOrDie();
}

}  // namespace parquet::arrow
