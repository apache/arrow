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

#include "arrow/extension/variant.h"

#include <string>

#include "arrow/extension_type.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"

namespace arrow::extension {

bool isBinary(Type::type type) {
  return type == Type::BINARY || type == Type::LARGE_BINARY;
}

bool VariantExtensionType::IsSupportedStorageType(
    std::shared_ptr<DataType> storage_type) {
  if (storage_type->id() == Type::STRUCT) {
    // TODO(neilechao) assertions for binary types, and non-nullable first field for
    // metadata
    return storage_type->num_fields() == 3;
  }

  return false;
}

Result<std::shared_ptr<DataType>> VariantExtensionType::Make(
    std::shared_ptr<DataType> storage_type) {
  if (!IsSupportedStorageType(storage_type)) {
    return Status::Invalid(
        "Invalid storage type for VariantExtensionType, must be struct with binary "
        "metadata, value, and typed_value fields: ",
        storage_type->ToString());
  }
  return std::make_shared<VariantExtensionType>(std::move(storage_type));
}

std::shared_ptr<DataType> variant(std::shared_ptr<DataType> storage_type) {
  return VariantExtensionType::Make(std::move(storage_type)).ValueOrDie();
}

}  // namespace arrow::extension
