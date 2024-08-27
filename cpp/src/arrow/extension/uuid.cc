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

#include <sstream>

#include "arrow/extension_type.h"
#include "arrow/util/logging.h"

#include "arrow/extension/uuid.h"

namespace arrow::extension {

bool UuidType::ExtensionEquals(const ExtensionType& other) const {
  return (other.extension_name() == this->extension_name());
}

std::shared_ptr<Array> UuidType::MakeArray(std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("arrow.uuid",
            static_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<UuidArray>(data);
}

Result<std::shared_ptr<DataType>> UuidType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized) const {
  if (!serialized.empty()) {
    return Status::Invalid("Unexpected serialized metadata: '", serialized, "'");
  }
  if (!storage_type->Equals(*fixed_size_binary(16))) {
    return Status::Invalid("Invalid storage type for UuidType: ",
                           storage_type->ToString());
  }
  return std::make_shared<UuidType>();
}

std::string UuidType::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << "extension<" << this->extension_name() << ">";
  return ss.str();
}

std::shared_ptr<DataType> uuid() { return std::make_shared<UuidType>(); }

}  // namespace arrow::extension
