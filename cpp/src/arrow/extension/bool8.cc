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

#include "arrow/extension/bool8.h"
#include "arrow/util/logging.h"

namespace arrow::extension {

bool Bool8Type::ExtensionEquals(const ExtensionType& other) const {
  return extension_name() == other.extension_name();
}

std::string Bool8Type::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << "extension<" << this->extension_name() << ">";
  return ss.str();
}

std::string Bool8Type::Serialize() const { return ""; }

Result<std::shared_ptr<DataType>> Bool8Type::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized_data) const {
  if (storage_type->id() != Type::INT8) {
    return Status::Invalid("Expected INT8 storage type, got ", storage_type->ToString());
  }
  if (serialized_data != "") {
    return Status::Invalid("Serialize data must be empty, got ", serialized_data);
  }
  return bool8();
}

std::shared_ptr<Array> Bool8Type::MakeArray(std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("arrow.bool8",
            internal::checked_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<Bool8Array>(data);
}

Result<std::shared_ptr<DataType>> Bool8Type::Make() {
  return std::make_shared<Bool8Type>();
}

std::shared_ptr<DataType> bool8() { return std::make_shared<Bool8Type>(); }

}  // namespace arrow::extension
