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

#include <memory>
#include <mutex>
#include <string>

#include "arrow/extension_type.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace extension {

std::once_flag init_flag;

std::shared_ptr<ExtensionType> json() {
  std::call_once(init_flag, []() {
    DCHECK_OK(RegisterExtensionType(std::make_shared<JsonExtensionType>()));
  });
  return GetExtensionType(JsonExtensionType::type_name());
}

bool JsonExtensionType::ExtensionEquals(const ExtensionType& other) const {
  const auto& other_ext = static_cast<const ExtensionType&>(other);
  return other_ext.extension_name() == this->extension_name();
}

Result<std::shared_ptr<DataType>> JsonExtensionType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized_data) const {
  if (serialized_data != JsonExtensionType::type_name()) {
    return Status::Invalid("Type identifier did not match");
  }
  return json();
}

std::string JsonExtensionType::Serialize() const {
  return JsonExtensionType::type_name();
}

}  // namespace extension
}  // namespace arrow
