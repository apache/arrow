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

#include "arrow/extension_type.h"

#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/visibility.h"

namespace arrow {

using internal::checked_cast;

DataTypeLayout ExtensionType::layout() const { return storage_type_->layout(); }

std::string ExtensionType::ToString() const {
  std::stringstream ss;
  ss << "extension<" << this->extension_name() << ">";
  return ss.str();
}

std::string ExtensionType::name() const { return "extension"; }

ExtensionArray::ExtensionArray(const std::shared_ptr<ArrayData>& data) { SetData(data); }

ExtensionArray::ExtensionArray(const std::shared_ptr<DataType>& type,
                               const std::shared_ptr<Array>& storage) {
  ARROW_CHECK_EQ(type->id(), Type::EXTENSION);
  ARROW_CHECK(
      storage->type()->Equals(*checked_cast<const ExtensionType&>(*type).storage_type()));
  auto data = storage->data()->Copy();
  // XXX This pointer is reverted below in SetData()...
  data->type = type;
  SetData(data);
}

void ExtensionArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::EXTENSION);
  this->Array::SetData(data);

  auto storage_data = data->Copy();
  storage_data->type = (static_cast<const ExtensionType&>(*data->type).storage_type());
  storage_ = MakeArray(storage_data);
}

std::unordered_map<std::string, std::shared_ptr<ExtensionType>> g_extension_registry;
std::mutex g_extension_registry_guard;

Status RegisterExtensionType(std::shared_ptr<ExtensionType> type) {
  std::lock_guard<std::mutex> lock_(g_extension_registry_guard);
  std::string type_name = type->extension_name();
  auto it = g_extension_registry.find(type_name);
  if (it != g_extension_registry.end()) {
    return Status::KeyError("A type extension with name ", type_name, " already defined");
  }
  g_extension_registry[type_name] = std::move(type);
  return Status::OK();
}

Status UnregisterExtensionType(const std::string& type_name) {
  std::lock_guard<std::mutex> lock_(g_extension_registry_guard);
  auto it = g_extension_registry.find(type_name);
  if (it == g_extension_registry.end()) {
    return Status::KeyError("No type extension with name ", type_name, " found");
  }
  g_extension_registry.erase(it);
  return Status::OK();
}

std::shared_ptr<ExtensionType> GetExtensionType(const std::string& type_name) {
  std::lock_guard<std::mutex> lock_(g_extension_registry_guard);
  auto it = g_extension_registry.find(type_name);
  if (it == g_extension_registry.end()) {
    return nullptr;
  } else {
    return it->second;
  }
  return nullptr;
}

}  // namespace arrow
