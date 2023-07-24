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

#include "arrow/array/util.h"
#include "arrow/chunked_array.h"
#include "arrow/config.h"
#ifdef ARROW_JSON
#include "arrow/extension/fixed_shape_tensor.h"
#endif
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

DataTypeLayout ExtensionType::layout() const { return storage_type_->layout(); }

std::string ExtensionType::ToString() const {
  std::stringstream ss;
  ss << "extension<" << this->extension_name() << ">";
  return ss.str();
}

std::shared_ptr<Array> ExtensionType::WrapArray(const std::shared_ptr<DataType>& type,
                                                const std::shared_ptr<Array>& storage) {
  DCHECK_EQ(type->id(), Type::EXTENSION);
  const auto& ext_type = checked_cast<const ExtensionType&>(*type);
  DCHECK_EQ(storage->type_id(), ext_type.storage_type()->id());
  auto data = storage->data()->Copy();
  data->type = type;
  return ext_type.MakeArray(std::move(data));
}

std::shared_ptr<ChunkedArray> ExtensionType::WrapArray(
    const std::shared_ptr<DataType>& type, const std::shared_ptr<ChunkedArray>& storage) {
  DCHECK_EQ(type->id(), Type::EXTENSION);
  const auto& ext_type = checked_cast<const ExtensionType&>(*type);
  DCHECK_EQ(storage->type()->id(), ext_type.storage_type()->id());

  ArrayVector out_chunks(storage->num_chunks());
  for (int i = 0; i < storage->num_chunks(); i++) {
    auto data = storage->chunk(i)->data()->Copy();
    data->type = type;
    out_chunks[i] = ext_type.MakeArray(std::move(data));
  }
  return std::make_shared<ChunkedArray>(std::move(out_chunks));
}

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

class ExtensionTypeRegistryImpl : public ExtensionTypeRegistry {
 public:
  ExtensionTypeRegistryImpl() {}

  Status RegisterType(std::shared_ptr<ExtensionType> type) override {
    std::lock_guard<std::mutex> lock(lock_);
    std::string type_name = type->extension_name();
    auto it = name_to_type_.find(type_name);
    if (it != name_to_type_.end()) {
      return Status::KeyError("A type extension with name ", type_name,
                              " already defined");
    }
    name_to_type_[type_name] = std::move(type);
    return Status::OK();
  }

  Status UnregisterType(const std::string& type_name) override {
    std::lock_guard<std::mutex> lock(lock_);
    auto it = name_to_type_.find(type_name);
    if (it == name_to_type_.end()) {
      return Status::KeyError("No type extension with name ", type_name, " found");
    }
    name_to_type_.erase(it);
    return Status::OK();
  }

  std::shared_ptr<ExtensionType> GetType(const std::string& type_name) override {
    std::lock_guard<std::mutex> lock(lock_);
    auto it = name_to_type_.find(type_name);
    if (it == name_to_type_.end()) {
      return nullptr;
    } else {
      return it->second;
    }
    return nullptr;
  }

 private:
  std::mutex lock_;
  std::unordered_map<std::string, std::shared_ptr<ExtensionType>> name_to_type_;
};

static std::shared_ptr<ExtensionTypeRegistry> g_registry;
static std::once_flag registry_initialized;

namespace internal {

static void CreateGlobalRegistry() {
  g_registry = std::make_shared<ExtensionTypeRegistryImpl>();

#ifdef ARROW_JSON
  // Register canonical extension types
  auto ext_type =
      checked_pointer_cast<ExtensionType>(extension::fixed_shape_tensor(int64(), {}));

  ARROW_CHECK_OK(g_registry->RegisterType(ext_type));
#endif
}

}  // namespace internal

std::shared_ptr<ExtensionTypeRegistry> ExtensionTypeRegistry::GetGlobalRegistry() {
  std::call_once(registry_initialized, internal::CreateGlobalRegistry);
  return g_registry;
}

Status RegisterExtensionType(std::shared_ptr<ExtensionType> type) {
  auto registry = ExtensionTypeRegistry::GetGlobalRegistry();
  return registry->RegisterType(type);
}

Status UnregisterExtensionType(const std::string& type_name) {
  auto registry = ExtensionTypeRegistry::GetGlobalRegistry();
  return registry->UnregisterType(type_name);
}

std::shared_ptr<ExtensionType> GetExtensionType(const std::string& type_name) {
  auto registry = ExtensionTypeRegistry::GetGlobalRegistry();
  return registry->GetType(type_name);
}

extern const char kExtensionTypeKeyName[] = "ARROW:extension:name";
extern const char kExtensionMetadataKeyName[] = "ARROW:extension:metadata";

}  // namespace arrow
