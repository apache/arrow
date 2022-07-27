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

// NOTE: API is EXPERIMENTAL and will change without going through a
// deprecation cycle

#include "arrow/engine/substrait/registry.h"

namespace arrow {

namespace engine {
class SubstraitConversionRegistry::SubstraitConversionRegistryImpl {
 public:
  explicit SubstraitConversionRegistryImpl(
      SubstraitConversionRegistryImpl* parent = NULLPTR)
      : parent_(parent) {}
  ~SubstraitConversionRegistryImpl() {}

  std::unique_ptr<SubstraitConversionRegistry> Make() {
    return std::unique_ptr<SubstraitConversionRegistry>(
        new SubstraitConversionRegistry());
  }

  std::unique_ptr<SubstraitConversionRegistry> Make(SubstraitConversionRegistry* parent) {
    return std::unique_ptr<SubstraitConversionRegistry>(new SubstraitConversionRegistry(
        new SubstraitConversionRegistry::SubstraitConversionRegistryImpl(
            parent->impl_.get())));
  }

  Status RegisterConverter(const std::string& kind_name, SubstraitConverter converter) {
    if (kind_name == "scan") {
      return Status::NotImplemented("Scan serialization not implemented");
    } else if (kind_name == "filter") {
      return Status::NotImplemented("Filter serialization not implemented");
    } else if (kind_name == "project") {
      return Status::NotImplemented("Project serialization not implemented");
    } else if (kind_name == "augmented_project") {
      return Status::NotImplemented("Augmented Project serialization not implemented");
    } else if (kind_name == "hashjoin") {
      return Status::NotImplemented("Filter serialization not implemented");
    } else if (kind_name == "asofjoin") {
      return Status::NotImplemented("Asof Join serialization not implemented");
    } else if (kind_name == "select_k_sink") {
      return Status::NotImplemented("SelectK serialization not implemented");
    } else if (kind_name == "union") {
      return Status::NotImplemented("Union serialization not implemented");
    } else if (kind_name == "write") {
      return Status::NotImplemented("Write serialization not implemented");
    } else if (kind_name == "tee") {
      return Status::NotImplemented("Tee serialization not implemented");
    } else {
      return Status::Invalid("Unsupported ExecNode: ", kind_name);
    }
  }

  SubstraitConversionRegistryImpl* parent_;
  std::mutex lock_;
  std::unordered_map<std::string, SubstraitConverter> name_to_converter_;
};

struct DefaultSubstraitConversionRegistry : SubstraitConversionRegistryImpl {
  DefaultSubstraitConversionRegistry() {}
};

SubstraitConversionRegistry* GetSubstraitConversionRegistry() {
  static DefaultSubstraitConversionRegistry impl_;
  return &impl_;
}

}  // namespace engine

}  // namespace arrow
