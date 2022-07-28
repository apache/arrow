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
#include "arrow/engine/substrait/relation_internal.h"

namespace arrow {

namespace engine {

class SubstraitConversionRegistryImpl : public SubstraitConversionRegistry {
 public:
  virtual ~SubstraitConversionRegistryImpl() {}

  Result<SubstraitConverter> GetConverter(const std::string& factory_name) override {
    auto it = name_to_converter_.find(factory_name);
    if (it == name_to_converter_.end()) {
      return Status::KeyError("SubstraitConverter named ", factory_name,
                              " not present in registry.");
    }
    return it->second;
  }

  Status RegisterConverter(std::string factory_name,
                           SubstraitConverter converter) override {
    auto it_success =
        name_to_converter_.emplace(std::move(factory_name), std::move(converter));

    if (!it_success.second) {
      const auto& factory_name = it_success.first->first;
      return Status::KeyError("SubstraitConverter named ", factory_name,
                              " already registered.");
    }
    return Status::OK();
  }

 private:
  std::unordered_map<std::string, SubstraitConverter> name_to_converter_;
};

struct DefaultSubstraitConversionRegistry : SubstraitConversionRegistryImpl {
  DefaultSubstraitConversionRegistry() {
    DCHECK_OK(RegisterConverter("scan", ScanRelationConverter));
  }
};

SubstraitConversionRegistry* default_substrait_conversion_registry() {
  static DefaultSubstraitConversionRegistry impl_;
  return &impl_;
}

}  // namespace engine
}  // namespace arrow
