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

#ifndef GANDIVA_EXPORTED_FUNCS_REGISTRY_H
#define GANDIVA_EXPORTED_FUNCS_REGISTRY_H

#include <memory>
#include <vector>

#include <gandiva/engine.h>

namespace gandiva {

class ExportedFuncsBase;

/// Registry for classes that export functions which can be accessed by
/// LLVM/IR code.
class ExportedFuncsRegistry {
 public:
  using list_type = std::vector<std::shared_ptr<ExportedFuncsBase>>;

  // Add functions from all the registered classes to the engine.
  static void AddMappings(Engine* engine);

  static bool Register(std::shared_ptr<ExportedFuncsBase> entry) {
    registered().push_back(entry);
    return true;
  }

 private:
  static list_type& registered() {
    static list_type registered_list;
    return registered_list;
  }
};

#define REGISTER_EXPORTED_FUNCS(classname) \
  static bool _registered_##classname =    \
      ExportedFuncsRegistry::Register(std::make_shared<classname>())

}  // namespace gandiva

#endif  // GANDIVA_EXPORTED_FUNCS_REGISTRY_H
