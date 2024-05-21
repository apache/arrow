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

#pragma once

#include <memory>
#include <vector>

#include <gandiva/engine.h>
#include <gandiva/visibility.h>

namespace gandiva {

class ExportedFuncsBase;

/// Registry for classes that export functions which can be accessed by
/// LLVM/IR code.
class GANDIVA_EXPORT ExportedFuncsRegistry {
 public:
  using list_type = std::vector<std::shared_ptr<ExportedFuncsBase>>;

  // Add functions from all the registered classes to the engine.
  static arrow::Status AddMappings(Engine* engine);

  static bool Register(std::shared_ptr<ExportedFuncsBase> entry) {
    registered()->emplace_back(std::move(entry));
    return true;
  }

  // list all the registered ExportedFuncsBase
  static const list_type& Registered();

 private:
  static list_type* registered();
};

#define REGISTER_EXPORTED_FUNCS(classname)               \
  [[maybe_unused]] static bool _registered_##classname = \
      ExportedFuncsRegistry::Register(std::make_shared<classname>())

}  // namespace gandiva
