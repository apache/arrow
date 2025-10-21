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

#include "gandiva/exported_funcs_registry.h"

#include "gandiva/exported_funcs.h"

namespace gandiva {

arrow::Status ExportedFuncsRegistry::AddMappings(Engine* engine) {
  for (const auto& entry : *registered()) {
    ARROW_RETURN_NOT_OK(entry->AddMappings(engine));
  }
  return arrow::Status::OK();
}

const ExportedFuncsRegistry::list_type& ExportedFuncsRegistry::Registered() {
  return *registered();
}

ExportedFuncsRegistry::list_type* ExportedFuncsRegistry::registered() {
  static list_type registered_list;
  return &registered_list;
}

}  // namespace gandiva
