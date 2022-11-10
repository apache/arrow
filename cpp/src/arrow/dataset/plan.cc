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

#include "arrow/dataset/plan.h"

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/scanner.h"

#include <mutex>

namespace arrow {
namespace dataset {
namespace internal {

void Initialize() {
  static std::once_flag flag;
  std::call_once(flag, [] {
    auto registry = compute::default_exec_factory_registry();
    if (registry) {
      InitializeScanner(registry);
      InitializeScannerV2(registry);
      InitializeDatasetWriter(registry);
    }
  });
}

}  // namespace internal
}  // namespace dataset
}  // namespace arrow
