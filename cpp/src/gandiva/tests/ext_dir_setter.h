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
#include <stdlib.h>
#include <filesystem>
#include <optional>
#include <string>
#include "arrow/util/io_util.h"

namespace gandiva {
#ifndef GANDIVA_EXTENSION_TEST_DIR
#define GANDIVA_EXTENSION_TEST_DIR "../gandiva_extension_tests"
#endif

using arrow::internal::DelEnvVar;
using arrow::internal::SetEnvVar;

struct ExtensionDirSetter {
  explicit ExtensionDirSetter(
      const std::string& ext_dir,
      const std::optional<std::function<void()>> env_reloader = std::nullopt)
      : env_reloader_(std::move(env_reloader)) {
    std::filesystem::path base(GANDIVA_EXTENSION_TEST_DIR);
    auto status = SetEnvVar("GANDIVA_EXTENSION_DIR", (base / ext_dir).string());
    if (env_reloader_.has_value()) {
      env_reloader_.value()();
    }
  }
  ~ExtensionDirSetter() {
    auto status = DelEnvVar("GANDIVA_EXTENSION_DIR");
    if (env_reloader_.has_value()) {
      env_reloader_.value()();
    }
  }

  const std::optional<std::function<void()>> env_reloader_;
};
}  // namespace gandiva
