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

#include <string>

#include "arrow/status.h"
#include "arrow/util/dynamic_library.h"
#include "arrow/util/arrow_plugin_context.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

class ARROW_EXPORT Plugin {
 public:
  Plugin(const std::string& path);

  DynamicLibrary& GetLibrary() { return library_;}

  ArrowPluginContext& GetContext() { return context_; }

  void* GetPluginPrivContext() { return context_.priv; }

  const std::string& GetName() const { return name_; }

  const std::string& GetVersion() const { return version_; }

  bool IsArrowPlugin();

  Status Initialize();

  Status Cleanup();

 private:
  std::string name_;
  std::string version_;
  DynamicLibrary library_;
  ArrowPluginContext context_;
};

}  // namespace util
}  // namespace arrow
