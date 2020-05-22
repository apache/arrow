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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

class Function;

/// \brief A mutable central function registry for built-in functions
/// and user-defined functions.
class ARROW_EXPORT FunctionRegistry {
 public:
  ~FunctionRegistry();

  /// \brief Construct a new kernel registry. Most users only need to use the
  /// global registry
  static std::unique_ptr<FunctionRegistry> Make();

  /// \brief Add a new kernel to the registry. Returns Status::KeyError if a
  /// kernel with the same name is already registered
  Status AddFunction(std::shared_ptr<Function> function, bool allow_overwrite = false);

  /// \brief Retrieve a kernel by name from the registry
  Result<std::shared_ptr<Function>> GetFunction(const std::string& name) const;

  /// \brief Return vector of all entry names in the registry. Helpful for
  /// displaying a manifest of available kernels
  std::vector<std::string> GetFunctionNames() const;

  /// \brief The number of currently registered functions
  int num_functions() const;

 private:
  FunctionRegistry();

  /// Use PIMPL pattern to not have std::unordered_map here
  class FunctionRegistryImpl;
  std::unique_ptr<FunctionRegistryImpl> impl_;
};

// \brief Return the process-global kernel registry
ARROW_EXPORT FunctionRegistry* GetFunctionRegistry();

}  // namespace compute
}  // namespace arrow
