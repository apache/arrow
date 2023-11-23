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
#include <optional>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "gandiva/function_holder.h"
#include "gandiva/function_holder_maker_registry.h"
#include "gandiva/function_registry_common.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/native_function.h"
#include "gandiva/visibility.h"

namespace gandiva {

///\brief Registry of pre-compiled IR functions.
class GANDIVA_EXPORT FunctionRegistry {
 public:
  using iterator = const NativeFunction*;
  using FunctionHolderMaker =
      std::function<arrow::Result<std::shared_ptr<FunctionHolder>>(
          const FunctionNode& function_node)>;

  FunctionRegistry();
  FunctionRegistry(const FunctionRegistry&) = delete;
  FunctionRegistry& operator=(const FunctionRegistry&) = delete;

  /// Lookup a pre-compiled function by its signature.
  const NativeFunction* LookupSignature(const FunctionSignature& signature) const;

  /// \brief register a set of functions into the function registry from a given bitcode
  /// file
  arrow::Status Register(const std::vector<NativeFunction>& funcs,
                         const std::string& bitcode_path);

  /// \brief register a set of functions into the function registry from a given bitcode
  /// buffer
  arrow::Status Register(const std::vector<NativeFunction>& funcs,
                         std::shared_ptr<arrow::Buffer> bitcode_buffer);

  /// \brief register a C function into the function registry
  /// @param func the registered function's metadata
  /// @param c_function_ptr the function pointer to the
  /// registered function's implementation
  /// @param function_holder_maker this will be used as the function holder if the
  /// function requires a function holder
  arrow::Status Register(
      NativeFunction func, void* c_function_ptr,
      std::optional<FunctionHolderMaker> function_holder_maker = std::nullopt);

  /// \brief get a list of bitcode memory buffers saved in the registry
  const std::vector<std::shared_ptr<arrow::Buffer>>& GetBitcodeBuffers() const;

  /// \brief get a list of C functions saved in the registry
  const std::vector<std::pair<NativeFunction, void*>>& GetCFunctions() const;

  const FunctionHolderMakerRegistry& GetFunctionHolderMakerRegistry() const;

  iterator begin() const;
  iterator end() const;
  iterator back() const;

  friend arrow::Result<std::shared_ptr<FunctionRegistry>> MakeDefaultFunctionRegistry();

 private:
  std::vector<NativeFunction> pc_registry_;
  SignatureMap pc_registry_map_;
  std::vector<std::shared_ptr<arrow::Buffer>> bitcode_memory_buffers_;
  std::vector<std::pair<NativeFunction, void*>> c_functions_;
  FunctionHolderMakerRegistry holder_maker_registry_;

  Status Add(NativeFunction func);
};

/// \brief get the default function registry
GANDIVA_EXPORT std::shared_ptr<FunctionRegistry> default_function_registry();

}  // namespace gandiva
