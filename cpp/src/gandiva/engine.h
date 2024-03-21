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

#include <cinttypes>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <llvm/Analysis/TargetTransformInfo.h>

#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "gandiva/configuration.h"
#include "gandiva/gandiva_object_cache.h"
#include "gandiva/llvm_includes.h"
#include "gandiva/llvm_types.h"
#include "gandiva/visibility.h"

namespace llvm::orc {
class LLJIT;
}  // namespace llvm::orc

namespace gandiva {

/// \brief LLVM Execution engine wrapper.
class GANDIVA_EXPORT Engine {
 public:
  ~Engine();
  llvm::LLVMContext* context() { return context_.get(); }
  llvm::IRBuilder<>* ir_builder() { return ir_builder_.get(); }
  LLVMTypes* types() { return &types_; }

  /// Retrieve LLVM module in the engine.
  /// This should only be called before `FinalizeModule` is called
  llvm::Module* module();

  /// Factory method to create and initialize the engine object.
  ///
  /// \param[in] config the engine configuration
  /// \param[in] cached flag to mark if the module is already compiled and cached
  /// \param[in] object_cache an optional object_cache used for building the module
  /// \return arrow::Result containing the created engine
  static Result<std::unique_ptr<Engine>> Make(
      const std::shared_ptr<Configuration>& config, bool cached,
      std::optional<std::reference_wrapper<GandivaObjectCache>> object_cache =
          std::nullopt);

  /// Add the function to the list of IR functions that need to be compiled.
  /// Compiling only the functions that are used by the module saves time.
  void AddFunctionToCompile(const std::string& fname) {
    DCHECK(!module_finalized_);
    functions_to_compile_.push_back(fname);
  }

  /// Optimise and compile the module.
  Status FinalizeModule();

  /// Set LLVM ObjectCache.
  Status SetLLVMObjectCache(GandivaObjectCache& object_cache);

  /// Get the compiled function corresponding to the irfunction.
  Result<void*> CompiledFunction(const std::string& function);

  // Create and add a mapping for the cpp function to make it accessible from LLVM.
  void AddGlobalMappingForFunc(const std::string& name, llvm::Type* ret_type,
                               const std::vector<llvm::Type*>& args, void* func);

  /// Return the generated IR for the module.
  const std::string& ir();

  /// Load the function IRs that can be accessed in the module.
  Status LoadFunctionIRs();

 private:
  Engine(const std::shared_ptr<Configuration>& conf,
         std::unique_ptr<llvm::orc::LLJIT> lljit,
         std::unique_ptr<llvm::TargetMachine> target_machine, bool cached);

  // Post construction init. This _must_ be called after the constructor.
  Status Init();

  static void InitOnce();

  /// load pre-compiled IR modules from precompiled_bitcode.cc and merge them into
  /// the main module.
  Status LoadPreCompiledIR();

  // load external pre-compiled bitcodes into module
  Status LoadExternalPreCompiledIR();

  // Create and add mappings for cpp functions that can be accessed from LLVM.
  arrow::Status AddGlobalMappings();

  // Remove unused functions to reduce compile time.
  Status RemoveUnusedFunctions();

  std::unique_ptr<llvm::LLVMContext> context_;
  std::unique_ptr<llvm::orc::LLJIT> lljit_;
  std::unique_ptr<llvm::IRBuilder<>> ir_builder_;
  std::unique_ptr<llvm::Module> module_;
  LLVMTypes types_;

  std::vector<std::string> functions_to_compile_;

  bool optimize_ = true;
  bool module_finalized_ = false;
  bool cached_;
  bool functions_loaded_ = false;
  std::shared_ptr<FunctionRegistry> function_registry_;
  std::string module_ir_;
  std::unique_ptr<llvm::TargetMachine> target_machine_;
  const std::shared_ptr<Configuration> conf_;
};

}  // namespace gandiva
