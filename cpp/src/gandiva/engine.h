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
#if LLVM_VERSION_MAJOR >= 11
#include <llvm/ExecutionEngine/Orc/Mangling.h>
#else
#include <llvm/ExecutionEngine/Orc/Core.h>
#endif

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
  /// \param[in] object_cache an optional object_cache used for building the module, if
  /// not provided, no caching is done
  /// \return arrow::Result containing the created engine
  static Result<std::unique_ptr<Engine>> Make(
      const std::shared_ptr<Configuration>& config, bool cached,
      llvm::ObjectCache* object_cache = NULLPTR);

  /// Add the function to the list of IR functions that need to be compiled.
  /// Compiling only the functions that are used by the module saves time.
  void AddFunctionToCompile(const std::string& fname) {
    ARROW_DCHECK(!module_finalized_);
    functions_to_compile_.push_back(fname);
  }

  /// Optimise and compile the module.
  Status FinalizeModule();

  /// Set cached LLVM ObjectCode
  Status SetCachedObjectCode(std::unique_ptr<llvm::MemoryBuffer> cached_buffer);

  /// Get the compiled function corresponding to the irfunction.
  Result<void*> CompiledFunction(const std::string& function);

  // Create and add a mapping for the cpp function to make it accessible from LLVM.
  void AddGlobalMappingForFunc(const std::string& name, llvm::Type* ret_type,
                               const std::vector<llvm::Type*>& args, void* func);

  /// Return the generated IR for the module.
  const std::string& ir();

  /// Load the function IRs that can be accessed in the module.
  Status LoadFunctionIRs();

  /// Post construction init. This _must_ be called after the constructor.
  /// @param[in] used_functions set of function names that are expected to be used by the
  /// engine
  Status Init(std::unordered_set<std::string> used_functions);

 private:
  Engine(const std::shared_ptr<Configuration>& conf,
         std::unique_ptr<llvm::orc::LLJIT> lljit,
         llvm::TargetIRAnalysis target_is_analysis, bool cached);

  static void InitOnce();

  /// load pre-compiled IR modules from precompiled_bitcode.cc and merge them into
  /// the main module.
  Status LoadPreCompiledIR();

  /// load mandatory pre-compiled IR modules from precompiled_bitcode.cc and merge them
  /// into the main module. Mandatory IR includes functions manipulating bitmaps
  Status LoadMandatoryPreCompiledIR();

  // load external pre-compiled bitcodes into module
  Status LoadExternalPreCompiledIR();

  // Create and add mappings for cpp functions that can be accessed from LLVM.
  arrow::Status AddGlobalMappings();

  // Remove unused functions to reduce compile time.
  Status RemoveUnusedFunctions();

  std::unique_ptr<llvm::LLVMContext> context_;
  std::unique_ptr<llvm::orc::LLJIT> lljit_;
  std::unique_ptr<llvm::orc::MangleAndInterner> mangle_;
  std::unique_ptr<llvm::IRBuilder<>> ir_builder_;
  std::unique_ptr<llvm::Module> module_;
  LLVMTypes types_;

  std::vector<std::string> functions_to_compile_;
  std::unordered_set<std::string> used_functions_;
  std::unordered_set<std::string> used_c_functions_;

  // all internally used C stub functions and IR function names
  static inline const std::unordered_set<std::string> internal_functions_ = {
      // internal C stub functions
      "gdv_fn_context_arena_malloc",
      "gdv_fn_context_set_error_msg",
      "gdv_fn_populate_varlen_vector",
      "gdv_fn_context_arena_reset",
      "gdv_fn_in_expr_lookup_int32",
      "gdv_fn_in_expr_lookup_int64",
      "gdv_fn_in_expr_lookup_float",
      "gdv_fn_in_expr_lookup_double",
      "gdv_fn_in_expr_lookup_decimal",
      "gdv_fn_in_expr_lookup_utf8",
      // internal IR functions
      "bitMapGetBit",
      "bitMapSetBit",
      "bitMapValidityGetBit",
      "bitMapClearBitIfFalse",
  };

  bool optimize_ = true;
  bool module_finalized_ = false;
  bool cached_;
  bool functions_loaded_ = false;
  bool mandatory_functions_loaded_ = false;
  std::shared_ptr<FunctionRegistry> function_registry_;
  std::string module_ir_;
  std::unique_ptr<llvm::TargetMachine> target_machine_;
  llvm::TargetIRAnalysis target_ir_analysis_;
  const std::shared_ptr<Configuration> conf_;
};

}  // namespace gandiva
