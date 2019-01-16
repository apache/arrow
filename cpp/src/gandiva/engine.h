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

#ifndef GANDIVA_ENGINE_H
#define GANDIVA_ENGINE_H

#include <memory>
#include <set>
#include <string>
#include <vector>

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>

#include "arrow/status.h"
#include "arrow/util/macros.h"

#include "gandiva/configuration.h"
#include "gandiva/llvm_types.h"
#include "gandiva/logging.h"

namespace gandiva {

class FunctionIRBuilder;

/// \brief LLVM Execution engine wrapper.
class Engine {
 public:
  llvm::LLVMContext* context() { return context_.get(); }
  llvm::IRBuilder<>* ir_builder() { return ir_builder_.get(); }
  LLVMTypes* types() { return types_.get(); }
  llvm::Module* module() { return module_; }

  /// factory method to create and initialize the engine object.
  ///
  /// \param[out] engine the created engine.
  static Status Make(std::shared_ptr<Configuration> config,
                     std::unique_ptr<Engine>* engine);

  /// Add the function to the list of IR functions that need to be compiled.
  /// Compiling only the functions that are used by the module saves time.
  void AddFunctionToCompile(const std::string& fname) {
    DCHECK(!module_finalized_);
    functions_to_compile_.push_back(fname);
  }

  /// Optimise and compile the module.
  Status FinalizeModule(bool optimise_ir, bool dump_ir);

  /// Get the compiled function corresponding to the irfunction.
  void* CompiledFunction(llvm::Function* irFunction);

  // Create and add a mapping for the cpp function to make it accessible from LLVM.
  void AddGlobalMappingForFunc(const std::string& name, llvm::Type* ret_type,
                               const std::vector<llvm::Type*>& args, void* func);

 private:
  /// private constructor to ensure engine is created
  /// only through the factory.
  Engine() : module_finalized_(false) {}

  /// do one time inits.
  static void InitOnce();
  static bool init_once_done_;

  llvm::ExecutionEngine& execution_engine() { return *execution_engine_.get(); }

  /// load pre-compiled IR modules and merge them into the main module.
  Status LoadPreCompiledIRFiles(const std::string& byte_code_file_path);

  // Create and add mappings for cpp functions that can be accessed from LLVM.
  void AddGlobalMappings();

  // Remove unused functions to reduce compile time.
  Status RemoveUnusedFunctions();

  /// dump the IR code to stdout with the prefix string.
  void DumpIR(std::string prefix);

  std::unique_ptr<llvm::LLVMContext> context_;
  std::unique_ptr<llvm::ExecutionEngine> execution_engine_;
  std::unique_ptr<LLVMTypes> types_;
  std::unique_ptr<llvm::IRBuilder<>> ir_builder_;
  llvm::Module* module_;  // This is owned by the execution_engine_, so doesn't need to be
                          // explicitly deleted.

  std::vector<std::string> functions_to_compile_;

  bool module_finalized_;
  std::string llvm_error_;

  static std::set<std::string> loaded_libs_;
  static std::mutex mtx_;
};

}  // namespace gandiva

#endif  // GANDIVA_ENGINE_H
