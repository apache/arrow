/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include "llvm/ExecutionEngine/MCJIT.h"
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Vectorize.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include "codegen/codegen_exception.h"
#include "codegen/engine.h"

namespace gandiva {

const char Engine::kLibPreCompiledIRDir[] = "/tmp/";
bool Engine::init_once_done_ = false;
std::once_flag init_once_flag;

/*
 * One-time initializations.
 */
void Engine::InitOnce() {
  assert(!init_once_done_);

  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();
  llvm::InitializeNativeTargetDisassembler();

  init_once_done_ = true;
}

Engine::Engine()
    : module_finalized_(false) {
  std::call_once(init_once_flag, InitOnce);
  context_.reset(new llvm::LLVMContext());
  ir_builder_.reset(new llvm::IRBuilder<>(*context()));

  /* Create the execution engine */
  std::unique_ptr<llvm::Module> cg_module(new llvm::Module("codegen", *context()));
  module_ = cg_module.get();

  llvm::EngineBuilder engineBuilder(std::move(cg_module));
  engineBuilder.setEngineKind(llvm::EngineKind::JIT);
  engineBuilder.setOptLevel(llvm::CodeGenOpt::Aggressive);
  engineBuilder.setErrorStr(&llvm_error_);
  execution_engine_.reset(engineBuilder.create());
  if (execution_engine_ == NULL) {
    module_ = NULL;
    throw CodeGenException(llvm_error_);
  }

  LoadPreCompiledIRFiles();
}

/*
 * Handling for pre-compiled IR libraries.
 */
void Engine::LoadPreCompiledIRFiles() {
  std::string fileName = std::string(kLibPreCompiledIRDir) + "irhelpers.bc";

  /// Read from file into memory buffer.
  llvm::ErrorOr<std::unique_ptr<llvm::MemoryBuffer>> buffer_or_error =
      llvm::MemoryBuffer::getFile(fileName);
  if (!buffer_or_error) {
    std::stringstream ss;
    ss << "Could not load module from IR " << fileName << ": " <<
          buffer_or_error.getError().message();
    throw CodeGenException(ss.str());
  }
  std::unique_ptr<llvm::MemoryBuffer> buffer = move(buffer_or_error.get());

  /// Parse the IR module.
  llvm::Expected<std::unique_ptr<llvm::Module>>
      module_or_error = llvm::getOwningLazyBitcodeModule(move(buffer), *context());
  if (!module_or_error) {
    std::string error_string;
    llvm::handleAllErrors(module_or_error.takeError(), [&](llvm::ErrorInfoBase &eib) {
      error_string = eib.message();
    });
    throw CodeGenException(error_string);
  }
  std::unique_ptr<llvm::Module> ir_module = move(module_or_error.get());

  /// Verify the IR module
  if (llvm::verifyModule(*ir_module.get(), &llvm::errs())) {
    throw CodeGenException("verify of IR Module failed");
  }

  // Link this to the primary module.
  if (llvm::Linker::linkModules(*module_, move(ir_module))) {
    throw CodeGenException("failed to link IR Modules");
  }
}

/*
 * Optimise and compile the module.
 */
void
Engine::FinalizeModule(bool optimise_ir, bool dump_ir) {
  if (dump_ir) {
    DumpIR("Before optimise");
  }

  // Setup an optimiser pipeline
  if (optimise_ir) {
    std::unique_ptr<llvm::legacy::PassManager>
        pass_manager(new llvm::legacy::PassManager());

    /* First round : get rid of all functions that don't need to be compiled.
     * This helps in reducing the overall compilation time.
     *
     * Done by marking all the unused functions as internal, and then, running
     * a pass for dead code elimination.
     */
    std::unordered_set<std::string> used_functions;
    used_functions.insert(functions_to_compile_.begin(), functions_to_compile_.end());

    pass_manager->add(
        llvm::createInternalizePass([&used_functions](const llvm::GlobalValue &func) {
          return (used_functions.find(func.getName().str()) != used_functions.end());
        }));
    pass_manager->add(llvm::createGlobalDCEPass());
    pass_manager->run(*module_);

    /*
     * Second round : misc passes to allow for inlining, vectorization, ..
     */
    pass_manager.reset(new llvm::legacy::PassManager());
    llvm::TargetIRAnalysis target_analysis =
        execution_engine_->getTargetMachine()->getTargetIRAnalysis();
    pass_manager->add(llvm::createTargetTransformInfoWrapperPass(target_analysis));
    pass_manager->add(llvm::createFunctionInliningPass());
    pass_manager->add(llvm::createInstructionCombiningPass());
    pass_manager->add(llvm::createPromoteMemoryToRegisterPass());
    pass_manager->add(llvm::createGVNPass());
    pass_manager->add(llvm::createNewGVNPass());
    pass_manager->add(llvm::createCFGSimplificationPass());
    pass_manager->add(llvm::createLoopVectorizePass());
    pass_manager->add(llvm::createSLPVectorizerPass());
    pass_manager->add(llvm::createGlobalOptimizerPass());

    // run the optimiser
    llvm::PassManagerBuilder pass_builder;
    pass_builder.OptLevel = 2;
    pass_builder.populateModulePassManager(*pass_manager);
    pass_manager->run(*module_);

    if (dump_ir) {
      DumpIR("After optimise");
    }
  }

  if (llvm::verifyModule(*module_, &llvm::errs())) {
    throw CodeGenException("verify of module failed after optimisation passes");
  }

  // do the compilation
  execution_engine_->finalizeObject();
  module_finalized_ = true;
}

void *Engine::CompiledFunction(llvm::Function *irFunction) {
  assert(module_finalized_);
  return execution_engine_->getPointerToFunction(irFunction);
}

void Engine::DumpIR(std::string prefix) {
  std::string str;

  llvm::raw_string_ostream stream(str);
  module_->print(stream, NULL);
  std::cout << "====" << prefix << "===" << str << "\n";
}

} // namespace gandiva
