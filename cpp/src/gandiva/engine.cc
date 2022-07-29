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

// TODO(wesm): LLVM 7 produces pesky C4244 that disable pragmas around the LLVM
// includes seem to not fix as with LLVM 6
#if defined(_MSC_VER)
#pragma warning(disable : 4244)
#endif

#include "gandiva/engine.h"

#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>

#include "arrow/util/logging.h"

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4141)
#pragma warning(disable : 4146)
#pragma warning(disable : 4244)
#pragma warning(disable : 4267)
#pragma warning(disable : 4624)
#endif

#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Linker/Linker.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/Host.h>
#if LLVM_VERSION_MAJOR >= 14
#include <llvm/MC/TargetRegistry.h>
#else
#include <llvm/Support/TargetRegistry.h>
#endif
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <llvm/Transforms/Utils.h>
#include <llvm/Transforms/Vectorize.h>

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

#include "arrow/util/make_unique.h"
#include "gandiva/configuration.h"
#include "gandiva/decimal_ir.h"
#include "gandiva/exported_funcs_registry.h"

namespace gandiva {

extern const unsigned char kPrecompiledBitcode[];
extern const size_t kPrecompiledBitcodeSize;

std::once_flag llvm_init_once_flag;
static bool llvm_init = false;
static llvm::StringRef cpu_name;
static llvm::SmallVector<std::string, 10> cpu_attrs;

void Engine::InitOnce() {
  DCHECK_EQ(llvm_init, false);

  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();
  llvm::InitializeNativeTargetDisassembler();
  llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);

  cpu_name = llvm::sys::getHostCPUName();
  llvm::StringMap<bool> host_features;
  std::string cpu_attrs_str;
  if (llvm::sys::getHostCPUFeatures(host_features)) {
    for (auto& f : host_features) {
      std::string attr = f.second ? std::string("+") + f.first().str()
                                  : std::string("-") + f.first().str();
      cpu_attrs.push_back(attr);
      cpu_attrs_str += " " + attr;
    }
  }
  ARROW_LOG(INFO) << "Detected CPU Name : " << cpu_name.str();
  ARROW_LOG(INFO) << "Detected CPU Features:" << cpu_attrs_str;
  llvm_init = true;
}

Engine::Engine(const std::shared_ptr<Configuration>& conf,
               std::unique_ptr<llvm::LLVMContext> ctx,
               std::unique_ptr<llvm::ExecutionEngine> engine, llvm::Module* module)
    : context_(std::move(ctx)),
      execution_engine_(std::move(engine)),
      ir_builder_(arrow::internal::make_unique<llvm::IRBuilder<>>(*context_)),
      module_(module),
      types_(*context_),
      optimize_(conf->optimize()) {}

Status Engine::Init() {
  // Add mappings for functions that can be accessed from LLVM/IR module.
  AddGlobalMappings();

  ARROW_RETURN_NOT_OK(LoadPreCompiledIR());
  ARROW_RETURN_NOT_OK(DecimalIR::AddFunctions(this));

  return Status::OK();
}

/// factory method to construct the engine.
Status Engine::Make(const std::shared_ptr<Configuration>& conf,
                    std::unique_ptr<Engine>* out) {
  std::call_once(llvm_init_once_flag, InitOnce);

  auto ctx = arrow::internal::make_unique<llvm::LLVMContext>();
  auto module = arrow::internal::make_unique<llvm::Module>("codegen", *ctx);

  // Capture before moving, ExecutionEngine does not allow retrieving the
  // original Module.
  auto module_ptr = module.get();

  auto opt_level =
      conf->optimize() ? llvm::CodeGenOpt::Aggressive : llvm::CodeGenOpt::None;

  // Note that the lifetime of the error string is not captured by the
  // ExecutionEngine but only for the lifetime of the builder. Found by
  // inspecting LLVM sources.
  std::string builder_error;

  llvm::EngineBuilder engine_builder(std::move(module));

  engine_builder.setEngineKind(llvm::EngineKind::JIT)
      .setOptLevel(opt_level)
      .setErrorStr(&builder_error);

  if (conf->target_host_cpu()) {
    engine_builder.setMCPU(cpu_name);
    engine_builder.setMAttrs(cpu_attrs);
  }
  std::unique_ptr<llvm::ExecutionEngine> exec_engine{engine_builder.create()};

  if (exec_engine == nullptr) {
    return Status::CodeGenError("Could not instantiate llvm::ExecutionEngine: ",
                                builder_error);
  }

  std::unique_ptr<Engine> engine{
      new Engine(conf, std::move(ctx), std::move(exec_engine), module_ptr)};
  ARROW_RETURN_NOT_OK(engine->Init());
  *out = std::move(engine);
  return Status::OK();
}

// This method was modified from its original version for a part of MLIR
// Original source from
// https://github.com/llvm/llvm-project/blob/9f2ce5b915a505a5488a5cf91bb0a8efa9ddfff7/mlir/lib/ExecutionEngine/ExecutionEngine.cpp
// The original copyright notice follows.

// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

static void SetDataLayout(llvm::Module* module) {
  auto target_triple = llvm::sys::getDefaultTargetTriple();
  std::string error_message;
  auto target = llvm::TargetRegistry::lookupTarget(target_triple, error_message);
  if (!target) {
    return;
  }

  std::string cpu(llvm::sys::getHostCPUName());
  llvm::SubtargetFeatures features;
  llvm::StringMap<bool> host_features;

  if (llvm::sys::getHostCPUFeatures(host_features)) {
    for (auto& f : host_features) {
      features.AddFeature(f.first(), f.second);
    }
  }

  std::unique_ptr<llvm::TargetMachine> machine(
      target->createTargetMachine(target_triple, cpu, features.getString(), {}, {}));

  module->setDataLayout(machine->createDataLayout());
}
// end of the mofified method from MLIR

// Handling for pre-compiled IR libraries.
Status Engine::LoadPreCompiledIR() {
  auto bitcode = llvm::StringRef(reinterpret_cast<const char*>(kPrecompiledBitcode),
                                 kPrecompiledBitcodeSize);

  /// Read from file into memory buffer.
  llvm::ErrorOr<std::unique_ptr<llvm::MemoryBuffer>> buffer_or_error =
      llvm::MemoryBuffer::getMemBuffer(bitcode, "precompiled", false);

  ARROW_RETURN_IF(!buffer_or_error,
                  Status::CodeGenError("Could not load module from IR: ",
                                       buffer_or_error.getError().message()));

  std::unique_ptr<llvm::MemoryBuffer> buffer = move(buffer_or_error.get());

  /// Parse the IR module.
  llvm::Expected<std::unique_ptr<llvm::Module>> module_or_error =
      llvm::getOwningLazyBitcodeModule(move(buffer), *context());
  if (!module_or_error) {
    // NOTE: llvm::handleAllErrors() fails linking with RTTI-disabled LLVM builds
    // (ARROW-5148)
    std::string str;
    llvm::raw_string_ostream stream(str);
    stream << module_or_error.takeError();
    return Status::CodeGenError(stream.str());
  }
  std::unique_ptr<llvm::Module> ir_module = move(module_or_error.get());

  // set dataLayout
  SetDataLayout(ir_module.get());

  ARROW_RETURN_IF(llvm::verifyModule(*ir_module, &llvm::errs()),
                  Status::CodeGenError("verify of IR Module failed"));
  ARROW_RETURN_IF(llvm::Linker::linkModules(*module_, move(ir_module)),
                  Status::CodeGenError("failed to link IR Modules"));

  return Status::OK();
}

// Get rid of all functions that don't need to be compiled.
// This helps in reducing the overall compilation time. This pass is trivial,
// and is always done since the number of functions in gandiva is very high.
// (Adapted from Apache Impala)
//
// Done by marking all the unused functions as internal, and then, running
// a pass for dead code elimination.
Status Engine::RemoveUnusedFunctions() {
  // Setup an optimiser pipeline
  std::unique_ptr<llvm::legacy::PassManager> pass_manager(
      new llvm::legacy::PassManager());

  std::unordered_set<std::string> used_functions;
  used_functions.insert(functions_to_compile_.begin(), functions_to_compile_.end());

  pass_manager->add(
      llvm::createInternalizePass([&used_functions](const llvm::GlobalValue& func) {
        return (used_functions.find(func.getName().str()) != used_functions.end());
      }));
  pass_manager->add(llvm::createGlobalDCEPass());
  pass_manager->run(*module_);
  return Status::OK();
}

// Optimise and compile the module.
Status Engine::FinalizeModule() {
  ARROW_RETURN_NOT_OK(RemoveUnusedFunctions());

  if (optimize_) {
    // misc passes to allow for inlining, vectorization, ..
    std::unique_ptr<llvm::legacy::PassManager> pass_manager(
        new llvm::legacy::PassManager());

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
    pass_builder.OptLevel = 3;
    pass_builder.populateModulePassManager(*pass_manager);
    pass_manager->run(*module_);
  }

  ARROW_RETURN_IF(llvm::verifyModule(*module_, &llvm::errs()),
                  Status::CodeGenError("Module verification failed after optimizer"));

  // do the compilation
  execution_engine_->finalizeObject();
  module_finalized_ = true;

  return Status::OK();
}

void* Engine::CompiledFunction(llvm::Function* irFunction) {
  DCHECK(module_finalized_);
  return execution_engine_->getPointerToFunction(irFunction);
}

void Engine::AddGlobalMappingForFunc(const std::string& name, llvm::Type* ret_type,
                                     const std::vector<llvm::Type*>& args,
                                     void* function_ptr) {
  constexpr bool is_var_arg = false;
  auto prototype = llvm::FunctionType::get(ret_type, args, is_var_arg);
  constexpr auto linkage = llvm::GlobalValue::ExternalLinkage;
  auto fn = llvm::Function::Create(prototype, linkage, name, module());
  execution_engine_->addGlobalMapping(fn, function_ptr);
}

void Engine::AddGlobalMappings() { ExportedFuncsRegistry::AddMappings(this); }

std::string Engine::DumpIR() {
  std::string ir;
  llvm::raw_string_ostream stream(ir);
  module_->print(stream, nullptr);
  return ir;
}

}  // namespace gandiva
