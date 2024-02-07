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

#include <arrow/util/io_util.h>
#include <arrow/util/logging.h>

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
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Transforms/Utils/Cloning.h>
#if LLVM_VERSION_MAJOR >= 17
#include <llvm/TargetParser/SubtargetFeature.h>
#else
#include <llvm/MC/SubtargetFeature.h>
#endif
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/DynamicLibrary.h>
#if LLVM_VERSION_MAJOR >= 18
#include <llvm/TargetParser/Host.h>
#else
#include <llvm/Support/Host.h>
#endif
#include <llvm/Transforms/IPO/GlobalDCE.h>
#include <llvm/Transforms/IPO/Internalize.h>
#if LLVM_VERSION_MAJOR >= 14
#include <llvm/IR/PassManager.h>
#include <llvm/MC/TargetRegistry.h>
#include <llvm/Passes/PassPlugin.h>
#include <llvm/Transforms/IPO/GlobalOpt.h>
#include <llvm/Transforms/Scalar/NewGVN.h>
#include <llvm/Transforms/Scalar/SimplifyCFG.h>
#include <llvm/Transforms/Utils/Mem2Reg.h>
#include <llvm/Transforms/Vectorize/LoopVectorize.h>
#include <llvm/Transforms/Vectorize/SLPVectorizer.h>
#else
#include <llvm/Support/TargetRegistry.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#endif
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <llvm/Transforms/Utils.h>
#if LLVM_VERSION_MAJOR <= 17
#include <llvm/Transforms/Vectorize.h>
#endif

// JITLink is available in LLVM 9+
// but the `InProcessMemoryManager::Create` API was added since LLVM 14
#if LLVM_VERSION_MAJOR >= 14 && !defined(_WIN32)
#define JIT_LINK_SUPPORTED
#include <llvm/ExecutionEngine/Orc/ObjectLinkingLayer.h>
#endif

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

#include "gandiva/configuration.h"
#include "gandiva/decimal_ir.h"
#include "gandiva/exported_funcs.h"
#include "gandiva/exported_funcs_registry.h"

namespace gandiva {

extern const unsigned char kPrecompiledBitcode[];
extern const size_t kPrecompiledBitcodeSize;

std::once_flag llvm_init_once_flag;
static bool llvm_init = false;
static llvm::StringRef cpu_name;
static std::vector<std::string> cpu_attrs;
std::once_flag register_exported_funcs_flag;

template <typename T>
arrow::Result<T> AsArrowResult(llvm::Expected<T>& expected,
                               const std::string& error_context) {
  if (!expected) {
    return Status::CodeGenError(error_context, llvm::toString(expected.takeError()));
  }
  return std::move(expected.get());
}

Result<llvm::orc::JITTargetMachineBuilder> MakeTargetMachineBuilder(
    const Configuration& conf) {
  llvm::orc::JITTargetMachineBuilder jtmb(
      (llvm::Triple(llvm::sys::getDefaultTargetTriple())));
  if (conf.target_host_cpu()) {
    jtmb.setCPU(cpu_name.str());
    jtmb.addFeatures(cpu_attrs);
  }
#if LLVM_VERSION_MAJOR >= 18
  using CodeGenOptLevel = llvm::CodeGenOptLevel;
#else
  using CodeGenOptLevel = llvm::CodeGenOpt::Level;
#endif
  auto const opt_level =
      conf.optimize() ? CodeGenOptLevel::Aggressive : CodeGenOptLevel::None;
  jtmb.setCodeGenOptLevel(opt_level);
  return jtmb;
}

std::string DumpModuleIR(const llvm::Module& module) {
  std::string ir;
  llvm::raw_string_ostream stream(ir);
  module.print(stream, nullptr);
  return ir;
}

void AddAbsoluteSymbol(llvm::orc::LLJIT& lljit, const std::string& name,
                       void* function_ptr) {
  llvm::orc::MangleAndInterner mangle(lljit.getExecutionSession(), lljit.getDataLayout());

  // https://github.com/llvm/llvm-project/commit/8b1771bd9f304be39d4dcbdcccedb6d3bcd18200#diff-77984a824d9182e5c67a481740f3bc5da78d5bd4cf6e1716a083ddb30a4a4931
  // LLVM 17 introduced ExecutorSymbolDef and move most of ORC APIs to ExecutorAddr
#if LLVM_VERSION_MAJOR >= 17
  llvm::orc::ExecutorSymbolDef symbol(
      llvm::orc::ExecutorAddr(reinterpret_cast<uint64_t>(function_ptr)),
      llvm::JITSymbolFlags::Exported);
#else
  llvm::JITEvaluatedSymbol symbol(reinterpret_cast<llvm::JITTargetAddress>(function_ptr),
                                  llvm::JITSymbolFlags::Exported);
#endif

  auto error = lljit.getMainJITDylib().define(
      llvm::orc::absoluteSymbols({{mangle(name), symbol}}));
  llvm::cantFail(std::move(error));
}

// add current process symbol to dylib
// LLVM >= 18 does this automatically
void AddProcessSymbol(llvm::orc::LLJIT& lljit) {
  lljit.getMainJITDylib().addGenerator(
      llvm::cantFail(llvm::orc::DynamicLibrarySearchGenerator::GetForCurrentProcess(
          lljit.getDataLayout().getGlobalPrefix())));
  // the `atexit` symbol cannot be found for ASAN
#ifdef ADDRESS_SANITIZER
  if (!lljit.lookup("atexit")) {
    AddAbsoluteSymbol(lljit, "atexit", reinterpret_cast<void*>(atexit));
  }
#endif
}

#ifdef JIT_LINK_SUPPORTED
Result<std::unique_ptr<llvm::jitlink::InProcessMemoryManager>> CreateMemmoryManager() {
  auto maybe_mem_manager = llvm::jitlink::InProcessMemoryManager::Create();
  return AsArrowResult(maybe_mem_manager, "Could not create memory manager: ");
}

Status UseJITLinkIfEnabled(llvm::orc::LLJITBuilder& jit_builder) {
  static auto maybe_use_jit_link = ::arrow::internal::GetEnvVar("GANDIVA_USE_JIT_LINK");
  if (maybe_use_jit_link.ok()) {
    ARROW_ASSIGN_OR_RAISE(static auto memory_manager, CreateMemmoryManager());
    jit_builder.setObjectLinkingLayerCreator(
        [&](llvm::orc::ExecutionSession& ES, const llvm::Triple& TT) {
          return std::make_unique<llvm::orc::ObjectLinkingLayer>(ES, *memory_manager);
        });
  }
  return Status::OK();
}
#endif

Result<std::unique_ptr<llvm::orc::LLJIT>> BuildJIT(
    llvm::orc::JITTargetMachineBuilder jtmb,
    std::optional<std::reference_wrapper<GandivaObjectCache>>& object_cache) {
  llvm::orc::LLJITBuilder jit_builder;

#ifdef JIT_LINK_SUPPORTED
  ARROW_RETURN_NOT_OK(UseJITLinkIfEnabled(jit_builder));
#endif

  jit_builder.setJITTargetMachineBuilder(std::move(jtmb));
  if (object_cache.has_value()) {
    jit_builder.setCompileFunctionCreator(
        [&object_cache](llvm::orc::JITTargetMachineBuilder JTMB)
            -> llvm::Expected<std::unique_ptr<llvm::orc::IRCompileLayer::IRCompiler>> {
          auto target_machine = JTMB.createTargetMachine();
          if (!target_machine) {
            return target_machine.takeError();
          }
          // after compilation, the object code will be stored into the given object
          // cache
          return std::make_unique<llvm::orc::TMOwningSimpleCompiler>(
              std::move(*target_machine), &object_cache.value().get());
        });
  }
  auto maybe_jit = jit_builder.create();
  ARROW_ASSIGN_OR_RAISE(auto jit,
                        AsArrowResult(maybe_jit, "Could not create LLJIT instance: "));

  AddProcessSymbol(*jit);
  return jit;
}

Status Engine::SetLLVMObjectCache(GandivaObjectCache& object_cache) {
  auto cached_buffer = object_cache.getObject(nullptr);
  if (cached_buffer) {
    auto error = lljit_->addObjectFile(std::move(cached_buffer));
    if (error) {
      return Status::CodeGenError("Failed to add cached object file to LLJIT: ",
                                  llvm::toString(std::move(error)));
    }
  }
  return Status::OK();
}

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
  ARROW_LOG(INFO) << "Detected CPU Features: [" << cpu_attrs_str << "]";
  llvm_init = true;
}

Engine::Engine(const std::shared_ptr<Configuration>& conf,
               std::unique_ptr<llvm::orc::LLJIT> lljit,
               std::unique_ptr<llvm::TargetMachine> target_machine, bool cached)
    : context_(std::make_unique<llvm::LLVMContext>()),
      lljit_(std::move(lljit)),
      ir_builder_(std::make_unique<llvm::IRBuilder<>>(*context_)),
      types_(*context_),
      optimize_(conf->optimize()),
      cached_(cached),
      function_registry_(conf->function_registry()),
      target_machine_(std::move(target_machine)),
      conf_(conf) {
  // LLVM 10 doesn't like the expr function name to be the same as the module name
  auto module_id = "gdv_module_" + std::to_string(reinterpret_cast<uintptr_t>(this));
  module_ = std::make_unique<llvm::Module>(module_id, *context_);
}

Engine::~Engine() {}

Status Engine::Init() {
  std::call_once(register_exported_funcs_flag, gandiva::RegisterExportedFuncs);

  // Add mappings for global functions that can be accessed from LLVM/IR module.
  ARROW_RETURN_NOT_OK(AddGlobalMappings());
  return Status::OK();
}

Status Engine::LoadFunctionIRs() {
  if (!functions_loaded_) {
    ARROW_RETURN_NOT_OK(LoadPreCompiledIR());
    ARROW_RETURN_NOT_OK(DecimalIR::AddFunctions(this));
    ARROW_RETURN_NOT_OK(LoadExternalPreCompiledIR());
    functions_loaded_ = true;
  }
  return Status::OK();
}

/// factory method to construct the engine.
Result<std::unique_ptr<Engine>> Engine::Make(
    const std::shared_ptr<Configuration>& conf, bool cached,
    std::optional<std::reference_wrapper<GandivaObjectCache>> object_cache) {
  std::call_once(llvm_init_once_flag, InitOnce);

  ARROW_ASSIGN_OR_RAISE(auto jtmb, MakeTargetMachineBuilder(*conf));
  ARROW_ASSIGN_OR_RAISE(auto jit, BuildJIT(jtmb, object_cache));
  auto maybe_tm = jtmb.createTargetMachine();
  ARROW_ASSIGN_OR_RAISE(auto target_machine,
                        AsArrowResult(maybe_tm, "Could not create target machine: "));

  std::unique_ptr<Engine> engine{
      new Engine(conf, std::move(jit), std::move(target_machine), cached)};

  ARROW_RETURN_NOT_OK(engine->Init());
  return engine;
}

static arrow::Status VerifyAndLinkModule(
    llvm::Module& dest_module,
    llvm::Expected<std::unique_ptr<llvm::Module>> src_module_or_error) {
  ARROW_ASSIGN_OR_RAISE(
      auto src_ir_module,
      AsArrowResult(src_module_or_error, "Failed to verify and link module: "));

  src_ir_module->setDataLayout(dest_module.getDataLayout());

  std::string error_info;
  llvm::raw_string_ostream error_stream(error_info);
  ARROW_RETURN_IF(
      llvm::verifyModule(*src_ir_module, &error_stream),
      Status::CodeGenError("verify of IR Module failed: " + error_stream.str()));

  ARROW_RETURN_IF(llvm::Linker::linkModules(dest_module, std::move(src_ir_module)),
                  Status::CodeGenError("failed to link IR Modules"));

  return Status::OK();
}

llvm::Module* Engine::module() {
  DCHECK(!module_finalized_) << "module cannot be accessed after finalized";
  return module_.get();
}

// Handling for pre-compiled IR libraries.
Status Engine::LoadPreCompiledIR() {
  auto const bitcode = llvm::StringRef(reinterpret_cast<const char*>(kPrecompiledBitcode),
                                       kPrecompiledBitcodeSize);

  /// Read from file into memory buffer.
  llvm::ErrorOr<std::unique_ptr<llvm::MemoryBuffer>> buffer_or_error =
      llvm::MemoryBuffer::getMemBuffer(bitcode, "precompiled", false);

  ARROW_RETURN_IF(!buffer_or_error,
                  Status::CodeGenError("Could not load module from IR: ",
                                       buffer_or_error.getError().message()));

  std::unique_ptr<llvm::MemoryBuffer> buffer = std::move(buffer_or_error.get());

  /// Parse the IR module.
  llvm::Expected<std::unique_ptr<llvm::Module>> module_or_error =
      llvm::getOwningLazyBitcodeModule(std::move(buffer), *context());
  // NOTE: llvm::handleAllErrors() fails linking with RTTI-disabled LLVM builds
  // (ARROW-5148)
  ARROW_RETURN_NOT_OK(VerifyAndLinkModule(*module_, std::move(module_or_error)));
  return Status::OK();
}

static llvm::MemoryBufferRef AsLLVMMemoryBuffer(const arrow::Buffer& arrow_buffer) {
  auto const data = reinterpret_cast<const char*>(arrow_buffer.data());
  auto const size = arrow_buffer.size();
  return {llvm::StringRef(data, size), "external_bitcode"};
}

Status Engine::LoadExternalPreCompiledIR() {
  auto const& buffers = function_registry_->GetBitcodeBuffers();
  for (auto const& buffer : buffers) {
    auto llvm_memory_buffer_ref = AsLLVMMemoryBuffer(*buffer);
    auto module_or_error = llvm::parseBitcodeFile(llvm_memory_buffer_ref, *context());
    ARROW_RETURN_NOT_OK(VerifyAndLinkModule(*module_, std::move(module_or_error)));
  }

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
  llvm::PassBuilder pass_builder;
  llvm::ModuleAnalysisManager module_am;

  pass_builder.registerModuleAnalyses(module_am);
  llvm::ModulePassManager module_pm;

  std::unordered_set<std::string> used_functions;
  used_functions.insert(functions_to_compile_.begin(), functions_to_compile_.end());

  module_pm.addPass(
      llvm::InternalizePass([&used_functions](const llvm::GlobalValue& variable) -> bool {
        return used_functions.find(variable.getName().str()) != used_functions.end();
      }));
  module_pm.addPass(llvm::GlobalDCEPass());

  module_pm.run(*module_, module_am);
  return Status::OK();
}

// several passes requiring LLVM 14+ that are not available in the legacy pass manager
#if LLVM_VERSION_MAJOR >= 14
static void OptimizeModuleWithNewPassManager(llvm::Module& module,
                                             llvm::TargetIRAnalysis target_analysis) {
  // Setup an optimiser pipeline
  llvm::PassBuilder pass_builder;
  llvm::LoopAnalysisManager loop_am;
  llvm::FunctionAnalysisManager function_am;
  llvm::CGSCCAnalysisManager cgscc_am;
  llvm::ModuleAnalysisManager module_am;

  function_am.registerPass([&] { return target_analysis; });

  // Register required analysis managers
  pass_builder.registerModuleAnalyses(module_am);
  pass_builder.registerCGSCCAnalyses(cgscc_am);
  pass_builder.registerFunctionAnalyses(function_am);
  pass_builder.registerLoopAnalyses(loop_am);
  pass_builder.crossRegisterProxies(loop_am, function_am, cgscc_am, module_am);

  pass_builder.registerPipelineStartEPCallback([&](llvm::ModulePassManager& module_pm,
                                                   llvm::OptimizationLevel Level) {
    module_pm.addPass(llvm::ModuleInlinerPass());

    llvm::FunctionPassManager function_pm;
    function_pm.addPass(llvm::InstCombinePass());
    function_pm.addPass(llvm::PromotePass());
    function_pm.addPass(llvm::GVNPass());
    function_pm.addPass(llvm::NewGVNPass());
    function_pm.addPass(llvm::SimplifyCFGPass());
    function_pm.addPass(llvm::LoopVectorizePass());
    function_pm.addPass(llvm::SLPVectorizerPass());
    module_pm.addPass(llvm::createModuleToFunctionPassAdaptor(std::move(function_pm)));

    module_pm.addPass(llvm::GlobalOptPass());
  });

  pass_builder.buildPerModuleDefaultPipeline(llvm::OptimizationLevel::O3)
      .run(module, module_am);
}
#else
static void OptimizeModuleWithLegacyPassManager(llvm::Module& module,
                                                llvm::TargetIRAnalysis target_analysis) {
  std::unique_ptr<llvm::legacy::PassManager> pass_manager(
      new llvm::legacy::PassManager());

  pass_manager->add(
      llvm::createTargetTransformInfoWrapperPass(std::move(target_analysis)));
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
  pass_manager->run(module);
}
#endif

// Optimise and compile the module.
Status Engine::FinalizeModule() {
  if (!cached_) {
    ARROW_RETURN_NOT_OK(RemoveUnusedFunctions());

    if (optimize_) {
      auto target_analysis = target_machine_->getTargetIRAnalysis();
// misc passes to allow for inlining, vectorization, ..
#if LLVM_VERSION_MAJOR >= 14
      OptimizeModuleWithNewPassManager(*module_, std::move(target_analysis));
#else
      OptimizeModuleWithLegacyPassManager(*module_, std::move(target_analysis));
#endif
    }

    ARROW_RETURN_IF(llvm::verifyModule(*module_, &llvm::errs()),
                    Status::CodeGenError("Module verification failed after optimizer"));

    // print the module IR and save it for later use if IR dumping is needed
    // since the module will be moved to construct LLJIT instance, and it is not
    // available after LLJIT instance is constructed
    if (conf_->dump_ir()) {
      module_ir_ = DumpModuleIR(*module_);
    }

    llvm::orc::ThreadSafeModule tsm(std::move(module_), std::move(context_));
    auto error = lljit_->addIRModule(std::move(tsm));
    if (error) {
      return Status::CodeGenError("Failed to add IR module to LLJIT: ",
                                  llvm::toString(std::move(error)));
    }
  }
  module_finalized_ = true;

  return Status::OK();
}

Result<void*> Engine::CompiledFunction(const std::string& function) {
  DCHECK(module_finalized_)
      << "module must be finalized before getting compiled function";
  auto sym = lljit_->lookup(function);
  if (!sym) {
    return Status::CodeGenError("Failed to look up function: " + function +
                                " error: " + llvm::toString(sym.takeError()));
  }
  // Since LLVM 15, `LLJIT::lookup` returns ExecutorAddrs rather than
  // JITEvaluatedSymbols
#if LLVM_VERSION_MAJOR >= 15
  auto fn_addr = sym->getValue();
#else
  auto fn_addr = sym->getAddress();
#endif
  auto fn_ptr = reinterpret_cast<void*>(fn_addr);
  if (fn_ptr == nullptr) {
    return Status::CodeGenError("Failed to get address for function: " + function);
  }
  return fn_ptr;
}

void Engine::AddGlobalMappingForFunc(const std::string& name, llvm::Type* ret_type,
                                     const std::vector<llvm::Type*>& args, void* func) {
  auto const prototype = llvm::FunctionType::get(ret_type, args, /*is_var_arg*/ false);
  llvm::Function::Create(prototype, llvm::GlobalValue::ExternalLinkage, name, module());
  AddAbsoluteSymbol(*lljit_, name, func);
}

arrow::Status Engine::AddGlobalMappings() {
  ARROW_RETURN_NOT_OK(ExportedFuncsRegistry::AddMappings(this));
  ExternalCFunctions c_funcs(function_registry_);
  return c_funcs.AddMappings(this);
}

const std::string& Engine::ir() {
  DCHECK(!module_ir_.empty()) << "dump_ir in Configuration must be set for dumping IR";
  return module_ir_;
}

}  // namespace gandiva
