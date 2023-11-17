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
// under the License

#include <llvm/IR/Type.h>

#include "gandiva/engine.h"
#include "gandiva/exported_funcs.h"

namespace {
// calculate the number of arguments for a function signature
size_t GetNumArgs(const gandiva::FunctionSignature& sig,
                  const gandiva::NativeFunction& func) {
  auto num_args = 0;
  num_args += func.NeedsContext() ? 1 : 0;
  num_args += func.NeedsFunctionHolder() ? 1 : 0;
  for (auto const& arg : sig.param_types()) {
    num_args += arg->id() == arrow::Type::STRING ? 2 : 1;
  }
  num_args += sig.ret_type()->id() == arrow::Type::STRING ? 1 : 0;
  return num_args;
}

// map from a NativeFunction's signature to the corresponding LLVM signature
arrow::Result<std::pair<std::vector<llvm::Type*>, llvm::Type*>> MapToLLVMSignature(
    const gandiva::FunctionSignature& sig, const gandiva::NativeFunction& func,
    gandiva::LLVMTypes* types) {
  std::vector<llvm::Type*> arg_llvm_types;
  arg_llvm_types.reserve(GetNumArgs(sig, func));

  if (func.NeedsContext()) {
    arg_llvm_types.push_back(types->i64_type());
  }
  if (func.NeedsFunctionHolder()) {
    arg_llvm_types.push_back(types->i64_type());
  }
  for (auto const& arg : sig.param_types()) {
    arg_llvm_types.push_back(types->IRType(arg->id()));
    if (arg->id() == arrow::Type::STRING) {
      // string type needs an additional length argument
      arg_llvm_types.push_back(types->i32_type());
    }
  }
  if (sig.ret_type()->id() == arrow::Type::STRING) {
    // for string output, the last arg is the output length
    arg_llvm_types.push_back(types->i32_ptr_type());
  }
  auto ret_llvm_type = types->IRType(sig.ret_type()->id());
  return std::make_pair(std::move(arg_llvm_types), ret_llvm_type);
}
}  // namespace

namespace gandiva {
Status ExternalCFunctions::AddMappings(Engine* engine) const {
  auto const& c_funcs = function_registry_->GetCFunctions();
  auto const types = engine->types();
  for (auto& [func, func_ptr] : c_funcs) {
    for (auto const& sig : func.signatures()) {
      ARROW_ASSIGN_OR_RAISE(auto llvm_signature, MapToLLVMSignature(sig, func, types));
      auto& [args, ret_llvm_type] = llvm_signature;
      engine->AddGlobalMappingForFunc(func.pc_name(), ret_llvm_type, args, func_ptr);
    }
  }
  return Status::OK();
}
}  // namespace gandiva
