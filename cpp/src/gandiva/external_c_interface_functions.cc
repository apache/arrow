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

#include "llvm/IR/Type.h"

#include "gandiva/engine.h"
#include "gandiva/exported_funcs.h"

namespace gandiva {
static arrow::Result<llvm::Type*> AsLLVMType(const DataTypePtr& from_type,
                                             LLVMTypes* types) {
  switch (from_type->id()) {
    case arrow::Type::BOOL:
      return types->i1_type();
    case arrow::Type::INT8:
    case arrow::Type::UINT8:
      return types->i8_type();
    case arrow::Type::INT16:
    case arrow::Type::UINT16:
      return types->i16_type();
    case arrow::Type::INT32:
    case arrow::Type::UINT32:
      return types->i32_type();
    case arrow::Type::INT64:
    case arrow::Type::UINT64:
      return types->i64_type();
    case arrow::Type::FLOAT:
      return types->float_type();
    case arrow::Type::DOUBLE:
      return types->double_type();
    default:
      return Status::NotImplemented("Unsupported arrow data type: " +
                                    from_type->ToString());
  }
}

// map from a NativeFunction's signature to the corresponding LLVM signature
static arrow::Result<std::pair<std::vector<llvm::Type*>, llvm::Type*>> MapToLLVMSignature(
    const FunctionSignature& sig, const NativeFunction& func, LLVMTypes* types) {
  std::vector<llvm::Type*> args;
  args.reserve(sig.param_types().size());
  if (func.NeedsContext()) {
    args.emplace_back(types->i64_type());
  }
  if (func.NeedsFunctionHolder()) {
    args.emplace_back(types->i64_type());
  }
  for (auto const& arg : sig.param_types()) {
    if (arg->id() == arrow::Type::STRING) {
      args.emplace_back(types->i8_ptr_type());
      args.emplace_back(types->i32_type());
    } else {
      ARROW_ASSIGN_OR_RAISE(auto arg_llvm_type, AsLLVMType(arg, types));
      args.emplace_back(arg_llvm_type);
    }
  }
  llvm::Type* ret_llvm_type;
  if (sig.ret_type()->id() == arrow::Type::STRING) {
    // for string output, the last arg is the output length
    args.emplace_back(types->i32_ptr_type());
    ret_llvm_type = types->i8_ptr_type();
  } else {
    ARROW_ASSIGN_OR_RAISE(ret_llvm_type, AsLLVMType(sig.ret_type(), types));
  }
  auto return_type = AsLLVMType(sig.ret_type(), types);
  return std::make_pair(args, ret_llvm_type);
}

arrow::Status ExternalCInterfaceFunctions::AddMappings(Engine* engine) const {
  auto const& c_interface_funcs = function_registry_->GetCInterfaceFunctions();
  auto types = engine->types();
  for (auto& [func, func_ptr] : c_interface_funcs) {
    for (auto const& sig : func.signatures()) {
      ARROW_ASSIGN_OR_RAISE(auto llvm_signature, MapToLLVMSignature(sig, func, types));
      auto& [args, ret_llvm_type] = llvm_signature;
      engine->AddGlobalMappingForFunc(func.pc_name(), ret_llvm_type, args, func_ptr);
    }
  }
  return arrow::Status::OK();
}
}  // namespace gandiva
