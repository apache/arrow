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

#include "gandiva/function_ir_builder.h"

namespace gandiva {

llvm::Value* FunctionIRBuilder::BuildIfElse(llvm::Value* condition,
                                            llvm::Type* return_type,
                                            std::function<llvm::Value*()> then_func,
                                            std::function<llvm::Value*()> else_func) {
  llvm::IRBuilder<>* builder = ir_builder();
  llvm::Function* function = builder->GetInsertBlock()->getParent();
  DCHECK_NE(function, nullptr);

  // Create blocks for the then, else and merge cases.
  llvm::BasicBlock* then_bb = llvm::BasicBlock::Create(*context(), "then", function);
  llvm::BasicBlock* else_bb = llvm::BasicBlock::Create(*context(), "else", function);
  llvm::BasicBlock* merge_bb = llvm::BasicBlock::Create(*context(), "merge", function);

  builder->CreateCondBr(condition, then_bb, else_bb);

  // Emit the then block.
  builder->SetInsertPoint(then_bb);
  auto then_value = then_func();
  builder->CreateBr(merge_bb);

  // refresh then_bb for phi (could have changed due to code generation of then_value).
  then_bb = builder->GetInsertBlock();

  // Emit the else block.
  builder->SetInsertPoint(else_bb);
  auto else_value = else_func();
  builder->CreateBr(merge_bb);

  // refresh else_bb for phi (could have changed due to code generation of else_value).
  else_bb = builder->GetInsertBlock();

  // Emit the merge block.
  builder->SetInsertPoint(merge_bb);
  llvm::PHINode* result_value = builder->CreatePHI(return_type, 2, "res_value");
  result_value->addIncoming(then_value, then_bb);
  result_value->addIncoming(else_value, else_bb);
  return result_value;
}

llvm::Function* FunctionIRBuilder::BuildFunction(const std::string& function_name,
                                                 llvm::Type* return_type,
                                                 std::vector<NamedArg> in_args) {
  std::vector<llvm::Type*> arg_types;
  for (auto& arg : in_args) {
    arg_types.push_back(arg.type);
  }
  auto prototype = llvm::FunctionType::get(return_type, arg_types, false /*isVarArg*/);
  auto function = llvm::Function::Create(prototype, llvm::GlobalValue::ExternalLinkage,
                                         function_name, module());

  uint32_t i = 0;
  for (auto& fn_arg : function->args()) {
    DCHECK_LT(i, in_args.size());
    fn_arg.setName(in_args[i].name);
    ++i;
  }
  return function;
}

}  // namespace gandiva
