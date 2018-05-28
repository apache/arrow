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

#include <gtest/gtest.h>
#include "codegen/engine.h"
#include "codegen/llvm_types.h"
#include "codegen/codegen_exception.h"

namespace gandiva {

typedef int64_t (*add_vector_func_t)(int64_t *elements, int nelements);

class TestEngine : public ::testing::Test {
 protected:
  llvm::Function *BuildVecAdd(Engine *engine, LLVMTypes *types);
};

llvm::Function *TestEngine::BuildVecAdd(Engine *engine, LLVMTypes *types) {
  llvm::IRBuilder<> &builder = engine->ir_builder();
  llvm::LLVMContext *context = engine->context();

  // Create fn prototype :
  //   int64_t add_longs(int64_t *elements, int32_t nelements)
  std::vector<llvm::Type *> arguments;
  arguments.push_back(types->i64_ptr_type());
  arguments.push_back(types->i32_type());
  llvm::FunctionType *prototype = llvm::FunctionType::get(types->i64_type(),
                                                          arguments,
                                                          false /*isVarArg*/);

  // Create fn
  std::string func_name = "add_longs";
  engine->AddFunctionToCompile(func_name);
  llvm::Function *fn = llvm::Function::Create(prototype,
                                              llvm::GlobalValue::ExternalLinkage,
                                              func_name,
                                              engine->module());
  assert(fn != NULL);

  // Name the arguments
  llvm::Function::arg_iterator args = fn->arg_begin();
  llvm::Value *arg_elements = &*args;
  arg_elements->setName("elements");
  ++args;
  llvm::Value *arg_nelements = &*args;
  arg_nelements->setName("nelements");
  ++args;

  llvm::BasicBlock *loop_entry = llvm::BasicBlock::Create(*context, "entry", fn);
  llvm::BasicBlock *loop_body = llvm::BasicBlock::Create(*context, "loop", fn);
  llvm::BasicBlock *loop_exit = llvm::BasicBlock::Create(*context, "exit", fn);

  // Loop entry
  builder.SetInsertPoint(loop_entry);
  builder.CreateBr(loop_body);

  // Loop body
  builder.SetInsertPoint(loop_body);

  llvm::PHINode *loop_var = builder.CreatePHI(types->i32_type(), 2, "loop_var");
  llvm::PHINode *sum = builder.CreatePHI(types->i64_type(), 2, "sum");

  loop_var->addIncoming(types->i32_constant(0), loop_entry);
  sum->addIncoming(types->i64_constant(0), loop_entry);

  // setup loop PHI
  llvm::Value *loop_update = builder.CreateAdd(loop_var,
                                               types->i32_constant(1),
                                               "loop_var+1");
  loop_var->addIncoming(loop_update, loop_body);

  // get the current value
  llvm::Value *offset = builder.CreateGEP(arg_elements, loop_var, "offset");
  llvm::Value *current_value = builder.CreateLoad(offset, "value");

  // setup sum PHI
  llvm::Value *sum_update = builder.CreateAdd(sum, current_value, "sum+ith");
  sum->addIncoming(sum_update, loop_body);

  // check loop_var
  llvm::Value *loop_var_check = builder.CreateICmpSLT(loop_update,
                                                      arg_nelements,
                                                      "loop_var < nrec");
  builder.CreateCondBr(loop_var_check, loop_body, loop_exit);

  // Loop exit
  builder.SetInsertPoint(loop_exit);
  builder.CreateRet(sum_update);
  return fn;
}

TEST_F(TestEngine, TestAddUnoptimised) {
  Engine engine;
  LLVMTypes types(*engine.context());
  llvm::Function *ir_func = BuildVecAdd(&engine, &types);
  engine.FinalizeModule(false, false);

  add_vector_func_t add_func =
      reinterpret_cast<add_vector_func_t>(engine.CompiledFunction(ir_func));

  int64_t my_array[] = {1, 3, -5, 8, 10};
  EXPECT_EQ(add_func(my_array, 5), 17);
}

TEST_F(TestEngine, TestAddOptimised) {
  Engine engine;
  LLVMTypes types(*engine.context());
  llvm::Function *ir_func = BuildVecAdd(&engine, &types);
  engine.FinalizeModule(true, false);

  add_vector_func_t add_func =
      reinterpret_cast<add_vector_func_t>(engine.CompiledFunction(ir_func));

  int64_t my_array[] = {1, 3, -5, 8, 10};
  EXPECT_EQ(add_func(my_array, 5), 17);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

} // namespace gandiva
