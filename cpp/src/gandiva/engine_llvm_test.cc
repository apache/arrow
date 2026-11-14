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

#include "gandiva/engine.h"

#include <gtest/gtest.h>
#include <functional>
#include "gandiva/llvm_types.h"
#include "gandiva/tests/test_util.h"

namespace gandiva {

using add_vector_func_t = int64_t (*)(int64_t*, int);

class TestEngine : public ::testing::Test {
 protected:
  static std::string Normalize(const std::string& triple) {
    return NormalizeDarwinTriple(llvm::Triple(triple)).str();
  }

  std::string BuildVecAdd(Engine* gdv_engine) {
    auto types = gdv_engine->types();
    llvm::IRBuilder<>* builder = gdv_engine->ir_builder();
    llvm::LLVMContext* context = gdv_engine->context();

    // Create fn prototype :
    //   int64_t add_longs(int64_t *elements, int32_t nelements)
    std::vector<llvm::Type*> arguments;
    arguments.push_back(types->i64_ptr_type());
    arguments.push_back(types->i32_type());
    llvm::FunctionType* prototype =
        llvm::FunctionType::get(types->i64_type(), arguments, false /*isVarArg*/);

    // Create fn
    std::string func_name = "add_longs_test_expr";
    gdv_engine->AddFunctionToCompile(func_name);
    llvm::Function* fn = llvm::Function::Create(
        prototype, llvm::GlobalValue::ExternalLinkage, func_name, gdv_engine->module());
    assert(fn != nullptr);

    // Name the arguments
    llvm::Function::arg_iterator args = fn->arg_begin();
    llvm::Value* arg_elements = &*args;
    arg_elements->setName("elements");
    ++args;
    llvm::Value* arg_nelements = &*args;
    arg_nelements->setName("nelements");
    ++args;

    llvm::BasicBlock* loop_entry = llvm::BasicBlock::Create(*context, "entry", fn);
    llvm::BasicBlock* loop_body = llvm::BasicBlock::Create(*context, "loop", fn);
    llvm::BasicBlock* loop_exit = llvm::BasicBlock::Create(*context, "exit", fn);

    // Loop entry
    builder->SetInsertPoint(loop_entry);
    builder->CreateBr(loop_body);

    // Loop body
    builder->SetInsertPoint(loop_body);

    llvm::PHINode* loop_var = builder->CreatePHI(types->i32_type(), 2, "loop_var");
    llvm::PHINode* sum = builder->CreatePHI(types->i64_type(), 2, "sum");

    loop_var->addIncoming(types->i32_constant(0), loop_entry);
    sum->addIncoming(types->i64_constant(0), loop_entry);

    // setup loop PHI
    llvm::Value* loop_update =
        builder->CreateAdd(loop_var, types->i32_constant(1), "loop_var+1");
    loop_var->addIncoming(loop_update, loop_body);

    // get the current value
    llvm::Value* offset =
        builder->CreateGEP(types->i64_type(), arg_elements, loop_var, "offset");
    llvm::Value* current_value = builder->CreateLoad(types->i64_type(), offset, "value");

    // setup sum PHI
    llvm::Value* sum_update = builder->CreateAdd(sum, current_value, "sum+ith");
    sum->addIncoming(sum_update, loop_body);

    // check loop_var
    llvm::Value* loop_var_check =
        builder->CreateICmpSLT(loop_update, arg_nelements, "loop_var < nrec");
    builder->CreateCondBr(loop_var_check, loop_body, loop_exit);

    // Loop exit
    builder->SetInsertPoint(loop_exit);
    builder->CreateRet(sum_update);
    return func_name;
  }

  void BuildEngine() {
    ASSERT_OK_AND_ASSIGN(engine, Engine::Make(TestConfiguration(), false));
  }

  std::unique_ptr<Engine> engine;
  std::shared_ptr<Configuration> configuration = TestConfiguration();
};

TEST_F(TestEngine, TestAddUnoptimised) {
  configuration->set_optimize(false);
  BuildEngine();

  std::string fn_name = BuildVecAdd(engine.get());
  ASSERT_OK(engine->FinalizeModule());
  ASSERT_OK_AND_ASSIGN(auto fn_ptr, engine->CompiledFunction(fn_name));
  auto add_func = reinterpret_cast<add_vector_func_t>(fn_ptr);

  int64_t my_array[] = {1, 3, -5, 8, 10};
  EXPECT_EQ(add_func(my_array, 5), 17);
}

TEST_F(TestEngine, TestAddOptimised) {
  configuration->set_optimize(true);
  BuildEngine();

  std::string fn_name = BuildVecAdd(engine.get());
  ASSERT_OK(engine->FinalizeModule());
  EXPECT_OK_AND_ASSIGN(auto fn_ptr, engine->CompiledFunction(fn_name));
  auto add_func = reinterpret_cast<add_vector_func_t>(fn_ptr);

  int64_t my_array[] = {1, 3, -5, 8, 10};
  EXPECT_EQ(add_func(my_array, 5), 17);
}

TEST_F(TestEngine, NormalizeDarwinTripleDarwinBecomesMacOSX) {
  llvm::Triple arm = NormalizeDarwinTriple(llvm::Triple("arm64-apple-darwin27.2.0"));
  EXPECT_EQ(arm.getOS(), llvm::Triple::MacOSX);
  EXPECT_TRUE(arm.isAArch64());
  llvm::VersionTuple v;
  ASSERT_TRUE(arm.getMacOSXVersion(v));
  EXPECT_GE(v, llvm::VersionTuple(11, 0));

  EXPECT_EQ(Normalize("x86_64-apple-darwin19.0.0"), "x86_64-apple-macosx10.15");
}

TEST_F(TestEngine, NormalizeDarwinTripleUnversionedOrOldUsesFloor) {
  EXPECT_EQ(Normalize("arm64-apple-darwin"), "arm64-apple-macosx11.0");
  EXPECT_EQ(Normalize("arm64-apple-darwin8.0.0"), "arm64-apple-macosx11.0");
  EXPECT_EQ(Normalize("x86_64-apple-darwin3.0.0"), "x86_64-apple-macosx10.15");
}

TEST_F(TestEngine, NormalizeDarwinTripleOtherTriplesUnchanged) {
  // Already-macosx triples are left as-is
  EXPECT_EQ(Normalize("arm64-apple-macosx26.0.0"), "arm64-apple-macosx26.0.0");
  // Non-Darwin Apple OSes
  EXPECT_EQ(Normalize("arm64-apple-ios17.0.0"), "arm64-apple-ios17.0.0");
  // Linux
  EXPECT_EQ(Normalize("x86_64-unknown-linux-gnu"), "x86_64-unknown-linux-gnu");
  EXPECT_EQ(Normalize("aarch64-unknown-linux-musl"), "aarch64-unknown-linux-musl");
  // Windows
  EXPECT_EQ(Normalize("x86_64-pc-windows-msvc"), "x86_64-pc-windows-msvc");
}

TEST_F(TestEngine, NormalizeDarwinTripleFloorBoundary) {
  // darwin19 is macOS 10.15: x86 stays at its floor, arm64 is raised to 11.0
  EXPECT_EQ(Normalize("x86_64-apple-darwin19.0.0"), "x86_64-apple-macosx10.15");
  EXPECT_EQ(Normalize("arm64-apple-darwin19.0.0"), "arm64-apple-macosx11.0");
  EXPECT_EQ(Normalize("arm64-apple-darwin20.0.0"), "arm64-apple-macosx11.0");
}

TEST_F(TestEngine, NormalizeDarwinTripleArchAliases) {
  EXPECT_EQ(Normalize("aarch64-apple-darwin"), "aarch64-apple-macosx11.0");
  EXPECT_EQ(Normalize("arm64e-apple-darwin"), "arm64e-apple-macosx11.0");
  EXPECT_EQ(Normalize("i386-apple-darwin"), "i386-apple-macosx10.15");
}

TEST_F(TestEngine, NormalizeDarwinTripleIdempotent) {
  for (const char* triple : {"arm64-apple-darwin23.1.0", "arm64-apple-darwin",
                             "x86_64-apple-darwin8", "x86_64-unknown-linux-gnu"}) {
    std::string once = Normalize(triple);
    EXPECT_EQ(Normalize(once), once) << triple;
  }
}

}  // namespace gandiva
