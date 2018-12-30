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

#include "gandiva/llvm_generator.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>
#include "gandiva/configuration.h"
#include "gandiva/dex.h"
#include "gandiva/expression.h"
#include "gandiva/func_descriptor.h"
#include "gandiva/function_registry.h"
#include "gandiva/tests/test_util.h"

namespace gandiva {

typedef int64_t (*add_vector_func_t)(int64_t* elements, int nelements);

class TestLLVMGenerator : public ::testing::Test {
 protected:
  FunctionRegistry registry_;
};

// Verify that a valid pc function exists for every function in the registry.
TEST_F(TestLLVMGenerator, VerifyPCFunctions) {
  std::unique_ptr<LLVMGenerator> generator;
  auto status = LLVMGenerator::Make(TestConfiguration(), &generator);
  EXPECT_TRUE(status.ok()) << status.message();

  llvm::Module* module = generator->module();
  for (auto& iter : registry_) {
    llvm::Function* fn = module->getFunction(iter.pc_name());
    EXPECT_NE(fn, nullptr) << "function " << iter.pc_name()
                           << " missing in precompiled module\n";
  }
}

TEST_F(TestLLVMGenerator, TestAdd) {
  // Setup LLVM generator to do an arithmetic add of two vectors
  std::unique_ptr<LLVMGenerator> generator;
  auto status = LLVMGenerator::Make(TestConfiguration(), &generator);
  EXPECT_TRUE(status.ok());
  Annotator annotator;

  auto field0 = std::make_shared<arrow::Field>("f0", arrow::int32());
  auto desc0 = annotator.CheckAndAddInputFieldDescriptor(field0);
  auto validity_dex0 = std::make_shared<VectorReadValidityDex>(desc0);
  auto value_dex0 = std::make_shared<VectorReadFixedLenValueDex>(desc0);
  auto pair0 = std::make_shared<ValueValidityPair>(validity_dex0, value_dex0);

  auto field1 = std::make_shared<arrow::Field>("f1", arrow::int32());
  auto desc1 = annotator.CheckAndAddInputFieldDescriptor(field1);
  auto validity_dex1 = std::make_shared<VectorReadValidityDex>(desc1);
  auto value_dex1 = std::make_shared<VectorReadFixedLenValueDex>(desc1);
  auto pair1 = std::make_shared<ValueValidityPair>(validity_dex1, value_dex1);

  DataTypeVector params{arrow::int32(), arrow::int32()};
  auto func_desc = std::make_shared<FuncDescriptor>("add", params, arrow::int32());
  FunctionSignature signature(func_desc->name(), func_desc->params(),
                              func_desc->return_type());
  const NativeFunction* native_func =
      generator->function_registry_.LookupSignature(signature);

  std::vector<ValueValidityPairPtr> pairs{pair0, pair1};
  auto func_dex = std::make_shared<NonNullableFuncDex>(func_desc, native_func,
                                                       FunctionHolderPtr(nullptr), pairs);

  auto field_sum = std::make_shared<arrow::Field>("out", arrow::int32());
  auto desc_sum = annotator.CheckAndAddInputFieldDescriptor(field_sum);

  llvm::Function* ir_func = nullptr;

  status = generator->CodeGenExprValue(func_dex, desc_sum, 0, &ir_func,
                                       SelectionVector::MODE_NONE);
  EXPECT_TRUE(status.ok()) << status.message();

  status = generator->engine_->FinalizeModule(true, false);
  EXPECT_TRUE(status.ok()) << status.message();

  EvalFunc eval_func = (EvalFunc)generator->engine_->CompiledFunction(ir_func);

  int num_records = 4;
  uint32_t a0[] = {1, 2, 3, 4};
  uint32_t a1[] = {5, 6, 7, 8};
  uint64_t in_bitmap = 0xffffffffffffffffull;

  uint32_t out[] = {0, 0, 0, 0};
  uint64_t out_bitmap = 0;

  uint8_t* addrs[] = {
      reinterpret_cast<uint8_t*>(a0),  reinterpret_cast<uint8_t*>(&in_bitmap),
      reinterpret_cast<uint8_t*>(a1),  reinterpret_cast<uint8_t*>(&in_bitmap),
      reinterpret_cast<uint8_t*>(out), reinterpret_cast<uint8_t*>(&out_bitmap),
  };
  eval_func(addrs, nullptr, nullptr, 0 /* dummy context ptr */, num_records);

  uint32_t expected[] = {6, 8, 10, 12};
  for (int i = 0; i < num_records; i++) {
    EXPECT_EQ(expected[i], out[i]);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace gandiva
