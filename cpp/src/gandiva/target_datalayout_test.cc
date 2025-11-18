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

#include <gtest/gtest.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/TargetParser/Host.h>

#include "gandiva/llvm_generator.h"
#include "gandiva/tests/test_util.h"

namespace gandiva {

class TestTargetDataLayout : public ::testing::Test {
 protected:
  void SetUp() override {}
};

// Test that verifies the target data layout string representation
// is populated.
TEST_F(TestTargetDataLayout, VerifyDataLayoutForArchitecture) {
  ASSERT_OK_AND_ASSIGN(auto generator, LLVMGenerator::Make(TestConfiguration(), false));

  llvm::Module* module = generator->module();
  ASSERT_NE(module, nullptr);

  const llvm::DataLayout& data_layout = module->getDataLayout();
  std::string data_layout_str = data_layout.getStringRepresentation();

  EXPECT_FALSE(data_layout_str.empty());

  std::string host_cpu = llvm::sys::getHostCPUName().str();
  std::string triple = llvm::sys::getDefaultTargetTriple();
  // Log the information for debugging
  std::cout << "Host CPU: " << host_cpu << std::endl;
  std::cout << "Target Triple: " << triple << std::endl;
  std::cout << "Data Layout: " << data_layout_str << std::endl;
  }
}  // namespace gandiva

