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

#include "gandiva/expression_registry.h"

#include <algorithm>
#include <vector>

#include <gtest/gtest.h>
#include "gandiva/function_registry.h"
#include "gandiva/function_signature.h"
#include "gandiva/llvm_types.h"

namespace gandiva {

typedef int64_t (*add_vector_func_t)(int64_t* elements, int nelements);

class TestExpressionRegistry : public ::testing::Test {
 protected:
  std::shared_ptr<FunctionRegistry> registry_ = default_function_registry();
};

// Verify all functions in registry are exported.
TEST_F(TestExpressionRegistry, VerifySupportedFunctions) {
  std::vector<FunctionSignature> functions;
  ExpressionRegistry expr_registry;
  for (auto iter = expr_registry.function_signature_begin();
       iter != expr_registry.function_signature_end(); iter++) {
    functions.push_back((*iter));
  }
  for (auto& iter : *registry_) {
    for (auto& func_iter : iter.signatures()) {
      auto element = std::find(functions.begin(), functions.end(), func_iter);
      EXPECT_NE(element, functions.end()) << "function signature " << func_iter.ToString()
                                          << " missing in supported functions.\n";
    }
  }
}

// Verify all types are supported.
TEST_F(TestExpressionRegistry, VerifyDataTypes) {
  DataTypeVector data_types = ExpressionRegistry::supported_types();
  llvm::LLVMContext llvm_context;
  LLVMTypes llvm_types(llvm_context);
  auto supported_arrow_types = llvm_types.GetSupportedArrowTypes();
  for (auto& type_id : supported_arrow_types) {
    auto element =
        std::find(supported_arrow_types.begin(), supported_arrow_types.end(), type_id);
    EXPECT_NE(element, supported_arrow_types.end())
        << "data type  " << type_id << " missing in supported data types.\n";
  }
}

}  // namespace gandiva
