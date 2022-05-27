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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/compute/function.h"
#include "arrow/compute/registry.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace compute {

class TestRegistry : public ::testing::Test {
 public:
  void SetUp() { registry_ = FunctionRegistry::Make(); }

 protected:
  std::unique_ptr<FunctionRegistry> registry_;
};

TEST_F(TestRegistry, CreateBuiltInRegistry) {
  // This does DCHECK_OK internally for now so this will fail in debug builds
  // if there is a problem initializing the global function registry
  FunctionRegistry* registry = GetFunctionRegistry();
  ARROW_UNUSED(registry);
}

TEST_F(TestRegistry, Basics) {
  ASSERT_EQ(0, registry_->num_functions());

  std::shared_ptr<Function> func = std::make_shared<ScalarFunction>(
      "f1", Arity::Unary(), /*doc=*/FunctionDoc::Empty());
  ASSERT_OK(registry_->AddFunction(func));
  ASSERT_EQ(1, registry_->num_functions());

  func = std::make_shared<VectorFunction>("f0", Arity::Binary(),
                                          /*doc=*/FunctionDoc::Empty());
  ASSERT_OK(registry_->AddFunction(func));
  ASSERT_EQ(2, registry_->num_functions());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<const Function> f1, registry_->GetFunction("f1"));
  ASSERT_EQ("f1", f1->name());

  // Non-existent function
  ASSERT_RAISES(KeyError, registry_->GetFunction("f2"));

  // Try adding a function with name collision
  func = std::make_shared<ScalarAggregateFunction>("f1", Arity::Unary(),
                                                   /*doc=*/FunctionDoc::Empty());
  ASSERT_RAISES(KeyError, registry_->AddFunction(func));

  // Allow overwriting by flag
  ASSERT_OK(registry_->AddFunction(func, /*allow_overwrite=*/true));
  ASSERT_OK_AND_ASSIGN(f1, registry_->GetFunction("f1"));
  ASSERT_EQ(Function::SCALAR_AGGREGATE, f1->kind());

  std::vector<std::string> expected_names = {"f0", "f1"};
  ASSERT_EQ(expected_names, registry_->GetFunctionNames());

  // Aliases
  ASSERT_RAISES(KeyError, registry_->AddAlias("f33", "f3"));
  ASSERT_OK(registry_->AddAlias("f11", "f1"));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<const Function> f2, registry_->GetFunction("f11"));
  ASSERT_EQ(func, f2);
}

}  // namespace compute
}  // namespace arrow
