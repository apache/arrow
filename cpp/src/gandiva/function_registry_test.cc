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

#include "gandiva/function_registry.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <string>
#include <unordered_set>

namespace gandiva {

class TestFunctionRegistry : public ::testing::Test {
 protected:
  FunctionRegistry registry_;
};

TEST_F(TestFunctionRegistry, TestFound) {
  FunctionSignature add_i32_i32("add", {arrow::int32(), arrow::int32()}, arrow::int32());

  const NativeFunction* function = registry_.LookupSignature(add_i32_i32);
  EXPECT_NE(function, nullptr);
  EXPECT_THAT(function->signatures(), testing::Contains(add_i32_i32));
  EXPECT_EQ(function->pc_name(), "add_int32_int32");
}

TEST_F(TestFunctionRegistry, TestNotFound) {
  FunctionSignature addX_i32_i32("addX", {arrow::int32(), arrow::int32()},
                                 arrow::int32());
  EXPECT_EQ(registry_.LookupSignature(addX_i32_i32), nullptr);

  FunctionSignature add_i32_i32_ret64("add", {arrow::int32(), arrow::int32()},
                                      arrow::int64());
  EXPECT_EQ(registry_.LookupSignature(add_i32_i32_ret64), nullptr);
}

TEST_F(TestFunctionRegistry, TestDuplicates) {
  std::unordered_set<std::string> pc_func_sigs;
  std::unordered_set<std::string> duplicates;
  for (auto native_func_it = registry_.begin(); native_func_it != registry_.end();
       ++native_func_it) {
    auto& sig = native_func_it->signature();
    auto pc_func_sig =
        FunctionSignature(native_func_it->pc_name(), sig.param_types(), sig.ret_type())
            .ToString();
    if (pc_func_sigs.count(pc_func_sig) == 0) {
      pc_func_sigs.insert(pc_func_sig);
    } else {
      duplicates.insert(pc_func_sig);
    }
  }
  std::ostringstream stream;
  std::copy(duplicates.begin(), duplicates.end(),
            std::ostream_iterator<std::string>(stream, "\n"));
  std::string result = stream.str();
  EXPECT_TRUE(duplicates.empty())
      << "Registry has duplicates.\nMultiple NativeFunction objects refer to the "
         "following precompiled functions:\n"
      << result;
}

}  // namespace gandiva
