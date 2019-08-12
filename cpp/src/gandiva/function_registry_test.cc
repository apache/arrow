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

// one nativefunction object per precompiled function
TEST_F(TestFunctionRegistry, TestNoDuplicates) {
  std::unordered_set<std::string> pc_func_sigs;
  std::unordered_set<std::string> native_func_duplicates;
  std::unordered_set<std::string> func_sigs;
  std::unordered_set<std::string> func_sig_duplicates;
  for (auto native_func_it = registry_.begin(); native_func_it != registry_.end();
       ++native_func_it) {
    auto& first_sig = native_func_it->signatures().front();
    auto pc_func_sig = FunctionSignature(native_func_it->pc_name(),
                                         first_sig.param_types(), first_sig.ret_type())
                           .ToString();
    if (pc_func_sigs.count(pc_func_sig) == 0) {
      pc_func_sigs.insert(pc_func_sig);
    } else {
      native_func_duplicates.insert(pc_func_sig);
    }

    for (auto& sig : native_func_it->signatures()) {
      auto sig_str = sig.ToString();
      if (func_sigs.count(sig_str) == 0) {
        func_sigs.insert(sig_str);
      } else {
        func_sig_duplicates.insert(sig_str);
      }
    }
  }
  std::ostringstream stream;
  std::copy(native_func_duplicates.begin(), native_func_duplicates.end(),
            std::ostream_iterator<std::string>(stream, "\n"));
  std::string result = stream.str();
  EXPECT_TRUE(native_func_duplicates.empty())
      << "Registry has duplicates.\nMultiple NativeFunction objects refer to the "
         "following precompiled functions:\n"
      << result;

  stream.clear();
  std::copy(func_sig_duplicates.begin(), func_sig_duplicates.end(),
            std::ostream_iterator<std::string>(stream, "\n"));
  EXPECT_TRUE(func_sig_duplicates.empty())
      << "The following signatures are defined more than once possibly pointing to "
         "different precompiled functions:\n"
      << stream.str();
}
}  // namespace gandiva
