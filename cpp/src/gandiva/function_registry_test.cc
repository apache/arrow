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
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "gandiva/tests/test_util.h"

namespace gandiva {

class TestFunctionRegistry : public ::testing::Test {
 protected:
  std::shared_ptr<FunctionRegistry> registry_ = gandiva::default_function_registry();

  static std::unique_ptr<FunctionRegistry> MakeFunctionRegistryWithExternalFunction() {
    auto registry = std::make_unique<FunctionRegistry>();
    ARROW_EXPECT_OK(
        registry->Register({GetTestExternalFunction()}, GetTestFunctionLLVMIRPath()));
    return registry;
  }
};

TEST_F(TestFunctionRegistry, TestAddDuplicateSignatureKeepsFirstRegistration) {
  auto registry = std::make_unique<FunctionRegistry>();
  auto buffer = arrow::Buffer::FromString("");

  NativeFunction first("foo", {}, DataTypeVector{arrow::int32()}, arrow::int32(),
                       kResultNullIfNull, "foo_first");
  NativeFunction second("foo", {}, DataTypeVector{arrow::int32()}, arrow::int32(),
                        kResultNullIfNull, "foo_second");

  ARROW_EXPECT_OK(registry->Register({first}, buffer));

  testing::internal::CaptureStderr();
  ARROW_EXPECT_OK(registry->Register({second}, buffer));
  std::string log = testing::internal::GetCapturedStderr();

  EXPECT_NE(log.find("Duplicate function signature registered"), std::string::npos)
      << "stderr was: " << log;
  EXPECT_NE(log.find("foo_first"), std::string::npos) << "stderr was: " << log;
  EXPECT_NE(log.find("foo_second"), std::string::npos) << "stderr was: " << log;

  // The first registration wins; lookup returns the original pc_name.
  FunctionSignature sig("foo", {arrow::int32()}, arrow::int32());
  const NativeFunction* fn = registry->LookupSignature(sig);
  ASSERT_NE(fn, nullptr);
  EXPECT_EQ(fn->pc_name(), "foo_first");
}

TEST_F(TestFunctionRegistry, TestAddCallShapeCollisionLogsAndKeepsBoth) {
  auto registry = std::make_unique<FunctionRegistry>();
  auto buffer = arrow::Buffer::FromString("");

  NativeFunction returns_int32("foo", {}, DataTypeVector{arrow::int32()}, arrow::int32(),
                               kResultNullIfNull, "foo_int32_to_int32");
  NativeFunction returns_int64("foo", {}, DataTypeVector{arrow::int32()}, arrow::int64(),
                               kResultNullIfNull, "foo_int32_to_int64");

  ARROW_EXPECT_OK(registry->Register({returns_int32}, buffer));

  testing::internal::CaptureStderr();
  ARROW_EXPECT_OK(registry->Register({returns_int64}, buffer));
  std::string log = testing::internal::GetCapturedStderr();

  EXPECT_NE(log.find("Function alias collision"), std::string::npos)
      << "stderr was: " << log;
  EXPECT_NE(log.find("foo_int32_to_int32"), std::string::npos) << "stderr was: " << log;
  EXPECT_NE(log.find("foo_int32_to_int64"), std::string::npos) << "stderr was: " << log;

  // Both signatures remain reachable because they differ in return type.
  FunctionSignature as_int32("foo", {arrow::int32()}, arrow::int32());
  FunctionSignature as_int64("foo", {arrow::int32()}, arrow::int64());
  const NativeFunction* fn_int32 = registry->LookupSignature(as_int32);
  const NativeFunction* fn_int64 = registry->LookupSignature(as_int64);
  ASSERT_NE(fn_int32, nullptr);
  ASSERT_NE(fn_int64, nullptr);
  EXPECT_EQ(fn_int32->pc_name(), "foo_int32_to_int32");
  EXPECT_EQ(fn_int64->pc_name(), "foo_int32_to_int64");
}

TEST_F(TestFunctionRegistry, TestAddCallShapeCollisionAcrossDecimalPrecision) {
  // FunctionSignature::operator== treats decimals as equal when byte_width matches,
  // regardless of precision/scale. The alias-collision diagnostic must use the same
  // identity rule, so two registrations differing only in decimal precision/scale
  // should still be flagged.
  auto registry = std::make_unique<FunctionRegistry>();
  auto buffer = arrow::Buffer::FromString("");

  NativeFunction precise("foo", {}, DataTypeVector{arrow::decimal128(10, 2)},
                         arrow::int32(), kResultNullIfNull, "foo_decimal128_precise");
  NativeFunction wide("foo", {}, DataTypeVector{arrow::decimal128(18, 4)}, arrow::int64(),
                      kResultNullIfNull, "foo_decimal128_wide");

  ARROW_EXPECT_OK(registry->Register({precise}, buffer));

  testing::internal::CaptureStderr();
  ARROW_EXPECT_OK(registry->Register({wide}, buffer));
  std::string log = testing::internal::GetCapturedStderr();

  EXPECT_NE(log.find("Function alias collision"), std::string::npos)
      << "stderr was: " << log;
  EXPECT_NE(log.find("foo_decimal128_precise"), std::string::npos)
      << "stderr was: " << log;
  EXPECT_NE(log.find("foo_decimal128_wide"), std::string::npos) << "stderr was: " << log;
}

TEST_F(TestFunctionRegistry, TestFound) {
  FunctionSignature add_i32_i32("add", {arrow::int32(), arrow::int32()}, arrow::int32());

  const NativeFunction* function = registry_->LookupSignature(add_i32_i32);
  EXPECT_NE(function, nullptr);
  EXPECT_THAT(function->signatures(), testing::Contains(add_i32_i32));
  EXPECT_EQ(function->pc_name(), "add_int32_int32");
}

TEST_F(TestFunctionRegistry, TestNotFound) {
  FunctionSignature addX_i32_i32("addX", {arrow::int32(), arrow::int32()},
                                 arrow::int32());
  EXPECT_EQ(registry_->LookupSignature(addX_i32_i32), nullptr);

  FunctionSignature add_i32_i32_ret64("add", {arrow::int32(), arrow::int32()},
                                      arrow::int64());
  EXPECT_EQ(registry_->LookupSignature(add_i32_i32_ret64), nullptr);
}

TEST_F(TestFunctionRegistry, TestCustomFunctionRegistry) {
  auto registry = MakeFunctionRegistryWithExternalFunction();

  auto multiply_by_two_func = GetTestExternalFunction();
  auto multiply_by_two_int32_ret64 = multiply_by_two_func.signatures().front();
  EXPECT_NE(registry->LookupSignature(multiply_by_two_int32_ret64), nullptr);

  FunctionSignature add_i32_i32_ret64("add", {arrow::int32(), arrow::int32()},
                                      arrow::int64());
  EXPECT_EQ(registry->LookupSignature(add_i32_i32_ret64), nullptr);
}

TEST_F(TestFunctionRegistry, TestGetBitcodeMemoryBuffersDefaultFunctionRegistry) {
  EXPECT_EQ(registry_->GetBitcodeBuffers().size(), 0);
}

TEST_F(TestFunctionRegistry, TestGetBitcodeMemoryBuffersCustomFunctionRegistry) {
  auto registry = MakeFunctionRegistryWithExternalFunction();
  EXPECT_EQ(registry->GetBitcodeBuffers().size(), 1);
}

// one nativefunction object per precompiled function
TEST_F(TestFunctionRegistry, TestNoDuplicates) {
  std::unordered_set<std::string> pc_func_sigs;
  std::unordered_set<std::string> native_func_duplicates;
  std::unordered_set<std::string> func_sigs;
  std::unordered_set<std::string> func_sig_duplicates;
  // (name, param-types) -> ret_type seen first; ignores return type to detect
  // call-shape ambiguity, where the same user-facing call could resolve to two
  // functions with different return types.
  std::unordered_map<std::string, std::string> call_shapes;
  std::unordered_set<std::string> call_shape_duplicates;
  for (const auto& native_func_it : *registry_) {
    auto& first_sig = native_func_it.signatures().front();
    auto pc_func_sig = FunctionSignature(native_func_it.pc_name(),
                                         first_sig.param_types(), first_sig.ret_type())
                           .ToString();
    if (pc_func_sigs.count(pc_func_sig) == 0) {
      pc_func_sigs.insert(pc_func_sig);
    } else {
      native_func_duplicates.insert(pc_func_sig);
    }

    for (auto& sig : native_func_it.signatures()) {
      auto sig_str = sig.ToString();
      if (func_sigs.count(sig_str) == 0) {
        func_sigs.insert(sig_str);
      } else {
        func_sig_duplicates.insert(sig_str);
      }

      auto call_shape = sig.CallShape();
      auto ret_str = sig.ret_type()->ToString();
      auto it = call_shapes.find(call_shape);
      if (it == call_shapes.end()) {
        call_shapes.emplace(call_shape, ret_str);
      } else if (it->second != ret_str) {
        call_shape_duplicates.insert(call_shape + " resolves to both " + it->second +
                                     " and " + ret_str);
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

  stream.str("");
  stream.clear();
  std::copy(func_sig_duplicates.begin(), func_sig_duplicates.end(),
            std::ostream_iterator<std::string>(stream, "\n"));
  EXPECT_TRUE(func_sig_duplicates.empty())
      << "The following signatures are defined more than once possibly pointing to "
         "different precompiled functions:\n"
      << stream.str();

  std::ostringstream shape_stream;
  std::copy(call_shape_duplicates.begin(), call_shape_duplicates.end(),
            std::ostream_iterator<std::string>(shape_stream, "\n"));
  EXPECT_TRUE(call_shape_duplicates.empty())
      << "The following calls have the same name and parameter types but different "
         "return types, so callers will get different results depending on the inferred "
         "return type:\n"
      << shape_stream.str();
}
}  // namespace gandiva
