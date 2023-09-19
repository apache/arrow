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

#include <filesystem>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "gandiva/function_registry_external.h"

namespace gandiva {
class TestExternalFunctionRegistry : public ::testing::Test {
 public:
  arrow::Result<std::vector<NativeFunction>> GetFuncs(const std::string& registry_dir) {
    std::filesystem::path base(GANDIVA_EXTENSION_TEST_DIR);
    return GetExternalFunctionRegistry((base / registry_dir).string());
  }
};

TEST_F(TestExternalFunctionRegistry, EmptyDir) {
  ASSERT_OK_AND_ASSIGN(auto funcs, GetExternalFunctionRegistry(""));
  ASSERT_TRUE(funcs.empty());
}

TEST_F(TestExternalFunctionRegistry, FunctionWithoutName) {
  ASSERT_RAISES(Invalid, GetFuncs("no_name_func_registry"));
}

TEST_F(TestExternalFunctionRegistry, DirWithJsonRegistry) {
  ASSERT_OK_AND_ASSIGN(auto funcs, GetFuncs("simple_registry"));
  ASSERT_EQ(funcs.size(), 1);
  ASSERT_EQ(funcs[0].result_nullable_type(), ResultNullableType::kResultNullNever);
  ASSERT_EQ(funcs[0].CanReturnErrors(), true);
  ASSERT_EQ(funcs[0].pc_name(), "say_hello_utf8");
}

TEST_F(TestExternalFunctionRegistry, DirWithMultiJsonRegistry) {
  ASSERT_OK_AND_ASSIGN(auto funcs, GetFuncs("multiple_registries"));
  ASSERT_EQ(funcs.size(), 2);
  auto sigs_0 = funcs[0].signatures();
  ASSERT_EQ(sigs_0.size(), 2);
  ASSERT_EQ(sigs_0[0].param_types().size(), 1);
  ASSERT_EQ(sigs_0[0].param_types()[0]->id(), arrow::Type::STRING);
  ASSERT_EQ(sigs_0[0].ret_type()->id(), arrow::Type::INT64);
  ASSERT_EQ(sigs_0[1].param_types().size(), 1);
  ASSERT_EQ(sigs_0[1].param_types()[0]->id(), arrow::Type::STRING);
  ASSERT_EQ(sigs_0[1].ret_type()->id(), arrow::Type::INT64);
  ASSERT_EQ(funcs[0].pc_name(), "say_hello_utf8");
  ASSERT_EQ(funcs[0].result_nullable_type(), ResultNullableType::kResultNullInternal);

  ASSERT_EQ(funcs[1].result_nullable_type(), ResultNullableType::kResultNullIfNull);
  auto sigs_1 = funcs[1].signatures();
  ASSERT_EQ(sigs_1.size(), 2);
  ASSERT_TRUE(sigs_1[0].param_types().empty());
  ASSERT_EQ(sigs_1[0].ret_type()->id(), arrow::Type::STRING);
  ASSERT_EQ(funcs[1].pc_name(), "say_goodbye");
}

TEST_F(TestExternalFunctionRegistry, DirWithMultiFunctionRegistry) {
  ASSERT_OK_AND_ASSIGN(auto funcs, GetFuncs("multiple_functions_registry"));
  ASSERT_EQ(funcs.size(), 2);
  ASSERT_EQ(funcs[0].pc_name(), "say_hello_utf8");
  ASSERT_EQ(funcs[1].pc_name(), "say_goodbye");
}

TEST_F(TestExternalFunctionRegistry, DirWithComplexTypeRegistry) {
  ASSERT_OK_AND_ASSIGN(auto funcs, GetFuncs("complex_type_registry"));
  ASSERT_EQ(funcs.size(), 1);
  ASSERT_EQ(funcs[0].pc_name(), "greet_timestamp_list");
  auto sigs = funcs[0].signatures();
  ASSERT_EQ(sigs.size(), 1);
  ASSERT_EQ(sigs[0].param_types().size(), 2);
  ASSERT_EQ(sigs[0].param_types()[0]->id(), arrow::Type::TIMESTAMP);
  ASSERT_EQ(sigs[0].param_types()[1]->id(), arrow::Type::LIST);
  ASSERT_EQ(sigs[0].param_types()[1]->ToString(), "list<item: int32>");
  ASSERT_EQ(sigs[0].ret_type()->id(), arrow::Type::DECIMAL);
  ASSERT_EQ(sigs[0].ret_type()->ToString(), "decimal128(10, 2)");
}
}  // namespace gandiva
