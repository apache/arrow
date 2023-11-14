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

#include "gandiva/tests/test_util.h"

#include <filesystem>

namespace gandiva {
std::shared_ptr<Configuration> TestConfiguration() {
  auto builder = ConfigurationBuilder();
  return builder.DefaultConfiguration();
}

#ifndef GANDIVA_EXTENSION_TEST_DIR
#define GANDIVA_EXTENSION_TEST_DIR "."
#endif

std::string GetTestFunctionLLVMIRPath() {
  std::filesystem::path base(GANDIVA_EXTENSION_TEST_DIR);
  std::filesystem::path ir_file = base / "multiply_by_two.bc";
  return ir_file.string();
}

NativeFunction GetTestExternalFunction() {
  NativeFunction multiply_by_two_func(
      "multiply_by_two", {}, {arrow::int32()}, arrow::int64(),
      ResultNullableType::kResultNullIfNull, "multiply_by_two_int32");
  return multiply_by_two_func;
}

std::shared_ptr<Configuration> TestConfigurationWithFunctionRegistry(
    std::shared_ptr<FunctionRegistry> registry) {
  ARROW_EXPECT_OK(
      registry->Register({GetTestExternalFunction()}, GetTestFunctionLLVMIRPath()));
  auto external_func_config = ConfigurationBuilder().build(std::move(registry));
  return external_func_config;
}
}  // namespace gandiva
