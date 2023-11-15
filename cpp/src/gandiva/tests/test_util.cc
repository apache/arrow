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

#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

namespace gandiva {
std::shared_ptr<Configuration> TestConfiguration() {
  return ConfigurationBuilder::DefaultConfiguration();
}

#ifndef GANDIVA_EXTENSION_TEST_DIR
#define GANDIVA_EXTENSION_TEST_DIR "."
#endif

std::string GetTestFunctionLLVMIRPath() {
  const auto base =
      arrow::internal::PlatformFilename::FromString(GANDIVA_EXTENSION_TEST_DIR);
  DCHECK_OK(base.status());
  return base->Join("multiply_by_two.bc")->ToString();
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
