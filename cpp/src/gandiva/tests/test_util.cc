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

#include <memory>

#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "gandiva/function_holder.h"

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

static NativeFunction GetTestExternalStubFunction() {
  NativeFunction multiply_by_three_func(
      "multiply_by_three", {}, {arrow::int32()}, arrow::int64(),
      ResultNullableType::kResultNullIfNull, "multiply_by_three_int32");
  return multiply_by_three_func;
}

static NativeFunction GetTestFunctionWithFunctionHolder() {
  // the 2nd parameter is expected to be an int32 literal
  NativeFunction multiply_by_n_func("multiply_by_n", {}, {arrow::int32(), arrow::int32()},
                                    arrow::int64(), ResultNullableType::kResultNullIfNull,
                                    "multiply_by_n_int32_int32",
                                    NativeFunction::kNeedsFunctionHolder);
  return multiply_by_n_func;
}

std::shared_ptr<Configuration> TestConfigurationWithFunctionRegistry(
    std::shared_ptr<FunctionRegistry> registry) {
  ARROW_EXPECT_OK(
      registry->Register({GetTestExternalFunction()}, GetTestFunctionLLVMIRPath()));
  auto external_func_config = ConfigurationBuilder().build(std::move(registry));
  return external_func_config;
}

class MultiplyHolder : public FunctionHolder {
 public:
  MultiplyHolder(int32_t num) : num_(num){};

  static Status Make(const FunctionNode& node, std::shared_ptr<MultiplyHolder>* holder) {
    ARROW_RETURN_IF(node.children().size() != 2,
                    Status::Invalid("'multiply_by_n' function requires two parameters"));

    auto literal = dynamic_cast<LiteralNode*>(node.children().at(1).get());
    ARROW_RETURN_IF(
        literal == nullptr,
        Status::Invalid(
            "'multiply_by_n' function requires a literal as the 2nd parameter"));

    auto literal_type = literal->return_type()->id();
    ARROW_RETURN_IF(
        literal_type != arrow::Type::INT32,
        Status::Invalid(
            "'multiply_by_n' function requires an int32 literal as the 2nd parameter"));

    *holder = std::make_shared<MultiplyHolder>(
        literal->is_null() ? 0 : std::get<int32_t>(literal->holder()));
    return Status::OK();
  }

  int32_t operator()() { return num_; }

 private:
  int32_t num_;
};

extern "C" {
// this function is used as an external stub function for testing so it has to be declared
// with extern C
static int64_t multiply_by_three(int32_t value) { return value * 3; }

// this function requires a function holder
static int64_t multiply_by_n(int64_t holder_ptr, int32_t value) {
  MultiplyHolder* holder = reinterpret_cast<MultiplyHolder*>(holder_ptr);
  return value * (*holder)();
}
}

std::shared_ptr<Configuration> TestConfigurationWithExternalStubFunctionRegistry(
    std::shared_ptr<FunctionRegistry> registry) {
  ARROW_EXPECT_OK(registry->Register(GetTestExternalStubFunction(),
                                     reinterpret_cast<void*>(multiply_by_three)));
  auto external_func_config = ConfigurationBuilder().build(std::move(registry));
  return external_func_config;
}

std::shared_ptr<Configuration> TestConfigurationWithFunctionHolderRegistry(
    std::shared_ptr<FunctionRegistry> registry) {
  ARROW_EXPECT_OK(registry->Register(
      GetTestFunctionWithFunctionHolder(), reinterpret_cast<void*>(multiply_by_n),
      [](const FunctionNode& node) -> arrow::Result<FunctionHolderPtr> {
        std::shared_ptr<MultiplyHolder> derived_instance;
        ARROW_RETURN_NOT_OK(MultiplyHolder::Make(node, &derived_instance));
        return derived_instance;
      }));
  auto external_func_config = ConfigurationBuilder().build(std::move(registry));
  return external_func_config;
}
}  // namespace gandiva
