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
#include <utility>

#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "gandiva/function_holder.h"
#include "gandiva/gdv_function_stubs.h"

namespace gandiva {
std::shared_ptr<Configuration> TestConfiguration() {
  return ConfigurationBuilder::DefaultConfiguration();
}

std::shared_ptr<Configuration> TestConfigWithIrDumping() {
  return ConfigurationBuilder().build_with_ir_dumping(true);
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

static NativeFunction GetTestExternalCFunction() {
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

static NativeFunction GetTestFunctionWithContext() {
  NativeFunction multiply_by_two_formula(
      "multiply_by_two_formula", {}, {arrow::utf8()}, arrow::utf8(),
      ResultNullableType::kResultNullIfNull, "multiply_by_two_formula_utf8",
      NativeFunction::kNeedsContext);
  return multiply_by_two_formula;
}

static std::shared_ptr<Configuration> BuildConfigurationWithRegistry(
    std::shared_ptr<FunctionRegistry> registry,
    const std::function<arrow::Status(std::shared_ptr<FunctionRegistry>)>&
        register_func) {
  ARROW_EXPECT_OK(register_func(registry));
  return ConfigurationBuilder().build(std::move(registry));
}

std::shared_ptr<Configuration> TestConfigWithFunctionRegistry(
    std::shared_ptr<FunctionRegistry> registry) {
  return BuildConfigurationWithRegistry(std::move(registry), [](auto reg) {
    return reg->Register({GetTestExternalFunction()}, GetTestFunctionLLVMIRPath());
  });
}

class MultiplyHolder : public FunctionHolder {
 public:
  explicit MultiplyHolder(int32_t num) : num_(num) {}

  static arrow::Result<std::shared_ptr<MultiplyHolder>> Make(const FunctionNode& node) {
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

    return std::make_shared<MultiplyHolder>(
        literal->is_null() ? 0 : std::get<int32_t>(literal->holder()));
  }

  int32_t operator()() const { return num_; }

 private:
  int32_t num_;
};

extern "C" {
// this function is used as an external C function for testing so it has to be declared
// with extern C
static int64_t multiply_by_three(int32_t value) { return value * 3; }

// this function requires a function holder
static int64_t multiply_by_n(int64_t holder_ptr, int32_t value) {
  auto* holder = reinterpret_cast<MultiplyHolder*>(holder_ptr);
  return value * (*holder)();
}

// given a number string, return a string "{number}x2"
static const char* multiply_by_two_formula(int64_t ctx, const char* value,
                                           int32_t value_len, int32_t* out_len) {
  auto result = std::string(value, value_len) + "x2";
  *out_len = static_cast<int32_t>(result.length());
  auto out = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(ctx, *out_len));
  if (out == nullptr) {
    gdv_fn_context_set_error_msg(ctx, "Could not allocate memory for output string");
    *out_len = 0;
    return "";
  }
  memcpy(out, result.c_str(), *out_len);
  return out;
}
}

std::shared_ptr<Configuration> TestConfigWithCFunction(
    std::shared_ptr<FunctionRegistry> registry) {
  return BuildConfigurationWithRegistry(std::move(registry), [](auto reg) {
    return reg->Register(GetTestExternalCFunction(),
                         reinterpret_cast<void*>(multiply_by_three));
  });
}

std::shared_ptr<Configuration> TestConfigWithHolderFunction(
    std::shared_ptr<FunctionRegistry> registry) {
  return BuildConfigurationWithRegistry(std::move(registry), [](auto reg) {
    return reg->Register(
        GetTestFunctionWithFunctionHolder(), reinterpret_cast<void*>(multiply_by_n),
        [](const FunctionNode& node) { return MultiplyHolder::Make(node); });
  });
}

std::shared_ptr<Configuration> TestConfigWithContextFunction(
    std::shared_ptr<FunctionRegistry> registry) {
  return BuildConfigurationWithRegistry(std::move(registry), [](auto reg) {
    return reg->Register(GetTestFunctionWithContext(),
                         reinterpret_cast<void*>(multiply_by_two_formula));
  });
}
}  // namespace gandiva
