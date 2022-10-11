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

using MakeFunctionRegistry = std::function<std::unique_ptr<FunctionRegistry>()>;
using GetNumFunctions = std::function<int()>;
using GetFunctionNames = std::function<std::vector<std::string>()>;
using TestRegistryParams =
    std::tuple<MakeFunctionRegistry, GetNumFunctions, GetFunctionNames, std::string>;

struct TestRegistry : public ::testing::TestWithParam<TestRegistryParams> {};

TEST(TestRegistry, CreateBuiltInRegistry) {
  // This does DCHECK_OK internally for now so this will fail in debug builds
  // if there is a problem initializing the global function registry
  FunctionRegistry* registry = GetFunctionRegistry();
  ARROW_UNUSED(registry);
}

TEST_P(TestRegistry, Basics) {
  auto registry_factory = std::get<0>(GetParam());
  auto registry_ = registry_factory();
  auto get_num_funcs = std::get<1>(GetParam());
  int n_funcs = get_num_funcs();
  auto get_func_names = std::get<2>(GetParam());
  std::vector<std::string> func_names = get_func_names();
  ASSERT_EQ(n_funcs, registry_->num_functions());

  std::shared_ptr<Function> func = std::make_shared<ScalarFunction>(
      "f1", Arity::Unary(), /*doc=*/FunctionDoc::Empty());
  ASSERT_OK(registry_->AddFunction(func));
  ASSERT_EQ(n_funcs + 1, registry_->num_functions());

  func = std::make_shared<VectorFunction>("f0", Arity::Binary(),
                                          /*doc=*/FunctionDoc::Empty());
  ASSERT_OK(registry_->AddFunction(func));
  ASSERT_EQ(n_funcs + 2, registry_->num_functions());

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

  std::vector<std::string> expected_names(func_names);
  for (auto name : {"f0", "f1"}) {
    expected_names.push_back(name);
  }
  std::sort(expected_names.begin(), expected_names.end());
  ASSERT_EQ(expected_names, registry_->GetFunctionNames());

  // Aliases
  ASSERT_RAISES(KeyError, registry_->AddAlias("f33", "f3"));
  ASSERT_OK(registry_->AddAlias("f11", "f1"));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<const Function> f2, registry_->GetFunction("f11"));
  ASSERT_EQ(func, f2);
}

// Define a custom print since the default Googletest print trips Valgrind
void PrintTo(const TestRegistryParams& param, std::ostream* os) {
  (*os) << "TestRegistryParams{"
        << "get_num_funcs()=" << std::get<1>(param)() << ", get_func_names()=";
  for (std::string func_name : std::get<2>(param)()) {
    (*os) << func_name;
  }
  (*os) << ", name=" << std::get<3>(param) << "}";
}

INSTANTIATE_TEST_SUITE_P(
    TestRegistry, TestRegistry,
    testing::Values(
        TestRegistryParams{std::make_tuple(
            static_cast<MakeFunctionRegistry>([]() { return FunctionRegistry::Make(); }),
            []() { return 0; }, []() { return std::vector<std::string>{}; }, "default")},
        TestRegistryParams{std::make_tuple(
            static_cast<MakeFunctionRegistry>([]() {
              return FunctionRegistry::Make(GetFunctionRegistry());
            }),
            []() { return GetFunctionRegistry()->num_functions(); },
            []() { return GetFunctionRegistry()->GetFunctionNames(); }, "nested")}));

TEST(TestRegistry, RegisterTempFunctions) {
  auto default_registry = GetFunctionRegistry();
  constexpr int rounds = 3;
  for (int i = 0; i < rounds; i++) {
    auto registry = FunctionRegistry::Make(default_registry);
    for (std::string func_name : {"f1", "f2"}) {
      std::shared_ptr<Function> func = std::make_shared<ScalarFunction>(
          func_name, Arity::Unary(), /*doc=*/FunctionDoc::Empty());
      ASSERT_OK(registry->CanAddFunction(func));
      ASSERT_OK(registry->AddFunction(func));
      ASSERT_RAISES(KeyError, registry->CanAddFunction(func));
      ASSERT_RAISES(KeyError, registry->AddFunction(func));
      ASSERT_OK(default_registry->CanAddFunction(func));
    }
  }
}

TEST(TestRegistry, RegisterTempAliases) {
  auto default_registry = GetFunctionRegistry();
  std::vector<std::string> func_names = default_registry->GetFunctionNames();
  constexpr int rounds = 3;
  for (int i = 0; i < rounds; i++) {
    auto registry = FunctionRegistry::Make(default_registry);
    for (std::string func_name : func_names) {
      std::string alias_name = "alias_of_" + func_name;
      std::shared_ptr<Function> func = std::make_shared<ScalarFunction>(
          func_name, Arity::Unary(), /*doc=*/FunctionDoc::Empty());
      ASSERT_RAISES(KeyError, registry->GetFunction(alias_name));
      ASSERT_OK(registry->CanAddAlias(alias_name, func_name));
      ASSERT_OK(registry->AddAlias(alias_name, func_name));
      ASSERT_OK(registry->GetFunction(alias_name));
      ASSERT_OK(default_registry->GetFunction(func_name));
      ASSERT_RAISES(KeyError, default_registry->GetFunction(alias_name));
    }
  }
}

template <int kExampleSeqNum>
class ExampleOptions : public FunctionOptions {
 public:
  explicit ExampleOptions(std::shared_ptr<Scalar> value);
  std::shared_ptr<Scalar> value;
};

template <int kExampleSeqNum>
class ExampleOptionsType : public FunctionOptionsType {
 public:
  static const FunctionOptionsType* GetInstance() {
    static std::unique_ptr<FunctionOptionsType> instance(
        new ExampleOptionsType<kExampleSeqNum>());
    return instance.get();
  }
  const char* type_name() const override {
    static std::string name = std::string("example") + std::to_string(kExampleSeqNum);
    return name.c_str();
  }
  std::string Stringify(const FunctionOptions& options) const override {
    return type_name();
  }
  bool Compare(const FunctionOptions& options,
               const FunctionOptions& other) const override {
    return true;
  }
  std::unique_ptr<FunctionOptions> Copy(const FunctionOptions& options) const override {
    const auto& opts = static_cast<const ExampleOptions<kExampleSeqNum>&>(options);
    return std::make_unique<ExampleOptions<kExampleSeqNum>>(opts.value);
  }
};
template <int kExampleSeqNum>
ExampleOptions<kExampleSeqNum>::ExampleOptions(std::shared_ptr<Scalar> value)
    : FunctionOptions(ExampleOptionsType<kExampleSeqNum>::GetInstance()),
      value(std::move(value)) {}

TEST(TestRegistry, RegisterTempFunctionOptionsType) {
  auto default_registry = GetFunctionRegistry();
  std::vector<const FunctionOptionsType*> options_types = {
      ExampleOptionsType<1>::GetInstance(),
      ExampleOptionsType<2>::GetInstance(),
  };
  constexpr int rounds = 3;
  for (int i = 0; i < rounds; i++) {
    auto registry = FunctionRegistry::Make(default_registry);
    for (auto options_type : options_types) {
      ASSERT_OK(registry->CanAddFunctionOptionsType(options_type));
      ASSERT_OK(registry->AddFunctionOptionsType(options_type));
      ASSERT_RAISES(KeyError, registry->CanAddFunctionOptionsType(options_type));
      ASSERT_RAISES(KeyError, registry->AddFunctionOptionsType(options_type));
      ASSERT_OK(default_registry->CanAddFunctionOptionsType(options_type));
    }
  }
}

TEST(TestRegistry, RegisterNestedFunctions) {
  auto default_registry = GetFunctionRegistry();
  std::shared_ptr<Function> func1 = std::make_shared<ScalarFunction>(
      "f1", Arity::Unary(), /*doc=*/FunctionDoc::Empty());
  std::shared_ptr<Function> func2 = std::make_shared<ScalarFunction>(
      "f2", Arity::Unary(), /*doc=*/FunctionDoc::Empty());
  constexpr int rounds = 3;
  for (int i = 0; i < rounds; i++) {
    auto registry1 = FunctionRegistry::Make(default_registry);

    ASSERT_OK(registry1->CanAddFunction(func1));
    ASSERT_OK(registry1->AddFunction(func1));

    for (int j = 0; j < rounds; j++) {
      auto registry2 = FunctionRegistry::Make(registry1.get());

      ASSERT_OK(registry2->CanAddFunction(func2));
      ASSERT_OK(registry2->AddFunction(func2));
      ASSERT_RAISES(KeyError, registry2->CanAddFunction(func2));
      ASSERT_RAISES(KeyError, registry2->AddFunction(func2));
      ASSERT_OK(default_registry->CanAddFunction(func2));
    }

    ASSERT_RAISES(KeyError, registry1->CanAddFunction(func1));
    ASSERT_RAISES(KeyError, registry1->AddFunction(func1));
    ASSERT_OK(default_registry->CanAddFunction(func1));
  }
}

}  // namespace compute
}  // namespace arrow
