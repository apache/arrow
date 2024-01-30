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

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"

using testing::ElementsAre;
using testing::Eq;
using testing::HasSubstr;
using testing::UnorderedElementsAre;

namespace arrow {

using internal::checked_cast;

namespace engine {

// an extension-id-registry provider to be used as a test parameter
//
// we cannot pass a pointer to a nested registry as a test parameter because the
// shared_ptr in which it is made would not be held and get destructed too early,
// nor can we pass a shared_ptr to the default nested registry as a test parameter
// because it is global and must never be cleaned up, so we pass a shared_ptr to a
// provider that either owns or does not own the registry it provides, depending
// on the case.
struct ExtensionIdRegistryProvider {
  virtual ExtensionIdRegistry* get() const = 0;
};

struct DefaultExtensionIdRegistryProvider : public ExtensionIdRegistryProvider {
  virtual ~DefaultExtensionIdRegistryProvider() {}
  ExtensionIdRegistry* get() const override { return default_extension_id_registry(); }
};

struct NestedExtensionIdRegistryProvider : public ExtensionIdRegistryProvider {
  virtual ~NestedExtensionIdRegistryProvider() {}
  std::shared_ptr<ExtensionIdRegistry> registry_ = MakeExtensionIdRegistry();
  ExtensionIdRegistry* get() const override { return &*registry_; }
};

bool operator==(const Id& id1, const Id& id2) {
  return id1.uri == id2.uri && id1.name == id2.name;
}

bool operator!=(const Id& id1, const Id& id2) { return !(id1 == id2); }

struct TypeName {
  std::shared_ptr<DataType> type;
  std::string_view name;
};

static const std::vector<TypeName> kTypeNames = {
    TypeName{uint8(), "u8"},
    TypeName{uint16(), "u16"},
    TypeName{uint32(), "u32"},
    TypeName{uint64(), "u64"},
    TypeName{float16(), "fp16"},
    TypeName{null(), "null"},
    TypeName{month_interval(), "interval_month"},
    TypeName{day_time_interval(), "interval_day_milli"},
    TypeName{month_day_nano_interval(), "interval_month_day_nano"},
};

static const std::vector<Id> kFunctionIds = {
    {kSubstraitArithmeticFunctionsUri, "add"},
};

static const std::vector<Id> kAggregateIds = {
    {kSubstraitArithmeticFunctionsUri, "sum"},
};

static const std::vector<std::string_view> kTempFunctionNames = {
    "temp_func_1",
    "temp_func_2",
};

static const std::vector<TypeName> kTempTypeNames = {
    TypeName{timestamp(TimeUnit::SECOND, "temp_tz_1"), "temp_type_1"},
    TypeName{timestamp(TimeUnit::SECOND, "temp_tz_2"), "temp_type_2"},
};

static Id kNonExistentId{kArrowExtTypesUri, "non_existent"};
static TypeName kNonExistentTypeName{timestamp(TimeUnit::SECOND, "non_existent_tz_1"),
                                     "non_existent_type_1"};

ExtensionIdRegistry::SubstraitAggregateToArrow MakeSubstraitAggregateToArrow(
    const std::string& name) {
  return [&name](const SubstraitCall& call) -> Result<arrow::compute::Aggregate> {
    return arrow::compute::Aggregate{std::string(call.id().name), std::string(name)};
  };
}

using ExtensionIdRegistryParams =
    std::tuple<std::shared_ptr<ExtensionIdRegistryProvider>, std::string>;

struct ExtensionIdRegistryTest
    : public testing::TestWithParam<ExtensionIdRegistryParams> {};

TEST_P(ExtensionIdRegistryTest, GetTypes) {
  auto provider = std::get<0>(GetParam());
  auto registry = provider->get();

  for (TypeName e : kTypeNames) {
    auto id = Id{kArrowExtTypesUri, e.name};
    for (auto typerec_opt : {registry->GetType(id), registry->GetType(*e.type)}) {
      ASSERT_TRUE(typerec_opt);
      auto typerec = typerec_opt.value();
      ASSERT_EQ(id, typerec.id);
      ASSERT_EQ(*e.type, *typerec.type);
    }
  }
  ASSERT_FALSE(registry->GetType(kNonExistentId));
  ASSERT_FALSE(registry->GetType(*kNonExistentTypeName.type));
}

TEST_P(ExtensionIdRegistryTest, ReregisterTypes) {
  auto provider = std::get<0>(GetParam());
  auto registry = provider->get();

  for (TypeName e : kTypeNames) {
    auto id = Id{kArrowExtTypesUri, e.name};
    ASSERT_RAISES(Invalid, registry->CanRegisterType(id, e.type));
    ASSERT_RAISES(Invalid, registry->RegisterType(id, e.type));
  }
}

TEST_P(ExtensionIdRegistryTest, GetFunctions) {
  auto provider = std::get<0>(GetParam());
  auto registry = provider->get();

  for (Id func_id : kFunctionIds) {
    ASSERT_OK_AND_ASSIGN(ExtensionIdRegistry::SubstraitCallToArrow converter,
                         registry->GetSubstraitCallToArrow(func_id));
    ASSERT_TRUE(converter);
  }
  ASSERT_RAISES(NotImplemented, registry->GetSubstraitCallToArrow(kNonExistentId));
  ASSERT_FALSE(registry->GetType(kNonExistentId));
  ASSERT_FALSE(registry->GetType(*kNonExistentTypeName.type));
}

TEST_P(ExtensionIdRegistryTest, ReregisterFunctions) {
  auto provider = std::get<0>(GetParam());
  auto registry = provider->get();

  for (Id function_id : kFunctionIds) {
    ASSERT_RAISES(Invalid, registry->CanAddSubstraitCallToArrow(function_id));
    ASSERT_RAISES(Invalid, registry->AddSubstraitCallToArrow(
                               function_id, std::string(function_id.name)));
  }
}

TEST_P(ExtensionIdRegistryTest, GetAggregates) {
  auto provider = std::get<0>(GetParam());
  auto registry = provider->get();

  for (Id func_id : kAggregateIds) {
    ASSERT_OK_AND_ASSIGN(auto converter, registry->GetSubstraitAggregateToArrow(func_id));
    ASSERT_TRUE(converter);
  }
  ASSERT_RAISES(NotImplemented, registry->GetSubstraitAggregateToArrow(kNonExistentId));
  ASSERT_FALSE(registry->GetType(kNonExistentId));
  ASSERT_FALSE(registry->GetType(*kNonExistentTypeName.type));
}

TEST_P(ExtensionIdRegistryTest, ReregisterAggregates) {
  auto provider = std::get<0>(GetParam());
  auto registry = provider->get();

  for (Id function_id : kAggregateIds) {
    ASSERT_RAISES(Invalid, registry->CanAddSubstraitAggregateToArrow(function_id));
    ASSERT_RAISES(
        Invalid,
        registry->AddSubstraitAggregateToArrow(
            function_id, MakeSubstraitAggregateToArrow(std::string(function_id.name))));
  }
}

INSTANTIATE_TEST_SUITE_P(
    Substrait, ExtensionIdRegistryTest,
    testing::Values(
        std::make_tuple(std::make_shared<DefaultExtensionIdRegistryProvider>(),
                        "default"),
        std::make_tuple(std::make_shared<NestedExtensionIdRegistryProvider>(),
                        "nested")));

TEST(ExtensionIdRegistryTest, GetSupportedSubstraitFunctions) {
  ExtensionIdRegistry* default_registry = default_extension_id_registry();
  std::vector<std::string> supported_functions =
      default_registry->GetSupportedSubstraitFunctions();
  std::size_t num_functions = supported_functions.size();
  ASSERT_GT(num_functions, 0);

  std::shared_ptr<ExtensionIdRegistry> nested =
      nested_extension_id_registry(default_registry);
  ASSERT_OK(nested->AddSubstraitCallToArrow(kNonExistentId, "some_function"));

  std::size_t num_nested_functions = nested->GetSupportedSubstraitFunctions().size();
  ASSERT_EQ(num_functions + 1, num_nested_functions);
}

TEST(ExtensionIdRegistryTest, RegisterTempTypes) {
  auto default_registry = default_extension_id_registry();
  constexpr int rounds = 3;
  for (int i = 0; i < rounds; i++) {
    auto registry = MakeExtensionIdRegistry();

    for (TypeName e : kTempTypeNames) {
      auto id = Id{kArrowExtTypesUri, e.name};
      ASSERT_OK(registry->CanRegisterType(id, e.type));
      ASSERT_OK(registry->RegisterType(id, e.type));
      ASSERT_RAISES(Invalid, registry->CanRegisterType(id, e.type));
      ASSERT_RAISES(Invalid, registry->RegisterType(id, e.type));
      ASSERT_OK(default_registry->CanRegisterType(id, e.type));
    }
  }
}

TEST(ExtensionIdRegistryTest, RegisterTempFunctions) {
  auto default_registry = default_extension_id_registry();
  constexpr int rounds = 3;
  for (int i = 0; i < rounds; i++) {
    auto registry = MakeExtensionIdRegistry();

    for (std::string_view name : kTempFunctionNames) {
      auto id = Id{kArrowExtTypesUri, name};
      ASSERT_OK(registry->CanAddSubstraitCallToArrow(id));
      ASSERT_OK(registry->AddSubstraitCallToArrow(id, std::string(name)));
      ASSERT_RAISES(Invalid, registry->CanAddSubstraitCallToArrow(id));
      ASSERT_RAISES(Invalid, registry->AddSubstraitCallToArrow(id, std::string(name)));
      ASSERT_OK(default_registry->CanAddSubstraitCallToArrow(id));
    }
  }
}

TEST(ExtensionIdRegistryTest, RegisterTempAggregates) {
  auto default_registry = default_extension_id_registry();
  constexpr int rounds = 3;
  for (int i = 0; i < rounds; i++) {
    auto registry = MakeExtensionIdRegistry();

    for (std::string_view name : kTempFunctionNames) {
      auto id = Id{kArrowExtTypesUri, name};
      auto converter = MakeSubstraitAggregateToArrow(std::string(name));
      ASSERT_OK(registry->CanAddSubstraitAggregateToArrow(id));
      ASSERT_OK(registry->AddSubstraitAggregateToArrow(id, converter));
      ASSERT_RAISES(Invalid, registry->CanAddSubstraitAggregateToArrow(id));
      ASSERT_RAISES(Invalid, registry->AddSubstraitAggregateToArrow(id, converter));
      ASSERT_OK(default_registry->CanAddSubstraitAggregateToArrow(id));
    }
  }
}

TEST(ExtensionIdRegistryTest, RegisterNestedTypes) {
  std::shared_ptr<DataType> type1 = kTempTypeNames[0].type;
  std::shared_ptr<DataType> type2 = kTempTypeNames[1].type;
  auto id1 = Id{kArrowExtTypesUri, kTempTypeNames[0].name};
  auto id2 = Id{kArrowExtTypesUri, kTempTypeNames[1].name};

  auto default_registry = default_extension_id_registry();
  constexpr int rounds = 3;
  for (int i = 0; i < rounds; i++) {
    auto registry1 = nested_extension_id_registry(default_registry);

    ASSERT_OK(registry1->CanRegisterType(id1, type1));
    ASSERT_OK(registry1->RegisterType(id1, type1));

    for (int j = 0; j < rounds; j++) {
      auto registry2 = nested_extension_id_registry(&*registry1);

      ASSERT_OK(registry2->CanRegisterType(id2, type2));
      ASSERT_OK(registry2->RegisterType(id2, type2));
      ASSERT_RAISES(Invalid, registry2->CanRegisterType(id2, type2));
      ASSERT_RAISES(Invalid, registry2->RegisterType(id2, type2));
      ASSERT_OK(default_registry->CanRegisterType(id2, type2));
    }

    ASSERT_RAISES(Invalid, registry1->CanRegisterType(id1, type1));
    ASSERT_RAISES(Invalid, registry1->RegisterType(id1, type1));
    ASSERT_OK(default_registry->CanRegisterType(id1, type1));
  }
}

TEST(ExtensionIdRegistryTest, RegisterNestedFunctions) {
  std::string_view name1 = kTempFunctionNames[0];
  std::string_view name2 = kTempFunctionNames[1];
  auto id1 = Id{kArrowExtTypesUri, name1};
  auto id2 = Id{kArrowExtTypesUri, name2};

  auto default_registry = default_extension_id_registry();
  constexpr int rounds = 3;
  for (int i = 0; i < rounds; i++) {
    auto registry1 = MakeExtensionIdRegistry();

    ASSERT_OK(registry1->CanAddSubstraitCallToArrow(id1));
    ASSERT_OK(registry1->AddSubstraitCallToArrow(id1, std::string(name1)));

    for (int j = 0; j < rounds; j++) {
      auto registry2 = MakeExtensionIdRegistry();

      ASSERT_OK(registry2->CanAddSubstraitCallToArrow(id2));
      ASSERT_OK(registry2->AddSubstraitCallToArrow(id2, std::string(name2)));
      ASSERT_RAISES(Invalid, registry2->CanAddSubstraitCallToArrow(id2));
      ASSERT_RAISES(Invalid, registry2->AddSubstraitCallToArrow(id2, std::string(name2)));
      ASSERT_OK(default_registry->CanAddSubstraitCallToArrow(id2));
    }

    ASSERT_RAISES(Invalid, registry1->CanAddSubstraitCallToArrow(id1));
    ASSERT_RAISES(Invalid, registry1->AddSubstraitCallToArrow(id1, std::string(name1)));
    ASSERT_OK(default_registry->CanAddSubstraitCallToArrow(id1));
  }
}

TEST(ExtensionIdRegistryTest, RegisterNestedAggregates) {
  std::string_view name1 = kTempFunctionNames[0];
  std::string_view name2 = kTempFunctionNames[1];
  auto id1 = Id{kArrowExtTypesUri, name1};
  auto id2 = Id{kArrowExtTypesUri, name2};
  auto converter1 = MakeSubstraitAggregateToArrow(std::string(name1));
  auto converter2 = MakeSubstraitAggregateToArrow(std::string(name2));

  auto default_registry = default_extension_id_registry();
  constexpr int rounds = 3;
  for (int i = 0; i < rounds; i++) {
    auto registry1 = MakeExtensionIdRegistry();

    ASSERT_OK(registry1->CanAddSubstraitAggregateToArrow(id1));
    ASSERT_OK(registry1->AddSubstraitAggregateToArrow(id1, converter1));

    for (int j = 0; j < rounds; j++) {
      auto registry2 = MakeExtensionIdRegistry();

      ASSERT_OK(registry2->CanAddSubstraitAggregateToArrow(id2));
      ASSERT_OK(registry2->AddSubstraitAggregateToArrow(id2, converter2));
      ASSERT_RAISES(Invalid, registry2->CanAddSubstraitAggregateToArrow(id2));
      ASSERT_RAISES(Invalid, registry2->AddSubstraitAggregateToArrow(id2, converter2));
      ASSERT_OK(default_registry->CanAddSubstraitAggregateToArrow(id2));
    }

    ASSERT_RAISES(Invalid, registry1->CanAddSubstraitAggregateToArrow(id1));
    ASSERT_RAISES(Invalid, registry1->AddSubstraitAggregateToArrow(id1, converter1));
    ASSERT_OK(default_registry->CanAddSubstraitAggregateToArrow(id1));
  }
}

TEST(ExtensionIdRegistryTest, GetSimpleExtension) {
  auto default_registry = default_extension_id_registry();
  std::string call_name("does_not_matter");
  SubstraitCall call{Id{kArrowSimpleExtensionFunctionsUri, call_name}, int32(),
                     /*output_nullable=*/false};
  call.SetValueArg(0, arrow::compute::field_ref("anything_goes"));
  ASSERT_OK_AND_ASSIGN(auto func_converter,
                       default_registry->GetSubstraitCallToArrow(call.id()));
  ASSERT_OK_AND_ASSIGN(arrow::compute::Expression func_expr, func_converter(call));
  const arrow::compute::Expression::Call* func_call = func_expr.call();
  ASSERT_TRUE(func_call);
  ASSERT_EQ(call_name, func_call->function_name);
  ASSERT_OK_AND_ASSIGN(auto aggr_converter,
                       default_registry->GetSubstraitAggregateToArrow(call.id()));
  ASSERT_OK_AND_ASSIGN(arrow::compute::Aggregate aggr, aggr_converter(call));
  ASSERT_EQ(call_name, aggr.function);
}

}  // namespace engine
}  // namespace arrow
