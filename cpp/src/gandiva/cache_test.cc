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

#include "gandiva/cache.h"

#include <cmath>
#include <cstring>
#include <future>
#include <thread>
#include <unordered_set>
#include <utility>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "gandiva/condition.h"
#include "gandiva/expression_cache_key.h"
#include "gandiva/tree_expr_builder.h"

#include <gtest/gtest.h>

namespace gandiva {

class TestCacheKey {
 public:
  explicit TestCacheKey(int value) : value_(value) {}
  std::size_t Hash() const { return value_; }
  bool operator==(const TestCacheKey& other) const { return value_ == other.value_; }

 private:
  int value_;
};

TEST(TestCache, TestGetPut) {
  Cache<TestCacheKey, std::shared_ptr<std::string>> cache(2);
  auto hello = std::make_shared<std::string>("hello");
  cache.PutObjectCode(TestCacheKey(1), hello);
  auto world = std::make_shared<std::string>("world");
  cache.PutObjectCode(TestCacheKey(2), world);
  ASSERT_EQ(cache.GetObjectCode(TestCacheKey(1)), hello);
  ASSERT_EQ(cache.GetObjectCode(TestCacheKey(2)), world);
}

namespace {
constexpr auto cache_capacity_env_var = "GANDIVA_CACHE_SIZE";
constexpr auto default_cache_capacity = 5000;
}  // namespace

TEST(TestCache, TestGetCacheCapacityDefault) {
  ASSERT_EQ(GetCacheCapacity(), default_cache_capacity);
}

TEST(TestCache, TestGetCacheCapacityEnvVar) {
  using ::arrow::EnvVarGuard;

  // Empty.
  {
    EnvVarGuard guard(cache_capacity_env_var, "");
    ASSERT_EQ(internal::GetCacheCapacityFromEnvVar(), default_cache_capacity);
  }

  // Non-number.
  {
    EnvVarGuard guard(cache_capacity_env_var, "invalid");
    ASSERT_EQ(internal::GetCacheCapacityFromEnvVar(), default_cache_capacity);
  }

  // Number with invalid suffix.
  {
    EnvVarGuard guard(cache_capacity_env_var, "42MB");
    ASSERT_EQ(internal::GetCacheCapacityFromEnvVar(), default_cache_capacity);
  }

  // Valid positive number.
  {
    EnvVarGuard guard(cache_capacity_env_var, "42");
    ASSERT_EQ(internal::GetCacheCapacityFromEnvVar(), 42);
  }

  // Int max.
  {
    auto str = std::to_string(std::numeric_limits<int>::max());
    EnvVarGuard guard(cache_capacity_env_var, str.c_str());
    ASSERT_EQ(internal::GetCacheCapacityFromEnvVar(), std::numeric_limits<int>::max());
  }

  // Zero.
  {
    EnvVarGuard guard(cache_capacity_env_var, "0");
    ASSERT_EQ(internal::GetCacheCapacityFromEnvVar(), default_cache_capacity);
  }

  // Negative number.
  {
    EnvVarGuard guard(cache_capacity_env_var, "-1");
    ASSERT_EQ(internal::GetCacheCapacityFromEnvVar(), default_cache_capacity);
  }

  // Over int max.
  {
    auto str = std::to_string(static_cast<int64_t>(std::numeric_limits<int>::max()) + 1);
    EnvVarGuard guard(cache_capacity_env_var, str.c_str());
    ASSERT_EQ(internal::GetCacheCapacityFromEnvVar(), default_cache_capacity);
  }
}

namespace {

ExpressionCacheKey MakeKey(const NodePtr& node, const DataTypePtr& type) {
  return ExpressionCacheKey(
      arrow::schema({}), ConfigurationBuilder::DefaultConfiguration(),
      {TreeExprBuilder::MakeExpression(node, arrow::field("out", type))},
      SelectionVector::MODE_NONE);
}

}  // namespace

TEST(TestExpressionCacheKey, IfLiteralBoundaries) {
  auto truth = TreeExprBuilder::MakeLiteral(true);
  const std::string delimiter = "' } else { (const string) '";
  auto long_then = TreeExprBuilder::MakeStringLiteral("x" + delimiter + "y");
  auto short_then = TreeExprBuilder::MakeStringLiteral("x");
  auto short_else = TreeExprBuilder::MakeStringLiteral("z");
  auto long_else = TreeExprBuilder::MakeStringLiteral("y" + delimiter + "z");
  auto a = TreeExprBuilder::MakeIf(truth, long_then, short_else, arrow::utf8());
  auto b = TreeExprBuilder::MakeIf(truth, short_then, long_else, arrow::utf8());
  ASSERT_EQ(a->ToString(), b->ToString());
  EXPECT_NE(MakeKey(a, arrow::utf8()), MakeKey(b, arrow::utf8()));
  EXPECT_EQ(MakeKey(a, arrow::utf8()), MakeKey(a, arrow::utf8()));
}

TEST(TestExpressionCacheKey, OutputTypeAndAlias) {
  auto schema = arrow::schema({});
  auto config = ConfigurationBuilder::DefaultConfiguration();
  auto node = TreeExprBuilder::MakeLiteral(int32_t{1});
  auto make_key = [&](const FieldPtr& result) {
    return ExpressionCacheKey(schema, config,
                              {TreeExprBuilder::MakeExpression(node, result)},
                              SelectionVector::MODE_NONE);
  };
  auto key = make_key(arrow::field("first", arrow::int32()));
  EXPECT_EQ(key, make_key(arrow::field("second", arrow::int32(), false)));
  EXPECT_NE(key, make_key(arrow::field("first", arrow::int64())));
  auto expression =
      TreeExprBuilder::MakeExpression(node, arrow::field("first", arrow::int32()));
  EXPECT_NE(key, ExpressionCacheKey(schema, config, {expression},
                                    SelectionVector::MODE_UINT16));
  Condition condition(node);
  EXPECT_NE(MakeKey(node, arrow::boolean()),
            ExpressionCacheKey(schema, config, condition));
}

TEST(TestExpressionCacheKey, FloatingPointBitsAndNulls) {
  auto check = [](const auto& first, const auto& second, const DataTypePtr& type) {
    EXPECT_NE(MakeKey(TreeExprBuilder::MakeLiteral(first), type),
              MakeKey(TreeExprBuilder::MakeLiteral(second), type));
  };
  check(0.0f, -0.0f, arrow::float32());
  check(0.0, -0.0, arrow::float64());
  check(1.0f, std::nextafter(1.0f, 2.0f), arrow::float32());
  check(1.0, std::nextafter(1.0, 2.0), arrow::float64());
  uint64_t bits_a = 0x7ff8000000000001ULL;
  uint64_t bits_b = 0x7ff8000000000002ULL;
  double nan_a;
  double nan_b;
  std::memcpy(&nan_a, &bits_a, sizeof(nan_a));
  std::memcpy(&nan_b, &bits_b, sizeof(nan_b));
  check(nan_a, nan_b, arrow::float64());
  EXPECT_NE(MakeKey(TreeExprBuilder::MakeNull(arrow::utf8()), arrow::utf8()),
            MakeKey(TreeExprBuilder::MakeStringLiteral("null"), arrow::utf8()));
  EXPECT_NE(
      MakeKey(TreeExprBuilder::MakeStringLiteral(std::string("a\0b", 3)), arrow::utf8()),
      MakeKey(TreeExprBuilder::MakeStringLiteral("a"), arrow::utf8()));
}

TEST(TestExpressionCacheKey, InSetsAreDelimitedAndOrdered) {
  auto node = TreeExprBuilder::MakeStringLiteral("a");
  auto one = TreeExprBuilder::MakeInExpressionString(node, {"a, b"});
  auto two = TreeExprBuilder::MakeInExpressionString(node, {"a", "b"});
  ASSERT_NE(MakeKey(one, arrow::boolean()), MakeKey(two, arrow::boolean()));
  EXPECT_EQ(MakeKey(two, arrow::boolean()),
            MakeKey(TreeExprBuilder::MakeInExpressionString(node, {"b", "a"}),
                    arrow::boolean()));
  auto float_node = TreeExprBuilder::MakeLiteral(1.0f);
  EXPECT_NE(MakeKey(TreeExprBuilder::MakeInExpressionFloat(float_node, {1.0f}),
                    arrow::boolean()),
            MakeKey(TreeExprBuilder::MakeInExpressionFloat(float_node,
                                                           {std::nextafter(1.0f, 2.0f)}),
                    arrow::boolean()));
  auto double_node = TreeExprBuilder::MakeLiteral(1.0);
  EXPECT_NE(MakeKey(TreeExprBuilder::MakeInExpressionDouble(double_node, {1.0}),
                    arrow::boolean()),
            MakeKey(TreeExprBuilder::MakeInExpressionDouble(double_node,
                                                            {std::nextafter(1.0, 2.0)}),
                    arrow::boolean()));
  auto binary_node = TreeExprBuilder::MakeBinaryLiteral("a");
  EXPECT_NE(MakeKey(TreeExprBuilder::MakeInExpressionBinary(binary_node, {"a"}),
                    arrow::boolean()),
            MakeKey(TreeExprBuilder::MakeInExpressionString(binary_node, {"a"}),
                    arrow::boolean()));
  std::unordered_set<DecimalScalar128> decimals_a = {DecimalScalar128("123", 10, 2)};
  std::unordered_set<DecimalScalar128> decimals_b = {DecimalScalar128("124", 10, 2)};
  auto decimal_node = TreeExprBuilder::MakeDecimalLiteral(DecimalScalar128("123", 10, 2));
  EXPECT_NE(MakeKey(TreeExprBuilder::MakeInExpressionDecimal(decimal_node, decimals_a),
                    arrow::boolean()),
            MakeKey(TreeExprBuilder::MakeInExpressionDecimal(decimal_node, decimals_b),
                    arrow::boolean()));
}

TEST(TestExpressionCacheKey, BooleanStructure) {
  auto a = TreeExprBuilder::MakeField(arrow::field("a", arrow::boolean()));
  auto b = TreeExprBuilder::MakeField(arrow::field("b", arrow::boolean()));
  auto c = TreeExprBuilder::MakeField(arrow::field("c", arrow::boolean()));
  auto first = TreeExprBuilder::MakeAnd({a, TreeExprBuilder::MakeOr({b, c})});
  auto second = TreeExprBuilder::MakeOr({TreeExprBuilder::MakeAnd({a, b}), c});
  ASSERT_EQ(first->ToString(), second->ToString());
  EXPECT_NE(MakeKey(first, arrow::boolean()), MakeKey(second, arrow::boolean()));
}

namespace {

void ExpectAllDistinct(const std::vector<ExpressionCacheKey>& keys) {
  for (size_t i = 0; i < keys.size(); ++i) {
    for (size_t j = i + 1; j < keys.size(); ++j) {
      EXPECT_NE(keys[i], keys[j]) << "keys " << i << " and " << j << " are equal";
    }
  }
}

NodePtr Function(const std::string& name, const NodeVector& children,
                 const DataTypePtr& type) {
  return TreeExprBuilder::MakeFunction(name, children, type);
}

NodePtr Field(const std::string& name, const DataTypePtr& type, bool nullable = true) {
  return TreeExprBuilder::MakeField(arrow::field(name, type, nullable));
}

}  // namespace

TEST(TestExpressionCacheKey, TypeParametersAreKept) {
  // Literals of different types can share a holder, so only the type tells them apart.
  auto int_literal = [](const DataTypePtr& type) -> NodePtr {
    return std::make_shared<LiteralNode>(type, LiteralHolder(int32_t{1}), false);
  };
  auto long_literal = [](const DataTypePtr& type) -> NodePtr {
    return std::make_shared<LiteralNode>(type, LiteralHolder(int64_t{1}), false);
  };
  auto decimal_literal = [](const DataTypePtr& type) -> NodePtr {
    return std::make_shared<LiteralNode>(
        type, LiteralHolder(DecimalScalar128("1", 10, 2)), false);
  };

  std::vector<ExpressionCacheKey> keys;
  for (const auto& type :
       {arrow::int32(), arrow::date32(), arrow::time32(arrow::TimeUnit::SECOND),
        arrow::time32(arrow::TimeUnit::MILLI)}) {
    keys.push_back(MakeKey(int_literal(type), type));
  }
  for (const auto& type :
       {arrow::int64(), arrow::date64(), arrow::timestamp(arrow::TimeUnit::SECOND),
        arrow::timestamp(arrow::TimeUnit::MILLI),
        arrow::timestamp(arrow::TimeUnit::MICRO),
        arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"),
        arrow::timestamp(arrow::TimeUnit::MILLI, "Asia/Tokyo"),
        arrow::time64(arrow::TimeUnit::MICRO), arrow::time64(arrow::TimeUnit::NANO)}) {
    keys.push_back(MakeKey(long_literal(type), type));
  }
  for (const auto& type : {arrow::decimal128(10, 2), arrow::decimal128(10, 3),
                           arrow::decimal128(11, 2), arrow::decimal128(38, 2)}) {
    keys.push_back(MakeKey(decimal_literal(type), type));
  }
  ExpectAllDistinct(keys);

  // The same type built twice is the same type.
  EXPECT_EQ(MakeKey(decimal_literal(arrow::decimal128(10, 2)), arrow::decimal128(10, 2)),
            MakeKey(decimal_literal(arrow::decimal128(10, 2)), arrow::decimal128(10, 2)));
  EXPECT_EQ(MakeKey(long_literal(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
                    arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
            MakeKey(long_literal(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
                    arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")));

  // A decimal output type is part of the key even when the root has the same text.
  auto node = decimal_literal(arrow::decimal128(10, 2));
  EXPECT_NE(MakeKey(node, arrow::decimal128(10, 2)),
            MakeKey(node, arrow::decimal128(10, 3)));
}

TEST(TestExpressionCacheKey, RepeatedNamesAndTypesStayDistinct) {
  // Repeated names and types are written as references. These pairs differ only in
  // where a name or type repeats, or in the order the types first appear.
  auto i32 = arrow::int32();
  auto i64 = arrow::int64();
  std::vector<ExpressionCacheKey> keys;
  auto add = [&](const NodePtr& node) { keys.push_back(MakeKey(node, i64)); };

  add(Function("f", {Field("a", i32), Field("a", i32)}, i64));
  add(Function("f", {Field("a", i32), Field("b", i32)}, i64));
  add(Function("f", {Field("a", i32), Field("a", i64)}, i64));
  add(Function("f", {Field("a", i32), Field("b", i64), Field("c", i32)}, i64));
  add(Function("f", {Field("a", i32), Field("b", i64), Field("c", i64)}, i64));
  add(Function("f", {Field("a", i64), Field("b", i32), Field("c", i64)}, i64));
  add(Function("f", {Field("a", i32), Field("b", i32), Field("c", i64)}, i64));
  // A function name and a field name share one name table.
  add(Function("x", {Field("x", i32)}, i64));
  add(Function("x", {Field("y", i32)}, i64));
  add(Function("y", {Field("x", i32)}, i64));
  add(Function("x", {Function("x", {Field("x", i32)}, i64)}, i64));
  // Nullability and function result types.
  add(Function("f", {Field("a", i32, false)}, i64));
  add(Function("f", {Field("a", i32, true)}, i64));
  add(Function("f", {Field("a", i32, true)}, i32));
  ExpectAllDistinct(keys);

  // Different field objects with the same content give the same key.
  EXPECT_EQ(MakeKey(Function("f", {Field("a", i32), Field("a", i32)}, i64), i64),
            MakeKey(Function("f", {Field("a", i32), Field("a", i32)}, i64), i64));
}

TEST(TestExpressionCacheKey, NullChildNodes) {
  // The builder refuses null children, but some node constructors do not. The key is
  // built before validation, so it must handle them and leave the error to the
  // validator. FunctionNode is not covered: its constructor reads every child.
  auto i32 = arrow::int32();
  auto a = Field("a", i32);
  auto b = Field("b", i32);
  auto boolean = arrow::boolean();
  const NodePtr null_node;
  std::vector<ExpressionCacheKey> keys;
  auto add = [&](const NodePtr& node, const DataTypePtr& type) {
    keys.push_back(MakeKey(node, type));
  };

  add(std::make_shared<IfNode>(null_node, a, b, i32), i32);
  add(std::make_shared<IfNode>(a, null_node, b, i32), i32);
  add(std::make_shared<IfNode>(a, b, null_node, i32), i32);
  add(std::make_shared<IfNode>(a, b, b, i32), i32);
  add(std::make_shared<BooleanNode>(BooleanNode::AND, NodeVector{null_node, a}), boolean);
  add(std::make_shared<BooleanNode>(BooleanNode::AND, NodeVector{a, null_node}), boolean);
  add(std::make_shared<BooleanNode>(BooleanNode::OR, NodeVector{null_node, a}), boolean);
  add(std::make_shared<InExpressionNode<int32_t>>(null_node,
                                                  std::unordered_set<int32_t>{1}),
      boolean);
  add(std::make_shared<InExpressionNode<int32_t>>(null_node,
                                                  std::unordered_set<int32_t>{2}),
      boolean);
  add(std::make_shared<InExpressionNode<int32_t>>(a, std::unordered_set<int32_t>{1}),
      boolean);
  add(std::make_shared<InExpressionNode<int64_t>>(null_node,
                                                  std::unordered_set<int64_t>{1}),
      boolean);
  add(std::make_shared<InExpressionNode<float>>(null_node, std::unordered_set<float>{1}),
      boolean);
  add(std::make_shared<InExpressionNode<double>>(null_node,
                                                 std::unordered_set<double>{1}),
      boolean);
  add(std::make_shared<InExpressionNode<std::string>>(
          null_node, std::unordered_set<std::string>{"a"}),
      boolean);
  std::unordered_set<DecimalScalar128> decimals{DecimalScalar128("1", 10, 2)};
  add(std::make_shared<InExpressionNode<DecimalScalar128>>(null_node, decimals, 10, 2),
      boolean);
  add(null_node, i32);
  ExpectAllDistinct(keys);

  // The same tree built twice gives the same key.
  EXPECT_EQ(MakeKey(std::make_shared<IfNode>(null_node, a, b, i32), i32),
            MakeKey(std::make_shared<IfNode>(null_node, a, b, i32), i32));
  EXPECT_EQ(MakeKey(std::make_shared<InExpressionNode<std::string>>(
                        null_node, std::unordered_set<std::string>{"a"}),
                    boolean),
            MakeKey(std::make_shared<InExpressionNode<std::string>>(
                        null_node, std::unordered_set<std::string>{"a"}),
                    boolean));

  // A null expression and a null result field are also keyed without a crash.
  auto key_of = [](const ExpressionPtr& expression) {
    return ExpressionCacheKey(arrow::schema({}),
                              ConfigurationBuilder::DefaultConfiguration(), {expression},
                              SelectionVector::MODE_NONE);
  };
  auto without_result = key_of(std::make_shared<Expression>(a, nullptr));
  auto without_expression = key_of(nullptr);
  EXPECT_NE(without_result, without_expression);
  EXPECT_NE(without_result, MakeKey(a, i32));
}

TEST(TestExpressionCacheKey, LikeThreadUniquifier) {
  auto literal = TreeExprBuilder::MakeStringLiteral("text like(");
  auto like = TreeExprBuilder::MakeFunction(
      "like", {literal, TreeExprBuilder::MakeStringLiteral("%")}, arrow::boolean());
  auto nested = TreeExprBuilder::MakeIf(like, literal, literal, arrow::utf8());
  using ThreadKeys = std::pair<size_t, std::pair<ExpressionCacheKey, ExpressionCacheKey>>;
  auto make_keys = [&]() -> ThreadKeys {
    return {std::hash<std::thread::id>()(std::this_thread::get_id()) % 16,
            {MakeKey(nested, arrow::utf8()), MakeKey(literal, arrow::utf8())}};
  };
  auto main_keys = make_keys();
  for (int i = 0; i < 32; ++i) {
    auto other = std::async(std::launch::async, make_keys).get();
    EXPECT_EQ(main_keys.first == other.first,
              main_keys.second.first == other.second.first);
    EXPECT_EQ(main_keys.second.second, other.second.second);
  }
}

}  // namespace gandiva
