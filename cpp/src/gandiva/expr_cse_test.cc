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

#include "gandiva/expr_cse.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "arrow/testing/gtest_util.h"
#include "gandiva/function_registry.h"
#include "gandiva/node.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {
namespace {

ExpressionPtr MakeExpression(NodePtr root, const std::string& name = "out") {
  auto return_type = root->return_type();
  return TreeExprBuilder::MakeExpression(std::move(root),
                                         arrow::field(name, return_type));
}

NodePtr MakeBinary(const std::string& name, const FieldPtr& left, const FieldPtr& right,
                   const DataTypePtr& return_type) {
  return TreeExprBuilder::MakeFunction(
      name, {TreeExprBuilder::MakeField(left), TreeExprBuilder::MakeField(right)},
      return_type);
}

void ExpectDuplicateRootsNotFolded(const NodePtr& left, const NodePtr& right) {
  auto registry = default_function_registry();
  auto folded = FoldCommonSubexpressions(
      *registry, {MakeExpression(left, "left"), MakeExpression(right, "right")});
  ASSERT_EQ(2, folded.size());
  EXPECT_NE(folded[0]->root(), folded[1]->root());
}

int32_t VolatileIdentity(int32_t value) { return value; }

TEST(CommonSubexpressionFolderTest, FoldsRepeatedSafeSubtrees) {
  auto left = arrow::field("left", arrow::int32());
  auto right = arrow::field("right", arrow::int32());
  auto root =
      TreeExprBuilder::MakeFunction("multiply",
                                    {MakeBinary("add", left, right, arrow::int32()),
                                     MakeBinary("add", left, right, arrow::int32())},
                                    arrow::int32());

  auto folded =
      FoldCommonSubexpressions(*default_function_registry(), {MakeExpression(root)});
  auto folded_root = std::dynamic_pointer_cast<FunctionNode>(folded[0]->root());
  ASSERT_NE(nullptr, folded_root);
  ASSERT_EQ(2, folded_root->children().size());
  EXPECT_EQ(folded_root->children()[0], folded_root->children()[1]);
}

TEST(CommonSubexpressionFolderTest, FoldsDeepSharedDagOncePerNode) {
  auto value = arrow::field("value", arrow::int32());
  NodePtr root = TreeExprBuilder::MakeField(value);
  constexpr int kDepth = 24;
  for (int depth = 0; depth < kDepth; ++depth) {
    root = TreeExprBuilder::MakeFunction("add", {root, root}, arrow::int32());
  }

  auto folded =
      FoldCommonSubexpressions(*default_function_registry(), {MakeExpression(root)});
  auto current = folded[0]->root();
  for (int depth = 0; depth < kDepth; ++depth) {
    auto function = std::dynamic_pointer_cast<FunctionNode>(current);
    ASSERT_NE(nullptr, function);
    ASSERT_EQ(2, function->children().size());
    EXPECT_EQ(function->children()[0], function->children()[1]);
    current = function->children()[0];
  }
  EXPECT_NE(nullptr, std::dynamic_pointer_cast<FieldNode>(current));
}

TEST(CommonSubexpressionFolderTest, FoldsAcrossMultipleOutputExpressions) {
  auto left = arrow::field("left", arrow::int32());
  auto right = arrow::field("right", arrow::int32());
  auto folded = FoldCommonSubexpressions(
      *default_function_registry(),
      {MakeExpression(MakeBinary("add", left, right, arrow::int32()), "out1"),
       MakeExpression(MakeBinary("add", left, right, arrow::int32()), "out2")});

  ASSERT_EQ(2, folded.size());
  EXPECT_EQ(folded[0]->root(), folded[1]->root());
}

TEST(CommonSubexpressionFolderTest, DoesNotApplyBooleanAlgebra) {
  auto left = arrow::field("left", arrow::boolean());
  auto right = arrow::field("right", arrow::boolean());
  auto make_and = [&] {
    return TreeExprBuilder::MakeAnd(
        {TreeExprBuilder::MakeField(left), TreeExprBuilder::MakeField(right)});
  };
  auto root = TreeExprBuilder::MakeOr({make_and(), make_and()});

  auto folded =
      FoldCommonSubexpressions(*default_function_registry(), {MakeExpression(root)});
  auto folded_or = std::dynamic_pointer_cast<BooleanNode>(folded[0]->root());
  ASSERT_NE(nullptr, folded_or);
  ASSERT_EQ(2, folded_or->children().size());
  EXPECT_NE(folded_or->children()[0], folded_or->children()[1]);

  auto first_and = std::dynamic_pointer_cast<BooleanNode>(folded_or->children()[0]);
  auto second_and = std::dynamic_pointer_cast<BooleanNode>(folded_or->children()[1]);
  ASSERT_NE(nullptr, first_and);
  ASSERT_NE(nullptr, second_and);
  EXPECT_EQ(first_and->children()[0], second_and->children()[0]);
  EXPECT_EQ(first_and->children()[1], second_and->children()[1]);
}

TEST(CommonSubexpressionFolderTest, DoesNotApplyIfAlgebra) {
  auto condition = arrow::field("condition", arrow::boolean());
  auto value = arrow::field("value", arrow::int32());
  auto root = TreeExprBuilder::MakeIf(TreeExprBuilder::MakeField(condition),
                                      TreeExprBuilder::MakeField(value),
                                      TreeExprBuilder::MakeField(value), arrow::int32());

  auto folded =
      FoldCommonSubexpressions(*default_function_registry(), {MakeExpression(root)});
  auto folded_if = std::dynamic_pointer_cast<IfNode>(folded[0]->root());
  ASSERT_NE(nullptr, folded_if);
  EXPECT_EQ(folded_if->then_node(), folded_if->else_node());
}

TEST(CommonSubexpressionFolderTest, DoesNotFoldNullInternalFunctions) {
  auto type = arrow::decimal128(38, 0);
  auto value = arrow::field("value", type);
  auto make_node = [&] {
    return TreeExprBuilder::MakeFunction("castDECIMALNullOnOverflow",
                                         {TreeExprBuilder::MakeField(value)}, type);
  };
  ExpectDuplicateRootsNotFolded(make_node(), make_node());
}

TEST(CommonSubexpressionFolderTest, DoesNotFoldFunctionsNeedingContext) {
  auto value = arrow::field("value", arrow::int32());
  auto make_node = [&] {
    return TreeExprBuilder::MakeFunction("chr", {TreeExprBuilder::MakeField(value)},
                                         arrow::utf8());
  };
  ExpectDuplicateRootsNotFolded(make_node(), make_node());
}

TEST(CommonSubexpressionFolderTest, DoesNotFoldFunctionsNeedingHolder) {
  auto make_node = [] {
    return TreeExprBuilder::MakeFunction("random", {}, arrow::float64());
  };
  ExpectDuplicateRootsNotFolded(make_node(), make_node());
}

TEST(CommonSubexpressionFolderTest, DoesNotFoldFunctionsThatCanReturnErrors) {
  auto left = arrow::field("left", arrow::int32());
  auto right = arrow::field("right", arrow::int32());
  ExpectDuplicateRootsNotFolded(MakeBinary("divide", left, right, arrow::int32()),
                                MakeBinary("divide", left, right, arrow::int32()));
}

TEST(CommonSubexpressionFolderTest, DoesNotFoldSafeParentWithUnsafeChild) {
  auto left = arrow::field("left", arrow::int32());
  auto right = arrow::field("right", arrow::int32());
  auto make_node = [&] {
    auto divide = MakeBinary("divide", left, right, arrow::int32());
    return TreeExprBuilder::MakeFunction(
        "add", {divide, TreeExprBuilder::MakeLiteral(int32_t{1})}, arrow::int32());
  };
  ExpectDuplicateRootsNotFolded(make_node(), make_node());
}

TEST(CommonSubexpressionFolderTest, DoesNotFoldUnknownFunctions) {
  auto value = arrow::field("value", arrow::int32());
  auto make_node = [&] {
    return TreeExprBuilder::MakeFunction(
        "unknown_function", {TreeExprBuilder::MakeField(value)}, arrow::int32());
  };
  ExpectDuplicateRootsNotFolded(make_node(), make_node());
}

TEST(CommonSubexpressionFolderTest, DoesNotFoldRegisteredFunctionsByDefault) {
  auto registry = std::make_shared<FunctionRegistry>();
  NativeFunction function("volatile_identity", {}, {arrow::int32()}, arrow::int32(),
                          kResultNullIfNull, "volatile_identity_int32");
  ASSERT_OK(registry->Register(std::move(function),
                               reinterpret_cast<void*>(&VolatileIdentity)));

  auto value = arrow::field("value", arrow::int32());
  auto make_node = [&] {
    return TreeExprBuilder::MakeFunction(
        "volatile_identity", {TreeExprBuilder::MakeField(value)}, arrow::int32());
  };
  auto folded = FoldCommonSubexpressions(
      *registry,
      {MakeExpression(make_node(), "left"), MakeExpression(make_node(), "right")});

  ASSERT_EQ(2, folded.size());
  EXPECT_NE(folded[0]->root(), folded[1]->root());
}

TEST(CommonSubexpressionFolderTest, KeepsInExpressionsOpaque) {
  auto value = arrow::field("value", arrow::int32());
  auto make_node = [&] {
    return TreeExprBuilder::MakeInExpressionInt32(TreeExprBuilder::MakeField(value),
                                                  std::unordered_set<int32_t>{1, 2, 3});
  };
  ExpectDuplicateRootsNotFolded(make_node(), make_node());
}

TEST(CommonSubexpressionFolderTest, DistinguishesStructuralDifferences) {
  auto left = arrow::field("left", arrow::int32());
  auto right = arrow::field("right", arrow::int32());
  auto registry = default_function_registry();
  auto folded = FoldCommonSubexpressions(
      *registry,
      {MakeExpression(MakeBinary("add", left, right, arrow::int32()), "ordered"),
       MakeExpression(MakeBinary("add", right, left, arrow::int32()), "reversed"),
       MakeExpression(TreeExprBuilder::MakeLiteral(int32_t{1}), "int32_literal"),
       MakeExpression(TreeExprBuilder::MakeLiteral(int32_t{2}), "other_value"),
       MakeExpression(TreeExprBuilder::MakeLiteral(int64_t{1}), "int64_literal")});

  ASSERT_EQ(5, folded.size());
  EXPECT_NE(folded[0]->root(), folded[1]->root());
  EXPECT_NE(folded[2]->root(), folded[3]->root());
  EXPECT_NE(folded[2]->root(), folded[4]->root());
}

}  // namespace
}  // namespace gandiva
