// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "gandiva/tree_expr_builder.h"

#include <gtest/gtest.h>
#include "codegen/annotator.h"
#include "codegen/dex.h"
#include "codegen/expr_decomposer.h"
#include "codegen/function_signature.h"
#include "codegen/function_registry.h"
#include "codegen/node.h"
#include "gandiva/gandiva_aliases.h"

namespace gandiva {

using arrow::int32;
using arrow::boolean;

class TestExprTree : public ::testing::Test {
 public:
  void SetUp() {
    i0_ = field("i0", int32());
    i1_ = field("i1", int32());

    b0_ = field("b0", boolean());
  }

 protected:
  FieldPtr i0_; // int32
  FieldPtr i1_; // int32

  FieldPtr b0_; // bool
  FunctionRegistry registry_;
};

TEST_F(TestExprTree, TestField) {
  Annotator annotator;

  auto n0 = TreeExprBuilder::MakeField(i0_);
  EXPECT_EQ(n0->return_type(), int32());

  auto n1 = TreeExprBuilder::MakeField(b0_);
  EXPECT_EQ(n1->return_type(), boolean());

  ExprDecomposer decomposer(registry_, annotator);
  auto pair = decomposer.Decompose(*n1);
  auto value = pair->value_expr();
  auto value_dex = std::dynamic_pointer_cast<VectorReadValueDex>(value);
  EXPECT_EQ(value_dex->FieldType(), boolean());

  EXPECT_EQ(pair->validity_exprs().size(), 1);
  auto validity = pair->validity_exprs().at(0);
  auto validity_dex = std::dynamic_pointer_cast<VectorReadValidityDex>(validity);
  EXPECT_NE(validity_dex->ValidityIdx(), value_dex->DataIdx());
}

TEST_F(TestExprTree, TestBinary) {
  Annotator annotator;

  auto left = TreeExprBuilder::MakeField(i0_);
  auto right = TreeExprBuilder::MakeField(i1_);

  auto n = TreeExprBuilder::MakeFunction("add", {left, right}, int32());
  auto add = std::dynamic_pointer_cast<FunctionNode>(n);

  auto func_desc = add->descriptor();
  FunctionSignature sign(func_desc->name(),
                         func_desc->params(),
                         func_desc->return_type());

  EXPECT_EQ(add->return_type(), int32());
  EXPECT_TRUE(sign == FunctionSignature("add", {int32(), int32()}, int32()));

  ExprDecomposer decomposer(registry_, annotator);
  auto pair = decomposer.Decompose(*n);
  auto value = pair->value_expr();
  auto null_if_null = std::dynamic_pointer_cast<NonNullableFuncDex>(value);

  FunctionSignature signature("add", {int32(), int32()}, int32());
  const NativeFunction *fn = registry_.LookupSignature(signature);
  EXPECT_EQ(null_if_null->native_function(), fn);
}

TEST_F(TestExprTree, TestUnary) {
  Annotator annotator;

  auto arg = TreeExprBuilder::MakeField(i0_);
  auto n = TreeExprBuilder::MakeFunction("isnumeric", {arg}, boolean());

  auto unaryFn = std::dynamic_pointer_cast<FunctionNode>(n);
  auto func_desc = unaryFn->descriptor();
  FunctionSignature sign(func_desc->name(),
                         func_desc->params(),
                         func_desc->return_type());
  EXPECT_EQ(unaryFn->return_type(), boolean());
  EXPECT_TRUE(sign == FunctionSignature("isnumeric", {int32()}, boolean()));

  ExprDecomposer decomposer(registry_, annotator);
  auto pair = decomposer.Decompose(*n);
  auto value = pair->value_expr();
  auto never_null = std::dynamic_pointer_cast<NullableNeverFuncDex>(value);

  FunctionSignature signature("isnumeric", {int32()}, boolean());
  const NativeFunction *fn = registry_.LookupSignature(signature);
  EXPECT_EQ(never_null->native_function(), fn);
}

TEST_F(TestExprTree, TestExpression) {
  Annotator annotator;
  auto left = TreeExprBuilder::MakeField(i0_);
  auto right = TreeExprBuilder::MakeField(i1_);

  auto n = TreeExprBuilder::MakeFunction("add", {left, right}, int32());
  auto e = TreeExprBuilder::MakeExpression(n, field("r", int32()));
  auto root_node = e->root();
  EXPECT_EQ(root_node->return_type(), int32());

  auto add_node = std::dynamic_pointer_cast<FunctionNode>(root_node);
  auto func_desc = add_node->descriptor();
  FunctionSignature sign(func_desc->name(),
                         func_desc->params(),
                         func_desc->return_type());
  EXPECT_TRUE(sign == FunctionSignature("add", {int32(), int32()}, int32()));

  ExprDecomposer decomposer(registry_, annotator);
  auto pair = decomposer.Decompose(*root_node);
  auto value = pair->value_expr();
  auto null_if_null = std::dynamic_pointer_cast<NonNullableFuncDex>(value);

  FunctionSignature signature("add", {int32(), int32()}, int32());
  const NativeFunction *fn = registry_.LookupSignature(signature);
  EXPECT_EQ(null_if_null->native_function(), fn);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

} // namespace gandiva
