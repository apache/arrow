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

#include "gandiva/expr_decomposer.h"

#include <gtest/gtest.h>

#include "gandiva/annotator.h"
#include "gandiva/dex.h"
#include "gandiva/function_registry.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/node.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::int32;

class TestExprDecomposer : public ::testing::Test {
 protected:
  FunctionRegistry registry_;
};

TEST_F(TestExprDecomposer, TestStackSimple) {
  Annotator annotator;
  ExprDecomposer decomposer(registry_, annotator);

  // if (a) _
  // else _
  IfNode node_a(nullptr, nullptr, nullptr, int32());

  decomposer.PushConditionEntry(node_a);
  decomposer.PopConditionEntry(node_a);

  int idx_a = decomposer.PushThenEntry(node_a, false);
  EXPECT_EQ(idx_a, 0);
  decomposer.PopThenEntry(node_a);

  decomposer.PushElseEntry(node_a, idx_a);
  bool is_terminal_a = decomposer.PopElseEntry(node_a);
  EXPECT_EQ(is_terminal_a, true);
  EXPECT_EQ(decomposer.if_entries_stack_.empty(), true);
}

TEST_F(TestExprDecomposer, TestNested) {
  Annotator annotator;
  ExprDecomposer decomposer(registry_, annotator);

  // if (a) _
  // else _
  //   if (b) _
  //   else _
  IfNode node_a(nullptr, nullptr, nullptr, int32());
  IfNode node_b(nullptr, nullptr, nullptr, int32());

  decomposer.PushConditionEntry(node_a);
  decomposer.PopConditionEntry(node_a);

  int idx_a = decomposer.PushThenEntry(node_a, false);
  EXPECT_EQ(idx_a, 0);
  decomposer.PopThenEntry(node_a);

  decomposer.PushElseEntry(node_a, idx_a);

  {  // start b
    decomposer.PushConditionEntry(node_b);
    decomposer.PopConditionEntry(node_b);

    int idx_b = decomposer.PushThenEntry(node_b, true);
    EXPECT_EQ(idx_b, 0);  // must reuse bitmap.
    decomposer.PopThenEntry(node_b);

    decomposer.PushElseEntry(node_b, idx_b);
    bool is_terminal_b = decomposer.PopElseEntry(node_b);
    EXPECT_EQ(is_terminal_b, true);
  }  // end b

  bool is_terminal_a = decomposer.PopElseEntry(node_a);
  EXPECT_EQ(is_terminal_a, false);  // there was a nested if.

  EXPECT_EQ(decomposer.if_entries_stack_.empty(), true);
}

TEST_F(TestExprDecomposer, TestInternalIf) {
  Annotator annotator;
  ExprDecomposer decomposer(registry_, annotator);

  // if (a) _
  //   if (b) _
  //   else _
  // else _
  IfNode node_a(nullptr, nullptr, nullptr, int32());
  IfNode node_b(nullptr, nullptr, nullptr, int32());

  decomposer.PushConditionEntry(node_a);
  decomposer.PopConditionEntry(node_a);

  int idx_a = decomposer.PushThenEntry(node_a, false);
  EXPECT_EQ(idx_a, 0);

  {  // start b
    decomposer.PushConditionEntry(node_b);
    decomposer.PopConditionEntry(node_b);

    int idx_b = decomposer.PushThenEntry(node_b, false);
    EXPECT_EQ(idx_b, 1);  // must not reuse bitmap.
    decomposer.PopThenEntry(node_b);

    decomposer.PushElseEntry(node_b, idx_b);
    bool is_terminal_b = decomposer.PopElseEntry(node_b);
    EXPECT_EQ(is_terminal_b, true);
  }  // end b

  decomposer.PopThenEntry(node_a);
  decomposer.PushElseEntry(node_a, idx_a);

  bool is_terminal_a = decomposer.PopElseEntry(node_a);
  EXPECT_EQ(is_terminal_a, true);  // there was no nested if.

  EXPECT_EQ(decomposer.if_entries_stack_.empty(), true);
}

TEST_F(TestExprDecomposer, TestParallelIf) {
  Annotator annotator;
  ExprDecomposer decomposer(registry_, annotator);

  // if (a) _
  // else _
  // if (b) _
  // else _
  IfNode node_a(nullptr, nullptr, nullptr, int32());
  IfNode node_b(nullptr, nullptr, nullptr, int32());

  decomposer.PushConditionEntry(node_a);
  decomposer.PopConditionEntry(node_a);

  int idx_a = decomposer.PushThenEntry(node_a, false);
  EXPECT_EQ(idx_a, 0);

  decomposer.PopThenEntry(node_a);
  decomposer.PushElseEntry(node_a, idx_a);

  bool is_terminal_a = decomposer.PopElseEntry(node_a);
  EXPECT_EQ(is_terminal_a, true);  // there was no nested if.

  // start b
  decomposer.PushConditionEntry(node_b);
  decomposer.PopConditionEntry(node_b);

  int idx_b = decomposer.PushThenEntry(node_b, false);
  EXPECT_EQ(idx_b, 1);  // must not reuse bitmap.
  decomposer.PopThenEntry(node_b);

  decomposer.PushElseEntry(node_b, idx_b);
  bool is_terminal_b = decomposer.PopElseEntry(node_b);
  EXPECT_EQ(is_terminal_b, true);

  EXPECT_EQ(decomposer.if_entries_stack_.empty(), true);
}

TEST_F(TestExprDecomposer, TestIfInCondition) {
  Annotator annotator;
  ExprDecomposer decomposer(registry_, annotator);

  // if (if _ else _)   : a
  //   -
  // else
  //   if (if _ else _)  : b
  //    -
  //   else
  //    -
  IfNode node_a(nullptr, nullptr, nullptr, int32());
  IfNode node_b(nullptr, nullptr, nullptr, int32());
  IfNode cond_node_a(nullptr, nullptr, nullptr, int32());
  IfNode cond_node_b(nullptr, nullptr, nullptr, int32());

  // start a
  decomposer.PushConditionEntry(node_a);
  {
    // start cond_node_a
    decomposer.PushConditionEntry(cond_node_a);
    decomposer.PopConditionEntry(cond_node_a);

    int idx_cond_a = decomposer.PushThenEntry(cond_node_a, false);
    EXPECT_EQ(idx_cond_a, 0);
    decomposer.PopThenEntry(cond_node_a);

    decomposer.PushElseEntry(cond_node_a, idx_cond_a);
    bool is_terminal = decomposer.PopElseEntry(cond_node_a);
    EXPECT_EQ(is_terminal, true);  // there was no nested if.
  }
  decomposer.PopConditionEntry(node_a);

  int idx_a = decomposer.PushThenEntry(node_a, false);
  EXPECT_EQ(idx_a, 1);  // no re-use
  decomposer.PopThenEntry(node_a);

  decomposer.PushElseEntry(node_a, idx_a);

  {  // start b
    decomposer.PushConditionEntry(node_b);
    {
      // start cond_node_b
      decomposer.PushConditionEntry(cond_node_b);
      decomposer.PopConditionEntry(cond_node_b);

      int idx_cond_b = decomposer.PushThenEntry(cond_node_b, false);
      EXPECT_EQ(idx_cond_b, 2);  // no re-use
      decomposer.PopThenEntry(cond_node_b);

      decomposer.PushElseEntry(cond_node_b, idx_cond_b);
      bool is_terminal = decomposer.PopElseEntry(cond_node_b);
      EXPECT_EQ(is_terminal, true);  // there was no nested if.
    }
    decomposer.PopConditionEntry(node_b);

    int idx_b = decomposer.PushThenEntry(node_b, true);
    EXPECT_EQ(idx_b, 1);  // must reuse bitmap.
    decomposer.PopThenEntry(node_b);

    decomposer.PushElseEntry(node_b, idx_b);
    bool is_terminal = decomposer.PopElseEntry(node_b);
    EXPECT_EQ(is_terminal, true);
  }  // end b

  bool is_terminal_a = decomposer.PopElseEntry(node_a);
  EXPECT_EQ(is_terminal_a, false);  // there was a nested if.

  EXPECT_EQ(decomposer.if_entries_stack_.empty(), true);
}

TEST_F(TestExprDecomposer, TestFunctionBetweenNestedIf) {
  Annotator annotator;
  ExprDecomposer decomposer(registry_, annotator);

  // if (a) _
  // else
  //      function(
  //          if (b) _
  //          else _
  //        )

  IfNode node_a(nullptr, nullptr, nullptr, int32());
  IfNode node_b(nullptr, nullptr, nullptr, int32());

  // start outer if
  decomposer.PushConditionEntry(node_a);
  decomposer.PopConditionEntry(node_a);

  int idx_a = decomposer.PushThenEntry(node_a, false);
  EXPECT_EQ(idx_a, 0);
  decomposer.PopThenEntry(node_a);

  decomposer.PushElseEntry(node_a, idx_a);
  {  // start b
    decomposer.PushConditionEntry(node_b);
    decomposer.PopConditionEntry(node_b);

    int idx_b = decomposer.PushThenEntry(node_b, false);  // not else node of parent if
    EXPECT_EQ(idx_b, 1);                                  // can't reuse bitmap.
    decomposer.PopThenEntry(node_b);

    decomposer.PushElseEntry(node_b, idx_b);
    bool is_terminal_b = decomposer.PopElseEntry(node_b);
    EXPECT_EQ(is_terminal_b, true);
  }
  bool is_terminal_a = decomposer.PopElseEntry(node_a);
  EXPECT_EQ(is_terminal_a, true);  // a else is also terminal

  EXPECT_TRUE(decomposer.if_entries_stack_.empty());
}

TEST_F(TestExprDecomposer, TestComplexIfCondition) {
  Annotator annotator;
  ExprDecomposer decomposer(registry_, annotator);

  // if (if _
  //     else
  //        if _
  //        else _
  //    )
  // then
  //    if _
  //     else
  //        if _
  //        else _
  //
  // else
  //    if _
  //    else
  //        if _
  //        else _

  IfNode node_a(nullptr, nullptr, nullptr, int32());

  IfNode cond_node_a(nullptr, nullptr, nullptr, int32());
  IfNode cond_node_a_inner_if(nullptr, nullptr, nullptr, int32());

  IfNode then_node_a(nullptr, nullptr, nullptr, int32());
  IfNode then_node_a_inner_if(nullptr, nullptr, nullptr, int32());

  IfNode else_node_a(nullptr, nullptr, nullptr, int32());
  IfNode else_node_a_inner_if(nullptr, nullptr, nullptr, int32());

  // start outer if
  decomposer.PushConditionEntry(node_a);
  {
    // start the nested if inside the condition of a
    decomposer.PushConditionEntry(cond_node_a);
    decomposer.PopConditionEntry(cond_node_a);

    int idx_cond_a = decomposer.PushThenEntry(cond_node_a, false);
    EXPECT_EQ(idx_cond_a, 0);
    decomposer.PopThenEntry(cond_node_a);

    decomposer.PushElseEntry(cond_node_a, idx_cond_a);
    {
      decomposer.PushConditionEntry(cond_node_a_inner_if);
      decomposer.PopConditionEntry(cond_node_a_inner_if);

      int idx_cond_a_inner_if = decomposer.PushThenEntry(cond_node_a_inner_if, true);
      EXPECT_EQ(idx_cond_a_inner_if,
                0);  // expect bitmap to be resused since nested if else
      decomposer.PopThenEntry(cond_node_a_inner_if);

      decomposer.PushElseEntry(cond_node_a_inner_if, idx_cond_a_inner_if);
      bool is_terminal = decomposer.PopElseEntry(cond_node_a_inner_if);
      EXPECT_TRUE(is_terminal);
    }
    EXPECT_FALSE(decomposer.PopElseEntry(cond_node_a));
  }
  decomposer.PopConditionEntry(node_a);

  int idx_a = decomposer.PushThenEntry(node_a, false);
  EXPECT_EQ(idx_a, 1);

  {
    // start the nested if inside the then node of a
    decomposer.PushConditionEntry(then_node_a);
    decomposer.PopConditionEntry(then_node_a);

    int idx_then_a = decomposer.PushThenEntry(then_node_a, false);
    EXPECT_EQ(idx_then_a, 2);
    decomposer.PopThenEntry(then_node_a);

    decomposer.PushElseEntry(then_node_a, idx_then_a);
    {
      decomposer.PushConditionEntry(then_node_a_inner_if);
      decomposer.PopConditionEntry(then_node_a_inner_if);

      int idx_then_a_inner_if = decomposer.PushThenEntry(then_node_a_inner_if, true);
      EXPECT_EQ(idx_then_a_inner_if,
                2);  // expect bitmap to be resused since nested if else
      decomposer.PopThenEntry(then_node_a_inner_if);

      decomposer.PushElseEntry(then_node_a_inner_if, idx_then_a_inner_if);
      bool is_terminal = decomposer.PopElseEntry(then_node_a_inner_if);
      EXPECT_TRUE(is_terminal);
    }
    EXPECT_FALSE(decomposer.PopElseEntry(then_node_a));
  }
  decomposer.PopThenEntry(node_a);

  decomposer.PushElseEntry(node_a, idx_a);
  {
    // start the nested if inside the else node of a
    decomposer.PushConditionEntry(else_node_a);
    decomposer.PopConditionEntry(else_node_a);

    int idx_else_a =
        decomposer.PushThenEntry(else_node_a, true);  // else node is another if-node
    EXPECT_EQ(idx_else_a, 1);  // reuse the outer if node bitmap since nested if-else
    decomposer.PopThenEntry(else_node_a);

    decomposer.PushElseEntry(else_node_a, idx_else_a);
    {
      decomposer.PushConditionEntry(else_node_a_inner_if);
      decomposer.PopConditionEntry(else_node_a_inner_if);

      int idx_else_a_inner_if = decomposer.PushThenEntry(else_node_a_inner_if, true);
      EXPECT_EQ(idx_else_a_inner_if,
                1);  // expect bitmap to be resused since nested if else
      decomposer.PopThenEntry(else_node_a_inner_if);

      decomposer.PushElseEntry(else_node_a_inner_if, idx_else_a_inner_if);
      bool is_terminal = decomposer.PopElseEntry(else_node_a_inner_if);
      EXPECT_TRUE(is_terminal);
    }
    EXPECT_FALSE(decomposer.PopElseEntry(else_node_a));
  }
  EXPECT_FALSE(decomposer.PopElseEntry(node_a));
  EXPECT_TRUE(decomposer.if_entries_stack_.empty());
}

}  // namespace gandiva
