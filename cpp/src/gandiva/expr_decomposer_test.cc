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
#include "gandiva/function_signature.h"
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

  int idx_a = decomposer.PushThenEntry(node_a);
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

  int idx_a = decomposer.PushThenEntry(node_a);
  EXPECT_EQ(idx_a, 0);
  decomposer.PopThenEntry(node_a);

  decomposer.PushElseEntry(node_a, idx_a);

  {  // start b
    int idx_b = decomposer.PushThenEntry(node_b);
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

  int idx_a = decomposer.PushThenEntry(node_a);
  EXPECT_EQ(idx_a, 0);

  {  // start b
    int idx_b = decomposer.PushThenEntry(node_b);
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

  int idx_a = decomposer.PushThenEntry(node_a);
  EXPECT_EQ(idx_a, 0);

  decomposer.PopThenEntry(node_a);
  decomposer.PushElseEntry(node_a, idx_a);

  bool is_terminal_a = decomposer.PopElseEntry(node_a);
  EXPECT_EQ(is_terminal_a, true);  // there was no nested if.

  // start b
  int idx_b = decomposer.PushThenEntry(node_b);
  EXPECT_EQ(idx_b, 1);  // must not reuse bitmap.
  decomposer.PopThenEntry(node_b);

  decomposer.PushElseEntry(node_b, idx_b);
  bool is_terminal_b = decomposer.PopElseEntry(node_b);
  EXPECT_EQ(is_terminal_b, true);

  EXPECT_EQ(decomposer.if_entries_stack_.empty(), true);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace gandiva
