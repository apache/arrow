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

#pragma once

#include <cmath>
#include <memory>
#include <stack>
#include <string>
#include <unordered_set>
#include <utility>

#include "gandiva/arrow.h"
#include "gandiva/expression.h"
#include "gandiva/node.h"
#include "gandiva/node_visitor.h"
#include "gandiva/visibility.h"

namespace gandiva {

class FunctionRegistry;
class Annotator;

/// \brief Decomposes an expression tree to separate out the validity and
/// value expressions.
class GANDIVA_EXPORT ExprDecomposer : public NodeVisitor {
 public:
  explicit ExprDecomposer(const FunctionRegistry& registry, Annotator& annotator)
      : registry_(registry), annotator_(annotator), nested_if_else_(false) {}

  Status Decompose(const Node& root, ValueValidityPairPtr* out) {
    auto status = root.Accept(*this);
    if (status.ok()) {
      *out = std::move(result_);
    }
    return status;
  }

  [[nodiscard]] const std::unordered_set<std::string>& UsedFunctions() const {
    return used_functions_;
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ExprDecomposer);

  FRIEND_TEST(TestExprDecomposer, TestStackSimple);
  FRIEND_TEST(TestExprDecomposer, TestNested);
  FRIEND_TEST(TestExprDecomposer, TestInternalIf);
  FRIEND_TEST(TestExprDecomposer, TestParallelIf);
  FRIEND_TEST(TestExprDecomposer, TestIfInCondition);
  FRIEND_TEST(TestExprDecomposer, TestFunctionBetweenNestedIf);
  FRIEND_TEST(TestExprDecomposer, TestComplexIfCondition);

  Status Visit(const FieldNode& node) override;
  Status Visit(const FunctionNode& node) override;
  Status Visit(const IfNode& node) override;
  Status Visit(const LiteralNode& node) override;
  Status Visit(const BooleanNode& node) override;
  Status Visit(const InExpressionNode<int32_t>& node) override;
  Status Visit(const InExpressionNode<int64_t>& node) override;
  Status Visit(const InExpressionNode<float>& node) override;
  Status Visit(const InExpressionNode<double>& node) override;
  Status Visit(const InExpressionNode<gandiva::DecimalScalar128>& node) override;
  Status Visit(const InExpressionNode<std::string>& node) override;

  template <typename ctype>
  Status VisitInGeneric(const InExpressionNode<ctype>& node);

  // Optimize a function node, if possible.
  const FunctionNode TryOptimize(const FunctionNode& node);

  enum StackEntryType { kStackEntryCondition, kStackEntryThen, kStackEntryElse };

  // stack of if nodes.
  class IfStackEntry {
   public:
    IfStackEntry(const IfNode& if_node, StackEntryType entry_type,
                 bool is_terminal_else = false, int local_bitmap_idx = 0)
        : if_node_(if_node),
          entry_type_(entry_type),
          is_terminal_else_(is_terminal_else),
          local_bitmap_idx_(local_bitmap_idx) {}

    const IfNode& if_node_;
    StackEntryType entry_type_;
    bool is_terminal_else_;
    int local_bitmap_idx_;

   private:
    ARROW_DISALLOW_COPY_AND_ASSIGN(IfStackEntry);
  };

  // pop 'condition entry' into stack.
  void PushConditionEntry(const IfNode& node);

  // pop 'condition entry' from stack.
  void PopConditionEntry(const IfNode& node);

  // push 'then entry' to stack. returns either a new local bitmap or the parent's
  // bitmap (in case of nested if-else).
  int PushThenEntry(const IfNode& node, bool reuse_bitmap);

  // pop 'then entry' from stack.
  void PopThenEntry(const IfNode& node);

  // push 'else entry' into stack.
  void PushElseEntry(const IfNode& node, int local_bitmap_idx);

  // pop 'else entry' from stack. returns 'true' if this is a terminal else condition
  // i.e no nested if condition below this node.
  bool PopElseEntry(const IfNode& node);

  ValueValidityPairPtr result() { return std::move(result_); }

  const FunctionRegistry& registry_;
  Annotator& annotator_;
  std::stack<std::unique_ptr<IfStackEntry>> if_entries_stack_;
  ValueValidityPairPtr result_;
  std::unordered_set<std::string> used_functions_;
  bool nested_if_else_;
};

}  // namespace gandiva
