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

#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gandiva/condition.h"
#include "gandiva/function_registry.h"
#include "gandiva/function_signature.h"
#include "gandiva/node.h"

namespace gandiva {

namespace {

struct FoldedNode {
  NodePtr node;
  std::string key;
  bool can_eliminate;
};

class CommonSubexpressionFolder {
 public:
  explicit CommonSubexpressionFolder(const FunctionRegistry& registry)
      : registry_(registry) {}

  ExpressionPtr FoldExpression(const ExpressionPtr& expression) {
    auto folded = Fold(expression->root());
    if (folded.node == expression->root()) {
      return expression;
    }
    return std::make_shared<Expression>(folded.node, expression->result());
  }

  ConditionPtr FoldCondition(const ConditionPtr& condition) {
    auto folded = Fold(condition->root());
    if (folded.node == condition->root()) {
      return condition;
    }
    return std::make_shared<Condition>(folded.node);
  }

 private:
  FoldedNode Fold(const NodePtr& node) {
    if (node == nullptr) {
      return {nullptr, "null", false};
    }

    if (auto field_node = std::dynamic_pointer_cast<FieldNode>(node)) {
      auto key = "field:" + field_node->field()->ToString();
      return {Intern(key, node), std::move(key), true};
    }

    if (auto literal_node = std::dynamic_pointer_cast<LiteralNode>(node)) {
      auto key = "literal:" + literal_node->ToString();
      return {Intern(key, node), std::move(key), true};
    }

    if (auto function_node = std::dynamic_pointer_cast<FunctionNode>(node)) {
      return FoldFunction(node, *function_node);
    }

    if (auto boolean_node = std::dynamic_pointer_cast<BooleanNode>(node)) {
      return FoldBoolean(node, *boolean_node);
    }

    if (auto if_node = std::dynamic_pointer_cast<IfNode>(node)) {
      return FoldIf(node, *if_node);
    }

    // InExpressionNode stores constants in unordered_sets, so ToString() is not a
    // stable structural key. Keep it opaque in this conservative pass.
    std::stringstream ss;
    ss << "opaque:" << node.get();
    return {node, ss.str(), false};
  }

  FoldedNode FoldFunction(const NodePtr& original, const FunctionNode& function_node) {
    NodeVector children;
    children.reserve(function_node.children().size());

    std::vector<std::string> child_keys;
    child_keys.reserve(function_node.children().size());

    bool children_unchanged = true;
    bool children_can_eliminate = true;
    for (const auto& child : function_node.children()) {
      auto folded = Fold(child);
      children_unchanged = children_unchanged && folded.node == child;
      children_can_eliminate = children_can_eliminate && folded.can_eliminate;
      children.push_back(folded.node);
      child_keys.push_back(std::move(folded.key));
    }

    auto desc = function_node.descriptor();
    auto return_type = desc->return_type() == NULLPTR ? std::string("untyped")
                                                      : desc->return_type()->ToString();
    auto key = JoinKey("function", desc->name(), return_type, child_keys);
    auto folded_node =
        children_unchanged
            ? original
            : std::make_shared<FunctionNode>(desc->name(), children, desc->return_type());
    bool can_eliminate = children_can_eliminate && IsFunctionSafe(function_node);
    return {can_eliminate ? Intern(key, folded_node) : folded_node, std::move(key),
            can_eliminate};
  }

  FoldedNode FoldBoolean(const NodePtr& original, const BooleanNode& boolean_node) {
    NodeVector folded_children;
    std::vector<std::string> folded_keys;
    std::unordered_set<std::string> seen_eliminable_children;
    bool children_unchanged = true;
    bool children_can_eliminate = true;

    for (const auto& child : boolean_node.children()) {
      auto folded = Fold(child);
      children_unchanged = children_unchanged && folded.node == child;
      AppendBooleanChild(boolean_node.expr_type(), std::move(folded),
                         &seen_eliminable_children, &folded_children, &folded_keys,
                         &children_unchanged, &children_can_eliminate);
    }

    if (boolean_node.children().size() > 1 && folded_children.size() == 1 &&
        children_can_eliminate) {
      return {folded_children[0], folded_keys[0], true};
    }

    auto op = boolean_node.expr_type() == BooleanNode::AND ? "and" : "or";
    auto key = JoinKey("boolean", op, "bool", folded_keys);
    auto folded_node =
        children_unchanged && folded_children.size() == boolean_node.children().size()
            ? original
            : std::make_shared<BooleanNode>(boolean_node.expr_type(), folded_children);
    return {children_can_eliminate ? Intern(key, folded_node) : folded_node,
            std::move(key), children_can_eliminate};
  }

  void AppendBooleanChild(BooleanNode::ExprType expr_type, FoldedNode folded,
                          std::unordered_set<std::string>* seen_eliminable_children,
                          NodeVector* folded_children,
                          std::vector<std::string>* folded_keys, bool* children_unchanged,
                          bool* children_can_eliminate) {
    auto nested_boolean = std::dynamic_pointer_cast<BooleanNode>(folded.node);
    if (nested_boolean != nullptr && nested_boolean->expr_type() == expr_type &&
        folded.can_eliminate) {
      *children_unchanged = false;
      for (const auto& nested_child : nested_boolean->children()) {
        auto nested_folded = Fold(nested_child);
        AppendBooleanChild(expr_type, std::move(nested_folded), seen_eliminable_children,
                           folded_children, folded_keys, children_unchanged,
                           children_can_eliminate);
      }
      return;
    }

    if (folded.can_eliminate) {
      if (!seen_eliminable_children->insert(folded.key).second) {
        *children_unchanged = false;
        return;
      }
    } else {
      *children_can_eliminate = false;
    }

    folded_children->push_back(folded.node);
    folded_keys->push_back(std::move(folded.key));
  }

  FoldedNode FoldIf(const NodePtr& original, const IfNode& if_node) {
    auto condition = Fold(if_node.condition());
    auto then_node = Fold(if_node.then_node());
    auto else_node = Fold(if_node.else_node());

    if (condition.can_eliminate && then_node.can_eliminate && else_node.can_eliminate &&
        then_node.key == else_node.key) {
      return then_node;
    }

    std::vector<std::string> child_keys{condition.key, then_node.key, else_node.key};
    auto key = JoinKey("if", "", if_node.return_type()->ToString(), child_keys);
    bool children_unchanged = condition.node == if_node.condition() &&
                              then_node.node == if_node.then_node() &&
                              else_node.node == if_node.else_node();
    auto folded_node = children_unchanged ? original
                                          : std::make_shared<IfNode>(
                                                condition.node, then_node.node,
                                                else_node.node, if_node.return_type());

    return {folded_node, std::move(key), false};
  }

  bool IsFunctionSafe(const FunctionNode& node) const {
    auto desc = node.descriptor();
    FunctionSignature signature(desc->name(), desc->params(), desc->return_type());
    const NativeFunction* native_function = registry_.LookupSignature(signature);
    if (native_function == nullptr) {
      return false;
    }
    return CanReuseNativeFunction(*native_function);
  }

  NodePtr Intern(const std::string& key, const NodePtr& node) {
    auto it = canonical_nodes_.find(key);
    if (it != canonical_nodes_.end()) {
      return it->second;
    }
    canonical_nodes_.emplace(key, node);
    return node;
  }

  std::string JoinKey(const std::string& kind, const std::string& name,
                      const std::string& return_type,
                      const std::vector<std::string>& child_keys) const {
    std::stringstream ss;
    ss << kind << ":" << name << ":" << return_type << "(";
    bool first = true;
    for (const auto& child_key : child_keys) {
      if (!first) {
        ss << ",";
      }
      ss << child_key.size() << ":" << child_key;
      first = false;
    }
    ss << ")";
    return ss.str();
  }

  const FunctionRegistry& registry_;
  std::unordered_map<std::string, NodePtr> canonical_nodes_;
};

}  // namespace

ExpressionVector FoldCommonSubexpressions(const FunctionRegistry& registry,
                                          const ExpressionVector& expressions) {
  CommonSubexpressionFolder folder(registry);
  ExpressionVector folded_expressions;
  folded_expressions.reserve(expressions.size());
  for (const auto& expression : expressions) {
    folded_expressions.push_back(folder.FoldExpression(expression));
  }
  return folded_expressions;
}

ConditionPtr FoldCommonSubexpressions(const FunctionRegistry& registry,
                                      const ConditionPtr& condition) {
  CommonSubexpressionFolder folder(registry);
  return folder.FoldCondition(condition);
}

}  // namespace gandiva
