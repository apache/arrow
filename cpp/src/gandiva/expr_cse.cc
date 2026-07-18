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

#include <cstddef>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/util/hash_util.h"
#include "gandiva/condition.h"
#include "gandiva/function_registry.h"
#include "gandiva/function_signature.h"
#include "gandiva/node.h"

namespace gandiva {

namespace {

enum class NodeKind {
  kField,
  kLiteral,
  kFunction,
};

struct NodeKey {
  NodeKind kind;
  std::string name;
  std::string type;
  std::vector<size_t> children;

  bool operator==(const NodeKey& other) const {
    return kind == other.kind && name == other.name && type == other.type &&
           children == other.children;
  }
};

struct NodeKeyHash {
  size_t operator()(const NodeKey& key) const {
    size_t hash = static_cast<size_t>(key.kind);
    arrow::internal::hash_combine(hash, key.name);
    arrow::internal::hash_combine(hash, key.type);
    for (auto child : key.children) {
      arrow::internal::hash_combine(hash, child);
    }
    return hash;
  }
};

struct FoldedNode {
  NodePtr node;
  size_t id;
  bool can_eliminate;
};

struct CanonicalNode {
  NodePtr node;
  size_t id;
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
      return Fresh(nullptr);
    }

    if (auto field_node = std::dynamic_pointer_cast<FieldNode>(node)) {
      return Intern({NodeKind::kField, field_node->field()->ToString(), "", {}}, node);
    }

    if (auto literal_node = std::dynamic_pointer_cast<LiteralNode>(node)) {
      return Intern({NodeKind::kLiteral, literal_node->ToString(), "", {}}, node);
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

    // InExpressionNode stores constants in unordered_sets, so keep it opaque in this
    // conservative pass.
    return Fresh(node);
  }

  FoldedNode FoldFunction(const NodePtr& original, const FunctionNode& function_node) {
    NodeVector children;
    children.reserve(function_node.children().size());

    std::vector<size_t> child_ids;
    child_ids.reserve(function_node.children().size());

    bool children_unchanged = true;
    bool children_can_eliminate = true;
    for (const auto& child : function_node.children()) {
      auto folded = Fold(child);
      children_unchanged = children_unchanged && folded.node == child;
      children_can_eliminate = children_can_eliminate && folded.can_eliminate;
      children.push_back(folded.node);
      child_ids.push_back(folded.id);
    }

    auto desc = function_node.descriptor();
    auto return_type = desc->return_type() == NULLPTR ? std::string("untyped")
                                                      : desc->return_type()->ToString();
    NodeKey key{NodeKind::kFunction, desc->name(), std::move(return_type),
                std::move(child_ids)};
    auto folded_node =
        children_unchanged
            ? original
            : std::make_shared<FunctionNode>(desc->name(), children, desc->return_type());
    bool can_eliminate = children_can_eliminate && IsFunctionSafe(function_node);
    return can_eliminate ? Intern(std::move(key), folded_node) : Fresh(folded_node);
  }

  FoldedNode FoldBoolean(const NodePtr& original, const BooleanNode& boolean_node) {
    NodeVector folded_children;
    folded_children.reserve(boolean_node.children().size());
    bool children_unchanged = true;

    for (const auto& child : boolean_node.children()) {
      auto folded = Fold(child);
      children_unchanged = children_unchanged && folded.node == child;
      folded_children.push_back(std::move(folded.node));
    }

    auto folded_node =
        children_unchanged
            ? original
            : std::make_shared<BooleanNode>(boolean_node.expr_type(), folded_children);
    return Fresh(folded_node);
  }

  FoldedNode FoldIf(const NodePtr& original, const IfNode& if_node) {
    auto condition = Fold(if_node.condition());
    auto then_node = Fold(if_node.then_node());
    auto else_node = Fold(if_node.else_node());

    bool children_unchanged = condition.node == if_node.condition() &&
                              then_node.node == if_node.then_node() &&
                              else_node.node == if_node.else_node();
    auto folded_node = children_unchanged ? original
                                          : std::make_shared<IfNode>(
                                                condition.node, then_node.node,
                                                else_node.node, if_node.return_type());

    return Fresh(folded_node);
  }

  bool IsFunctionSafe(const FunctionNode& node) const {
    auto desc = node.descriptor();
    FunctionSignature signature(desc->name(), desc->params(), desc->return_type());
    const NativeFunction* native_function = registry_.LookupSignature(signature);
    if (native_function == nullptr) {
      return false;
    }
    return CanReuseNativeFunction(registry_, *native_function);
  }

  FoldedNode Intern(NodeKey key, const NodePtr& node) {
    auto it = canonical_nodes_.find(key);
    if (it != canonical_nodes_.end()) {
      return {it->second.node, it->second.id, true};
    }
    auto id = next_id_++;
    canonical_nodes_.emplace(std::move(key), CanonicalNode{node, id});
    return {node, id, true};
  }

  FoldedNode Fresh(const NodePtr& node) { return {node, next_id_++, false}; }

  const FunctionRegistry& registry_;
  std::unordered_map<NodeKey, CanonicalNode, NodeKeyHash> canonical_nodes_;
  size_t next_id_ = 1;
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
