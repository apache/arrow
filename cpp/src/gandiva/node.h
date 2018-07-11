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

#ifndef GANDIVA_EXPR_NODE_H
#define GANDIVA_EXPR_NODE_H

#include <string>
#include <vector>

#include "codegen/func_descriptor.h"
#include "codegen/literal_holder.h"
#include "codegen/node_visitor.h"
#include "gandiva/arrow.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/status.h"

namespace gandiva {

/// \brief Represents a node in the expression tree. Validity and value are
/// in a joined state.
class Node {
 public:
  explicit Node(DataTypePtr return_type) : return_type_(return_type) {}

  const DataTypePtr &return_type() const { return return_type_; }

  /// Derived classes should simply invoke the Visit api of the visitor.
  virtual Status Accept(NodeVisitor &visitor) const = 0;

  virtual std::string ToString() = 0;

 protected:
  DataTypePtr return_type_;
};

/// \brief Node in the expression tree, representing a literal.
class LiteralNode : public Node {
 public:
  LiteralNode(DataTypePtr type, const LiteralHolder &holder, bool is_null)
      : Node(type), holder_(holder), is_null_(is_null) {}

  Status Accept(NodeVisitor &visitor) const override { return visitor.Visit(*this); }

  const LiteralHolder &holder() const { return holder_; }

  bool is_null() const { return is_null_; }

  std::string ToString() override {
    if (is_null()) {
      return std::string("null");
    }

    std::stringstream ss;
    ss << holder();
    return ss.str();
  }

 private:
  LiteralHolder holder_;
  bool is_null_;
};

/// \brief Node in the expression tree, representing an arrow field.
class FieldNode : public Node {
 public:
  explicit FieldNode(FieldPtr field) : Node(field->type()), field_(field) {}

  Status Accept(NodeVisitor &visitor) const override { return visitor.Visit(*this); }

  const FieldPtr &field() const { return field_; }

  std::string ToString() override { return field()->type()->name(); }

 private:
  FieldPtr field_;
};

/// \brief Node in the expression tree, representing a function.
class FunctionNode : public Node {
 public:
  FunctionNode(FuncDescriptorPtr descriptor, const NodeVector &children,
               DataTypePtr retType)
      : Node(retType), descriptor_(descriptor), children_(children) {}

  Status Accept(NodeVisitor &visitor) const override { return visitor.Visit(*this); }

  const FuncDescriptorPtr &descriptor() const { return descriptor_; }
  const NodeVector &children() const { return children_; }

  std::string ToString() override {
    std::stringstream ss;
    ss << descriptor()->return_type()->name() << " " << descriptor()->name() << "(";
    bool skip_comma = true;
    for (auto child : children()) {
      if (skip_comma) {
        ss << child->ToString();
        skip_comma = false;
      } else {
        ss << ", " << child->ToString();
      }
    }
    ss << ")";
    return ss.str();
  }

  /// Make a function node with params types specified by 'children', and
  /// having return type ret_type.
  static NodePtr MakeFunction(const std::string &name, const NodeVector &children,
                              DataTypePtr return_type);

 private:
  FuncDescriptorPtr descriptor_;
  NodeVector children_;
};

inline NodePtr FunctionNode::MakeFunction(const std::string &name,
                                          const NodeVector &children,
                                          DataTypePtr return_type) {
  DataTypeVector param_types;
  for (auto &child : children) {
    param_types.push_back(child->return_type());
  }

  auto func_desc = FuncDescriptorPtr(new FuncDescriptor(name, param_types, return_type));
  return NodePtr(new FunctionNode(func_desc, children, return_type));
}

/// \brief Node in the expression tree, representing an if-else expression.
class IfNode : public Node {
 public:
  IfNode(NodePtr condition, NodePtr then_node, NodePtr else_node, DataTypePtr result_type)
      : Node(result_type),
        condition_(condition),
        then_node_(then_node),
        else_node_(else_node) {}

  Status Accept(NodeVisitor &visitor) const override { return visitor.Visit(*this); }

  const NodePtr &condition() const { return condition_; }
  const NodePtr &then_node() const { return then_node_; }
  const NodePtr &else_node() const { return else_node_; }

  std::string ToString() override {
    std::stringstream ss;
    ss << "if (" << condition()->ToString() << ") { ";
    ss << then_node()->ToString() << " } else { ";
    ss << else_node()->ToString() << " }";
    return ss.str();
  }

 private:
  NodePtr condition_;
  NodePtr then_node_;
  NodePtr else_node_;
};

/// \brief Node in the expression tree, representing an and/or boolean expression.
class BooleanNode : public Node {
 public:
  enum ExprType : char { AND, OR };

  BooleanNode(ExprType expr_type, const NodeVector &children)
      : Node(arrow::boolean()), expr_type_(expr_type), children_(children) {}

  Status Accept(NodeVisitor &visitor) const override { return visitor.Visit(*this); }

  ExprType expr_type() const { return expr_type_; }

  const NodeVector &children() const { return children_; }

  std::string ToString() override {
    std::stringstream ss;
    ss << children_.at(0)->ToString();
    if (expr_type() == BooleanNode::AND) {
      ss << " && ";
    } else {
      ss << " || ";
    }
    ss << children_.at(1)->ToString();
    return ss.str();
  }

 private:
  ExprType expr_type_;
  NodeVector children_;
};

}  // namespace gandiva

#endif  // GANDIVA_EXPR_NODE_H
