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
#include "gandiva/arrow.h"
#include "gandiva/gandiva_aliases.h"
#include "codegen/node_visitor.h"
#include "codegen/func_descriptor.h"
#include "codegen/literal_holder.h"

namespace gandiva {

/// \brief Represents a node in the expression tree. Validity and value are
/// in a joined state.
class Node {
 public:
  explicit Node(DataTypePtr return_type)
    : return_type_(return_type) { }

  const DataTypePtr &return_type() const { return return_type_; }

  /// Derived classes should simply invoke the Visit api of the visitor.
  virtual void Accept(NodeVisitor &visitor) const = 0;

 protected:
  DataTypePtr return_type_;
};

/// \brief Node in the expression tree, representing a literal.
class LiteralNode : public Node {
 public:
  LiteralNode(DataTypePtr type, const LiteralHolder &holder)
    : Node(type),
      holder_(holder) {}

  void Accept(NodeVisitor &visitor) const override {
    visitor.Visit(*this);
  }

  const LiteralHolder &holder() const { return holder_; }

 private:
  LiteralHolder holder_;
};

/// \brief Node in the expression tree, representing an arrow field.
class FieldNode : public Node {
 public:
  explicit FieldNode(FieldPtr field)
    : Node(field->type()), field_(field) {}

  void Accept(NodeVisitor &visitor) const override {
    visitor.Visit(*this);
  }

  const FieldPtr &field() const { return field_; }

 private:
  FieldPtr field_;
};

/// \brief Node in the expression tree, representing a function.
class FunctionNode : public Node {
 public:
  FunctionNode(FuncDescriptorPtr descriptor,
               const NodeVector &children,
               DataTypePtr retType)
    : Node(retType), descriptor_(descriptor), children_(children) { }

  void Accept(NodeVisitor &visitor) const override {
    visitor.Visit(*this);
  }

  const FuncDescriptorPtr &descriptor() const { return descriptor_; }
  const NodeVector &children() const { return children_; }

  /// Make a function node with params types specified by 'children', and
  /// having return type ret_type.
  static NodePtr MakeFunction(const std::string &name,
                              const NodeVector &children,
                              DataTypePtr return_type);
 private:
  FuncDescriptorPtr descriptor_;
  NodeVector children_;
};

inline
NodePtr FunctionNode::MakeFunction(const std::string &name,
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
  IfNode(NodePtr condition,
         NodePtr then_node,
         NodePtr else_node,
         DataTypePtr result_type)
    : Node(result_type),
      condition_(condition),
      then_node_(then_node),
      else_node_(else_node) {}

  void Accept(NodeVisitor &visitor) const override {
    visitor.Visit(*this);
  }

  const NodePtr &condition() const { return condition_; }
  const NodePtr &then_node() const { return then_node_; }
  const NodePtr &else_node() const { return else_node_; }

 private:
  NodePtr condition_;
  NodePtr then_node_;
  NodePtr else_node_;
};

} // namespace gandiva

#endif // GANDIVA_EXPR_NODE_H
