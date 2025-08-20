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

#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "arrow/status.h"

#include "gandiva/arrow.h"
#include "gandiva/func_descriptor.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/literal_holder.h"
#include "gandiva/node_visitor.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// \brief Represents a node in the expression tree. Validity and value are
/// in a joined state.
class GANDIVA_EXPORT Node {
 public:
  explicit Node(DataTypePtr return_type) : return_type_(return_type) {}

  virtual ~Node() = default;

  const DataTypePtr& return_type() const { return return_type_; }

  /// Derived classes should simply invoke the Visit api of the visitor.
  virtual Status Accept(NodeVisitor& visitor) const = 0;

  virtual std::string ToString() const = 0;

  virtual std::string ToCacheKeyString() const = 0;

 protected:
  DataTypePtr return_type_;
};

/// \brief Node in the expression tree, representing a literal.
class GANDIVA_EXPORT LiteralNode : public Node {
 public:
  LiteralNode(DataTypePtr type, const LiteralHolder& holder, bool is_null)
      : Node(type), holder_(holder), is_null_(is_null) {}

  Status Accept(NodeVisitor& visitor) const override { return visitor.Visit(*this); }

  const LiteralHolder& holder() const { return holder_; }

  bool is_null() const { return is_null_; }

  std::string ToString() const override {
    std::stringstream ss;
    if (return_type_ == NULLPTR) {
      ss << "(const untyped) " << gandiva::ToString(holder_);
      return ss.str();
    }

    ss << "(const " << return_type()->ToString() << ") ";
    if (is_null()) {
      ss << std::string("null");
      return ss.str();
    }

    if (return_type()->id() == arrow::Type::STRING ||
        return_type()->id() == arrow::Type::LARGE_STRING) {
      ss << "'" << gandiva::ToString(holder_) << "'";
    } else {
      ss << gandiva::ToString(holder_);
    }
    // The default formatter prints in decimal can cause a loss in precision. so,
    // print in hex. Can't use hexfloat since gcc 4.9 doesn't support it.
    if (return_type()->id() == arrow::Type::DOUBLE) {
      double dvalue = std::get<double>(holder_);
      uint64_t bits;
      memcpy(&bits, &dvalue, sizeof(bits));
      ss << " raw(" << std::hex << bits << ")";
    } else if (return_type()->id() == arrow::Type::FLOAT) {
      float fvalue = std::get<float>(holder_);
      uint32_t bits;
      memcpy(&bits, &fvalue, sizeof(bits));
      ss << " raw(" << std::hex << bits << ")";
    }
    return ss.str();
  }

  std::string ToCacheKeyString() const override { return ToString(); }

 private:
  LiteralHolder holder_;
  bool is_null_;
};

/// \brief Node in the expression tree, representing an arrow field.
class GANDIVA_EXPORT FieldNode : public Node {
 public:
  explicit FieldNode(FieldPtr field) : Node(field->type()), field_(field) {}

  Status Accept(NodeVisitor& visitor) const override { return visitor.Visit(*this); }

  const FieldPtr& field() const { return field_; }

  std::string ToString() const override {
    return "(" + field()->type()->ToString() + ") " + field()->name();
  }

  std::string ToCacheKeyString() const override {
    return "(" + field()->type()->ToString() + ") ";
  }

 private:
  FieldPtr field_;
};

/// \brief Node in the expression tree, representing a function.
class GANDIVA_EXPORT FunctionNode : public Node {
 public:
  FunctionNode(const std::string& name, const NodeVector& children, DataTypePtr retType);

  Status Accept(NodeVisitor& visitor) const override { return visitor.Visit(*this); }

  const FuncDescriptorPtr& descriptor() const { return descriptor_; }
  const NodeVector& children() const { return children_; }

  std::string ToString() const override {
    std::stringstream ss;
    ss << ((return_type() == NULLPTR) ? "untyped"
                                      : descriptor()->return_type()->ToString())
       << " " << descriptor()->name() << "(";
    bool skip_comma = true;
    for (auto& child : children()) {
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

  std::string ToCacheKeyString() const override {
    std::stringstream ss;
    ss << ((return_type() == NULLPTR) ? "untyped"
                                      : descriptor()->return_type()->ToString())
       << " " << descriptor()->name() << "(";
    bool skip_comma = true;
    for (auto& child : children()) {
      if (skip_comma) {
        ss << child->ToCacheKeyString();
        skip_comma = false;
      } else {
        ss << ", " << child->ToCacheKeyString();
      }
    }
    ss << ")";
    return ss.str();
  }

 private:
  FuncDescriptorPtr descriptor_;
  NodeVector children_;
};

inline FunctionNode::FunctionNode(const std::string& name, const NodeVector& children,
                                  DataTypePtr return_type)
    : Node(return_type), children_(children) {
  DataTypeVector param_types;
  for (auto& child : children) {
    param_types.push_back(child->return_type());
  }

  descriptor_ = FuncDescriptorPtr(new FuncDescriptor(name, param_types, return_type));
}

/// \brief Node in the expression tree, representing an if-else expression.
class GANDIVA_EXPORT IfNode : public Node {
 public:
  IfNode(NodePtr condition, NodePtr then_node, NodePtr else_node, DataTypePtr result_type)
      : Node(result_type),
        condition_(condition),
        then_node_(then_node),
        else_node_(else_node) {}

  Status Accept(NodeVisitor& visitor) const override { return visitor.Visit(*this); }

  const NodePtr& condition() const { return condition_; }
  const NodePtr& then_node() const { return then_node_; }
  const NodePtr& else_node() const { return else_node_; }

  std::string ToString() const override {
    std::stringstream ss;
    ss << "if (" << condition()->ToString() << ") { ";
    ss << then_node()->ToString() << " } else { ";
    ss << else_node()->ToString() << " }";
    return ss.str();
  }

  std::string ToCacheKeyString() const override {
    std::stringstream ss;
    ss << "if (" << condition()->ToCacheKeyString() << ") { ";
    ss << then_node()->ToCacheKeyString() << " } else { ";
    ss << else_node()->ToCacheKeyString() << " }";
    return ss.str();
  }

 private:
  NodePtr condition_;
  NodePtr then_node_;
  NodePtr else_node_;
};

/// \brief Node in the expression tree, representing an and/or boolean expression.
class GANDIVA_EXPORT BooleanNode : public Node {
 public:
  enum ExprType : char { AND, OR };

  BooleanNode(ExprType expr_type, const NodeVector& children)
      : Node(arrow::boolean()), expr_type_(expr_type), children_(children) {}

  Status Accept(NodeVisitor& visitor) const override { return visitor.Visit(*this); }

  ExprType expr_type() const { return expr_type_; }

  const NodeVector& children() const { return children_; }

  std::string ToString() const override {
    std::stringstream ss;
    bool first = true;
    for (auto& child : children_) {
      if (!first) {
        if (expr_type() == BooleanNode::AND) {
          ss << " && ";
        } else {
          ss << " || ";
        }
      }
      ss << child->ToString();
      first = false;
    }
    return ss.str();
  }

  std::string ToCacheKeyString() const override {
    std::stringstream ss;
    bool first = true;
    for (auto& child : children_) {
      if (!first) {
        if (expr_type() == BooleanNode::AND) {
          ss << " && ";
        } else {
          ss << " || ";
        }
      }
      ss << child->ToCacheKeyString();
      first = false;
    }
    return ss.str();
  }

 private:
  ExprType expr_type_;
  NodeVector children_;
};

/// \brief Node in expression tree, representing an in expression.
template <typename Type>
class InExpressionNode : public Node {
 public:
  InExpressionNode(NodePtr eval_expr, const std::unordered_set<Type>& values,
                   DataTypePtr type = NULLPTR)
      : Node(arrow::boolean()),
        eval_expr_(std::move(eval_expr)),
        values_(values),
        type_(std::move(type)) {}

  const NodePtr& eval_expr() const { return eval_expr_; }

  const std::unordered_set<Type>& values() const { return values_; }

  const DataTypePtr& type() const { return type_; }

  Status Accept(NodeVisitor& visitor) const override { return visitor.Visit(*this); }

  std::string ToString() const override {
    std::stringstream ss;
    ss << eval_expr_->ToString() << " IN (";
    bool add_comma = false;
    for (auto& value : values_) {
      if (add_comma) {
        ss << ", ";
      }
      // add type in the front to differentiate
      ss << value;
      add_comma = true;
    }
    ss << ")";
    return ss.str();
  }

  std::string ToCacheKeyString() const override {
    std::stringstream ss;
    ss << eval_expr_->ToCacheKeyString() << " IN (";
    bool add_comma = false;
    for (auto& value : values_) {
      if (add_comma) {
        ss << ", ";
      }
      // add type in the front to differentiate
      ss << value;
      add_comma = true;
    }
    ss << ")";
    return ss.str();
  }

 private:
  NodePtr eval_expr_;
  std::unordered_set<Type> values_;
  DataTypePtr type_;
};

template <>
class InExpressionNode<gandiva::DecimalScalar128> : public Node {
 public:
  InExpressionNode(NodePtr eval_expr,
                   std::unordered_set<gandiva::DecimalScalar128>& values,
                   int32_t precision, int32_t scale)
      : Node(arrow::boolean()),
        eval_expr_(std::move(eval_expr)),
        values_(std::move(values)),
        precision_(precision),
        scale_(scale) {}

  int32_t get_precision() const { return precision_; }

  int32_t get_scale() const { return scale_; }

  const NodePtr& eval_expr() const { return eval_expr_; }

  const std::unordered_set<gandiva::DecimalScalar128>& values() const { return values_; }

  Status Accept(NodeVisitor& visitor) const override { return visitor.Visit(*this); }

  std::string ToString() const override {
    std::stringstream ss;
    ss << eval_expr_->ToString() << " IN (";
    bool add_comma = false;
    for (auto& value : values_) {
      if (add_comma) {
        ss << ", ";
      }
      // add type in the front to differentiate
      ss << value;
      add_comma = true;
    }
    ss << ")";
    return ss.str();
  }

  std::string ToCacheKeyString() const override {
    std::stringstream ss;
    ss << eval_expr_->ToCacheKeyString() << " IN (";
    bool add_comma = false;
    for (auto& value : values_) {
      if (add_comma) {
        ss << ", ";
      }
      // add type in the front to differentiate
      ss << value;
      add_comma = true;
    }
    ss << ")";
    return ss.str();
  }

 private:
  NodePtr eval_expr_;
  std::unordered_set<gandiva::DecimalScalar128> values_;
  int32_t precision_, scale_;
};

}  // namespace gandiva
