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

#include "arrow/util/visibility.h"

#include "arrow/compute/expr/semantic_type.h"

namespace arrow {

class Status;

namespace compute {

class ExprName;
class ExprVisitor;
class Operation;
class SemType;

/// \brief Base class for all analytic expressions. Expressions may represent
/// data values (scalars, arrays, tables)
class ARROW_EXPORT Expr {
 public:
  virtual ~Expr() = default;

  /// \brief A unique string identifier for the kind of expression
  virtual const std::string& kind() const = 0;

  /// \brief Accept expression visitor
  virtual Status Accept(ExprVisitor* visitor) const = 0;

  /// \brief
  std::shared_ptr<Operation> op() const { return op_; }

 protected:
  /// \brief Instantiate expression from an abstract operation
  /// \param[in] op the operation that generates the expression
  explicit Expr(const std::shared_ptr<Operation>& op);
  std::shared_ptr<Operation> op_;
};

/// \brief Base class for a data-generated expression with a fixed and known
/// type. This includes arrays and scalars
class ARROW_EXPORT ValueExpr : public Expr {
 public:
  enum {
    SCALAR,
    ARRAY
  } Arity;

  /// \brief The name of the expression, if any. The default is unnamed
  virtual const ExprName& name() const;

  const ValueType& type() const { return type_; }

  Arity arity() const { return arity_; }

 protected:
  ValueExpr(const std::shared_ptr<Operation>& op, const ValueType& type);

  /// \brief The semantic data type of the expression
  ValueType type_;

  Arity arity_;

  ExprName name_;
};

const ValueExpr& expr = ...;

auto expr1 = ...;

op = ops.Cast(expr1, "string")
  expr = op.to_expr();

class Operation {
 public:
  /// \brief Type-check inputs and resolve well-typed output expression
  virtual Status ToExpr() const = 0;
};

auto op = ops::Cast(expr1, "string");
std::shared_ptr<Expr> out_expr;
RETURN_NOT_OK(op.ToExpr(&out_expr));

class InvalidExpr : public Expr {
 public:
  InvalidExpr(const std::shared_ptr<Operator>& parent_op,
              const std::string& explanation);

  Status Validate() const override {
    return Status::Invalid(explanation_);
  }
};

class OperatorArguments {

};

class ArgumentRules {
 public:
  ArgumentRules(const std::vector<ArgumentRule>& rules);
};

auto expr = ops::Cast(expr, "string").ToExpr();

ops::AnotherOp(expr)

Status s = expr->Validate();
if (!s.ok()) {
  expr->op()->Print();
  return s;
}

auto string_expr = LiteralString("foo");
auto int32_expr = LiteralInt32(5);

std::shared_ptr<Expr> expr;
RETURN_NOT_OK(ops::Cast(expr, "string").ToExpr(&expr));
RETURN_NOT_OK(ops::Cast(expr, "double").ToExpr(&expr));

auto op2 = ops::Cast(out_expr, "double");
std::shared_ptr<Expr> out_expr2;
RETURN_NOT_OK(op2.ToExpr(&out_expr2));

if (Is<AnyScalar>(expr)) {

}

expr.type().id() expr.arity() == ValueExpr::SCALAR


isinstance(expr, AnyValue)
isinstance(expr, AnyScalar)
isinstance(expr, IntegerValue)
isinstance(expr, IntegerArray)
isinstance(expr, IntegerScalar)

if (expr.type().id() == ::arrow::Type::NA) {

}

class ARROW_EXPORT ScalarExpr : public ValueExpr {};
class ARROW_EXPORT ArrayExpr : public ValueExpr {};



}  // namespace compute
}  // namespace arrow
