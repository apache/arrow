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

namespace arrow {

class Status;

namespace compute {

class ExprName;
class ExprVisitor;
class Operation;

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
  /// The value cardinality: one or many. These correspond to the arrow::Scalar
  /// and arrow::Array types
  enum {
    SCALAR,
    ARRAY
  } Rank;

  /// \brief The name of the expression, if any. The default is unnamed
  // virtual const ExprName& name() const;

  std::shared_ptr<TypeClass> type() const { return type_; }

  /// \brief The value cardinality (scalar or array) of the expression
  Rank rank() const { return rank_; }

 protected:
  ValueExpr(const std::shared_ptr<Operation>& op,
            const std::shared_ptr<TypeClass>& type,
            Rank rank);

  /// \brief The semantic data type of the expression
  std::shared_ptr<TypeClass> type_;

  Rank rank_;
};

class ARROW_EXPORT ScalarExpr : public ValueExpr {
 protected:
  ScalarExpr(const std::shared_ptr<Operation>& op,
             const std::shared_ptr<TypeClass>& type);
};

class ARROW_EXPORT IntegerValue : public ValueExpr {};
class ARROW_EXPORT FloatingValue : public ValueExpr {};
class ARROW_EXPORT BinaryValue : public ValueExpr {};
class ARROW_EXPORT Utf8Value : public BinaryValue {};

class Int8Scalar : virtual public ScalarExpr,
                   virtual public IntegerValue {};

class Int16Scalar : virtual public ScalarExpr,
                    virtual public IntegerValue {};

class Int32Scalar : virtual public ScalarExpr,
                    virtual public IntegerValue {};

class Int64Scalar : virtual public ScalarExpr,
                    virtual public IntegerValue {};

class BinaryScalar : virtual public ScalarExpr,
                     virtual public BinaryValue {};

class Utf8Scalar : virtual public ScalarExpr,
                   virtual public Utf8Value {};

class ARROW_EXPORT ArrayExpr : public ValueExpr {
 protected:
  ArrayExpr(const std::shared_ptr<Operation>& op,
            const std::shared_ptr<TypeClass>& type);
};

template <typename T, typename ObjectType>
inline bool IsInstance(const ObjectType* obj) {
  return dynamic_cast<const T*>(obj) != nullptr;
}

template <typename T, typename ObjectType>
inline bool IsInstance(const ObjectType& obj) {
  return dynamic_cast<const T*>(&obj) != nullptr;
}

// auto string_expr = LiteralString("foo");
// auto int32_expr = LiteralInt32(5);

// std::shared_ptr<Expr> expr;
// RETURN_NOT_OK(ops::Cast(expr, "string").ToExpr(&expr));
// RETURN_NOT_OK(ops::Cast(expr, "double").ToExpr(&expr));

// auto op2 = ops::Cast(out_expr, "double");
// std::shared_ptr<Expr> out_expr2;
// RETURN_NOT_OK(op2.ToExpr(&out_expr2));

}  // namespace compute
}  // namespace arrow
