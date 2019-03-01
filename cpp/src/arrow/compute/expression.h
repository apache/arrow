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

#include <memory>
#include <string>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Status;

namespace compute {

class LogicalType;
class ExprVisitor;
class Operation;

/// \brief Base class for all analytic expressions. Expressions may represent
/// data values (scalars, arrays, tables)
class ARROW_EXPORT Expr {
 public:
  virtual ~Expr() = default;

  /// \brief A unique string identifier for the kind of expression
  virtual std::string kind() const = 0;

  /// \brief Accept expression visitor
  /// TODO(wesm)
  // virtual Status Accept(ExprVisitor* visitor) const = 0;

  /// \brief
  std::shared_ptr<Operation> op() const { return op_; }

 protected:
  /// \brief Instantiate expression from an abstract operation
  /// \param[in] op the operation that generates the expression
  explicit Expr(std::shared_ptr<Operation> op);
  std::shared_ptr<Operation> op_;
};

/// \brief Base class for a data-generated expression with a fixed and known
/// type. This includes arrays and scalars
class ARROW_EXPORT ValueExpr : public Expr {
 public:
  /// The value cardinality: one or many. These correspond to the arrow::Scalar
  /// and arrow::Array types
  enum Rank { SCALAR, ARRAY };
  /// \brief The name of the expression, if any. The default is unnamed
  // virtual const ExprName& name() const;

  std::shared_ptr<LogicalType> type() const;

  /// \brief The value cardinality (scalar or array) of the expression
  Rank rank() const { return rank_; }

 protected:
  ValueExpr(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type, Rank rank);

  /// \brief The semantic data type of the expression
  std::shared_ptr<LogicalType> type_;

  Rank rank_;
};

class ARROW_EXPORT ArrayExpr : public ValueExpr {
 protected:
  ArrayExpr(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type);
  std::string kind() const override;
};

class ARROW_EXPORT ScalarExpr : public ValueExpr {
 protected:
  ScalarExpr(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type);
  std::string kind() const override;
};

namespace value {

class ValueClass {};
class Null : public ValueClass {};
class Bool : public ValueClass {};
class Number : public ValueClass {};
class Integer : public Number {};
class SignedInteger : public Integer {};
class Int8 : public SignedInteger {};
class Int16 : public SignedInteger {};
class Int32 : public SignedInteger {};
class Int64 : public SignedInteger {};
class UnsignedInteger : public Integer {};
class UInt8 : public UnsignedInteger {};
class UInt16 : public UnsignedInteger {};
class UInt32 : public UnsignedInteger {};
class UInt64 : public UnsignedInteger {};
class Floating : public Number {};
class HalfFloat : public Floating {};
class Float : public Floating {};
class Double : public Floating {};
class Binary : public ValueClass {};
class Utf8 : public Binary {};
class List : public ValueClass {};
class Struct : public ValueClass {};

}  // namespace value

namespace scalar {

#define DECLARE_SCALAR_EXPR(TYPE)                                   \
  class ARROW_EXPORT TYPE : public ScalarExpr, public value::TYPE { \
   public:                                                          \
    explicit TYPE(std::shared_ptr<Operation> op);                   \
    using ScalarExpr::kind;                                         \
  };

DECLARE_SCALAR_EXPR(Null)
DECLARE_SCALAR_EXPR(Bool)
DECLARE_SCALAR_EXPR(Int8)
DECLARE_SCALAR_EXPR(Int16)
DECLARE_SCALAR_EXPR(Int32)
DECLARE_SCALAR_EXPR(Int64)
DECLARE_SCALAR_EXPR(UInt8)
DECLARE_SCALAR_EXPR(UInt16)
DECLARE_SCALAR_EXPR(UInt32)
DECLARE_SCALAR_EXPR(UInt64)
DECLARE_SCALAR_EXPR(Float)
DECLARE_SCALAR_EXPR(Double)
DECLARE_SCALAR_EXPR(Binary)
DECLARE_SCALAR_EXPR(Utf8)

class ARROW_EXPORT List : public ScalarExpr, public value::List {
 public:
  List(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type);
  using ScalarExpr::kind;
};

class ARROW_EXPORT Struct : public ScalarExpr, public value::Struct {
 public:
  Struct(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type);
  using ScalarExpr::kind;
};

}  // namespace scalar

namespace array {

#define DECLARE_ARRAY_EXPR(TYPE)                                   \
  class ARROW_EXPORT TYPE : public ArrayExpr, public value::TYPE { \
   public:                                                         \
    explicit TYPE(std::shared_ptr<Operation> op);                  \
    using ArrayExpr::kind;                                         \
  };

DECLARE_ARRAY_EXPR(Null)
DECLARE_ARRAY_EXPR(Bool)
DECLARE_ARRAY_EXPR(Int8)
DECLARE_ARRAY_EXPR(Int16)
DECLARE_ARRAY_EXPR(Int32)
DECLARE_ARRAY_EXPR(Int64)
DECLARE_ARRAY_EXPR(UInt8)
DECLARE_ARRAY_EXPR(UInt16)
DECLARE_ARRAY_EXPR(UInt32)
DECLARE_ARRAY_EXPR(UInt64)
DECLARE_ARRAY_EXPR(Float)
DECLARE_ARRAY_EXPR(Double)
DECLARE_ARRAY_EXPR(Binary)
DECLARE_ARRAY_EXPR(Utf8)

class ARROW_EXPORT List : public ArrayExpr, public value::List {
 public:
  List(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type);
  using ArrayExpr::kind;
};

class ARROW_EXPORT Struct : public ArrayExpr, public value::Struct {
 public:
  Struct(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type);
  using ArrayExpr::kind;
};

}  // namespace array

template <typename T, typename ObjectType>
inline bool InheritsFrom(const ObjectType* obj) {
  return dynamic_cast<const T*>(obj) != NULLPTR;
}

template <typename T, typename ObjectType>
inline bool InheritsFrom(const ObjectType& obj) {
  return dynamic_cast<const T*>(&obj) != NULLPTR;
}

}  // namespace compute
}  // namespace arrow
