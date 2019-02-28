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

class Null : public ValueExpr {};
class Bool : public ValueExpr {};
class Number : public ValueExpr {};
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
class Binary : public ValueExpr {};
class Utf8 : public Binary {};
class List : public ValueExpr {};
class Struct : public ValueExpr {};

}  // namespace value

namespace scalar {

class ARROW_EXPORT Null : virtual public ScalarExpr, virtual public value::Null {
 public:
  explicit Null(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Bool : virtual public ScalarExpr, virtual public value::Bool {
 public:
  explicit Bool(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Int8 : virtual public ScalarExpr, virtual public value::Int8 {
 public:
  explicit Int8(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Int16 : virtual public ScalarExpr, virtual public value::Int16 {
 public:
  explicit Int16(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Int32 : virtual public ScalarExpr, virtual public value::Int32 {
 public:
  explicit Int32(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Int64 : virtual public ScalarExpr, virtual public value::Int64 {
 public:
  explicit Int64(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT UInt8 : virtual public ScalarExpr, virtual public value::UInt8 {
 public:
  explicit UInt8(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT UInt16 : virtual public ScalarExpr, virtual public value::UInt16 {
 public:
  explicit UInt16(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT UInt32 : virtual public ScalarExpr, virtual public value::UInt32 {
 public:
  explicit UInt32(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT UInt64 : virtual public ScalarExpr, virtual public value::UInt64 {
 public:
  explicit UInt64(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Float : virtual public ScalarExpr, virtual public value::Float {
 public:
  explicit Float(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Double : virtual public ScalarExpr, virtual public value::Double {
 public:
  explicit Double(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Binary : virtual public ScalarExpr, virtual public value::Binary {
 public:
  explicit Binary(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Utf8 : virtual public ScalarExpr, virtual public value::Utf8 {
 public:
  explicit Utf8(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT List : virtual public ScalarExpr, virtual public value::List {
 public:
  List(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type);
};

class ARROW_EXPORT Struct : virtual public ScalarExpr, virtual public value::Struct {
 public:
  Struct(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type);
};

}  // namespace scalar

namespace array {

class ARROW_EXPORT Null : virtual public ArrayExpr, virtual public value::Null {
 public:
  explicit Null(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Bool : virtual public ArrayExpr, virtual public value::Bool {
 public:
  explicit Bool(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Int8 : virtual public ArrayExpr, virtual public value::Int8 {
 public:
  explicit Int8(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Int16 : virtual public ArrayExpr, virtual public value::Int16 {
 public:
  explicit Int16(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Int32 : virtual public ArrayExpr, virtual public value::Int32 {
 public:
  explicit Int32(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Int64 : virtual public ArrayExpr, virtual public value::Int64 {
 public:
  explicit Int64(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT UInt8 : virtual public ArrayExpr, virtual public value::UInt8 {
 public:
  explicit UInt8(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT UInt16 : virtual public ArrayExpr, virtual public value::UInt16 {
 public:
  explicit UInt16(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT UInt32 : virtual public ArrayExpr, virtual public value::UInt32 {
 public:
  explicit UInt32(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT UInt64 : virtual public ArrayExpr, virtual public value::UInt64 {
 public:
  explicit UInt64(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Float : virtual public ArrayExpr, virtual public value::Float {
 public:
  explicit Float(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Double : virtual public ArrayExpr, virtual public value::Double {
 public:
  explicit Double(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Binary : virtual public ArrayExpr, virtual public value::Binary {
 public:
  explicit Binary(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT Utf8 : virtual public ArrayExpr, virtual public value::Utf8 {
 public:
  explicit Utf8(std::shared_ptr<Operation> op);
};

class ARROW_EXPORT List : virtual public ArrayExpr, virtual public value::List {
 public:
  List(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type);
};

class ARROW_EXPORT Struct : virtual public ArrayExpr, virtual public value::Struct {
 public:
  Struct(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type);
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
