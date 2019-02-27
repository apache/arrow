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
  enum { SCALAR, ARRAY } Rank;
  /// \brief The name of the expression, if any. The default is unnamed
  // virtual const ExprName& name() const;

  std::shared_ptr<LogicalType> type() const { return type_; }

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
};

class ARROW_EXPORT ScalarExpr : public ValueExpr {
 protected:
  ScalarExpr(std::shared_ptr<Operation> op, std::shared_ptr<LogicalType> type);
};

class ARROW_EXPORT NumericValue : public ValueExpr {};

// ----------------------------------------------------------------------
// Integer expr types

class ARROW_EXPORT IntegerValue : public NumericValue {};

class Int8Array : virtual public ArrayExpr, virtual public IntegerValue {
 public:
  explicit Int8Array(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class Int8Scalar : virtual public ScalarExpr, virtual public IntegerValue {
 public:
  explicit Int8Scalar(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class Int16Array : virtual public ArrayExpr, virtual public IntegerValue {
 public:
  explicit Int16Array(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class Int16Scalar : virtual public ScalarExpr, virtual public IntegerValue {
 public:
  explicit Int16Scalar(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class Int32Array : virtual public ArrayExpr, virtual public IntegerValue {
 public:
  explicit Int32Array(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class Int32Scalar : virtual public ScalarExpr, virtual public IntegerValue {
 public:
  explicit Int32Scalar(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class Int64Array : virtual public ArrayExpr, virtual public IntegerValue {
 public:
  explicit Int64Array(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class Int64Scalar : virtual public ScalarExpr, virtual public IntegerValue {
 public:
  explicit Int64Scalar(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class UInt8Array : virtual public ArrayExpr, virtual public IntegerValue {
 public:
  explicit UInt8Array(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class UInt8Scalar : virtual public ScalarExpr, virtual public IntegerValue {
 public:
  explicit UInt8Scalar(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class UInt16Array : virtual public ArrayExpr, virtual public IntegerValue {
 public:
  explicit UInt16Array(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class UInt16Scalar : virtual public ScalarExpr, virtual public IntegerValue {
 public:
  explicit UInt16Scalar(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class UInt32Array : virtual public ArrayExpr, virtual public IntegerValue {
 public:
  explicit UInt32Array(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class UInt32Scalar : virtual public ScalarExpr, virtual public IntegerValue {
 public:
  explicit UInt32Scalar(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class UInt64Array : virtual public ArrayExpr, virtual public IntegerValue {
 public:
  explicit UInt64Array(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

class UInt64Scalar : virtual public ScalarExpr, virtual public IntegerValue {
 public:
  explicit UInt64Scalar(std::shared_ptr<Operation> op);
  const std::string& kind() const override;
};

// ----------------------------------------------------------------------
// Floating point expr types

class ARROW_EXPORT FloatingValue : public NumericValue {};

// ----------------------------------------------------------------------
// Binary expr types

class ARROW_EXPORT BinaryValue : public ValueExpr {};

class ARROW_EXPORT Utf8Value : public BinaryValue {};

class BinaryScalar : virtual public ScalarExpr, virtual public BinaryValue {};

class Utf8Scalar : virtual public ScalarExpr, virtual public Utf8Value {};

template <typename T, typename ObjectType>
inline bool IsInstance(const ObjectType* obj) {
  return dynamic_cast<const T*>(obj) != NULLPTR;
}

template <typename T, typename ObjectType>
inline bool IsInstance(const ObjectType& obj) {
  return dynamic_cast<const T*>(&obj) != NULLPTR;
}

}  // namespace compute
}  // namespace arrow
