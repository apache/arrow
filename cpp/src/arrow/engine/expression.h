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
#include <unordered_map>
#include <vector>

#include "arrow/engine/catalog.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/variant.h"

namespace arrow {
namespace engine {

/// ExprType is a class representing the type of an Expression. The type is
/// composed of a shape and a DataType or a Schema depending on the shape.
///
/// ExprType is mainly used to validate arguments for operator expressions, e.g.
/// relational operator expressions expect inputs of Table shape.
///
/// The sum-type representation would be:
///
/// enum ExprType {
///   ScalarType(DataType),
///   ArrayType(DataType),
///   TableType(Schema),
/// }
class ARROW_EXPORT ExprType {
 public:
  enum Shape {
    // The expression yields a Scalar, e.g. "1".
    SCALAR,
    // The expression yields an Array, e.g. "[1, 2, 3]".
    ARRAY,
    // The expression yields a Table, e.g. "{'a': [1, 2], 'b': [true, false]}"
    TABLE,
  };

  /// Construct a Scalar type.
  static ExprType Scalar(std::shared_ptr<DataType> type);
  /// Construct an Array type.
  static ExprType Array(std::shared_ptr<DataType> type);
  /// Construct a Table type.
  static ExprType Table(std::shared_ptr<Schema> schema);

  /// \brief Shape of the expression.
  Shape shape() const { return shape_; }

  /// \brief DataType of the expression if a scalar or an array.
  std::shared_ptr<DataType> data_type() const;
  /// \brief Schema of the expression if of table shape.
  std::shared_ptr<Schema> schema() const;

  /// \brief Indicate if the type is a Scalar.
  bool IsScalar() const { return shape_ == SCALAR; }
  /// \brief Indicate if the type is an Array.
  bool IsArray() const { return shape_ == ARRAY; }
  /// \brief Indicate if the type is a Table.
  bool IsTable() const { return shape_ == TABLE; }

  template <Type::type TYPE_ID>
  bool HasType() const {
    return (shape_ == SCALAR || shape_ == ARRAY) &&
           util::get<std::shared_ptr<DataType>>(type_)->id() == TYPE_ID;
  }

  /// \brief Indicate if the type is a predicate, i.e. a boolean scalar.
  bool IsPredicate() const { return IsScalar() && HasType<Type::BOOL>(); }

  bool Equals(const ExprType& type) const;
  bool operator==(const ExprType& rhs) const;
  std::string ToString() const;

 private:
  /// Table constructor
  ExprType(std::shared_ptr<Schema> schema, Shape shape);
  /// Scalar or Array constructor
  ExprType(std::shared_ptr<DataType> type, Shape shape);

  util::variant<std::shared_ptr<DataType>, std::shared_ptr<Schema>> type_;
  Shape shape_;
};

/// Represents an expression tree
class ARROW_EXPORT Expr {
 public:
  // Tag identifier for the expression type.
  enum Kind {
    // A Scalar literal, i.e. a constant.
    SCALAR_LITERAL,
    // A Field reference in a schema.
    FIELD_REFERENCE,

    // Equal compare operator
    EQ_CMP_OP,
    // Not-Equal compare operator
    NE_CMP_OP,
    // Greater-Than compare operator
    GT_CMP_OP,
    // Greater-Equal-Than compare operator
    GE_CMP_OP,
    // Lower-Than compare operator
    LT_CMP_OP,
    // Lower-Equal-Than compare operator
    LE_CMP_OP,

    // Scan relational operator
    SCAN_REL,
    // Filter relational operator
    FILTER_REL,
  };

  // Return the type and shape of the resulting expression.
  virtual ExprType type() const = 0;

  Kind kind() const { return kind_; }

  /// Returns true iff the expressions are identical; does not check for equivalence.
  /// For example, (A and B) is not equal to (B and A) nor is (A and not A) equal to
  /// (false).
  bool Equals(const Expr& other) const;
  bool Equals(const std::shared_ptr<Expr>& other) const;

  /// Return a string representing the expression
  std::string ToString() const;

  virtual ~Expr() = default;

 protected:
  explicit Expr(Kind kind) : kind_(kind) {}

 private:
  Kind kind_;
};

//
// Value Expressions
//

// An unnamed scalar literal expression.
class ScalarExpr : public Expr {
 public:
  static Result<std::shared_ptr<ScalarExpr>> Make(std::shared_ptr<Scalar> scalar);

  const std::shared_ptr<Scalar>& scalar() const { return scalar_; }

  ExprType type() const override;

 private:
  explicit ScalarExpr(std::shared_ptr<Scalar> scalar);

  std::shared_ptr<Scalar> scalar_;
};

// References a column in a table/dataset
class FieldRefExpr : public Expr {
 public:
  static Result<std::shared_ptr<FieldRefExpr>> Make(std::shared_ptr<Field> field);

  const std::shared_ptr<Field>& field() const { return field_; }

  ExprType type() const override;

 private:
  explicit FieldRefExpr(std::shared_ptr<Field> field);

  std::shared_ptr<Field> field_;
};

//
// Operator expressions
//

using ExprVector = std::vector<std::shared_ptr<Expr>>;

class OpExpr {
 public:
  const ExprVector& inputs() const { return inputs_; }

 protected:
  explicit OpExpr(ExprVector inputs) : inputs_(std::move(inputs)) {}
  ExprVector inputs_;
};

template <Expr::Kind KIND>
class BinaryOpExpr : public Expr, private OpExpr {
 public:
  const std::shared_ptr<Expr>& left_operand() const { return inputs_[0]; }
  const std::shared_ptr<Expr>& right_operand() const { return inputs_[1]; }

 protected:
  BinaryOpExpr(std::shared_ptr<Expr> left, std::shared_ptr<Expr> right)
      : Expr(KIND), OpExpr({std::move(left), std::move(right)}) {}
};

//
// Comparison expressions
//

template <Expr::Kind KIND>
class CmpOpExpr : public BinaryOpExpr<KIND> {
 public:
  ExprType type() const override { return ExprType::Scalar(boolean()); };

 protected:
  using BinaryOpExpr<KIND>::BinaryOpExpr;
};

class EqualCmpExpr : public CmpOpExpr<Expr::EQ_CMP_OP> {
 public:
  static Result<std::shared_ptr<EqualCmpExpr>> Make(std::shared_ptr<Expr> left,
                                                    std::shared_ptr<Expr> right);

 protected:
  using CmpOpExpr::CmpOpExpr;
};

class NotEqualCmpExpr : public CmpOpExpr<Expr::NE_CMP_OP> {
 public:
  static Result<std::shared_ptr<NotEqualCmpExpr>> Make(std::shared_ptr<Expr> left,
                                                       std::shared_ptr<Expr> right);

 protected:
  using CmpOpExpr::CmpOpExpr;
};

class GreaterThanCmpExpr : public CmpOpExpr<Expr::GT_CMP_OP> {
 public:
  static Result<std::shared_ptr<GreaterThanCmpExpr>> Make(std::shared_ptr<Expr> left,
                                                          std::shared_ptr<Expr> right);

 protected:
  using CmpOpExpr::CmpOpExpr;
};

class GreaterEqualThanCmpExpr : public CmpOpExpr<Expr::GE_CMP_OP> {
 public:
  static Result<std::shared_ptr<GreaterEqualThanCmpExpr>> Make(
      std::shared_ptr<Expr> left, std::shared_ptr<Expr> right);

 protected:
  using CmpOpExpr::CmpOpExpr;
};

class LowerThanCmpExpr : public CmpOpExpr<Expr::LT_CMP_OP> {
 public:
  static Result<std::shared_ptr<LowerThanCmpExpr>> Make(std::shared_ptr<Expr> left,
                                                        std::shared_ptr<Expr> right);

 protected:
  using CmpOpExpr::CmpOpExpr;
};

class LowerEqualThanCmpExpr : public CmpOpExpr<Expr::LE_CMP_OP> {
 public:
  static Result<std::shared_ptr<LowerEqualThanCmpExpr>> Make(std::shared_ptr<Expr> left,
                                                             std::shared_ptr<Expr> right);

 protected:
  using CmpOpExpr::CmpOpExpr;
};

//
// Relational Expressions
//

class ScanRelExpr : public Expr {
 public:
  static Result<std::shared_ptr<ScanRelExpr>> Make(Catalog::Entry input);

  ExprType type() const override;

 private:
  explicit ScanRelExpr(Catalog::Entry input);

  Catalog::Entry input_;
};

class FilterRelExpr : public Expr {
 public:
  static Result<std::shared_ptr<FilterRelExpr>> Make(std::shared_ptr<Expr> input,
                                                     std::shared_ptr<Expr> predicate);

  ExprType type() const override;

 private:
  FilterRelExpr(std::shared_ptr<Expr> input, std::shared_ptr<Expr> predicate);

  std::shared_ptr<Expr> input_;
  std::shared_ptr<Expr> predicate_;
};

}  // namespace engine
}  // namespace arrow
