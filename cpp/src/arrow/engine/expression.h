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
#include <utility>

#include "arrow/engine/catalog.h"
#include "arrow/engine/type_fwd.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/compare.h"
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
class ARROW_EXPORT ExprType : public util::EqualityComparable<ExprType> {
 public:
  enum Shape : uint8_t {
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
  /// WARNING: You must ensure the proper shape before calling this accessor.
  const std::shared_ptr<DataType>& data_type() const { return data_type_; }
  /// \brief Schema of the expression if of table shape.
  /// WARNING: You must ensure the proper shape before calling this accessor.
  const std::shared_ptr<Schema>& schema() const { return schema_; }

  /// \brief Indicate if the type is a Scalar.
  bool IsScalar() const { return shape_ == SCALAR; }
  /// \brief Indicate if the type is an Array.
  bool IsArray() const { return shape_ == ARRAY; }
  /// \brief Indicate if the type is a Table.
  bool IsTable() const { return shape_ == TABLE; }

  template <Type::type TYPE_ID>
  bool IsTypedLike() const {
    return (IsScalar() || IsArray()) && data_type_->id() == TYPE_ID;
  }

  /// \brief Indicate if the type is a predicate, i.e. a boolean scalar.
  bool IsPredicate() const { return IsTypedLike<Type::BOOL>(); }

  /// \brief Cast to DataType/Schema if the shape allows it.
  Result<ExprType> CastTo(const std::shared_ptr<DataType>& data_type) const;
  Result<ExprType> CastTo(const std::shared_ptr<Schema>& schema) const;

  /// \brief Broadcasting align two types to the largest shape.
  ///
  /// \param[in] lhs, first type to broadcast
  /// \param[in] rhs, second type to broadcast
  /// \return broadcasted type or an error why it can't be broadcasted.
  ///
  /// Broadcasting promotes the shape of the smallest type to the bigger one if
  /// they share the same DataType. In functional pattern matching it would look
  /// like:
  ///
  /// ```
  /// Broadcast(rhs, lhs) = match(lhs, rhs) {
  ///   case: ScalarType(t1), ScalarType(t2) if t1 == t2 => ScalarType(t)
  ///   case: ScalarType(t1), ArrayType(t2)  if t1 == t2 => ArrayType(t)
  ///   case: ArrayType(t1),  ScalarType(t2) if t1 == t2 => ArrayType(t)
  ///   case: ArrayType(t1),  ArrayType(t2)  if t1 == t2 => ArrayType(t)
  ///   case: _ => Error("Types not compatible for broadcasting")
  /// }
  /// ```
  static Result<ExprType> Broadcast(const ExprType& lhs, const ExprType& rhs);

  bool Equals(const ExprType& type) const;

  std::string ToString() const;

  ExprType(const ExprType& copy);
  ExprType(ExprType&& copy);
  ~ExprType();

 private:
  /// Table constructor
  ExprType(std::shared_ptr<Schema> schema, Shape shape);
  /// Scalar or Array constructor
  ExprType(std::shared_ptr<DataType> type, Shape shape);

  union {
    // Zero initialize the pointer or Copy/Assign constructors will fail.
    std::shared_ptr<DataType> data_type_{};
    std::shared_ptr<Schema> schema_;
  };
  Shape shape_;
};

/// Represents an expression tree
class ARROW_EXPORT Expr : public util::EqualityComparable<Expr> {
 public:
  // Tag identifier for the expression type.
  enum Kind : uint8_t {
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
    // Less-Than compare operator
    LT_CMP_OP,
    // Less-Equal-Than compare operator
    LE_CMP_OP,

    // Empty relation with a known schema.
    EMPTY_REL,
    // Scan relational operator
    SCAN_REL,
    // Filter relational operator
    FILTER_REL,
  };

  /// \brief Return the kind of the expression.
  Kind kind() const { return kind_; }
  /// \brief Return a string representation of the kind.
  std::string kind_name() const;

  /// \brief Return the type and shape of the resulting expression.
  const ExprType& type() const { return type_; }

  /// \brief Indicate if the expressions
  bool Equals(const Expr& other) const;
  using util::EqualityComparable<Expr>::Equals;

  /// \brief Return a string representing the expression
  std::string ToString() const;

  virtual ~Expr() = default;

 protected:
  explicit Expr(Kind kind, ExprType type) : type_(std::move(type)), kind_(kind) {}

  ExprType type_;
  Kind kind_;
};

// The following traits are used to break cycle between CRTP base classes and
// their derived counterparts to extract the Expr::Kind and other static
// properties from the forward declared class.
template <typename T>
struct expr_traits;

template <>
struct expr_traits<ScalarExpr> {
  static constexpr Expr::Kind kind_id = Expr::SCALAR_LITERAL;
};

template <>
struct expr_traits<FieldRefExpr> {
  static constexpr Expr::Kind kind_id = Expr::FIELD_REFERENCE;
};

template <>
struct expr_traits<EqualCmpExpr> {
  static constexpr Expr::Kind kind_id = Expr::EQ_CMP_OP;
};

template <>
struct expr_traits<NotEqualCmpExpr> {
  static constexpr Expr::Kind kind_id = Expr::NE_CMP_OP;
};

template <>
struct expr_traits<GreaterThanCmpExpr> {
  static constexpr Expr::Kind kind_id = Expr::GT_CMP_OP;
};

template <>
struct expr_traits<GreaterEqualThanCmpExpr> {
  static constexpr Expr::Kind kind_id = Expr::GE_CMP_OP;
};

template <>
struct expr_traits<LessThanCmpExpr> {
  static constexpr Expr::Kind kind_id = Expr::LT_CMP_OP;
};

template <>
struct expr_traits<LessEqualThanCmpExpr> {
  static constexpr Expr::Kind kind_id = Expr::LE_CMP_OP;
};

template <>
struct expr_traits<EmptyRelExpr> {
  static constexpr Expr::Kind kind_id = Expr::EMPTY_REL;
};

template <>
struct expr_traits<ScanRelExpr> {
  static constexpr Expr::Kind kind_id = Expr::SCAN_REL;
};

template <>
struct expr_traits<FilterRelExpr> {
  static constexpr Expr::Kind kind_id = Expr::FILTER_REL;
};

//
// Value Expressions
//

// An unnamed scalar literal expression.
class ARROW_EXPORT ScalarExpr : public Expr {
 public:
  static Result<std::shared_ptr<ScalarExpr>> Make(std::shared_ptr<Scalar> scalar);

  const std::shared_ptr<Scalar>& scalar() const { return scalar_; }

 private:
  explicit ScalarExpr(std::shared_ptr<Scalar> scalar);

  std::shared_ptr<Scalar> scalar_;
};

// References a column in a table/dataset
class ARROW_EXPORT FieldRefExpr : public Expr {
 public:
  static Result<std::shared_ptr<FieldRefExpr>> Make(std::shared_ptr<Field> field);

  const std::shared_ptr<Field>& field() const { return field_; }

 private:
  explicit FieldRefExpr(std::shared_ptr<Field> field);

  std::shared_ptr<Field> field_;
};

//
// Operator expressions
//

class ARROW_EXPORT UnaryOpExpr {
 public:
  const std::shared_ptr<Expr>& operand() const { return operand_; }

 protected:
  explicit UnaryOpExpr(std::shared_ptr<Expr> operand) : operand_(std::move(operand)) {}

  std::shared_ptr<Expr> operand_;
};

class ARROW_EXPORT BinaryOpExpr {
 public:
  const std::shared_ptr<Expr>& left_operand() const { return left_operand_; }
  const std::shared_ptr<Expr>& right_operand() const { return right_operand_; }

 protected:
  BinaryOpExpr(std::shared_ptr<Expr> left, std::shared_ptr<Expr> right)
      : left_operand_(std::move(left)), right_operand_(std::move(right)) {}

  std::shared_ptr<Expr> left_operand_;
  std::shared_ptr<Expr> right_operand_;
};

//
// Comparison expressions
//

template <typename Self>
class ARROW_EXPORT CmpOpExpr : public BinaryOpExpr, public Expr {
 public:
  static Result<std::shared_ptr<Self>> Make(std::shared_ptr<Expr> left,
                                            std::shared_ptr<Expr> right) {
    if (left == NULLPTR || right == NULLPTR) {
      return Status::Invalid("Compare operands must be non-nulls");
    }

    // Broadcast ensures that types are compatible in shape and type.
    auto broadcast = ExprType::Broadcast(left->type(), right->type());
    // The type of comparison is always a boolean predicate.
    auto cast = [](const ExprType& t) { return t.CastTo(boolean()); };
    ARROW_ASSIGN_OR_RAISE(auto type, broadcast.Map(cast));

    return std::shared_ptr<Self>(
        new Self(std::move(type), std::move(left), std::move(right)));
  }

 protected:
  CmpOpExpr(ExprType type, std::shared_ptr<Expr> left, std::shared_ptr<Expr> right)
      : BinaryOpExpr(std::move(left), std::move(right)),
        Expr(expr_traits<Self>::kind_id, std::move(type)) {}
};

class ARROW_EXPORT EqualCmpExpr : public CmpOpExpr<EqualCmpExpr> {
 protected:
  using CmpOpExpr<EqualCmpExpr>::CmpOpExpr;
};

class ARROW_EXPORT NotEqualCmpExpr : public CmpOpExpr<NotEqualCmpExpr> {
 protected:
  using CmpOpExpr<NotEqualCmpExpr>::CmpOpExpr;
};

class ARROW_EXPORT GreaterThanCmpExpr : public CmpOpExpr<GreaterThanCmpExpr> {
 protected:
  using CmpOpExpr<GreaterThanCmpExpr>::CmpOpExpr;
};

class ARROW_EXPORT GreaterEqualThanCmpExpr : public CmpOpExpr<GreaterEqualThanCmpExpr> {
 protected:
  using CmpOpExpr<GreaterEqualThanCmpExpr>::CmpOpExpr;
};

class ARROW_EXPORT LessThanCmpExpr : public CmpOpExpr<LessThanCmpExpr> {
 protected:
  using CmpOpExpr<LessThanCmpExpr>::CmpOpExpr;
};

class ARROW_EXPORT LessEqualThanCmpExpr : public CmpOpExpr<LessEqualThanCmpExpr> {
 protected:
  using CmpOpExpr<LessEqualThanCmpExpr>::CmpOpExpr;
};

//
// Relational Expressions
//

template <typename Self>
class ARROW_EXPORT RelExpr : public Expr {
 public:
  const std::shared_ptr<Schema>& schema() const { return schema_; }

 protected:
  explicit RelExpr(std::shared_ptr<Schema> schema)
      : Expr(expr_traits<Self>::kind_id, ExprType::Table(schema)),
        schema_(std::move(schema)) {}

  std::shared_ptr<Schema> schema_;
};

class ARROW_EXPORT EmptyRelExpr : public RelExpr<EmptyRelExpr> {
 public:
  static Result<std::shared_ptr<EmptyRelExpr>> Make(std::shared_ptr<Schema> schema);

 protected:
  using RelExpr<EmptyRelExpr>::RelExpr;
};

class ARROW_EXPORT ScanRelExpr : public RelExpr<ScanRelExpr> {
 public:
  static Result<std::shared_ptr<ScanRelExpr>> Make(Catalog::Entry input);

  const Catalog::Entry& input() const { return input_; }

 private:
  explicit ScanRelExpr(Catalog::Entry input);

  Catalog::Entry input_;
};

class ARROW_EXPORT FilterRelExpr : public UnaryOpExpr, public RelExpr<FilterRelExpr> {
 public:
  static Result<std::shared_ptr<FilterRelExpr>> Make(std::shared_ptr<Expr> input,
                                                     std::shared_ptr<Expr> predicate);

  const std::shared_ptr<Expr>& predicate() const { return predicate_; }

 private:
  FilterRelExpr(std::shared_ptr<Expr> input, std::shared_ptr<Expr> predicate);

  std::shared_ptr<Expr> predicate_;
};

template <typename Visitor>
auto VisitExpr(const Expr& expr, Visitor&& visitor) -> decltype(visitor(expr)) {
  switch (expr.kind()) {
    case Expr::SCALAR_LITERAL:
      return visitor(internal::checked_cast<const ScalarExpr&>(expr));
    case Expr::FIELD_REFERENCE:
      return visitor(internal::checked_cast<const FieldRefExpr&>(expr));

    case Expr::EQ_CMP_OP:
      return visitor(internal::checked_cast<const EqualCmpExpr&>(expr));
    case Expr::NE_CMP_OP:
      return visitor(internal::checked_cast<const NotEqualCmpExpr&>(expr));
    case Expr::GT_CMP_OP:
      return visitor(internal::checked_cast<const GreaterThanCmpExpr&>(expr));
    case Expr::GE_CMP_OP:
      return visitor(internal::checked_cast<const GreaterEqualThanCmpExpr&>(expr));
    case Expr::LT_CMP_OP:
      return visitor(internal::checked_cast<const LessThanCmpExpr&>(expr));
    case Expr::LE_CMP_OP:
      return visitor(internal::checked_cast<const LessEqualThanCmpExpr&>(expr));

    case Expr::EMPTY_REL:
      return visitor(internal::checked_cast<const EmptyRelExpr&>(expr));
    case Expr::SCAN_REL:
      return visitor(internal::checked_cast<const ScanRelExpr&>(expr));
    case Expr::FILTER_REL:
      // LEAVE LAST or update the outer return cast by moving it here. This is
      // required for older compiler support.
      break;
  }

  return visitor(internal::checked_cast<const FilterRelExpr&>(expr));
}

}  // namespace engine
}  // namespace arrow
