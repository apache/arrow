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
#include <vector>

#include "arrow/engine/catalog.h"
#include "arrow/engine/type_fwd.h"
#include "arrow/engine/type_traits.h"
#include "arrow/engine/visibility.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/compare.h"
#include "arrow/util/macros.h"

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
class ARROW_EN_EXPORT ExprType : public util::EqualityComparable<ExprType> {
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
  static ExprType Table(std::vector<std::shared_ptr<Field>> fields);

  /// \brief Shape of the expression.
  Shape shape() const { return shape_; }

  /// \brief DataType of the expression if a scalar or an array.
  /// WARNING: You must ensure the proper shape before calling this accessor.
  const std::shared_ptr<DataType>& type() const { return data_type_; }
  /// \brief Schema of the expression if of table shape.
  /// WARNING: You must ensure the proper shape before calling this accessor.
  const std::shared_ptr<Schema>& schema() const { return schema_; }

  /// \brief Indicate if the type is a Scalar.
  bool IsScalar() const { return shape_ == SCALAR; }
  /// \brief Indicate if the type is an Array.
  bool IsArray() const { return shape_ == ARRAY; }
  /// \brief Indicate if the type is a Table.
  bool IsTable() const { return shape_ == TABLE; }

  bool HasType() const { return IsScalar() || IsArray(); }
  bool HasSchema() const { return IsTable(); }

  bool IsTypedLike(Type::type type_id) const {
    return HasType() && data_type_->id() == type_id;
  }

  /// \brief Static version of IsTypedLike
  template <Type::type TYPE_ID>
  bool IsTypedLike() const {
    return HasType() && data_type_->id() == TYPE_ID;
  }

  /// \brief Indicate if the type is a predicate, i.e. a boolean scalar.
  bool IsPredicate() const { return IsTypedLike<Type::BOOL>(); }

  /// \brief Cast the inner DataType/Schema while preserving the shape.
  Result<ExprType> WithType(const std::shared_ptr<DataType>& data_type) const;
  Result<ExprType> WithSchema(const std::shared_ptr<Schema>& schema) const;

  /// \brief Expand the smallest shape to the bigger one if possible.
  ///
  /// \param[in] lhs first type to broadcast
  /// \param[in] rhs second type to broadcast
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
    /// Zero initialize the pointer or Copy/Assign constructors will fail.
    std::shared_ptr<DataType> data_type_{};
    std::shared_ptr<Schema> schema_;
  };
  Shape shape_;
};

/// Represents an expression tree
class ARROW_EN_EXPORT Expr : public util::EqualityComparable<Expr> {
 public:
  /// \brief Return the kind of the expression.
  ExprKind kind() const { return kind_; }
  /// \brief Return a string representation of the kind.
  std::string kind_name() const;

  /// \brief Return the type and shape of the resulting expression.
  const ExprType& type() const { return type_; }

  /// \brief Indicate if the expressions are equal.
  bool Equals(const Expr& other) const;
  using util::EqualityComparable<Expr>::Equals;

  /// \brief Return a string representing the expression
  std::string ToString() const;

  virtual ~Expr() = default;

 protected:
  explicit Expr(ExprKind kind, ExprType type) : type_(std::move(type)), kind_(kind) {}

  ExprType type_;
  ExprKind kind_;
};

///
/// Operator expressions mixin.
///

class ARROW_EN_EXPORT UnaryOpMixin {
 public:
  const std::shared_ptr<Expr>& operand() const { return operand_; }

 protected:
  explicit UnaryOpMixin(std::shared_ptr<Expr> operand) : operand_(std::move(operand)) {}

  std::shared_ptr<Expr> operand_;
};

class ARROW_EN_EXPORT BinaryOpMixin {
 public:
  const std::shared_ptr<Expr>& left_operand() const { return left_operand_; }
  const std::shared_ptr<Expr>& right_operand() const { return right_operand_; }

 protected:
  BinaryOpMixin(std::shared_ptr<Expr> left, std::shared_ptr<Expr> right)
      : left_operand_(std::move(left)), right_operand_(std::move(right)) {}

  std::shared_ptr<Expr> left_operand_;
  std::shared_ptr<Expr> right_operand_;
};

class ARROW_EN_EXPORT MultiAryOpMixin {
 public:
  const std::vector<std::shared_ptr<Expr>>& operands() const { return operands_; }

 protected:
  explicit MultiAryOpMixin(std::vector<std::shared_ptr<Expr>> operands)
      : operands_(std::move(operands)) {}

  std::vector<std::shared_ptr<Expr>> operands_;
};

///
/// Value Expressions
///

/// An unnamed scalar literal expression.
class ARROW_EN_EXPORT ScalarExpr : public Expr {
 public:
  static Result<std::shared_ptr<ScalarExpr>> Make(std::shared_ptr<Scalar> scalar);

  const std::shared_ptr<Scalar>& scalar() const { return scalar_; }

 private:
  explicit ScalarExpr(std::shared_ptr<Scalar> scalar);

  std::shared_ptr<Scalar> scalar_;
};

/// References a column in a table/dataset
class ARROW_EN_EXPORT FieldRefExpr : public UnaryOpMixin, public Expr {
 public:
  static Result<std::shared_ptr<FieldRefExpr>> Make(std::shared_ptr<Expr> input,
                                                    int index);
  static Result<std::shared_ptr<FieldRefExpr>> Make(std::shared_ptr<Expr> input,
                                                    std::string field_name);

  int index() const { return index_; }

 private:
  FieldRefExpr(std::shared_ptr<Expr> input, int index);

  int index_;
};

///
/// Comparison expressions
///

class ARROW_EN_EXPORT CompareOpExpr : public BinaryOpMixin, public Expr {
 public:
  CompareKind compare_kind() const { return compare_kind_; }

  /// This inner-class is required because `using` statements can't use derived
  /// methods.
  template <typename Derived>
  struct MakeMixin {
    static Result<std::shared_ptr<Derived>> Make(std::shared_ptr<Expr> left,
                                                 std::shared_ptr<Expr> right) {
      if (left == NULLPTR || right == NULLPTR) {
        return Status::Invalid("Compare operands must be non-nulls");
      }

      // Broadcast the comparison to the biggest shape.
      ARROW_ASSIGN_OR_RAISE(auto broadcast,
                            ExprType::Broadcast(left->type(), right->type()));
      // And change this shape's type to boolean.
      ARROW_ASSIGN_OR_RAISE(auto type, broadcast.WithType(boolean()));

      return std::shared_ptr<Derived>(new Derived(std::move(type),
                                                  expr_traits<Derived>::compare_kind_id,
                                                  std::move(left), std::move(right)));
    }
  };

 protected:
  CompareOpExpr(ExprType type, CompareKind op, std::shared_ptr<Expr> left,
                std::shared_ptr<Expr> right)
      : BinaryOpMixin(std::move(left), std::move(right)),
        Expr(COMPARE_OP, std::move(type)),
        compare_kind_(op) {}

  CompareKind compare_kind_;
};

template <typename Derived>
class BaseCompareExpr : public CompareOpExpr, private CompareOpExpr::MakeMixin<Derived> {
 public:
  using CompareOpExpr::MakeMixin<Derived>::Make;

 protected:
  using CompareOpExpr::CompareOpExpr;
};

class ARROW_EN_EXPORT EqualExpr : public BaseCompareExpr<EqualExpr> {
 protected:
  using BaseCompareExpr<EqualExpr>::BaseCompareExpr;
};

class ARROW_EN_EXPORT NotEqualExpr : public BaseCompareExpr<NotEqualExpr> {
 protected:
  using BaseCompareExpr<NotEqualExpr>::BaseCompareExpr;
};

class ARROW_EN_EXPORT GreaterThanExpr : public BaseCompareExpr<GreaterThanExpr> {
 protected:
  using BaseCompareExpr<GreaterThanExpr>::BaseCompareExpr;
};

class ARROW_EN_EXPORT GreaterThanEqualExpr
    : public BaseCompareExpr<GreaterThanEqualExpr> {
 protected:
  using BaseCompareExpr<GreaterThanEqualExpr>::BaseCompareExpr;
};

class ARROW_EN_EXPORT LessThanExpr : public BaseCompareExpr<LessThanExpr> {
 protected:
  using BaseCompareExpr<LessThanExpr>::BaseCompareExpr;
};

class ARROW_EN_EXPORT LessThanEqualExpr : public BaseCompareExpr<LessThanEqualExpr> {
 protected:
  using BaseCompareExpr<LessThanEqualExpr>::BaseCompareExpr;
};

///
/// Relational Expressions
///

/// \brief Relational Expressions that acts on tables.
template <typename Derived>
class ARROW_EN_EXPORT RelExpr : public Expr {
 public:
  const std::shared_ptr<Schema>& schema() const { return schema_; }

 protected:
  explicit RelExpr(std::shared_ptr<Schema> schema)
      : Expr(expr_traits<Derived>::kind_id, ExprType::Table(schema)),
        schema_(std::move(schema)) {}

  std::shared_ptr<Schema> schema_;
};

/// \brief An empty relation that returns/contains no rows.
///
/// An EmptyRelExpr is usually not found in user constructed logical plan but
/// can appear due to optimization passes, e.g. replacing a FilterRelExpr with
/// an always false predicate. It is also subsequently used in constant
/// propagation-like optimizations, e.g Filter(EmptyRel) => EmptyRel, or
/// InnerJoin(_, EmptyRel) => EmptyRel.
///
/// \input schema, the schema of the empty relation
/// \ouput relation with no rows of the given input schema
class ARROW_EN_EXPORT EmptyRelExpr : public RelExpr<EmptyRelExpr> {
 public:
  static Result<std::shared_ptr<EmptyRelExpr>> Make(std::shared_ptr<Schema> schema);

 protected:
  using RelExpr<EmptyRelExpr>::RelExpr;
};

/// \brief Materialize a relation from a dataset.
///
/// The ScanRelExpr are found in the leaves of the Expr tree. A Scan materialize
/// the relation from a datasets. In essence, it is a relational operator that
/// has no relation input (except some auxiliary information like a catalog
/// entry), and output a relation.
///
/// \input table, a catalog entry pointing to a dataset
/// \ouput relation from the materialized dataset
///
/// ```
/// SELECT * FROM table;
/// ```
class ARROW_EN_EXPORT ScanRelExpr : public RelExpr<ScanRelExpr> {
 public:
  static Result<std::shared_ptr<ScanRelExpr>> Make(Catalog::Entry input);

  const Catalog::Entry& input() const { return input_; }

 private:
  explicit ScanRelExpr(Catalog::Entry input);

  Catalog::Entry input_;
};

/// \brief Project columns based on expressions.
///
/// A projection creates a relation with new columns based on expressions of
/// the input's columns. It could be a simple permutation or selection of
/// column via FieldRefExpr or more complex expressions like the sum of two
/// columns. The projection operator will usually change the output schema of
/// the input relation due to the expressions without changing the number of
/// rows.
///
/// \input relation, the input relation to compute the expressions from
/// \input expressions, the expressions to compute
/// \output relation where the columns are the expressions computed
///
/// ```
/// SELECT a, b, a + b, 1, mean(a) > b FROM relation;
/// ```
class ARROW_EN_EXPORT ProjectionRelExpr : public UnaryOpMixin,
                                          public RelExpr<ProjectionRelExpr> {
 public:
  static Result<std::shared_ptr<ProjectionRelExpr>> Make(
      std::shared_ptr<Expr> input, std::vector<std::shared_ptr<Expr>> expressions);

  const std::vector<std::shared_ptr<Expr>> expressions() const { return expressions_; }

 private:
  ProjectionRelExpr(std::shared_ptr<Expr> input, std::shared_ptr<Schema> schema,
                    std::vector<std::shared_ptr<Expr>> expressions);

  std::vector<std::shared_ptr<Expr>> expressions_;
};

/// \brief Filter the rows of a relation according to a predicate.
///
/// A filter removes rows that don't match a predicate or a mask column.
///
/// \input relation, the input relation to filter the rows from
/// \input predicate, a predicate to evaluate for each filter
/// \output relation where the rows are filtered according to the predicate
///
/// ```
/// SELECT * FROM relation WHERE predicate
/// ```
class ARROW_EN_EXPORT FilterRelExpr : public UnaryOpMixin, public RelExpr<FilterRelExpr> {
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
    case ExprKind::SCALAR_LITERAL:
      return visitor(internal::checked_cast<const ScalarExpr&>(expr));
    case ExprKind::FIELD_REFERENCE:
      return visitor(internal::checked_cast<const FieldRefExpr&>(expr));

    case ExprKind::COMPARE_OP: {
      const auto& cmp_expr = static_cast<const CompareOpExpr&>(expr);
      switch (cmp_expr.compare_kind()) {
        case CompareKind::EQUAL:
          return visitor(internal::checked_cast<const EqualExpr&>(expr));
        case CompareKind::NOT_EQUAL:
          return visitor(internal::checked_cast<const NotEqualExpr&>(expr));
        case CompareKind::GREATER_THAN:
          return visitor(internal::checked_cast<const GreaterThanExpr&>(expr));
        case CompareKind::GREATER_THAN_EQUAL:
          return visitor(internal::checked_cast<const GreaterThanEqualExpr&>(expr));
        case CompareKind::LESS_THAN:
          return visitor(internal::checked_cast<const LessThanExpr&>(expr));
        case CompareKind::LESS_THAN_EQUAL:
          return visitor(internal::checked_cast<const LessThanEqualExpr&>(expr));
      }

      ARROW_UNREACHABLE;
    }

    case ExprKind::EMPTY_REL:
      return visitor(internal::checked_cast<const EmptyRelExpr&>(expr));
    case ExprKind::SCAN_REL:
      return visitor(internal::checked_cast<const ScanRelExpr&>(expr));
    case ExprKind::PROJECTION_REL:
      return visitor(internal::checked_cast<const ProjectionRelExpr&>(expr));
    case ExprKind::FILTER_REL:
      return visitor(internal::checked_cast<const FilterRelExpr&>(expr));
  }

  ARROW_UNREACHABLE;
}

}  // namespace engine
}  // namespace arrow
