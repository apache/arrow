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

// This API is EXPERIMENTAL.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/variant.h"

namespace arrow {
namespace dataset {

using compute::CastOptions;
using compute::CompareOperator;

struct ExpressionType {
  enum type {
    /// a reference to a column within a record batch, will evaluate to an array
    FIELD,

    /// a literal singular value encapsulated in a Scalar
    SCALAR,

    /// a literal Array
    // TODO(bkietz) ARRAY,

    /// an inversion of another expression
    NOT,

    /// cast an expression to a given DataType
    CAST,

    /// a conjunction of multiple expressions (true if all operands are true)
    AND,

    /// a disjunction of multiple expressions (true if any operand is true)
    OR,

    /// a comparison of two other expressions
    COMPARISON,

    /// replace nulls with other expressions
    /// currently only boolean expressions may be coalesced
    // TODO(bkietz) COALESCE,

    /// extract validity as a boolean expression
    IS_VALID,

    /// check each element for membership in a set
    IN,

    /// custom user defined expression
    CUSTOM,
  };
};

class InExpression;
class CastExpression;
class IsValidExpression;

/// Represents an expression tree
class ARROW_DS_EXPORT Expression {
 public:
  explicit Expression(ExpressionType::type type) : type_(type) {}

  virtual ~Expression() = default;

  /// Returns true iff the expressions are identical; does not check for equivalence.
  /// For example, (A and B) is not equal to (B and A) nor is (A and not A) equal to
  /// (false).
  virtual bool Equals(const Expression& other) const = 0;

  bool Equals(const std::shared_ptr<Expression>& other) const;

  /// Overload for the common case of checking for equality to a specific scalar.
  template <typename T, typename Enable = decltype(MakeScalar(std::declval<T>()))>
  bool Equals(T&& t) const;

  /// If true, this Expression is a ScalarExpression wrapping a null scalar.
  bool IsNull() const;

  /// Validate this expression for execution against a schema. This will check that all
  /// reference fields are present (fields not in the schema will be replaced with null)
  /// and all subexpressions are executable. Returns the type to which this expression
  /// will evaluate.
  virtual Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const = 0;

  /// \brief Simplify to an equivalent Expression given assumed constraints on input.
  /// This can be used to do less filtering work using predicate push down.
  ///
  /// Both expressions must pass validation against a schema before Assume may be used.
  ///
  /// Two expressions can be considered equivalent for a given subset of possible inputs
  /// if they yield identical results. Formally, if given.Evaluate(input).Equals(input)
  /// then Assume guarantees that:
  ///     expr.Assume(given).Evaluate(input).Equals(expr.Evaluate(input))
  ///
  /// For example if we are given that all inputs will
  /// satisfy ("a"_ == 1) then the expression ("a"_ > 0 and "b"_ > 0) is equivalent to
  /// ("b"_ > 0). It is impossible that the comparison ("a"_ > 0) will evaluate false
  /// given ("a"_ == 1), so both expressions will yield identical results. Thus we can
  /// write:
  ///     ("a"_ > 0 and "b"_ > 0).Assume("a"_ == 1).Equals("b"_ > 0)
  ///
  /// filter.Assume(partition) is trivial if filter and partition are disjoint or if
  /// partition is a subset of filter. FIXME(bkietz) write this better
  /// - If the two are disjoint, then (false) may be substituted for filter.
  /// - If partition is a subset of filter then (true) may be substituted for filter.
  ///
  /// filter.Assume(partition) is straightforward if both filter and partition are simple
  /// comparisons.
  /// - filter may be a superset of partition, in which case the filter is
  ///   satisfied by all inputs:
  ///     ("a"_ > 0).Assume("a"_ == 1).Equals(true)
  /// - filter may be disjoint with partition, in which case there are no inputs which
  ///   satisfy filter:
  ///     ("a"_ < 0).Assume("a"_ == 1).Equals(false)
  /// - If neither of these is the case, partition provides no information which can
  ///   simplify filter:
  ///     ("a"_ == 1).Assume("a"_ > 0).Equals("a"_ == 1)
  ///     ("a"_ == 1).Assume("b"_ == 1).Equals("a"_ == 1)
  ///
  /// If filter is compound, Assume can be distributed across the boolean operator. To
  /// prove this is valid, we again demonstrate that the simplified expression will yield
  /// identical results. For conjunction of filters lhs and rhs:
  ///     (lhs.Assume(p) and rhs.Assume(p)).Evaluate(input)
  ///     == Intersection(lhs.Assume(p).Evaluate(input), rhs.Assume(p).Evaluate(input))
  ///     == Intersection(lhs.Evaluate(input), rhs.Evaluate(input))
  ///     == (lhs and rhs).Evaluate(input)
  /// - The proof for disjunction is symmetric; just replace Intersection with Union. Thus
  ///   we can write:
  ///     (lhs and rhs).Assume(p).Equals(lhs.Assume(p) and rhs.Assume(p))
  ///     (lhs or rhs).Assume(p).Equals(lhs.Assume(p) or rhs.Assume(p))
  /// - For negation:
  ///     (not e.Assume(p)).Evaluate(input)
  ///     == Difference(input, e.Assume(p).Evaluate(input))
  ///     == Difference(input, e.Evaluate(input))
  ///     == (not e).Evaluate(input)
  /// - Thus we can write:
  ///     (not e).Assume(p).Equals(not e.Assume(p))
  ///
  /// If the partition expression is a conjunction then each of its subexpressions is
  /// true for all input and can be used independently:
  ///     filter.Assume(lhs).Assume(rhs).Evaluate(input)
  ///     == filter.Assume(lhs).Evaluate(input)
  ///     == filter.Evaluate(input)
  /// - Thus we can write:
  ///     filter.Assume(lhs and rhs).Equals(filter.Assume(lhs).Assume(rhs))
  ///
  /// FIXME(bkietz) disjunction proof
  ///     filter.Assume(lhs or rhs).Equals(filter.Assume(lhs) and filter.Assume(rhs))
  /// - This may not result in a simpler expression so it is only used when
  ///     filter.Assume(lhs).Equals(filter.Assume(rhs))
  ///
  /// If the partition expression is a negation then we can use the above relations by
  /// replacing comparisons with their complements and using the properties:
  ///     (not (a and b)).Equals(not a or not b)
  ///     (not (a or b)).Equals(not a and not b)
  virtual std::shared_ptr<Expression> Assume(const Expression& given) const;

  std::shared_ptr<Expression> Assume(const std::shared_ptr<Expression>& given) const {
    return Assume(*given);
  }

  /// Indicates if the expression is satisfiable.
  ///
  /// This is a shortcut to check if the expression is neither null nor false.
  bool IsSatisfiable() const { return !IsNull() && !Equals(false); }

  /// Indicates if the expression is satisfiable given an other expression.
  ///
  /// This behaves like IsSatisfiable, but it simplifies the current expression
  /// with the given `other` information.
  bool IsSatisfiableWith(const Expression& other) const {
    return Assume(other)->IsSatisfiable();
  }

  bool IsSatisfiableWith(const std::shared_ptr<Expression>& other) const {
    return Assume(other)->IsSatisfiable();
  }

  /// returns a debug string representing this expression
  virtual std::string ToString() const = 0;

  /// serialize/deserialize an Expression.
  Result<std::shared_ptr<Buffer>> Serialize() const;
  static Result<std::shared_ptr<Expression>> Deserialize(const Buffer&);

  /// \brief Return the expression's type identifier
  ExpressionType::type type() const { return type_; }

  /// Copy this expression into a shared pointer.
  virtual std::shared_ptr<Expression> Copy() const = 0;

  InExpression In(std::shared_ptr<Array> set) const;

  IsValidExpression IsValid() const;

  CastExpression CastTo(std::shared_ptr<DataType> type,
                        CastOptions options = CastOptions()) const;

  CastExpression CastLike(const Expression& expr,
                          CastOptions options = CastOptions()) const;

  CastExpression CastLike(std::shared_ptr<Expression> expr,
                          CastOptions options = CastOptions()) const;

 protected:
  ExpressionType::type type_;
};

/// Helper class which implements Copy and forwards construction
template <typename Base, typename Derived, ExpressionType::type E>
class ExpressionImpl : public Base {
 public:
  static constexpr ExpressionType::type expression_type = E;

  template <typename A0, typename... A>
  explicit ExpressionImpl(A0&& arg0, A&&... args)
      : Base(expression_type, std::forward<A0>(arg0), std::forward<A>(args)...) {}

  std::shared_ptr<Expression> Copy() const override {
    return std::make_shared<Derived>(internal::checked_cast<const Derived&>(*this));
  }
};

/// Base class for an expression with exactly one operand
class ARROW_DS_EXPORT UnaryExpression : public Expression {
 public:
  const std::shared_ptr<Expression>& operand() const { return operand_; }

  bool Equals(const Expression& other) const override;

 protected:
  UnaryExpression(ExpressionType::type type, std::shared_ptr<Expression> operand)
      : Expression(type), operand_(std::move(operand)) {}

  std::shared_ptr<Expression> operand_;
};

/// Base class for an expression with exactly two operands
class ARROW_DS_EXPORT BinaryExpression : public Expression {
 public:
  const std::shared_ptr<Expression>& left_operand() const { return left_operand_; }

  const std::shared_ptr<Expression>& right_operand() const { return right_operand_; }

  bool Equals(const Expression& other) const override;

 protected:
  BinaryExpression(ExpressionType::type type, std::shared_ptr<Expression> left_operand,
                   std::shared_ptr<Expression> right_operand)
      : Expression(type),
        left_operand_(std::move(left_operand)),
        right_operand_(std::move(right_operand)) {}

  std::shared_ptr<Expression> left_operand_, right_operand_;
};

class ARROW_DS_EXPORT ComparisonExpression final
    : public ExpressionImpl<BinaryExpression, ComparisonExpression,
                            ExpressionType::COMPARISON> {
 public:
  ComparisonExpression(CompareOperator op, std::shared_ptr<Expression> left_operand,
                       std::shared_ptr<Expression> right_operand)
      : ExpressionImpl(std::move(left_operand), std::move(right_operand)), op_(op) {}

  std::string ToString() const override;

  bool Equals(const Expression& other) const override;

  std::shared_ptr<Expression> Assume(const Expression& given) const override;

  CompareOperator op() const { return op_; }

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;

 private:
  std::shared_ptr<Expression> AssumeGivenComparison(
      const ComparisonExpression& given) const;

  CompareOperator op_;
};

class ARROW_DS_EXPORT AndExpression final
    : public ExpressionImpl<BinaryExpression, AndExpression, ExpressionType::AND> {
 public:
  using ExpressionImpl::ExpressionImpl;

  std::string ToString() const override;

  std::shared_ptr<Expression> Assume(const Expression& given) const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;
};

class ARROW_DS_EXPORT OrExpression final
    : public ExpressionImpl<BinaryExpression, OrExpression, ExpressionType::OR> {
 public:
  using ExpressionImpl::ExpressionImpl;

  std::string ToString() const override;

  std::shared_ptr<Expression> Assume(const Expression& given) const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;
};

class ARROW_DS_EXPORT NotExpression final
    : public ExpressionImpl<UnaryExpression, NotExpression, ExpressionType::NOT> {
 public:
  using ExpressionImpl::ExpressionImpl;

  std::string ToString() const override;

  std::shared_ptr<Expression> Assume(const Expression& given) const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;
};

class ARROW_DS_EXPORT IsValidExpression final
    : public ExpressionImpl<UnaryExpression, IsValidExpression,
                            ExpressionType::IS_VALID> {
 public:
  using ExpressionImpl::ExpressionImpl;

  std::string ToString() const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;

  std::shared_ptr<Expression> Assume(const Expression& given) const override;
};

class ARROW_DS_EXPORT InExpression final
    : public ExpressionImpl<UnaryExpression, InExpression, ExpressionType::IN> {
 public:
  InExpression(std::shared_ptr<Expression> operand, std::shared_ptr<Array> set)
      : ExpressionImpl(std::move(operand)), set_(std::move(set)) {}

  std::string ToString() const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;

  std::shared_ptr<Expression> Assume(const Expression& given) const override;

  /// The set against which the operand will be compared
  const std::shared_ptr<Array>& set() const { return set_; }

 private:
  std::shared_ptr<Array> set_;
};

/// Explicitly cast an expression to a different type
class ARROW_DS_EXPORT CastExpression final
    : public ExpressionImpl<UnaryExpression, CastExpression, ExpressionType::CAST> {
 public:
  CastExpression(std::shared_ptr<Expression> operand, std::shared_ptr<DataType> to,
                 CastOptions options)
      : ExpressionImpl(std::move(operand)),
        to_(std::move(to)),
        options_(std::move(options)) {}

  /// The operand will be cast to whatever type `like` would evaluate to, given the same
  /// schema.
  CastExpression(std::shared_ptr<Expression> operand, std::shared_ptr<Expression> like,
                 CastOptions options)
      : ExpressionImpl(std::move(operand)),
        to_(std::move(like)),
        options_(std::move(options)) {}

  std::string ToString() const override;

  std::shared_ptr<Expression> Assume(const Expression& given) const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;

  const CastOptions& options() const { return options_; }

  /// Return the target type of this CastTo expression, or nullptr if this is a
  /// CastLike expression.
  const std::shared_ptr<DataType>& to_type() const;

  /// Return the target expression of this CastLike expression, or nullptr if
  /// this is a CastTo expression.
  const std::shared_ptr<Expression>& like_expr() const;

 private:
  util::variant<std::shared_ptr<DataType>, std::shared_ptr<Expression>> to_;
  CastOptions options_;
};

/// Represents a scalar value; thin wrapper around arrow::Scalar
class ARROW_DS_EXPORT ScalarExpression final : public Expression {
 public:
  explicit ScalarExpression(const std::shared_ptr<Scalar>& value)
      : Expression(ExpressionType::SCALAR), value_(std::move(value)) {}

  const std::shared_ptr<Scalar>& value() const { return value_; }

  std::string ToString() const override;

  bool Equals(const Expression& other) const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;

  std::shared_ptr<Expression> Copy() const override;

 private:
  std::shared_ptr<Scalar> value_;
};

/// Represents a reference to a field. Stores only the field's name (type and other
/// information is known only when a Schema is provided)
class ARROW_DS_EXPORT FieldExpression final : public Expression {
 public:
  explicit FieldExpression(std::string name)
      : Expression(ExpressionType::FIELD), name_(std::move(name)) {}

  std::string name() const { return name_; }

  std::string ToString() const override;

  bool Equals(const Expression& other) const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;

  std::shared_ptr<Expression> Copy() const override;

 private:
  std::string name_;
};

class ARROW_DS_EXPORT CustomExpression : public Expression {
 protected:
  CustomExpression() : Expression(ExpressionType::CUSTOM) {}
};

ARROW_DS_EXPORT std::shared_ptr<Expression> and_(std::shared_ptr<Expression> lhs,
                                                 std::shared_ptr<Expression> rhs);

ARROW_DS_EXPORT std::shared_ptr<Expression> and_(const ExpressionVector& subexpressions);

ARROW_DS_EXPORT AndExpression operator&&(const Expression& lhs, const Expression& rhs);

ARROW_DS_EXPORT std::shared_ptr<Expression> or_(std::shared_ptr<Expression> lhs,
                                                std::shared_ptr<Expression> rhs);

ARROW_DS_EXPORT std::shared_ptr<Expression> or_(const ExpressionVector& subexpressions);

ARROW_DS_EXPORT OrExpression operator||(const Expression& lhs, const Expression& rhs);

ARROW_DS_EXPORT std::shared_ptr<Expression> not_(std::shared_ptr<Expression> operand);

ARROW_DS_EXPORT NotExpression operator!(const Expression& rhs);

inline std::shared_ptr<Expression> scalar(std::shared_ptr<Scalar> value) {
  return std::make_shared<ScalarExpression>(std::move(value));
}

template <typename T>
auto scalar(T&& value) -> decltype(scalar(MakeScalar(std::forward<T>(value)))) {
  return scalar(MakeScalar(std::forward<T>(value)));
}

#define COMPARISON_FACTORY(NAME, FACTORY_NAME, OP)                                      \
  inline std::shared_ptr<ComparisonExpression> FACTORY_NAME(                            \
      const std::shared_ptr<Expression>& lhs, const std::shared_ptr<Expression>& rhs) { \
    return std::make_shared<ComparisonExpression>(CompareOperator::NAME, lhs, rhs);     \
  }                                                                                     \
                                                                                        \
  template <typename T, typename Enable = typename std::enable_if<!std::is_base_of<     \
                            Expression, typename std::decay<T>::type>::value>::type>    \
  ComparisonExpression operator OP(const Expression& lhs, T&& rhs) {                    \
    return ComparisonExpression(CompareOperator::NAME, lhs.Copy(),                      \
                                scalar(std::forward<T>(rhs)));                          \
  }                                                                                     \
                                                                                        \
  inline ComparisonExpression operator OP(const Expression& lhs,                        \
                                          const Expression& rhs) {                      \
    return ComparisonExpression(CompareOperator::NAME, lhs.Copy(), rhs.Copy());         \
  }
COMPARISON_FACTORY(EQUAL, equal, ==)
COMPARISON_FACTORY(NOT_EQUAL, not_equal, !=)
COMPARISON_FACTORY(GREATER, greater, >)
COMPARISON_FACTORY(GREATER_EQUAL, greater_equal, >=)
COMPARISON_FACTORY(LESS, less, <)
COMPARISON_FACTORY(LESS_EQUAL, less_equal, <=)
#undef COMPARISON_FACTORY

inline std::shared_ptr<Expression> field_ref(std::string name) {
  return std::make_shared<FieldExpression>(std::move(name));
}

inline namespace string_literals {
// clang-format off
inline FieldExpression operator"" _(const char* name, size_t name_length) {
  // clang-format on
  return FieldExpression({name, name_length});
}
}  // namespace string_literals

template <typename T, typename Enable>
bool Expression::Equals(T&& t) const {
  if (type_ != ExpressionType::SCALAR) {
    return false;
  }
  auto s = MakeScalar(std::forward<T>(t));
  return internal::checked_cast<const ScalarExpression&>(*this).value()->Equals(*s);
}

template <typename Visitor>
auto VisitExpression(const Expression& expr, Visitor&& visitor)
    -> decltype(visitor(expr)) {
  switch (expr.type()) {
    case ExpressionType::FIELD:
      return visitor(internal::checked_cast<const FieldExpression&>(expr));

    case ExpressionType::SCALAR:
      return visitor(internal::checked_cast<const ScalarExpression&>(expr));

    case ExpressionType::IN:
      return visitor(internal::checked_cast<const InExpression&>(expr));

    case ExpressionType::IS_VALID:
      return visitor(internal::checked_cast<const IsValidExpression&>(expr));

    case ExpressionType::AND:
      return visitor(internal::checked_cast<const AndExpression&>(expr));

    case ExpressionType::OR:
      return visitor(internal::checked_cast<const OrExpression&>(expr));

    case ExpressionType::NOT:
      return visitor(internal::checked_cast<const NotExpression&>(expr));

    case ExpressionType::CAST:
      return visitor(internal::checked_cast<const CastExpression&>(expr));

    case ExpressionType::COMPARISON:
      return visitor(internal::checked_cast<const ComparisonExpression&>(expr));

    case ExpressionType::CUSTOM:
    default:
      break;
  }
  return visitor(internal::checked_cast<const CustomExpression&>(expr));
}

/// \brief Visit each subexpression of an arbitrarily nested conjunction.
///
/// | given                          | visit                                       |
/// |--------------------------------|---------------------------------------------|
/// | a and b                        | visit(a), visit(b)                          |
/// | c                              | visit(c)                                    |
/// | (a and b) and ((c or d) and e) | visit(a), visit(b), visit(c or d), visit(e) |
ARROW_DS_EXPORT Status VisitConjunctionMembers(
    const Expression& expr, const std::function<Status(const Expression&)>& visitor);

/// \brief Insert CastExpressions where necessary to make a valid expression.
ARROW_DS_EXPORT Result<std::shared_ptr<Expression>> InsertImplicitCasts(
    const Expression& expr, const Schema& schema);

/// \brief Returns field names referenced in the expression.
ARROW_DS_EXPORT std::vector<std::string> FieldsInExpression(const Expression& expr);

ARROW_DS_EXPORT std::vector<std::string> FieldsInExpression(
    const std::shared_ptr<Expression>& expr);

/// Interface for evaluation of expressions against record batches.
class ARROW_DS_EXPORT ExpressionEvaluator {
 public:
  virtual ~ExpressionEvaluator() = default;

  /// Evaluate expr against each row of a RecordBatch.
  /// Returned Datum will be of either SCALAR or ARRAY kind.
  /// A return value of ARRAY kind will have length == batch.num_rows()
  /// An return value of SCALAR kind is equivalent to an array of the same type whose
  /// slots contain a single repeated value.
  ///
  /// expr must be validated against the schema of batch before calling this method.
  virtual Result<Datum> Evaluate(const Expression& expr, const RecordBatch& batch,
                                 MemoryPool* pool) const = 0;

  Result<Datum> Evaluate(const Expression& expr, const RecordBatch& batch) const {
    return Evaluate(expr, batch, default_memory_pool());
  }

  virtual Result<std::shared_ptr<RecordBatch>> Filter(
      const Datum& selection, const std::shared_ptr<RecordBatch>& batch,
      MemoryPool* pool) const = 0;

  Result<std::shared_ptr<RecordBatch>> Filter(
      const Datum& selection, const std::shared_ptr<RecordBatch>& batch) const {
    return Filter(selection, batch, default_memory_pool());
  }

  /// \brief Wrap an iterator of record batches with a filter expression. The resulting
  /// iterator will yield record batches filtered by the given expression.
  ///
  /// \note The ExpressionEvaluator must outlive the returned iterator.
  RecordBatchIterator FilterBatches(RecordBatchIterator unfiltered,
                                    std::shared_ptr<Expression> filter,
                                    MemoryPool* pool = default_memory_pool());

  /// construct an Evaluator which evaluates all expressions to null and does no
  /// filtering
  static std::shared_ptr<ExpressionEvaluator> Null();
};

/// construct an Evaluator which uses compute kernels to evaluate expressions and
/// filter record batches in depth first order
class ARROW_DS_EXPORT TreeEvaluator : public ExpressionEvaluator {
 public:
  Result<Datum> Evaluate(const Expression& expr, const RecordBatch& batch,
                         MemoryPool* pool) const override;

  Result<std::shared_ptr<RecordBatch>> Filter(const Datum& selection,
                                              const std::shared_ptr<RecordBatch>& batch,
                                              MemoryPool* pool) const override;

 protected:
  struct Impl;
};

/// \brief Assemble lists of indices of identical rows.
///
/// \param[in] by A StructArray whose columns will be used as grouping criteria.
/// \return A StructArray mapping unique rows (in field "values", represented as a
///         StructArray with the same fields as `by`) to lists of indices where
///         that row appears (in field "groupings").
ARROW_DS_EXPORT
Result<std::shared_ptr<StructArray>> MakeGroupings(const StructArray& by);

/// \brief Produce slices of an Array which correspond to the provided groupings.
ARROW_DS_EXPORT
Result<std::shared_ptr<ListArray>> ApplyGroupings(const ListArray& groupings,
                                                  const Array& array);
ARROW_DS_EXPORT
Result<RecordBatchVector> ApplyGroupings(const ListArray& groupings,
                                         const std::shared_ptr<RecordBatch>& batch);

}  // namespace dataset
}  // namespace arrow
