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

#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/compare.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/result.h"
#include "arrow/scalar.h"

namespace arrow {
namespace dataset {

struct FilterType {
  enum type {
    /// Simple boolean predicate consisting of comparisons and boolean
    /// logic (ALL, OR, NOT) involving Schema fields
    EXPRESSION,

    /// Non decomposable filter; must be evaluated against every record batch
    GENERIC
  };
};

class ARROW_DS_EXPORT Filter {
 public:
  explicit Filter(FilterType::type type) : type_(type) {}

  virtual ~Filter() = default;

  FilterType::type type() const { return type_; }

 private:
  FilterType::type type_;
};

/// Filter subclass encapsulating a simple boolean predicate consisting of comparisons
/// and boolean logic (ALL, OR, NOT) involving Schema fields
class ARROW_DS_EXPORT ExpressionFilter : public Filter {
 public:
  explicit ExpressionFilter(const std::shared_ptr<Expression>& expression)
      : Filter(FilterType::EXPRESSION), expression_(std::move(expression)) {}

  const std::shared_ptr<Expression>& expression() const { return expression_; }

 private:
  std::shared_ptr<Expression> expression_;
};

struct ExpressionType {
  enum type {
    /// a reference to a column within a record batch, will evaluate to an array
    FIELD,

    /// a literal singular value encapuslated in a Scalar
    SCALAR,

    /// a literal Array
    // TODO(bkietz) ARRAY,

    /// an inversion of another expression
    NOT,

    /// cast an expression to a given DataType
    // TODO(bkietz) CAST,

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
    // TODO(bkietz) IS_VALID,
  };
};

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

  /// Validate this expression for execution against a schema. This will check that all
  /// reference fields are present (fields not in the schema will be replaced with null)
  /// and all subexpressions are executable. Returns the type to which this expression
  /// will evaluate.
  virtual Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const = 0;

  /// Return a simplified form of this expression given some known conditions.
  /// For example, (a > 3).Assume(a == 5) == (true). This can be used to do less work
  /// in ExpressionFilter when partition conditions guarantee some of this expression.
  /// In the example above, *no* filtering need be done on record batches in the
  /// partition since (a == 5).
  virtual Result<std::shared_ptr<Expression>> Assume(const Expression& given) const {
    return Copy();
  }

  /// Evaluate this expression against each row of a RecordBatch.
  /// Returned Datum will be of either SCALAR or ARRAY kind.
  /// A return value of ARRAY kind will have length == batch.num_rows()
  /// An return value of SCALAR kind is equivalent to an array of the same type whose
  /// slots contain a single repeated value.
  virtual Result<compute::Datum> Evaluate(compute::FunctionContext* ctx,
                                          const RecordBatch& batch) const = 0;

  /// returns a debug string representing this expression
  virtual std::string ToString() const = 0;

  ExpressionType::type type() const { return type_; }

  /// If true, this Expression is a ScalarExpression wrapping a null scalar.
  bool IsNull() const;

  /// If true, this Expression is a ScalarExpression wrapping a
  /// BooleanScalar. Its value may be retrieved at the same time.
  bool IsTrivialCondition(bool* value = NULLPTR) const;

  /// Copy this expression into a shared pointer.
  virtual std::shared_ptr<Expression> Copy() const = 0;

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
  ComparisonExpression(compute::CompareOperator op,
                       std::shared_ptr<Expression> left_operand,
                       std::shared_ptr<Expression> right_operand)
      : ExpressionImpl(std::move(left_operand), std::move(right_operand)), op_(op) {}

  std::string ToString() const override;

  bool Equals(const Expression& other) const override;

  Result<std::shared_ptr<Expression>> Assume(const Expression& given) const override;

  compute::CompareOperator op() const { return op_; }

  Result<compute::Datum> Evaluate(compute::FunctionContext* ctx,
                                  const RecordBatch& batch) const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;

 private:
  Result<std::shared_ptr<Expression>> AssumeGivenComparison(
      const ComparisonExpression& given) const;

  compute::CompareOperator op_;
};

class ARROW_DS_EXPORT AndExpression final
    : public ExpressionImpl<BinaryExpression, AndExpression, ExpressionType::AND> {
 public:
  using ExpressionImpl::ExpressionImpl;

  std::string ToString() const override;

  Result<std::shared_ptr<Expression>> Assume(const Expression& given) const override;

  Result<compute::Datum> Evaluate(compute::FunctionContext* ctx,
                                  const RecordBatch& batch) const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;
};

class ARROW_DS_EXPORT OrExpression final
    : public ExpressionImpl<BinaryExpression, OrExpression, ExpressionType::OR> {
 public:
  using ExpressionImpl::ExpressionImpl;

  std::string ToString() const override;

  Result<std::shared_ptr<Expression>> Assume(const Expression& given) const override;

  Result<compute::Datum> Evaluate(compute::FunctionContext* ctx,
                                  const RecordBatch& batch) const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;
};

class ARROW_DS_EXPORT NotExpression final
    : public ExpressionImpl<UnaryExpression, NotExpression, ExpressionType::NOT> {
 public:
  using ExpressionImpl::ExpressionImpl;

  std::string ToString() const override;

  Result<std::shared_ptr<Expression>> Assume(const Expression& given) const override;

  Result<compute::Datum> Evaluate(compute::FunctionContext* ctx,
                                  const RecordBatch& batch) const override;

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;
};

/// Represents a scalar value; thin wrapper around arrow::Scalar
class ARROW_DS_EXPORT ScalarExpression final : public Expression {
 public:
  explicit ScalarExpression(const std::shared_ptr<Scalar>& value)
      : Expression(ExpressionType::SCALAR), value_(std::move(value)) {}

  const std::shared_ptr<Scalar>& value() const { return value_; }

  std::string ToString() const override;

  bool Equals(const Expression& other) const override;

  static std::shared_ptr<ScalarExpression> Make(bool value) {
    return std::make_shared<ScalarExpression>(std::make_shared<BooleanScalar>(value));
  }

  template <typename T>
  static typename std::enable_if<std::is_integral<T>::value ||
                                     std::is_floating_point<T>::value,
                                 std::shared_ptr<ScalarExpression>>::type
  Make(T value) {
    using ScalarType = typename CTypeTraits<T>::ScalarType;
    return std::make_shared<ScalarExpression>(std::make_shared<ScalarType>(value));
  }

  static std::shared_ptr<ScalarExpression> Make(std::string value);

  static std::shared_ptr<ScalarExpression> Make(const char* value);

  static std::shared_ptr<ScalarExpression> Make(std::shared_ptr<Scalar> value) {
    return std::make_shared<ScalarExpression>(std::move(value));
  }

  static std::shared_ptr<ScalarExpression> MakeNull(
      const std::shared_ptr<DataType>& type);

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override;

  Result<compute::Datum> Evaluate(compute::FunctionContext* ctx,
                                  const RecordBatch& batch) const override;

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

  Result<compute::Datum> Evaluate(compute::FunctionContext* ctx,
                                  const RecordBatch& batch) const override;

  std::shared_ptr<Expression> Copy() const override;

 private:
  std::string name_;
};

ARROW_DS_EXPORT std::shared_ptr<AndExpression> and_(std::shared_ptr<Expression> lhs,
                                                    std::shared_ptr<Expression> rhs);

ARROW_DS_EXPORT AndExpression operator&&(const Expression& lhs, const Expression& rhs);

ARROW_DS_EXPORT std::shared_ptr<OrExpression> or_(std::shared_ptr<Expression> lhs,
                                                  std::shared_ptr<Expression> rhs);

ARROW_DS_EXPORT OrExpression operator||(const Expression& lhs, const Expression& rhs);

ARROW_DS_EXPORT std::shared_ptr<NotExpression> not_(std::shared_ptr<Expression> operand);

ARROW_DS_EXPORT NotExpression operator!(const Expression& rhs);

#define COMPARISON_FACTORY(NAME, FACTORY_NAME, OP)                                     \
  inline std::shared_ptr<ComparisonExpression> FACTORY_NAME(                           \
      const std::shared_ptr<FieldExpression>& lhs,                                     \
      const std::shared_ptr<Expression>& rhs) {                                        \
    return std::make_shared<ComparisonExpression>(compute::CompareOperator::NAME, lhs, \
                                                  rhs);                                \
  }                                                                                    \
                                                                                       \
  template <typename T>                                                                \
  ComparisonExpression operator OP(const FieldExpression& lhs, T&& rhs) {              \
    return ComparisonExpression(compute::CompareOperator::NAME, lhs.Copy(),            \
                                ScalarExpression::Make(std::forward<T>(rhs)));         \
  }
COMPARISON_FACTORY(EQUAL, equal, ==)
COMPARISON_FACTORY(NOT_EQUAL, not_equal, !=)
COMPARISON_FACTORY(GREATER, greater, >)
COMPARISON_FACTORY(GREATER_EQUAL, greater_equal, >=)
COMPARISON_FACTORY(LESS, less, <)
COMPARISON_FACTORY(LESS_EQUAL, less_equal, <=)
#undef COMPARISON_FACTORY

template <typename T>
auto scalar(T&& value) -> decltype(ScalarExpression::Make(std::forward<T>(value))) {
  return ScalarExpression::Make(std::forward<T>(value));
}

inline std::shared_ptr<FieldExpression> field_ref(std::string name) {
  return std::make_shared<FieldExpression>(std::move(name));
}

inline namespace string_literals {
// clang-format off
inline FieldExpression operator"" _(const char* name, size_t name_length) {
  // clang-format on
  return FieldExpression({name, name_length});
}
}  // namespace string_literals

ARROW_DS_EXPORT Result<std::shared_ptr<Expression>> SelectorAssume(
    const std::shared_ptr<DataSelector>& selector,
    const std::shared_ptr<Expression>& given);

ARROW_DS_EXPORT std::shared_ptr<DataSelector> ExpressionSelector(
    std::shared_ptr<Expression> e);

}  // namespace dataset
}  // namespace arrow
