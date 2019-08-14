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

#include "arrow/dataset/visibility.h"
#include "arrow/result.h"
#include "arrow/scalar.h"

namespace arrow {

namespace compute {
class FunctionContext;
}

namespace dataset {

struct FilterType {
  enum type {
    /// Simple boolean predicate consisting of comparisons and boolean
    /// logic (AND, OR, NOT) involving Schema fields
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

class Expression;

/// Filter subclass encapsulating a simple boolean predicate consisting of comparisons and
/// boolean logic (AND, OR, NOT) involving Schema fields
class ARROW_DS_EXPORT ExpressionFilter : public Filter {
 public:
  explicit ExpressionFilter(const std::shared_ptr<Expression>& expression)
      : Filter(FilterType::EXPRESSION), expression_(std::move(expression)) {}

  const std::shared_ptr<Expression>& expression() const { return expression_; }

 private:
  std::shared_ptr<Expression> expression_;
};

/// Evaluate an expression producing a boolean array which encodes whether each row
/// satisfies the condition
Status EvaluateExpression(compute::FunctionContext* ctx, const Expression& condition,
                          const RecordBatch& batch,
                          std::shared_ptr<BooleanArray>* filter);

struct ExpressionType {
  enum type {
    FIELD,
    SCALAR,

    NOT,
    AND,
    OR,

    EQUAL,
    NOT_EQUAL,
    GREATER,
    GREATER_EQUAL,
    LESS,
    LESS_EQUAL,
  };
};

/// Represents an expression tree. The expression can be evaluated against a
/// RecordBatch via ExpressionFilter
class ARROW_DS_EXPORT Expression {
 public:
  explicit Expression(ExpressionType::type type) : type_(type) {}

  virtual ~Expression() = default;

  /// Returns true iff the expressions are identical; does not check for equivalence.
  /// For example, (A and B) is not equal to (B and A) nor is (A and not A) equal to
  /// (false).
  bool Equals(const Expression& other) const;

  bool Equals(const std::shared_ptr<Expression>& other) const;

  /// Validate this expression for execution against a schema. This will check that all
  /// reference fields are present and all subexpressions are executable. Returns a copy
  /// of this expression with schema information incorporated:
  /// - Scalars are cast to other data types if necessary to ensure comparisons are
  ///   between data of identical type
  // virtual Result<std::shared_ptr<Expression>> Validate(const Schema&) const;

  /// returns a debug string representing this expression
  virtual std::string ToString() const = 0;

  ExpressionType::type type() const { return type_; }

  /// If true, this Expression may be safely cast to OperatorExpression
  bool IsOperatorExpression() const;

  /// If true, this Expression may be safely cast to OperatorExpression
  /// and there will be exactly two operands representing the left and right hand sides of
  /// a comparison
  bool IsComparisonExpression() const;

  /// If true, this Expression is a ScalarExpression wrapping a boolean Scalar. Its value
  /// may be retrieved at the same time
  bool IsBooleanScalar(BooleanScalar* value = NULLPTR) const;

  /// Copy this expression into a shared pointer.
  virtual std::shared_ptr<Expression> Copy() const = 0;

 protected:
  ExpressionType::type type_;
};

/// Represents an compound expression; for example comparison between a field and a scalar
/// or a union of other expressions
class ARROW_DS_EXPORT OperatorExpression final : public Expression {
 public:
  OperatorExpression(ExpressionType::type type,
                     std::vector<std::shared_ptr<Expression>> operands)
      : Expression(type), operands_(std::move(operands)) {}

  /// Return a simplified form of this expression given some known conditions.
  /// For example, (a > 3).Assume(a == 5) == (true). This can be used to do less work
  /// in ExpressionFilter when partition conditions guarantee some of this expression.
  /// In the example above, *no* filtering need be done on record batches in the partition
  /// since (a == 5).
  Result<std::shared_ptr<Expression>> Assume(const Expression& given) const;

  const std::vector<std::shared_ptr<Expression>>& operands() const { return operands_; }

  virtual std::string ToString() const override;

  std::shared_ptr<Expression> Copy() const override;

 private:
  std::vector<std::shared_ptr<Expression>> operands_;
};

OperatorExpression operator and(const OperatorExpression& lhs,
                                const OperatorExpression& rhs);
OperatorExpression operator or(const OperatorExpression& lhs,
                               const OperatorExpression& rhs);
OperatorExpression operator not(const OperatorExpression& rhs);

/// Represents a scalar value; thin wrapper around arrow::Scalar
class ARROW_DS_EXPORT ScalarExpression final : public Expression {
 public:
  explicit ScalarExpression(const std::shared_ptr<Scalar>& value)
      : Expression(ExpressionType::SCALAR), value_(std::move(value)) {}

  const std::shared_ptr<Scalar>& value() const { return value_; }

  virtual std::string ToString() const override;

  static std::shared_ptr<ScalarExpression> Make(bool value) {
    return std::make_shared<ScalarExpression>(std::make_shared<BooleanScalar>(value));
  }

  template <typename T>
  static typename std::enable_if<std::is_integral<T>::value,
                                 std::shared_ptr<ScalarExpression>>::type
  Make(T value) {
    return std::make_shared<ScalarExpression>(std::make_shared<Int64Scalar>(value));
  }

  template <typename T>
  static typename std::enable_if<std::is_floating_point<T>::value,
                                 std::shared_ptr<ScalarExpression>>::type
  Make(T value) {
    return std::make_shared<ScalarExpression>(std::make_shared<DoubleScalar>(value));
  }

  static std::shared_ptr<ScalarExpression> Make(std::string value);

  static std::shared_ptr<ScalarExpression> Make(const char* value);

  static std::shared_ptr<ScalarExpression> MakeNull() {
    return std::make_shared<ScalarExpression>(std::make_shared<BooleanScalar>());
  }

  std::shared_ptr<Expression> Copy() const override;

 private:
  std::shared_ptr<Scalar> value_;
};

/// Represents a reference to a field. Stores only the field's name (type and other
/// information is known only when a Schema is provided)
class ARROW_DS_EXPORT FieldReferenceExpression final : public Expression {
 public:
  explicit FieldReferenceExpression(std::string name)
      : Expression(ExpressionType::FIELD), name_(std::move(name)) {}

  std::string name() const { return name_; }

  virtual std::string ToString() const override;

  std::shared_ptr<Expression> Copy() const override;

 private:
  std::string name_;
};

#define COMPARISON_FACTORY(NAME, OP)                                             \
  template <typename T>                                                          \
  OperatorExpression operator OP(const FieldReferenceExpression& lhs, T&& rhs) { \
    return OperatorExpression(                                                   \
        ExpressionType::NAME,                                                    \
        {lhs.Copy(), ScalarExpression::Make(std::forward<T>(rhs))});             \
  }
COMPARISON_FACTORY(EQUAL, ==)
COMPARISON_FACTORY(NOT_EQUAL, !=)
COMPARISON_FACTORY(GREATER, >)
COMPARISON_FACTORY(GREATER_EQUAL, >=)
COMPARISON_FACTORY(LESS, <)
COMPARISON_FACTORY(LESS_EQUAL, <=)
#undef COMPARISON_FACTORY

inline namespace string_literals {
FieldReferenceExpression operator""_(const char* name, size_t name_length) {
  return FieldReferenceExpression({name, name_length});
}
}  // namespace string_literals

}  // namespace dataset
}  // namespace arrow
