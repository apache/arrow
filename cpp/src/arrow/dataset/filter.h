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
namespace dataset {

struct FilterType {
  enum type {
    /// Simple boolean predicate consisting of comparisons and boolean
    /// logic (AND, OR, NOT) involving Schema fields
    EXPRESSION,

    ///
    GENERIC
  };
};

class ARROW_DS_EXPORT Filter {
 public:
  explicit Filter(FilterType::type type) : type_(type) {}

  virtual ~Filter() = 0;

  FilterType::type type() const { return type_; }

 private:
  FilterType::type type_;
};

class Expression;

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
    FIELD,
    SCALAR,
    FROM_STRING,

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

template <typename T>
std::shared_ptr<typename std::decay<T>::type> MakeShared(T&& t) {
  return std::make_shared<typename std::decay<T>::type>(std::forward<T>(t));
}

class ARROW_DS_EXPORT Expression {
 public:
  explicit Expression(ExpressionType::type type) : type_(type) {}

  virtual ~Expression() = default;

  ExpressionType::type type() const { return type_; }

  bool IsOperatorExpression() const {
    return static_cast<int>(type_) >= static_cast<int>(ExpressionType::NOT) &&
           static_cast<int>(type_) <= static_cast<int>(ExpressionType::LESS_EQUAL);
  }

  bool IsComparisonExpression() const {
    return static_cast<int>(type_) >= static_cast<int>(ExpressionType::EQUAL) &&
           static_cast<int>(type_) <= static_cast<int>(ExpressionType::LESS_EQUAL);
  }

 protected:
  ExpressionType::type type_;
};

class ARROW_DS_EXPORT OperatorExpression : public Expression {
 public:
  OperatorExpression(ExpressionType::type type,
                     std::vector<std::shared_ptr<Expression>> operands)
      : Expression(type), operands_(std::move(operands)) {}

  const std::vector<std::shared_ptr<Expression>>& operands() const { return operands_; }

  OperatorExpression operator and(const OperatorExpression& other) const {
    return OperatorExpression(ExpressionType::AND,
                              {MakeShared(*this), MakeShared(other)});
  }

  OperatorExpression operator or(const OperatorExpression& other) const {
    return OperatorExpression(ExpressionType::OR, {MakeShared(*this), MakeShared(other)});
  }

  OperatorExpression operator not() const {
    return OperatorExpression(ExpressionType::NOT, {MakeShared(*this)});
  }

  Result<std::shared_ptr<Expression>> Assume(const Expression& given) const;

 private:
  std::vector<std::shared_ptr<Expression>> operands_;
};

class ARROW_DS_EXPORT ScalarExpression : public Expression {
 public:
  explicit ScalarExpression(const std::shared_ptr<Scalar>& value)
      : Expression(ExpressionType::SCALAR), value_(std::move(value)) {}

  const std::shared_ptr<Scalar>& value() const { return value_; }

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

 private:
  std::shared_ptr<Scalar> value_;
};

class ARROW_DS_EXPORT FieldReferenceExpression : public Expression {
 public:
  explicit FieldReferenceExpression(std::string name)
      : Expression(ExpressionType::FIELD), name_(std::move(name)) {}

  std::string name() const { return name_; }

  template <typename T>
  OperatorExpression operator==(T&& value) const {
    return Comparison(ExpressionType::EQUAL, std::forward<T>(value));
  }

  template <typename T>
  OperatorExpression operator!=(T&& value) const {
    return Comparison(ExpressionType::NOT_EQUAL, std::forward<T>(value));
  }

  template <typename T>
  OperatorExpression operator>(T&& value) const {
    return Comparison(ExpressionType::GREATER, std::forward<T>(value));
  }

  template <typename T>
  OperatorExpression operator>=(T&& value) const {
    return Comparison(ExpressionType::GREATER_EQUAL, std::forward<T>(value));
  }

  template <typename T>
  OperatorExpression operator<(T&& value) const {
    return Comparison(ExpressionType::LESS, std::forward<T>(value));
  }

  template <typename T>
  OperatorExpression operator<=(T&& value) const {
    return Comparison(ExpressionType::LESS_EQUAL, std::forward<T>(value));
  }

 private:
  template <typename T>
  OperatorExpression Comparison(ExpressionType::type type, T&& value) const {
    return OperatorExpression(
        type, {MakeShared(*this), ScalarExpression::Make(std::forward<T>(value))});
  }

  std::string name_;
};

inline namespace string_literals {
FieldReferenceExpression operator""_(const char* name, size_t name_length) {
  return FieldReferenceExpression({name, name_length});
}
}  // namespace string_literals

}  // namespace dataset
}  // namespace arrow
