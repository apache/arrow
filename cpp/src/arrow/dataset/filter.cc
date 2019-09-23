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

#include "arrow/dataset/filter.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <unordered_map>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernels/boolean.h"
#include "arrow/compute/kernels/compare.h"
#include "arrow/dataset/dataset.h"
#include "arrow/record_batch.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace dataset {

using arrow::compute::Datum;
using internal::checked_cast;
using internal::checked_pointer_cast;

Result<Datum> ScalarExpression::Evaluate(compute::FunctionContext* ctx,
                                         const RecordBatch& batch) const {
  return value_;
}

Datum NullDatum() { return Datum(std::make_shared<NullScalar>()); }

Result<Datum> FieldExpression::Evaluate(compute::FunctionContext* ctx,
                                        const RecordBatch& batch) const {
  auto column = batch.GetColumnByName(name_);
  if (column == nullptr) {
    return NullDatum();
  }
  return std::move(column);
}

bool IsNullDatum(const Datum& datum) {
  if (datum.is_scalar()) {
    auto scalar = datum.scalar();
    return !scalar->is_valid;
  }

  auto array_data = datum.array();
  return array_data->GetNullCount() == array_data->length;
}

bool IsTrivialConditionDatum(const Datum& datum, bool* condition = nullptr) {
  if (!datum.is_scalar()) {
    return false;
  }

  auto scalar = datum.scalar();
  if (!scalar->is_valid) {
    return false;
  }

  if (scalar->type->id() != Type::BOOL) {
    return false;
  }

  if (condition) {
    *condition = checked_cast<const BooleanScalar&>(*scalar).value;
  }
  return true;
}

Result<Datum> NotExpression::Evaluate(compute::FunctionContext* ctx,
                                      const RecordBatch& batch) const {
  ARROW_ASSIGN_OR_RAISE(auto to_invert, operand_->Evaluate(ctx, batch));
  if (IsNullDatum(to_invert)) {
    return NullDatum();
  }

  bool trivial_condition;
  if (IsTrivialConditionDatum(to_invert, &trivial_condition)) {
    return Datum(std::make_shared<BooleanScalar>(!trivial_condition));
  }

  DCHECK(to_invert.is_array());
  Datum out;
  RETURN_NOT_OK(arrow::compute::Invert(ctx, Datum(to_invert), &out));
  return std::move(out);
}

Result<Datum> AndExpression::Evaluate(compute::FunctionContext* ctx,
                                      const RecordBatch& batch) const {
  ARROW_ASSIGN_OR_RAISE(auto lhs, left_operand_->Evaluate(ctx, batch));
  ARROW_ASSIGN_OR_RAISE(auto rhs, right_operand_->Evaluate(ctx, batch));

  if (IsNullDatum(lhs) || IsNullDatum(rhs)) {
    return NullDatum();
  }

  if (lhs.is_array() && rhs.is_array()) {
    Datum out;
    RETURN_NOT_OK(arrow::compute::And(ctx, lhs, rhs, &out));
    return std::move(out);
  }

  if (lhs.is_scalar() && rhs.is_scalar()) {
    return Datum(checked_cast<const BooleanScalar&>(*lhs.scalar()).value &&
                 checked_cast<const BooleanScalar&>(*rhs.scalar()).value);
  }

  auto array_operand = (lhs.is_array() ? lhs : rhs).make_array();
  bool scalar_operand =
      checked_cast<const BooleanScalar&>(*(lhs.is_scalar() ? lhs : rhs).scalar()).value;

  if (!scalar_operand) {
    // FIXME(bkietz) this is an error if array_operand contains nulls
    return Datum(false);
  }

  return Datum(array_operand);
}

Result<Datum> OrExpression::Evaluate(compute::FunctionContext* ctx,
                                     const RecordBatch& batch) const {
  ARROW_ASSIGN_OR_RAISE(auto lhs, left_operand_->Evaluate(ctx, batch));
  ARROW_ASSIGN_OR_RAISE(auto rhs, right_operand_->Evaluate(ctx, batch));

  if (IsNullDatum(lhs) || IsNullDatum(rhs)) {
    return NullDatum();
  }

  if (lhs.is_array() && rhs.is_array()) {
    Datum out;
    RETURN_NOT_OK(arrow::compute::Or(ctx, lhs, rhs, &out));
    return std::move(out);
  }

  if (lhs.is_scalar() && rhs.is_scalar()) {
    return Datum(checked_cast<const BooleanScalar&>(*lhs.scalar()).value &&
                 checked_cast<const BooleanScalar&>(*rhs.scalar()).value);
  }

  auto array_operand = (lhs.is_array() ? lhs : rhs).make_array();
  bool scalar_operand =
      checked_cast<const BooleanScalar&>(*(lhs.is_scalar() ? lhs : rhs).scalar()).value;

  if (!scalar_operand) {
    // FIXME(bkietz) this is an error if array_operand contains nulls
    return Datum(true);
  }

  return Datum(array_operand);
}

Result<Datum> ComparisonExpression::Evaluate(compute::FunctionContext* ctx,
                                             const RecordBatch& batch) const {
  ARROW_ASSIGN_OR_RAISE(auto lhs, left_operand_->Evaluate(ctx, batch));
  ARROW_ASSIGN_OR_RAISE(auto rhs, right_operand_->Evaluate(ctx, batch));

  if (IsNullDatum(lhs) || IsNullDatum(rhs)) {
    return NullDatum();
  }

  if (lhs.is_scalar()) {
    return Status::NotImplemented("comparison with scalar LHS");
  }

  Datum out;
  RETURN_NOT_OK(
      arrow::compute::Compare(ctx, lhs, rhs, arrow::compute::CompareOptions(op_), &out));
  return std::move(out);
}

std::shared_ptr<ScalarExpression> ScalarExpression::Make(std::string value) {
  return std::make_shared<ScalarExpression>(
      std::make_shared<StringScalar>(Buffer::FromString(std::move(value))));
}

std::shared_ptr<ScalarExpression> ScalarExpression::Make(const char* value) {
  return std::make_shared<ScalarExpression>(
      std::make_shared<StringScalar>(Buffer::Wrap(value, std::strlen(value))));
}

std::shared_ptr<ScalarExpression> ScalarExpression::MakeNull(
    const std::shared_ptr<DataType>& type) {
  std::shared_ptr<Scalar> null;
  DCHECK_OK(arrow::MakeNullScalar(type, &null));
  return Make(std::move(null));
}

struct Comparison {
  enum type {
    LESS,
    EQUAL,
    GREATER,
    NULL_,
  };
};

struct CompareVisitor {
  template <typename T>
  using ScalarType = typename TypeTraits<T>::ScalarType;

  Status Visit(const NullType&) {
    result_ = Comparison::NULL_;
    return Status::OK();
  }

  Status Visit(const BooleanType&) { return CompareValues<BooleanType>(); }

  template <typename T>
  enable_if_number<T, Status> Visit(const T&) {
    return CompareValues<T>();
  }

  template <typename T>
  enable_if_binary_like<T, Status> Visit(const T&) {
    auto lhs = checked_cast<const ScalarType<T>&>(lhs_).value;
    auto rhs = checked_cast<const ScalarType<T>&>(rhs_).value;
    auto cmp = std::memcmp(lhs->data(), rhs->data(), std::min(lhs->size(), rhs->size()));
    if (cmp == 0) {
      return CompareValues(lhs->size(), rhs->size());
    }
    return CompareValues(cmp, 0);
  }

  Status Visit(const Decimal128Type&) { return CompareValues<Decimal128Type>(); }

  // explicit because both integral and floating point conditions match half float
  Status Visit(const HalfFloatType&) {
    // TODO(bkietz) whenever we vendor a float16, this can be implemented
    return Status::NotImplemented("comparison of scalars of type ", *lhs_.type);
  }

  Status Visit(const DataType&) {
    return Status::NotImplemented("comparison of scalars of type ", *lhs_.type);
  }

  // defer comparison to ScalarType<T>::value
  template <typename T>
  Status CompareValues() {
    auto lhs = checked_cast<const ScalarType<T>&>(lhs_).value;
    auto rhs = checked_cast<const ScalarType<T>&>(rhs_).value;
    return CompareValues(lhs, rhs);
  }

  // defer comparison to explicit values
  template <typename Value>
  Status CompareValues(Value lhs, Value rhs) {
    result_ = lhs < rhs ? Comparison::LESS
                        : lhs == rhs ? Comparison::EQUAL : Comparison::GREATER;
    return Status::OK();
  }

  Comparison::type result_;
  const Scalar& lhs_;
  const Scalar& rhs_;
};

// Compare two scalars
// if either is null, return is null
Result<Comparison::type> Compare(const Scalar& lhs, const Scalar& rhs) {
  if (!lhs.type->Equals(*rhs.type)) {
    return Status::TypeError("Cannot compare scalars of differing type: ", *lhs.type,
                             " vs ", *rhs.type);
  }
  if (!lhs.is_valid || !rhs.is_valid) {
    return Comparison::NULL_;
  }
  CompareVisitor vis{Comparison::NULL_, lhs, rhs};
  RETURN_NOT_OK(VisitTypeInline(*lhs.type, &vis));
  return vis.result_;
}

compute::CompareOperator InvertCompareOperator(compute::CompareOperator op) {
  using compute::CompareOperator;

  switch (op) {
    case CompareOperator::EQUAL:
      return CompareOperator::NOT_EQUAL;

    case CompareOperator::NOT_EQUAL:
      return CompareOperator::EQUAL;

    case CompareOperator::GREATER:
      return CompareOperator::LESS_EQUAL;

    case CompareOperator::GREATER_EQUAL:
      return CompareOperator::LESS;

    case CompareOperator::LESS:
      return CompareOperator::GREATER_EQUAL;

    case CompareOperator::LESS_EQUAL:
      return CompareOperator::GREATER;

    default:
      break;
  }

  DCHECK(false);
  return CompareOperator::EQUAL;
}

template <typename Boolean>
Result<std::shared_ptr<Expression>> InvertBoolean(const Boolean& expr) {
  ARROW_ASSIGN_OR_RAISE(auto lhs, Invert(*expr.left_operand()));
  ARROW_ASSIGN_OR_RAISE(auto rhs, Invert(*expr.right_operand()));

  if (std::is_same<Boolean, AndExpression>::value) {
    return std::make_shared<OrExpression>(std::move(lhs), std::move(rhs));
  }

  if (std::is_same<Boolean, OrExpression>::value) {
    return std::make_shared<AndExpression>(std::move(lhs), std::move(rhs));
  }

  return Status::Invalid("unknown boolean expression ", expr.ToString());
}

Result<std::shared_ptr<Expression>> Invert(const Expression& expr) {
  switch (expr.type()) {
    case ExpressionType::NOT:
      return checked_cast<const NotExpression&>(expr).operand();

    case ExpressionType::AND:
      return InvertBoolean(checked_cast<const AndExpression&>(expr));

    case ExpressionType::OR:
      return InvertBoolean(checked_cast<const OrExpression&>(expr));

    case ExpressionType::COMPARISON: {
      const auto& comparison = checked_cast<const ComparisonExpression&>(expr);
      auto inverted_op = InvertCompareOperator(comparison.op());
      return std::make_shared<ComparisonExpression>(
          inverted_op, comparison.left_operand(), comparison.right_operand());
    }

    default:
      break;
  }
  return Status::NotImplemented("can't invert this expression");
}

Result<std::shared_ptr<Expression>> ComparisonExpression::Assume(
    const Expression& given) const {
  switch (given.type()) {
    case ExpressionType::COMPARISON: {
      return AssumeGivenComparison(checked_cast<const ComparisonExpression&>(given));
    }

    case ExpressionType::NOT: {
      const auto& to_invert = checked_cast<const NotExpression&>(given).operand();
      auto inverted = Invert(*to_invert);
      if (!inverted.ok()) {
        return Copy();
      }
      return Assume(*inverted.ValueOrDie());
    }

    case ExpressionType::OR: {
      const auto& given_or = checked_cast<const OrExpression&>(given);

      bool simplify_to_always = true;
      bool simplify_to_never = true;
      for (const auto& operand : {given_or.left_operand(), given_or.right_operand()}) {
        ARROW_ASSIGN_OR_RAISE(auto simplified, Assume(*operand));

        if (simplified->IsNull()) {
          // some subexpression of given is always null, return null
          return ScalarExpression::MakeNull(boolean());
        }

        bool trivial;
        if (!simplified->IsTrivialCondition(&trivial)) {
          // Or cannot be simplified unless all of its operands simplify to the same
          // trivial condition
          simplify_to_never = false;
          simplify_to_always = false;
          continue;
        }

        if (trivial == true) {
          simplify_to_never = false;
        } else {
          simplify_to_always = false;
        }
      }

      if (simplify_to_always) {
        return ScalarExpression::Make(true);
      }

      if (simplify_to_never) {
        return ScalarExpression::Make(false);
      }

      return Copy();
    }

    case ExpressionType::AND: {
      const auto& given_and = checked_cast<const AndExpression&>(given);

      auto simplified = Copy();
      for (const auto& operand : {given_and.left_operand(), given_and.right_operand()}) {
        if (simplified->IsNull()) {
          return ScalarExpression::MakeNull(boolean());
        }

        if (simplified->IsTrivialCondition()) {
          return std::move(simplified);
        }

        ARROW_ASSIGN_OR_RAISE(simplified, simplified->Assume(*operand));
      }
      return std::move(simplified);
    }

    default:
      break;
  }

  return Copy();
}

// Try to simplify one comparison against another comparison.
// For example,
// (x > 3) is a subset of (x > 2), so (x > 2).Assume(x > 3) == (true)
// (x < 0) is disjoint with (x > 2), so (x > 2).Assume(x < 0) == (false)
// If simplification to (true) or (false) is not possible, pass e through unchanged.
Result<std::shared_ptr<Expression>> ComparisonExpression::AssumeGivenComparison(
    const ComparisonExpression& given) const {
  for (auto comparison : {this, &given}) {
    if (comparison->left_operand_->type() != ExpressionType::FIELD) {
      return Status::Invalid("left hand side of comparison must be a field reference");
    }

    if (comparison->right_operand_->type() != ExpressionType::SCALAR) {
      return Status::Invalid("right hand side of comparison must be a scalar");
    }
  }

  const auto& this_lhs = checked_cast<const FieldExpression&>(*left_operand_);
  const auto& given_lhs = checked_cast<const FieldExpression&>(*given.left_operand_);
  if (this_lhs.name() != given_lhs.name()) {
    return Copy();
  }

  const auto& this_rhs = checked_cast<const ScalarExpression&>(*right_operand_).value();
  const auto& given_rhs =
      checked_cast<const ScalarExpression&>(*given.right_operand_).value();
  ARROW_ASSIGN_OR_RAISE(auto cmp, Compare(*this_rhs, *given_rhs));

  if (cmp == Comparison::NULL_) {
    // the RHS of e or given was null
    return ScalarExpression::MakeNull(boolean());
  }

  static auto always = ScalarExpression::Make(true);
  static auto never = ScalarExpression::Make(false);

  using compute::CompareOperator;

  if (cmp == Comparison::GREATER) {
    // the rhs of e is greater than that of given
    switch (op()) {
      case CompareOperator::EQUAL:
      case CompareOperator::GREATER:
      case CompareOperator::GREATER_EQUAL:
        switch (given.op()) {
          case CompareOperator::EQUAL:
          case CompareOperator::LESS:
          case CompareOperator::LESS_EQUAL:
            return never;
          default:
            return Copy();
        }
      case CompareOperator::NOT_EQUAL:
      case CompareOperator::LESS:
      case CompareOperator::LESS_EQUAL:
        switch (given.op()) {
          case CompareOperator::EQUAL:
          case CompareOperator::LESS:
          case CompareOperator::LESS_EQUAL:
            return always;
          default:
            return Copy();
        }
      default:
        return Copy();
    }
  }

  if (cmp == Comparison::LESS) {
    // the rhs of e is less than that of given
    switch (op()) {
      case CompareOperator::EQUAL:
      case CompareOperator::LESS:
      case CompareOperator::LESS_EQUAL:
        switch (given.op()) {
          case CompareOperator::EQUAL:
          case CompareOperator::GREATER:
          case CompareOperator::GREATER_EQUAL:
            return never;
          default:
            return Copy();
        }
      case CompareOperator::NOT_EQUAL:
      case CompareOperator::GREATER:
      case CompareOperator::GREATER_EQUAL:
        switch (given.op()) {
          case CompareOperator::EQUAL:
          case CompareOperator::GREATER:
          case CompareOperator::GREATER_EQUAL:
            return always;
          default:
            return Copy();
        }
      default:
        return Copy();
    }
  }

  DCHECK_EQ(cmp, Comparison::EQUAL);

  // the rhs of the comparisons are equal
  switch (op_) {
    case CompareOperator::EQUAL:
      switch (given.op()) {
        case CompareOperator::NOT_EQUAL:
        case CompareOperator::GREATER:
        case CompareOperator::LESS:
          return never;
        case CompareOperator::EQUAL:
          return always;
        default:
          return Copy();
      }
    case CompareOperator::NOT_EQUAL:
      switch (given.op()) {
        case CompareOperator::EQUAL:
          return never;
        case CompareOperator::NOT_EQUAL:
        case CompareOperator::GREATER:
        case CompareOperator::LESS:
          return always;
        default:
          return Copy();
      }
    case CompareOperator::GREATER:
      switch (given.op()) {
        case CompareOperator::EQUAL:
        case CompareOperator::LESS_EQUAL:
        case CompareOperator::LESS:
          return never;
        case CompareOperator::GREATER:
          return always;
        default:
          return Copy();
      }
    case CompareOperator::GREATER_EQUAL:
      switch (given.op()) {
        case CompareOperator::LESS:
          return never;
        case CompareOperator::EQUAL:
        case CompareOperator::GREATER:
        case CompareOperator::GREATER_EQUAL:
          return always;
        default:
          return Copy();
      }
    case CompareOperator::LESS:
      switch (given.op()) {
        case CompareOperator::EQUAL:
        case CompareOperator::GREATER:
        case CompareOperator::GREATER_EQUAL:
          return never;
        case CompareOperator::LESS:
          return always;
        default:
          return Copy();
      }
    case CompareOperator::LESS_EQUAL:
      switch (given.op()) {
        case CompareOperator::GREATER:
          return never;
        case CompareOperator::EQUAL:
        case CompareOperator::LESS:
        case CompareOperator::LESS_EQUAL:
          return always;
        default:
          return Copy();
      }
    default:
      return Copy();
  }
  return Copy();
}

Result<std::shared_ptr<Expression>> AndExpression::Assume(const Expression& given) const {
  ARROW_ASSIGN_OR_RAISE(auto left_operand, left_operand_->Assume(given));
  ARROW_ASSIGN_OR_RAISE(auto right_operand, right_operand_->Assume(given));

  // if either operand is trivially null then so is this AND
  if (left_operand->IsNull() || right_operand->IsNull()) {
    return ScalarExpression::MakeNull(boolean());
  }

  bool left_trivial, right_trivial;
  bool left_is_trivial = left_operand->IsTrivialCondition(&left_trivial);
  bool right_is_trivial = right_operand->IsTrivialCondition(&right_trivial);

  // if neither of the operands is trivial, simply construct a new AND
  if (!left_is_trivial && !right_is_trivial) {
    return std::make_shared<AndExpression>(std::move(left_operand),
                                           std::move(right_operand));
  }

  // if either of the operands is trivially false then so is this AND
  if ((left_is_trivial && left_trivial == false) ||
      (right_is_trivial && right_trivial == false)) {
    // FIXME(bkietz) if left is false and right is a column conaining nulls, this is an
    // error because we should be yielding null there rather than false
    return ScalarExpression::Make(false);
  }

  // at least one of the operands is trivially true; return the other operand
  return right_is_trivial ? std::move(left_operand) : std::move(right_operand);
}

Result<std::shared_ptr<Expression>> OrExpression::Assume(const Expression& given) const {
  ARROW_ASSIGN_OR_RAISE(auto left_operand, left_operand_->Assume(given));
  ARROW_ASSIGN_OR_RAISE(auto right_operand, right_operand_->Assume(given));

  // if either operand is trivially null then so is this OR
  if (left_operand->IsNull() || right_operand->IsNull()) {
    return ScalarExpression::MakeNull(boolean());
  }

  bool left_trivial, right_trivial;
  bool left_is_trivial = left_operand->IsTrivialCondition(&left_trivial);
  bool right_is_trivial = right_operand->IsTrivialCondition(&right_trivial);

  // if neither of the operands is trivial, simply construct a new OR
  if (!left_is_trivial && !right_is_trivial) {
    return std::make_shared<OrExpression>(std::move(left_operand),
                                          std::move(right_operand));
  }

  // if either of the operands is trivially true then so is this OR
  if ((left_is_trivial && left_trivial == true) ||
      (right_is_trivial && right_trivial == true)) {
    // FIXME(bkietz) if left is true but right is a column conaining nulls, this is an
    // error because we should be yielding null there rather than true
    return ScalarExpression::Make(true);
  }

  // at least one of the operands is trivially false; return the other operand
  return right_is_trivial ? std::move(left_operand) : std::move(right_operand);
}

Result<std::shared_ptr<Expression>> NotExpression::Assume(const Expression& given) const {
  ARROW_ASSIGN_OR_RAISE(auto operand, operand_->Assume(given));

  if (operand->IsNull()) {
    return ScalarExpression::MakeNull(boolean());
  }

  bool trivial;
  if (operand->IsTrivialCondition(&trivial)) {
    return ScalarExpression::Make(!trivial);
  }

  return Copy();
}

std::string FieldExpression::ToString() const {
  return std::string("field(") + name_ + ")";
}

std::string OperatorName(compute::CompareOperator op) {
  using compute::CompareOperator;
  switch (op) {
    case CompareOperator::EQUAL:
      return "EQUAL";
    case CompareOperator::NOT_EQUAL:
      return "NOT_EQUAL";
    case CompareOperator::LESS:
      return "LESS";
    case CompareOperator::LESS_EQUAL:
      return "LESS_EQUAL";
    case CompareOperator::GREATER:
      return "GREATER";
    case CompareOperator::GREATER_EQUAL:
      return "GREATER_EQUAL";
    default:
      DCHECK(false);
  }
  return "";
}

std::string ScalarExpression::ToString() const {
  if (!value_->is_valid) {
    return "scalar<" + value_->type->ToString() + ", null>()";
  }

  std::string value;
  switch (value_->type->id()) {
    case Type::BOOL:
      value = checked_cast<const BooleanScalar&>(*value_).value ? "true" : "false";
      break;
    case Type::INT32:
      value = std::to_string(checked_cast<const Int32Scalar&>(*value_).value);
      break;
    case Type::INT64:
      value = std::to_string(checked_cast<const Int64Scalar&>(*value_).value);
      break;
    case Type::DOUBLE:
      value = std::to_string(checked_cast<const DoubleScalar&>(*value_).value);
      break;
    case Type::STRING:
      value = checked_cast<const StringScalar&>(*value_).value->ToString();
      break;
    default:
      value = "TODO(bkietz)";
      break;
  }

  return "scalar<" + value_->type->ToString() + ">(" + value + ")";
}

static std::string EulerNotation(std::string fn, const ExpressionVector& operands) {
  fn += "(";
  bool comma = false;
  for (const auto& operand : operands) {
    if (comma) {
      fn += ", ";
    } else {
      comma = true;
    }
    fn += operand->ToString();
  }
  fn += ")";
  return fn;
}

std::string AndExpression::ToString() const {
  return EulerNotation("AND", {left_operand_, right_operand_});
}

std::string OrExpression::ToString() const {
  return EulerNotation("OR", {left_operand_, right_operand_});
}

std::string NotExpression::ToString() const { return EulerNotation("NOT", {operand_}); }

std::string ComparisonExpression::ToString() const {
  return EulerNotation(OperatorName(op()), {left_operand_, right_operand_});
}

bool UnaryExpression::Equals(const Expression& other) const {
  return type_ == other.type() &&
         operand_->Equals(checked_cast<const UnaryExpression&>(other).operand_);
}

bool BinaryExpression::Equals(const Expression& other) const {
  return type_ == other.type() &&
         left_operand_->Equals(
             checked_cast<const BinaryExpression&>(other).left_operand_) &&
         right_operand_->Equals(
             checked_cast<const BinaryExpression&>(other).right_operand_);
}

bool ComparisonExpression::Equals(const Expression& other) const {
  return BinaryExpression::Equals(other) &&
         op_ == checked_cast<const ComparisonExpression&>(other).op_;
}

bool ScalarExpression::Equals(const Expression& other) const {
  return other.type() == ExpressionType::SCALAR &&
         value_->Equals(checked_cast<const ScalarExpression&>(other).value_);
}

bool FieldExpression::Equals(const Expression& other) const {
  return other.type() == ExpressionType::FIELD &&
         name_ == checked_cast<const FieldExpression&>(other).name_;
}

bool Expression::Equals(const std::shared_ptr<Expression>& other) const {
  if (other == NULLPTR) {
    return false;
  }
  return Equals(*other);
}

bool Expression::IsNull() const {
  if (type_ != ExpressionType::SCALAR) {
    return false;
  }

  const auto& scalar = checked_cast<const ScalarExpression&>(*this).value();
  if (!scalar->is_valid) {
    return true;
  }

  return false;
}

bool Expression::IsTrivialCondition(bool* out) const {
  if (type_ != ExpressionType::SCALAR) {
    return false;
  }

  const auto& scalar = checked_cast<const ScalarExpression&>(*this).value();
  if (!scalar->is_valid) {
    return false;
  }

  if (scalar->type->id() != Type::BOOL) {
    return false;
  }

  if (out) {
    *out = checked_cast<const BooleanScalar&>(*scalar).value;
  }
  return true;
}

std::shared_ptr<Expression> FieldExpression::Copy() const {
  return std::make_shared<FieldExpression>(*this);
}

std::shared_ptr<Expression> ScalarExpression::Copy() const {
  return std::make_shared<ScalarExpression>(*this);
}

std::shared_ptr<AndExpression> and_(std::shared_ptr<Expression> lhs,
                                    std::shared_ptr<Expression> rhs) {
  return std::make_shared<AndExpression>(std::move(lhs), std::move(rhs));
}

std::shared_ptr<OrExpression> or_(std::shared_ptr<Expression> lhs,
                                  std::shared_ptr<Expression> rhs) {
  return std::make_shared<OrExpression>(std::move(lhs), std::move(rhs));
}

std::shared_ptr<NotExpression> not_(std::shared_ptr<Expression> operand) {
  return std::make_shared<NotExpression>(std::move(operand));
}

AndExpression operator&&(const Expression& lhs, const Expression& rhs) {
  return AndExpression(lhs.Copy(), rhs.Copy());
}

OrExpression operator||(const Expression& lhs, const Expression& rhs) {
  return OrExpression(lhs.Copy(), rhs.Copy());
}

NotExpression operator!(const Expression& rhs) { return NotExpression(rhs.Copy()); }

Result<std::shared_ptr<DataType>> ComparisonExpression::Validate(
    const Schema& schema) const {
  if (left_operand_->type() != ExpressionType::FIELD) {
    return Status::NotImplemented("comparison with non-FIELD RHS");
  }

  ARROW_ASSIGN_OR_RAISE(auto lhs_type, left_operand_->Validate(schema));
  ARROW_ASSIGN_OR_RAISE(auto rhs_type, right_operand_->Validate(schema));
  if (!lhs_type->Equals(rhs_type)) {
    return Status::TypeError("cannot compare expressions of differing type, ", *lhs_type,
                             " vs ", *rhs_type);
  }

  if (lhs_type->id() == Type::NA || rhs_type->id() == Type::NA) {
    return null();
  }

  return boolean();
}

Status EnsureNullOrBool(const std::string& msg_prefix,
                        const std::shared_ptr<DataType>& type) {
  if (type->id() == Type::BOOL || type->id() == Type::NA) {
    return Status::OK();
  }
  return Status::TypeError(msg_prefix, *type);
}

Result<std::shared_ptr<DataType>> ValidateBoolean(const ExpressionVector& operands,
                                                  const Schema& schema) {
  auto out = boolean();
  for (auto operand : operands) {
    ARROW_ASSIGN_OR_RAISE(auto type, operand->Validate(schema));
    RETURN_NOT_OK(
        EnsureNullOrBool("cannot combine expressions including one of type ", type));
    if (type->id() == Type::NA) {
      out = null();
    }
  }
  return std::move(out);
}

Result<std::shared_ptr<DataType>> AndExpression::Validate(const Schema& schema) const {
  return ValidateBoolean({left_operand_, right_operand_}, schema);
}

Result<std::shared_ptr<DataType>> OrExpression::Validate(const Schema& schema) const {
  return ValidateBoolean({left_operand_, right_operand_}, schema);
}

Result<std::shared_ptr<DataType>> NotExpression::Validate(const Schema& schema) const {
  return ValidateBoolean({operand_}, schema);
}

Result<std::shared_ptr<DataType>> ScalarExpression::Validate(const Schema& schema) const {
  return value_->type;
}

Result<std::shared_ptr<DataType>> FieldExpression::Validate(const Schema& schema) const {
  if (auto field = schema.GetFieldByName(name_)) {
    return field->type();
  }
  return null();
}

Result<std::shared_ptr<Expression>> SelectorAssume(
    const std::shared_ptr<DataSelector>& selector,
    const std::shared_ptr<Expression>& given) {
  if (selector == nullptr || selector->filters.size() == 0) {
    return ScalarExpression::Make(true);
  }

  auto get_expression = [](const std::shared_ptr<Filter>& f) {
    DCHECK_EQ(f->type(), FilterType::EXPRESSION);
    return checked_cast<const ExpressionFilter&>(*f).expression();
  };

  auto out_expr = get_expression(selector->filters[0]);
  for (size_t i = 1; i < selector->filters.size(); ++i) {
    out_expr = and_(std::move(out_expr), get_expression(selector->filters[i]));
  }

  if (given == nullptr) {
    return std::move(out_expr);
  }
  return out_expr->Assume(*given);
}

std::shared_ptr<DataSelector> ExpressionSelector(std::shared_ptr<Expression> e) {
  return std::make_shared<DataSelector>(
      DataSelector{FilterVector{std::make_shared<ExpressionFilter>(std::move(e))}});
}

}  // namespace dataset
}  // namespace arrow
