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

#include <cstring>

#include "arrow/buffer-builder.h"
#include "arrow/buffer.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernels/boolean.h"
#include "arrow/compute/kernels/compare.h"
#include "arrow/record_batch.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace dataset {

using arrow::compute::Datum;
using internal::checked_cast;
using internal::checked_pointer_cast;

Result<std::shared_ptr<BooleanArray>> ScalarExpression::Evaluate(
    compute::FunctionContext* ctx, const RecordBatch& batch) const {
  if (!value_->is_valid) {
    std::shared_ptr<Array> mask_array;
    RETURN_NOT_OK(
        MakeArrayOfNull(ctx->memory_pool(), boolean(), batch.num_rows(), &mask_array));

    return checked_pointer_cast<BooleanArray>(mask_array);
  }

  if (!value_->type->Equals(boolean())) {
    return Status::Invalid("can't evaluate ", ToString());
  }

  TypedBufferBuilder<bool> builder;
  RETURN_NOT_OK(builder.Append(batch.num_rows(),
                               checked_cast<const BooleanScalar&>(*value_).value));

  std::shared_ptr<Buffer> values;
  RETURN_NOT_OK(builder.Finish(&values));

  return std::make_shared<BooleanArray>(batch.num_rows(), values);
}

Result<std::shared_ptr<BooleanArray>> NotExpression::Evaluate(
    compute::FunctionContext* ctx, const RecordBatch& batch) const {
  ARROW_ASSIGN_OR_RAISE(auto to_invert, operand_->Evaluate(ctx, batch));
  Datum out;
  RETURN_NOT_OK(arrow::compute::Invert(ctx, Datum(to_invert), &out));
  return checked_pointer_cast<BooleanArray>(out.make_array());
}

template <typename Nnary>
Result<std::shared_ptr<BooleanArray>> EvaluateNnary(const Nnary& nnary,
                                                    compute::FunctionContext* ctx,
                                                    const RecordBatch& batch) {
  const auto& operands = nnary.operands();

  ARROW_ASSIGN_OR_RAISE(auto next, operands[0]->Evaluate(ctx, batch));
  Datum acc(next);

  for (size_t i_next = 1; i_next < operands.size(); ++i_next) {
    ARROW_ASSIGN_OR_RAISE(next, operands[i_next]->Evaluate(ctx, batch));

    if (std::is_same<Nnary, AndExpression>::value) {
      RETURN_NOT_OK(arrow::compute::And(ctx, Datum(acc), Datum(next), &acc));
    }

    if (std::is_same<Nnary, OrExpression>::value) {
      RETURN_NOT_OK(arrow::compute::Or(ctx, Datum(acc), Datum(next), &acc));
    }
  }

  return checked_pointer_cast<BooleanArray>(acc.make_array());
}

Result<std::shared_ptr<BooleanArray>> AndExpression::Evaluate(
    compute::FunctionContext* ctx, const RecordBatch& batch) const {
  return EvaluateNnary(*this, ctx, batch);
}

Result<std::shared_ptr<BooleanArray>> OrExpression::Evaluate(
    compute::FunctionContext* ctx, const RecordBatch& batch) const {
  return EvaluateNnary(*this, ctx, batch);
}

Result<std::shared_ptr<BooleanArray>> ComparisonExpression::Evaluate(
    compute::FunctionContext* ctx, const RecordBatch& batch) const {
  if (left_operand_->type() != ExpressionType::FIELD) {
    return Status::Invalid("left hand side of comparison must be a field reference");
  }

  if (right_operand_->type() != ExpressionType::SCALAR) {
    return Status::Invalid("right hand side of comparison must be a scalar");
  }

  const auto& lhs = checked_cast<const FieldReferenceExpression&>(*left_operand_);
  const auto& rhs = checked_cast<const ScalarExpression&>(*right_operand_);

  auto lhs_array = batch.GetColumnByName(lhs.name());
  if (lhs_array == nullptr) {
    // comparing a field absent from batch: return nulls
    return ScalarExpression::MakeNull()->Evaluate(ctx, batch);
  }

  Datum out;
  RETURN_NOT_OK(arrow::compute::Compare(ctx, Datum(lhs_array), Datum(rhs.value()),
                                        arrow::compute::CompareOptions(op_), &out));
  return checked_pointer_cast<BooleanArray>(out.make_array());
}

std::shared_ptr<ScalarExpression> ScalarExpression::Make(std::string value) {
  return std::make_shared<ScalarExpression>(
      std::make_shared<StringScalar>(Buffer::FromString(std::move(value))));
}

std::shared_ptr<ScalarExpression> ScalarExpression::Make(const char* value) {
  return std::make_shared<ScalarExpression>(
      std::make_shared<StringScalar>(Buffer::Wrap(value, std::strlen(value))));
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
  if (!lhs.is_valid || !rhs.is_valid) {
    return Comparison::NULL_;
  }
  if (!lhs.type->Equals(*rhs.type)) {
    return Status::TypeError("cannot compare scalars with differing type: ", *lhs.type,
                             " vs ", *rhs.type);
  }
  CompareVisitor vis{Comparison::NULL_, lhs, rhs};
  RETURN_NOT_OK(VisitTypeInline(*lhs.type, &vis));
  return vis.result_;
}

/*
Result<std::shared_ptr<Expression>> Invert(const Expression& op) {
  auto make_opposite = [&op](ExpressionType::type opposite_type) {
    return std::make_shared<OperatorExpression>(
        opposite_type, checked_cast<const OperatorExpression&>(op).operands());
  };

  switch (op.type()) {
    case ExpressionType::NOT:
      return checked_cast<const OperatorExpression&>(op).operands()[0];

    case ExpressionType::AND:
    case ExpressionType::OR: {
      ExpressionVector operands;
      for (auto operand : checked_cast<const OperatorExpression&>(op).operands()) {
        ARROW_ASSIGN_OR_RAISE(auto inverted_operand, Invert(*operand));
        operands.push_back(inverted_operand);
      }

      auto opposite_type =
          op.type() == ExpressionType::AND ? ExpressionType::OR : ExpressionType::AND;
      return std::make_shared<OperatorExpression>(opposite_type, std::move(operands));
    }

    case ExpressionType::EQUAL:
      return make_opposite(ExpressionType::NOT_EQUAL);

    case ExpressionType::NOT_EQUAL:
      return make_opposite(ExpressionType::EQUAL);

    case ExpressionType::LESS:
      return make_opposite(ExpressionType::GREATER_EQUAL);

    case ExpressionType::LESS_EQUAL:
      return make_opposite(ExpressionType::GREATER);

    case ExpressionType::GREATER:
      return make_opposite(ExpressionType::LESS_EQUAL);

    case ExpressionType::GREATER_EQUAL:
      return make_opposite(ExpressionType::LESS);

    default:
      return Status::NotImplemented("can't invert this expression");
  }

  return op.Copy();
}
*/

// If e can be cast to OperatorExpression try to simplify it against given.
// Otherwise pass e through unchanged.
/*
Result<std::shared_ptr<Expression>> AssumeIfOperator(const std::shared_ptr<Expression>& e,
                                                     const Expression& given) {
  if (!e->IsOperatorExpression()) {
    return e;
  }
  return checked_cast<const OperatorExpression&>(*e).Assume(given);
}
*/

Result<std::shared_ptr<Expression>> ComparisonExpression::Assume(
    const Expression& given) const {
  switch (given.type()) {
    case ExpressionType::COMPARISON: {
      return AssumeGivenComparison(checked_cast<const ComparisonExpression&>(given));
    }

    case ExpressionType::NOT: {
      // const auto& to_invert = checked_cast<const NotExpression&>(given).operand();
      // ARROW_ASSIGN_OR_RAISE(auto inverted, Invert(*to_invert));
      // return Assume(*inverted);
      return Copy();
    }

    case ExpressionType::OR: {
      bool simplify_to_always = true;
      bool simplify_to_never = true;
      for (const auto& operand : checked_cast<const OrExpression&>(given).operands()) {
        ARROW_ASSIGN_OR_RAISE(auto simplified, Assume(*operand));

        BooleanScalar scalar;
        if (!simplified->IsTrivialCondition(&scalar)) {
          simplify_to_never = false;
          simplify_to_always = false;
        }

        if (!scalar.is_valid) {
          // some subexpression of given is always null, return null
          return ScalarExpression::MakeNull();
        }

        if (scalar.value == true) {
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
      auto simplified = Copy();
      for (const auto& operand : checked_cast<const AndExpression&>(given).operands()) {
        BooleanScalar value;
        if (simplified->IsTrivialCondition(&value)) {
          // FIXME(bkietz) but what if something later is null?
          break;
        }

        ARROW_ASSIGN_OR_RAISE(simplified, simplified->Assume(*operand));
      }
      return simplified;
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

  const auto& this_lhs = checked_cast<const FieldReferenceExpression&>(*left_operand_);
  const auto& given_lhs =
      checked_cast<const FieldReferenceExpression&>(*given.left_operand_);
  if (this_lhs.name() != given_lhs.name()) {
    return Copy();
  }

  const auto& this_rhs = checked_cast<const ScalarExpression&>(*right_operand_).value();
  const auto& given_rhs =
      checked_cast<const ScalarExpression&>(*given.right_operand_).value();
  ARROW_ASSIGN_OR_RAISE(auto cmp, Compare(*this_rhs, *given_rhs));

  if (cmp == Comparison::NULL_) {
    // the RHS of e or given was null
    return ScalarExpression::MakeNull();
  }

  static auto always = ScalarExpression::Make(true);
  static auto never = ScalarExpression::Make(false);

  using compute::CompareOperator;

  if (cmp == Comparison::EQUAL) {
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
  } else if (cmp == Comparison::GREATER) {
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
  } else {
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

  return Copy();
}

template <typename Nnary>
Result<std::shared_ptr<Expression>> AssumeNnary(const Nnary& nnary,
                                                const Expression& given) {
  // if any of the operands matches trivial_condition, we can return a trivial
  // expression:
  // anything OR true => true
  // anything AND false => false
  constexpr bool trivial_condition = std::is_same<Nnary, OrExpression>::value;
  bool simplify_to_trivial = false;

  ExpressionVector operands;
  for (auto operand : nnary.operands()) {
    ARROW_ASSIGN_OR_RAISE(operand, operand->Assume(given));

    BooleanScalar scalar;
    if (operand->IsTrivialCondition(&scalar)) {
      if (!scalar.is_valid) {
        return ScalarExpression::MakeNull();
      }

      if (scalar.value == trivial_condition) {
        simplify_to_trivial = true;
      }
      continue;
    }

    if (!simplify_to_trivial) {
      operands.push_back(operand);
    }
  }

  if (simplify_to_trivial) {
    return ScalarExpression::Make(trivial_condition);
  }

  if (operands.size() == 1) {
    return operands[0];
  }

  if (operands.size() == 0) {
    return ScalarExpression::Make(!trivial_condition);
  }

  return std::make_shared<Nnary>(std::move(operands));
}

Result<std::shared_ptr<Expression>> AndExpression::Assume(const Expression& given) const {
  return AssumeNnary(*this, given);
}

Result<std::shared_ptr<Expression>> OrExpression::Assume(const Expression& given) const {
  return AssumeNnary(*this, given);
}

Result<std::shared_ptr<Expression>> NotExpression::Assume(const Expression& given) const {
  ARROW_ASSIGN_OR_RAISE(auto operand, operand_->Assume(given));

  BooleanScalar scalar;
  if (operand->IsTrivialCondition(&scalar)) {
    return Copy();
  }

  if (!scalar.is_valid) {
    return ScalarExpression::MakeNull();
  }

  return ScalarExpression::Make(!scalar.value);
}

std::string FieldReferenceExpression::ToString() const {
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
      value = "TODO";
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

std::string AndExpression::ToString() const { return EulerNotation("AND", operands_); }

std::string OrExpression::ToString() const { return EulerNotation("OR", operands_); }

std::string ComparisonExpression::ToString() const {
  return EulerNotation(OperatorName(op()), {left_operand_, right_operand_});
}

bool UnaryExpression::OperandsEqual(const UnaryExpression& other) const {
  return operand_->Equals(other.operand_);
}

bool BinaryExpression::OperandsEqual(const BinaryExpression& other) const {
  return left_operand_->Equals(other.left_operand_) &&
         right_operand_->Equals(other.right_operand_);
}

bool NnaryExpression::OperandsEqual(const NnaryExpression& other) const {
  if (operands_.size() != other.operands_.size()) {
    return false;
  }
  for (size_t i = 0; i < operands_.size(); ++i) {
    if (!operands_[i]->Equals(other.operands_[i])) {
      return false;
    }
  }
  return true;
}

struct ExpressionEqual {
  Status Visit(const FieldReferenceExpression& rhs) {
    result_ = checked_cast<const FieldReferenceExpression&>(lhs_).name() == rhs.name();
    return Status::OK();
  }

  Status Visit(const ScalarExpression& rhs) {
    result_ = checked_cast<const ScalarExpression&>(lhs_).value()->Equals(rhs.value());
    return Status::OK();
  }

  Status Visit(const ComparisonExpression& rhs) {
    const auto& lhs = checked_cast<const ComparisonExpression&>(lhs_);
    result_ = lhs.op() == rhs.op() && lhs.OperandsEqual(rhs);
    return Status::OK();
  }

  Status Visit(const NnaryExpression& rhs) {
    const auto& lhs = checked_cast<const NnaryExpression&>(lhs_);
    result_ = lhs.OperandsEqual(rhs);
    return Status::OK();
  }

  Status Visit(const UnaryExpression& rhs) {
    const auto& lhs = checked_cast<const UnaryExpression&>(lhs_);
    result_ = lhs.OperandsEqual(rhs);
    return Status::OK();
  }

  Status Visit(const Expression& rhs) { return Status::NotImplemented("halp"); }

  bool Compare(const Expression& rhs) && {
    DCHECK_OK(rhs.Accept(*this));
    return result_;
  }

  const Expression& lhs_;
  bool result_;
};

bool Expression::Equals(const Expression& other) const {
  if (type_ != other.type()) {
    return false;
  }

  return ExpressionEqual{*this, false}.Compare(other);
}

bool Expression::Equals(const std::shared_ptr<Expression>& other) const {
  if (other == NULLPTR) {
    return false;
  }
  return Equals(*other);
}

bool Expression::IsTrivialCondition(BooleanScalar* out) const {
  if (type_ != ExpressionType::SCALAR) {
    return false;
  }

  const auto& scalar = checked_cast<const ScalarExpression&>(*this).value();
  if (!scalar->is_valid) {
    if (out) {
      *out = BooleanScalar();
    }
    return true;
  }

  if (scalar->type->id() != Type::BOOL) {
    return false;
  }

  if (out) {
    *out = BooleanScalar(checked_cast<const BooleanScalar&>(*scalar).value);
  }
  return true;
}

std::shared_ptr<Expression> FieldReferenceExpression::Copy() const {
  return std::make_shared<FieldReferenceExpression>(*this);
}

std::shared_ptr<Expression> ScalarExpression::Copy() const {
  return std::make_shared<ScalarExpression>(*this);
}

std::shared_ptr<AndExpression> and_(ExpressionVector operands) {
  return std::make_shared<AndExpression>(std::move(operands));
}

std::shared_ptr<OrExpression> or_(ExpressionVector operands) {
  return std::make_shared<OrExpression>(std::move(operands));
}

std::shared_ptr<NotExpression> not_(std::shared_ptr<Expression> operand) {
  return std::make_shared<NotExpression>(std::move(operand));
}

// flatten chains of and/or to a single OperatorExpression
template <typename Out>
Out MaybeCombine(const Expression& lhs, const Expression& rhs) {
  if (lhs.type() != Out::expression_type && rhs.type() != Out::expression_type) {
    return Out(ExpressionVector{lhs.Copy(), rhs.Copy()});
  }

  ExpressionVector operands;
  for (auto side : {&lhs, &rhs}) {
    if (side->type() != Out::expression_type) {
      operands.emplace_back(side->Copy());
      continue;
    }

    for (auto operand : checked_cast<const Out&>(*side).operands()) {
      operands.emplace_back(std::move(operand));
    }
  }

  return Out(std::move(operands));
}

AndExpression operator and(const Expression& lhs, const Expression& rhs) {
  return MaybeCombine<AndExpression>(lhs, rhs);
}

OrExpression operator or(const Expression& lhs, const Expression& rhs) {
  return MaybeCombine<OrExpression>(lhs, rhs);
}

NotExpression operator not(const Expression& rhs) { return NotExpression(rhs.Copy()); }

}  // namespace dataset
}  // namespace arrow
