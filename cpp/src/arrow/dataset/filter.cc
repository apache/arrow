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
#include <utility>

#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
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

    if (std::is_same<Nnary, AllExpression>::value) {
      RETURN_NOT_OK(arrow::compute::And(ctx, Datum(acc), Datum(next), &acc));
    }

    if (std::is_same<Nnary, AnyExpression>::value) {
      RETURN_NOT_OK(arrow::compute::Or(ctx, Datum(acc), Datum(next), &acc));
    }
  }

  return checked_pointer_cast<BooleanArray>(acc.make_array());
}

Result<std::shared_ptr<BooleanArray>> AllExpression::Evaluate(
    compute::FunctionContext* ctx, const RecordBatch& batch) const {
  return EvaluateNnary(*this, ctx, batch);
}

Result<std::shared_ptr<BooleanArray>> AnyExpression::Evaluate(
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

  const auto& lhs = checked_cast<const FieldExpression&>(*left_operand_);
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

std::shared_ptr<Expression> Invert(const ComparisonExpression& comparison) {
  using compute::CompareOperator;
  auto make_opposite = [&](CompareOperator opposite) {
    return std::make_shared<ComparisonExpression>(opposite, comparison.left_operand(),
                                                  comparison.right_operand());
  };

  switch (comparison.op()) {
    case CompareOperator::EQUAL:
      return make_opposite(CompareOperator::NOT_EQUAL);

    case CompareOperator::NOT_EQUAL:
      return make_opposite(CompareOperator::EQUAL);

    case CompareOperator::GREATER:
      return make_opposite(CompareOperator::LESS_EQUAL);

    case CompareOperator::GREATER_EQUAL:
      return make_opposite(CompareOperator::LESS);

    case CompareOperator::LESS:
      return make_opposite(CompareOperator::GREATER_EQUAL);

    case CompareOperator::LESS_EQUAL:
      return make_opposite(CompareOperator::GREATER);

    default:
      break;
  }

  DCHECK(false);
  return nullptr;
}

Result<std::shared_ptr<Expression>> Invert(const Expression& op) {
  switch (op.type()) {
    case ExpressionType::NOT:
      return checked_cast<const NotExpression&>(op).operand();

    case ExpressionType::ALL:
    case ExpressionType::ANY: {
      ExpressionVector inverted_operands;
      for (auto operand : checked_cast<const NnaryExpression&>(op).operands()) {
        ARROW_ASSIGN_OR_RAISE(auto inverted_operand, Invert(*operand));
        inverted_operands.push_back(inverted_operand);
      }

      if (op.type() == ExpressionType::ALL) {
        return std::make_shared<AnyExpression>(std::move(inverted_operands));
      }
      return std::make_shared<AllExpression>(std::move(inverted_operands));
    }

    case ExpressionType::COMPARISON:
      return Invert(checked_cast<const ComparisonExpression&>(op));

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

    case ExpressionType::ANY: {
      bool simplify_to_always = true;
      bool simplify_to_never = true;
      for (const auto& operand : checked_cast<const AnyExpression&>(given).operands()) {
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

    case ExpressionType::ALL: {
      auto simplified = Copy();
      for (const auto& operand : checked_cast<const AllExpression&>(given).operands()) {
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
    return ScalarExpression::MakeNull();
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
}

template <typename Nnary>
Result<std::shared_ptr<Expression>> AssumeNnary(const Nnary& nnary,
                                                const Expression& given) {
  // if any of the operands matches trivial_condition, we can return a trivial
  // expression:
  // anything ANY true => true
  // anything ALL false => false
  constexpr bool trivial_condition = std::is_same<Nnary, AnyExpression>::value;
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

Result<std::shared_ptr<Expression>> AllExpression::Assume(const Expression& given) const {
  return AssumeNnary(*this, given);
}

Result<std::shared_ptr<Expression>> AnyExpression::Assume(const Expression& given) const {
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

std::string AllExpression::ToString() const { return EulerNotation("ALL", operands_); }

std::string AnyExpression::ToString() const { return EulerNotation("ANY", operands_); }

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

bool NnaryExpression::Equals(const Expression& other) const {
  if (type_ != other.type()) {
    return false;
  }
  const auto& other_operands = checked_cast<const NnaryExpression&>(other).operands_;
  if (operands_.size() != other_operands.size()) {
    return false;
  }
  for (size_t i = 0; i < operands_.size(); ++i) {
    if (!operands_[i]->Equals(other_operands[i])) {
      return false;
    }
  }
  return true;
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

std::shared_ptr<Expression> FieldExpression::Copy() const {
  return std::make_shared<FieldExpression>(*this);
}

std::shared_ptr<Expression> ScalarExpression::Copy() const {
  return std::make_shared<ScalarExpression>(*this);
}

std::shared_ptr<AllExpression> all(ExpressionVector operands) {
  return std::make_shared<AllExpression>(std::move(operands));
}

std::shared_ptr<AnyExpression> any(ExpressionVector operands) {
  return std::make_shared<AnyExpression>(std::move(operands));
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

AllExpression operator and(const Expression& lhs, const Expression& rhs) {
  return MaybeCombine<AllExpression>(lhs, rhs);
}

AnyExpression operator or(const Expression& lhs, const Expression& rhs) {
  return MaybeCombine<AnyExpression>(lhs, rhs);
}

NotExpression operator not(const Expression& rhs) { return NotExpression(rhs.Copy()); }

}  // namespace dataset
}  // namespace arrow
