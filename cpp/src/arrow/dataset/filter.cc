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

Status TrivialMask(MemoryPool* pool, const BooleanScalar& value, const RecordBatch& batch,
                   std::shared_ptr<BooleanArray>* mask) {
  if (!value.is_valid) {
    std::shared_ptr<Array> mask_array;
    RETURN_NOT_OK(MakeArrayOfNull(boolean(), batch.num_rows(), &mask_array));

    *mask = checked_pointer_cast<BooleanArray>(mask_array);
    return Status::OK();
  }

  TypedBufferBuilder<bool> builder;
  RETURN_NOT_OK(builder.Append(batch.num_rows(), value.value));

  std::shared_ptr<Buffer> values;
  RETURN_NOT_OK(builder.Finish(&values));

  *mask = std::make_shared<BooleanArray>(batch.num_rows(), values);
  return Status::OK();
}

Status EvaluateExpression(compute::FunctionContext* ctx, const Expression& condition,
                          const RecordBatch& batch, std::shared_ptr<BooleanArray>* mask) {
  BooleanScalar value;
  if (condition.IsNullScalar() || condition.IsBooleanScalar(&value)) {
    return TrivialMask(ctx->memory_pool(), value, batch, mask);
  }

  if (!condition.IsOperatorExpression()) {
    return Status::Invalid("can't execute condition ", condition.ToString());
  }

  const auto& op = checked_cast<const OperatorExpression&>(condition).operands();

  if (condition.IsComparisonExpression()) {
    const auto& lhs = checked_cast<const FieldReferenceExpression&>(*op[0]);
    const auto& rhs = checked_cast<const ScalarExpression&>(*op[1]);
    Datum out;
    auto lhs_array = batch.GetColumnByName(lhs.name());
    if (lhs_array == nullptr) {
      // comparing a field absent from batch: return nulls
      return TrivialMask(ctx->memory_pool(), BooleanScalar(), batch, mask);
    }
    using arrow::compute::CompareOperator;
    arrow::compute::CompareOptions opts(static_cast<CompareOperator>(
        static_cast<int>(CompareOperator::EQUAL) + static_cast<int>(condition.type()) -
        static_cast<int>(ExpressionType::EQUAL)));
    RETURN_NOT_OK(
        arrow::compute::Compare(ctx, Datum(lhs_array), Datum(rhs.value()), opts, &out));
    *mask = checked_pointer_cast<BooleanArray>(out.make_array());
    return Status::OK();
  }

  if (condition.type() == ExpressionType::NOT) {
    std::shared_ptr<BooleanArray> to_invert;
    RETURN_NOT_OK(EvaluateExpression(ctx, *op[0], batch, &to_invert));
    Datum out;
    RETURN_NOT_OK(arrow::compute::Invert(ctx, Datum(to_invert), &out));
    *mask = checked_pointer_cast<BooleanArray>(out.make_array());
    return Status::OK();
  }

  DCHECK(condition.type() == ExpressionType::OR ||
         condition.type() == ExpressionType::AND);

  std::shared_ptr<BooleanArray> next;
  RETURN_NOT_OK(EvaluateExpression(ctx, *op[0], batch, &next));
  Datum acc(next);

  for (size_t i_next = 1; i_next < op.size(); ++i_next) {
    RETURN_NOT_OK(EvaluateExpression(ctx, *op[i_next], batch, &next));

    if (condition.type() == ExpressionType::OR) {
      RETURN_NOT_OK(arrow::compute::Or(ctx, Datum(acc), Datum(next), &acc));
    } else {
      RETURN_NOT_OK(arrow::compute::And(ctx, Datum(acc), Datum(next), &acc));
    }
  }

  *mask = checked_pointer_cast<BooleanArray>(acc.make_array());
  return Status::OK();
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

// If e can be cast to OperatorExpression try to simplify it against given.
// Otherwise pass e through unchanged.
Result<std::shared_ptr<Expression>> AssumeIfOperator(const std::shared_ptr<Expression>& e,
                                                     const Expression& given) {
  if (!e->IsOperatorExpression()) {
    return e;
  }
  return checked_cast<const OperatorExpression&>(*e).Assume(given);
}

// Try to simplify one comparison against another comparison.
// For example,
// (x > 3) is a subset of (x > 2), so (x > 2).Assume(x > 3) == (true)
// (x < 0) is disjoint with (x > 2), so (x > 2).Assume(x < 0) == (false)
// If simplification to (true) or (false) is not possible, pass e through unchanged.
Result<std::shared_ptr<Expression>> AssumeComparisonComparison(
    const OperatorExpression& e, const OperatorExpression& given) {
  if (e.operands()[1]->type() != ExpressionType::SCALAR ||
      given.operands()[1]->type() != ExpressionType::SCALAR) {
    // TODO(bkietz) allow the RHS of e to be FIELD
    return Status::Invalid("right hand side of comparison must be a scalar");
  }

  auto e_rhs = checked_cast<const ScalarExpression&>(*e.operands()[1]).value();
  auto given_rhs = checked_cast<const ScalarExpression&>(*given.operands()[1]).value();

  ARROW_ASSIGN_OR_RAISE(auto cmp, Compare(*e_rhs, *given_rhs));

  if (cmp == Comparison::NULL_) {
    // the RHS of e or given was null
    return ScalarExpression::MakeNull();
  }

  static auto always = ScalarExpression::Make(true);
  static auto never = ScalarExpression::Make(false);
  auto unsimplified = e.Copy();

  if (cmp == Comparison::EQUAL) {
    // the rhs of the comparisons are equal
    switch (e.type()) {
      case ExpressionType::EQUAL:
        switch (given.type()) {
          case ExpressionType::NOT_EQUAL:
          case ExpressionType::GREATER:
          case ExpressionType::LESS:
            return never;
          case ExpressionType::EQUAL:
            return always;
          default:
            return unsimplified;
        }
      case ExpressionType::NOT_EQUAL:
        switch (given.type()) {
          case ExpressionType::EQUAL:
            return never;
          case ExpressionType::NOT_EQUAL:
          case ExpressionType::GREATER:
          case ExpressionType::LESS:
            return always;
          default:
            return unsimplified;
        }
      case ExpressionType::GREATER:
        switch (given.type()) {
          case ExpressionType::EQUAL:
          case ExpressionType::LESS_EQUAL:
          case ExpressionType::LESS:
            return never;
          case ExpressionType::GREATER:
            return always;
          default:
            return unsimplified;
        }
      case ExpressionType::GREATER_EQUAL:
        switch (given.type()) {
          case ExpressionType::LESS:
            return never;
          case ExpressionType::EQUAL:
          case ExpressionType::GREATER:
          case ExpressionType::GREATER_EQUAL:
            return always;
          default:
            return unsimplified;
        }
      case ExpressionType::LESS:
        switch (given.type()) {
          case ExpressionType::EQUAL:
          case ExpressionType::GREATER:
          case ExpressionType::GREATER_EQUAL:
            return never;
          case ExpressionType::LESS:
            return always;
          default:
            return unsimplified;
        }
      case ExpressionType::LESS_EQUAL:
        switch (given.type()) {
          case ExpressionType::GREATER:
            return never;
          case ExpressionType::EQUAL:
          case ExpressionType::LESS:
          case ExpressionType::LESS_EQUAL:
            return always;
          default:
            return unsimplified;
        }
      default:
        return unsimplified;
    }
  } else if (cmp == Comparison::GREATER) {
    // the rhs of e is greater than that of given
    switch (e.type()) {
      case ExpressionType::EQUAL:
      case ExpressionType::GREATER:
      case ExpressionType::GREATER_EQUAL:
        switch (given.type()) {
          case ExpressionType::EQUAL:
          case ExpressionType::LESS:
          case ExpressionType::LESS_EQUAL:
            return never;
          default:
            return unsimplified;
        }
      case ExpressionType::NOT_EQUAL:
      case ExpressionType::LESS:
      case ExpressionType::LESS_EQUAL:
        switch (given.type()) {
          case ExpressionType::EQUAL:
          case ExpressionType::LESS:
          case ExpressionType::LESS_EQUAL:
            return always;
          default:
            return unsimplified;
        }
      default:
        return unsimplified;
    }
  } else {
    // the rhs of e is less than that of given
    switch (e.type()) {
      case ExpressionType::EQUAL:
      case ExpressionType::LESS:
      case ExpressionType::LESS_EQUAL:
        switch (given.type()) {
          case ExpressionType::EQUAL:
          case ExpressionType::GREATER:
          case ExpressionType::GREATER_EQUAL:
            return never;
          default:
            return unsimplified;
        }
      case ExpressionType::NOT_EQUAL:
      case ExpressionType::GREATER:
      case ExpressionType::GREATER_EQUAL:
        switch (given.type()) {
          case ExpressionType::EQUAL:
          case ExpressionType::GREATER:
          case ExpressionType::GREATER_EQUAL:
            return always;
          default:
            return unsimplified;
        }
      default:
        return unsimplified;
    }
  }

  return unsimplified;
}

// Try to simplify a comparison against a compound expression.
// The operands of the compound expression must be examined individually.
Result<std::shared_ptr<Expression>> AssumeComparisonCompound(
    const OperatorExpression& e, const OperatorExpression& given) {
  auto unsimplified = e.Copy();

  switch (given.type()) {
    case ExpressionType::NOT: {
      ARROW_ASSIGN_OR_RAISE(auto inverted, Invert(*given.operands()[0]));
      return e.Assume(*inverted);
    }

    case ExpressionType::OR: {
      bool simplify_to_always = true;
      bool simplify_to_never = true;
      for (auto operand : given.operands()) {
        ARROW_ASSIGN_OR_RAISE(auto simplified, e.Assume(*operand));
        BooleanScalar scalar;
        if (!simplified->IsBooleanScalar(&scalar)) {
          return unsimplified;
        }

        // an expression should never simplify to null
        DCHECK(scalar.is_valid);

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

      return unsimplified;
    }

    case ExpressionType::AND: {
      std::shared_ptr<Expression> simplified = unsimplified;
      for (auto operand : given.operands()) {
        if (simplified->IsBooleanScalar()) {
          break;
        }

        DCHECK(simplified->IsOperatorExpression());
        const auto& simplified_op = checked_cast<const OperatorExpression&>(*simplified);
        ARROW_ASSIGN_OR_RAISE(simplified, simplified_op.Assume(*operand));
      }
      return simplified;
    }

    default:
      DCHECK(false);
  }

  return unsimplified;
}

Result<std::shared_ptr<Expression>> AssumeCompound(const OperatorExpression& e,
                                                   const Expression& given) {
  auto unsimplified = e.Copy();

  if (e.type() == ExpressionType::NOT) {
    DCHECK_EQ(e.operands().size(), 1);
    ARROW_ASSIGN_OR_RAISE(auto operand, AssumeIfOperator(e.operands()[0], given));

    if (operand->IsNullScalar()) {
      return operand;
    }

    BooleanScalar scalar;
    if (!operand->IsBooleanScalar(&scalar)) {
      return unsimplified;
    }

    return ScalarExpression::Make(!scalar.value);
  }

  DCHECK(e.type() == ExpressionType::OR || e.type() == ExpressionType::AND);

  // if any of the operands matches trivial_condition, we can return a trivial
  // expression:
  // anything OR true => true
  // anything AND false => false
  bool trivial_condition = e.type() == ExpressionType::OR;
  bool simplify_to_trivial = false;

  ExpressionVector operands;
  for (auto operand : e.operands()) {
    ARROW_ASSIGN_OR_RAISE(operand, AssumeIfOperator(operand, given));

    if (operand->IsNullScalar()) {
      return operand;
    }

    if (simplify_to_trivial) {
      continue;
    }

    BooleanScalar scalar;
    if (!operand->IsBooleanScalar(&scalar)) {
      operands.push_back(operand);
      continue;
    }

    if (scalar.value == trivial_condition) {
      simplify_to_trivial = true;
      continue;
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

  return std::make_shared<OperatorExpression>(e.type(), std::move(operands));
}

Result<std::shared_ptr<Expression>> OperatorExpression::Assume(
    const Expression& given) const {
  auto unsimplified = Copy();

  if (IsComparisonExpression()) {
    if (!given.IsOperatorExpression()) {
      return unsimplified;
    }

    const auto& given_op = checked_cast<const OperatorExpression&>(given);

    if (given.IsComparisonExpression()) {
      // Both this and given are simple comparisons. If they constrain
      // the same field, try to simplify this assuming given
      DCHECK_EQ(operands_.size(), 2);
      DCHECK_EQ(given_op.operands_.size(), 2);
      auto get_name = [](const Expression& e) {
        DCHECK_EQ(e.type(), ExpressionType::FIELD);
        return checked_cast<const FieldReferenceExpression&>(e).name();
      };
      if (get_name(*operands_[0]) != get_name(*given_op.operands_[0])) {
        return unsimplified;
      }
      return AssumeComparisonComparison(*this, given_op);
    }

    // must be NOT, AND, OR- decompose given
    return AssumeComparisonCompound(*this, given_op);
  }

  return AssumeCompound(*this, given);
}

std::string FieldReferenceExpression::ToString() const {
  return std::string("field(") + name_ + ")";
}

std::string OperatorName(ExpressionType::type type) {
  switch (type) {
    case ExpressionType::AND:
      return "AND";
    case ExpressionType::OR:
      return "OR";
    case ExpressionType::NOT:
      return "NOT";
    case ExpressionType::EQUAL:
      return "EQUAL";
    case ExpressionType::NOT_EQUAL:
      return "NOT_EQUAL";
    case ExpressionType::LESS:
      return "LESS";
    case ExpressionType::LESS_EQUAL:
      return "LESS_EQUAL";
    case ExpressionType::GREATER:
      return "GREATER";
    case ExpressionType::GREATER_EQUAL:
      return "GREATER_EQUAL";
    default:
      DCHECK(false);
  }
  return "";
}

std::string OperatorExpression::ToString() const {
  auto out = OperatorName(type_) + "(";
  bool comma = false;
  for (const auto& operand : operands_) {
    if (comma) {
      out += ", ";
    } else {
      comma = true;
    }
    out += operand->ToString();
  }
  out += ")";
  return out;
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

bool Expression::Equals(const Expression& other) const {
  if (type_ != other.type()) {
    return false;
  }

  switch (type_) {
    case ExpressionType::FIELD:
      return checked_cast<const FieldReferenceExpression&>(*this).name() ==
             checked_cast<const FieldReferenceExpression&>(other).name();

    case ExpressionType::SCALAR: {
      auto this_value = checked_cast<const ScalarExpression&>(*this).value();
      auto other_value = checked_cast<const ScalarExpression&>(other).value();
      return this_value->Equals(other_value);
    }

    default: {
      DCHECK(IsOperatorExpression());
      const auto& this_op = checked_cast<const OperatorExpression&>(*this).operands();
      const auto& other_op = checked_cast<const OperatorExpression&>(other).operands();
      if (this_op.size() != other_op.size()) {
        return false;
      }

      for (size_t i = 0; i < this_op.size(); ++i) {
        if (!this_op[i]->Equals(*other_op[i])) {
          return false;
        }
      }

      return true;
    }
  }

  return true;
}

bool Expression::Equals(const std::shared_ptr<Expression>& other) const {
  if (other == NULLPTR) {
    return false;
  }
  return Equals(*other);
}

bool Expression::IsOperatorExpression() const {
  return static_cast<int>(type_) >= static_cast<int>(ExpressionType::NOT) &&
         static_cast<int>(type_) <= static_cast<int>(ExpressionType::LESS_EQUAL);
}

bool Expression::IsComparisonExpression() const {
  return static_cast<int>(type_) >= static_cast<int>(ExpressionType::EQUAL) &&
         static_cast<int>(type_) <= static_cast<int>(ExpressionType::LESS_EQUAL);
}

bool Expression::IsNullScalar() const {
  if (type_ != ExpressionType::SCALAR) {
    return false;
  }

  return !checked_cast<const ScalarExpression&>(*this).value()->is_valid;
}

bool Expression::IsBooleanScalar(BooleanScalar* out) const {
  if (type_ != ExpressionType::SCALAR) {
    return false;
  }

  auto scalar = checked_cast<const ScalarExpression&>(*this).value();
  if (scalar->type->id() != Type::BOOL) {
    return false;
  }

  if (out) {
    out->type = boolean();
    out->is_valid = scalar->is_valid;
    out->value = checked_cast<const BooleanScalar&>(*scalar).value;
  }
  return true;
}

std::shared_ptr<Expression> OperatorExpression::Copy() const {
  return std::make_shared<OperatorExpression>(*this);
}

std::shared_ptr<Expression> FieldReferenceExpression::Copy() const {
  return std::make_shared<FieldReferenceExpression>(*this);
}

std::shared_ptr<Expression> ScalarExpression::Copy() const {
  return std::make_shared<ScalarExpression>(*this);
}

std::shared_ptr<OperatorExpression> and_(ExpressionVector operands) {
  return std::make_shared<OperatorExpression>(ExpressionType::AND, std::move(operands));
}

std::shared_ptr<OperatorExpression> or_(ExpressionVector operands) {
  return std::make_shared<OperatorExpression>(ExpressionType::OR, std::move(operands));
}

std::shared_ptr<OperatorExpression> not_(std::shared_ptr<Expression> operand) {
  return std::make_shared<OperatorExpression>(ExpressionType::NOT,
                                              ExpressionVector{std::move(operand)});
}

// flatten chains of and/or to a single OperatorExpression
OperatorExpression MaybeCombine(ExpressionType::type type, const OperatorExpression& lhs,
                                const OperatorExpression& rhs) {
  if (lhs.type() != type && rhs.type() != type) {
    return OperatorExpression(type, {lhs.Copy(), rhs.Copy()});
  }
  ExpressionVector operands;
  if (lhs.type() == type) {
    operands = lhs.operands();
    if (rhs.type() == type) {
      for (auto operand : rhs.operands()) {
        operands.emplace_back(std::move(operand));
      }
    } else {
      operands.emplace_back(rhs.Copy());
    }
  } else {
    operands = rhs.operands();
    operands.emplace(operands.begin(), lhs.Copy());
  }
  return OperatorExpression(type, std::move(operands));
}

OperatorExpression operator and(const OperatorExpression& lhs,
                                const OperatorExpression& rhs) {
  return MaybeCombine(ExpressionType::AND, lhs, rhs);
}

OperatorExpression operator or(const OperatorExpression& lhs,
                               const OperatorExpression& rhs) {
  return MaybeCombine(ExpressionType::OR, lhs, rhs);
}

OperatorExpression operator not(const OperatorExpression& rhs) {
  return OperatorExpression(ExpressionType::NOT, {rhs.Copy()});
}

}  // namespace dataset
}  // namespace arrow
