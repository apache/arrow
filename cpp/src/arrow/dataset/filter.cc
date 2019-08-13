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

#include "arrow/buffer.h"
#include "arrow/compute/kernels/boolean.h"
#include "arrow/compute/kernels/compare.h"
#include "arrow/record_batch.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace dataset {

using internal::checked_cast;

Status ExpressionFilter::Execute(compute::FunctionContext* ctx, const RecordBatch& batch,
                                 std::shared_ptr<BooleanArray>* filter) const {
  using arrow::compute::Datum;
  if (!expression_->IsOperatorExpression()) {
    return Status::Invalid("can't execute expression ", expression_->ToString());
  }

  const auto& op = checked_cast<const OperatorExpression&>(*expression_).operands();

  if (expression_->IsComparisonExpression()) {
    const auto& lhs = checked_cast<const FieldReferenceExpression&>(*op[0]);
    const auto& rhs = checked_cast<const ScalarExpression&>(*op[1]);
    using arrow::compute::CompareOperator;
    arrow::compute::CompareOptions opts(static_cast<CompareOperator>(
        static_cast<int>(CompareOperator::EQUAL) + static_cast<int>(expression_->type()) -
        static_cast<int>(ExpressionType::EQUAL)));
    Datum out;
    RETURN_NOT_OK(arrow::compute::Compare(ctx, Datum(batch.GetColumnByName(lhs.name())),
                                          Datum(rhs.value()), opts, &out));
    *filter = internal::checked_pointer_cast<BooleanArray>(out.make_array());
    return Status::OK();
  }

  if (expression_->type() == ExpressionType::NOT) {
    std::shared_ptr<BooleanArray> to_invert;
    RETURN_NOT_OK(ExpressionFilter(op[0]).Execute(ctx, batch, &to_invert));
    Datum out;
    RETURN_NOT_OK(arrow::compute::Invert(ctx, Datum(to_invert), &out));
    *filter = internal::checked_pointer_cast<BooleanArray>(out.make_array());
    return Status::OK();
  }

  DCHECK(expression_->type() == ExpressionType::OR ||
         expression_->type() == ExpressionType::AND);

  std::shared_ptr<BooleanArray> next;
  RETURN_NOT_OK(ExpressionFilter(op[0]).Execute(ctx, batch, &next));
  Datum acc(next);

  for (size_t i_next = 1; i_next < op.size(); ++i_next) {
    RETURN_NOT_OK(ExpressionFilter(op[i_next]).Execute(ctx, batch, &next));

    if (expression_->type() == ExpressionType::OR) {
      RETURN_NOT_OK(arrow::compute::Or(ctx, Datum(acc), Datum(next), &acc));
    } else {
      RETURN_NOT_OK(arrow::compute::And(ctx, Datum(acc), Datum(next), &acc));
    }
  }

  *filter = internal::checked_pointer_cast<BooleanArray>(acc.make_array());
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

// return a pair<e is a boolean scalar expression, value of that expression>
std::pair<bool, bool> IsBoolean(const Expression& e) {
  if (e.type() == ExpressionType::SCALAR) {
    auto value = checked_cast<const ScalarExpression&>(e).value();
    if (value->type->id() == Type::BOOL) {
      // FIXME(bkietz) null scalars do what?
      return {true, checked_cast<const BooleanScalar&>(*value).value};
    }
  }
  return {false, false};
}

Result<std::shared_ptr<Expression>> AssumeIfOperator(const std::shared_ptr<Expression>& e,
                                                     const Expression& given) {
  if (!e->IsOperatorExpression()) {
    return e;
  }
  return checked_cast<const OperatorExpression&>(*e).Assume(given);
}

struct CompareVisitor {
  Status Visit(const BooleanType&) {
    result_ = checked_cast<const BooleanScalar&>(lhs_).value -
              checked_cast<const BooleanScalar&>(rhs_).value;
    return Status::OK();
  }

  Status Visit(const Int64Type&) {
    result_ = checked_cast<const Int64Scalar&>(lhs_).value -
              checked_cast<const Int64Scalar&>(rhs_).value;
    return Status::OK();
  }

  Status Visit(const DoubleType&) {
    double result = checked_cast<const DoubleScalar&>(lhs_).value -
                    checked_cast<const DoubleScalar&>(rhs_).value;
    result_ = result < 0.0 ? -1 : result > 0.0 ? +1 : 0;
    return Status::OK();
  }

  Status Visit(const StringType&) {
    auto lhs = checked_cast<const StringScalar&>(lhs_).value;
    auto rhs = checked_cast<const StringScalar&>(rhs_).value;
    result_ = std::memcmp(lhs->data(), rhs->data(), std::min(lhs->size(), rhs->size()));
    if (result_ == 0) {
      result_ = lhs->size() - rhs->size();
    }
    return Status::OK();
  }

  Status Visit(const DataType&) {
    return Status::NotImplemented("comparison of scalars of type ", *lhs_.type);
  }

  int64_t result_;
  const Scalar& lhs_;
  const Scalar& rhs_;
};

Result<int64_t> Compare(const Scalar& lhs, const Scalar& rhs) {
  CompareVisitor vis{0, lhs, rhs};
  RETURN_NOT_OK(VisitTypeInline(*lhs.type, &vis));
  return vis.result_;
}

Result<std::shared_ptr<Expression>> AssumeComparison(const OperatorExpression& e,
                                                     const OperatorExpression& given) {
  // TODO(bkietz) allow the RHS of e to be FIELD
  auto e_rhs = checked_cast<const ScalarExpression&>(*e.operands()[1]).value();
  // TODO(bkietz) allow the RHS of given to be FROM_STRING
  auto given_rhs = checked_cast<const ScalarExpression&>(*given.operands()[1]).value();

  ARROW_ASSIGN_OR_RAISE(auto cmp, Compare(*e_rhs, *given_rhs));

  static auto always = ScalarExpression::Make(true);
  static auto never = ScalarExpression::Make(false);
  auto unsimplified = MakeShared(e);

  if (cmp == 0) {
    // the rhs of the comparisons are equal
    switch (e.type()) {
      case ExpressionType::EQUAL:
        switch (given.type()) {
          case ExpressionType::NOT_EQUAL:
          case ExpressionType::GREATER:
          case ExpressionType::LESS:
            return never;
          case ExpressionType::EQUAL:
          case ExpressionType::GREATER_EQUAL:
          case ExpressionType::LESS_EQUAL:
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
  } else if (cmp > 0) {
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

std::string GetName(const Expression& e) {
  DCHECK_EQ(e.type(), ExpressionType::FIELD);
  return checked_cast<const FieldReferenceExpression&>(e).name();
}

Result<std::shared_ptr<Expression>> OperatorExpression::Assume(
    const Expression& given) const {
  auto unsimplified = MakeShared(*this);

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
      if (GetName(*operands_[0]) != GetName(*given_op.operands_[0])) {
        return unsimplified;
      }
      return AssumeComparison(*this, given_op);
    }

    // must be NOT, AND, OR- decompose given
    switch (given.type()) {
      case ExpressionType::NOT: {
        return unsimplified;
      }

      case ExpressionType::OR: {
        bool simplify_to_always = true;
        bool simplify_to_never = true;
        for (auto operand : given_op.operands_) {
          ARROW_ASSIGN_OR_RAISE(auto simplified, Assume(*operand));
          auto isbool_value = IsBoolean(*simplified);
          if (!isbool_value.first) {
            return unsimplified;
          }

          if (isbool_value.second) {
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
        for (auto operand : given_op.operands_) {
          auto isbool_value = IsBoolean(*simplified);
          if (isbool_value.first) {
            break;
          }
          DCHECK(simplified->IsOperatorExpression());
          const auto& simplified_op =
              checked_cast<const OperatorExpression&>(*simplified);
          ARROW_ASSIGN_OR_RAISE(simplified, simplified_op.Assume(*operand));
        }
        return simplified;
      }

      default:
        DCHECK(false);
    }
  }

  switch (type_) {
    case ExpressionType::NOT: {
      DCHECK_EQ(operands_.size(), 1);
      ARROW_ASSIGN_OR_RAISE(auto operand, AssumeIfOperator(operands_[0], given));
      auto isbool_value = IsBoolean(*operand);
      if (isbool_value.first) {
        return ScalarExpression::Make(!isbool_value.second);
      }
      return std::make_shared<OperatorExpression>(
          ExpressionType::NOT, std::vector<std::shared_ptr<Expression>>{operand});
    }

    case ExpressionType::OR:
    case ExpressionType::AND: {
      // if any of the operands matches trivial_condition, we can return a trivial
      // expression:
      // anything OR true => true
      // anything AND false => false
      bool trivial_condition = type_ == ExpressionType::OR;

      std::vector<std::shared_ptr<Expression>> operands;
      for (auto operand : operands_) {
        ARROW_ASSIGN_OR_RAISE(operand, AssumeIfOperator(operand, given));

        auto isbool_value = IsBoolean(*operand);
        if (isbool_value.first) {
          if (isbool_value.second == trivial_condition) {
            return ScalarExpression::Make(trivial_condition);
          }
          continue;
        }

        operands.push_back(operand);
      }

      return std::make_shared<OperatorExpression>(type_, std::move(operands));
    }

    default:
      DCHECK(false);
  }

  return unsimplified;
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
  return "scalar<" + value_->type->ToString() + ">(TODO)";
}

bool Expression::Equals(const Expression& other) const {
  if (type_ != other.type()) {
    return false;
  }

  // FIXME(bkietz) create FromStringExpression
  DCHECK_NE(type_, ExpressionType::FROM_STRING);

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

}  // namespace dataset
}  // namespace arrow
