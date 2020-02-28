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

#include "arrow/engine/expression.h"
#include "arrow/engine/type_traits.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace engine {

//
// ExprType
//

ExprType ExprType::Scalar(std::shared_ptr<DataType> type) {
  return ExprType(std::move(type), Shape::SCALAR);
}

ExprType ExprType::Array(std::shared_ptr<DataType> type) {
  return ExprType(std::move(type), Shape::ARRAY);
}

ExprType ExprType::Table(std::shared_ptr<Schema> schema) {
  return ExprType(std::move(schema), Shape::TABLE);
}

ExprType::ExprType(std::shared_ptr<Schema> schema, Shape shape)
    : type_(std::move(schema)), shape_(shape) {
  DCHECK_EQ(shape, Shape::TABLE);
}

ExprType::ExprType(std::shared_ptr<DataType> type, Shape shape)
    : type_(std::move(type)), shape_(shape) {
  DCHECK_NE(shape, Shape::TABLE);
}

std::shared_ptr<Schema> ExprType::schema() const {
  if (shape_ == TABLE) {
    return util::get<std::shared_ptr<Schema>>(type_);
  }

  return nullptr;
}

std::shared_ptr<DataType> ExprType::data_type() const {
  if (shape_ != TABLE) {
    return util::get<std::shared_ptr<DataType>>(type_);
  }

  return nullptr;
}

bool ExprType::Equals(const ExprType& type) const {
  if (this == &type) {
    return true;
  }

  if (shape() != type.shape()) {
    return false;
  }

  switch (shape()) {
    case SCALAR:
      return data_type()->Equals(type.data_type());
    case ARRAY:
      return data_type()->Equals(type.data_type());
    case TABLE:
      return schema()->Equals(type.schema());
    default:
      break;
  }

  return false;
}

#define ERROR_IF(cond, ...)                \
  do {                                     \
    if (ARROW_PREDICT_FALSE(cond)) {       \
      return Status::Invalid(__VA_ARGS__); \
    }                                      \
  } while (false)

//
// Expr
//

std::string Expr::kind_name() const {
  switch (kind_) {
    case Expr::SCALAR_LITERAL:
      return "scalar";
    case Expr::FIELD_REFERENCE:
      return "field_ref";

    case Expr::EQ_CMP_OP:
      return "eq_cmp";
    case Expr::NE_CMP_OP:
      return "ne_cmp";
    case Expr::GT_CMP_OP:
      return "gt_cmp";
    case Expr::GE_CMP_OP:
      return "ge_cmp";
    case Expr::LT_CMP_OP:
      return "lt_cmp";
    case Expr::LE_CMP_OP:
      return "le_cmp";

    case Expr::EMPTY_REL:
      return "empty_rel";
    case Expr::SCAN_REL:
      return "scan_rel";
    case Expr::FILTER_REL:
      return "filter_rel";
  }

  return "unknown expr";
}

struct ExprEqualityVisitor {
  bool operator()(const ScalarExpr& rhs) const {
    auto lhs_scalar = internal::checked_cast<const ScalarExpr&>(lhs);
    return lhs_scalar.scalar()->Equals(*rhs.scalar());
  }

  bool operator()(const FieldRefExpr& rhs) const {
    auto lhs_field = internal::checked_cast<const FieldRefExpr&>(lhs);
    return lhs_field.field()->Equals(*rhs.field());
  }

  template <typename E>
  enable_if_compare_expr<E, bool> operator()(const E& rhs) const {
    auto lhs_cmp = internal::checked_cast<const E&>(lhs);
    return (lhs_cmp.left_operand()->Equals(rhs.left_operand()) &&
            lhs_cmp.right_operand()->Equals(rhs.right_operand())) ||
           (lhs_cmp.left_operand()->Equals(rhs.right_operand()) &&
            lhs_cmp.left_operand()->Equals(rhs.right_operand()));
  }

  bool operator()(const EmptyRelExpr& rhs) const {
    auto lhs_empty = internal::checked_cast<const EmptyRelExpr&>(lhs);
    return lhs_empty.schema()->Equals(rhs.schema());
  }

  bool operator()(const ScanRelExpr& rhs) const {
    auto lhs_scan = internal::checked_cast<const ScanRelExpr&>(lhs);
    // Performs a pointer equality on Table/Dataset
    return lhs_scan.input() == rhs.input();
  }

  bool operator()(const Expr&) const { return false; }

  static bool Visit(const Expr& lhs, const Expr& rhs) {
    return VisitExpr(rhs, ExprEqualityVisitor{lhs});
  }

  const Expr& lhs;
};

bool Expr::Equals(const Expr& other) const {
  if (this == &other) {
    return true;
  }

  if (kind() != other.kind() || type() != other.type()) {
    return false;
  }

  return ExprEqualityVisitor::Visit(*this, other);
}

//
// ScalarExpr
//

ScalarExpr::ScalarExpr(std::shared_ptr<Scalar> scalar)
    : Expr(SCALAR_LITERAL), scalar_(std::move(scalar)) {}

Result<std::shared_ptr<ScalarExpr>> ScalarExpr::Make(std::shared_ptr<Scalar> scalar) {
  ERROR_IF(scalar == nullptr, "ScalarExpr's scalar must be non-null");

  return std::shared_ptr<ScalarExpr>(new ScalarExpr(std::move(scalar)));
}

ExprType ScalarExpr::type() const { return ExprType::Scalar(scalar_->type); }

//
// FieldRefExpr
//

FieldRefExpr::FieldRefExpr(std::shared_ptr<Field> field)
    : Expr(FIELD_REFERENCE), field_(std::move(field)) {}

Result<std::shared_ptr<FieldRefExpr>> FieldRefExpr::Make(std::shared_ptr<Field> field) {
  ERROR_IF(field == nullptr, "FieldRefExpr's field must be non-null");

  return std::shared_ptr<FieldRefExpr>(new FieldRefExpr(std::move(field)));
}

ExprType FieldRefExpr::type() const { return ExprType::Scalar(field_->type()); }

//
// Comparisons
//

Status ValidateCompareOpInputs(const std::shared_ptr<Expr>& left,
                               const std::shared_ptr<Expr>& right) {
  ERROR_IF(left == nullptr, "EqualCmpExpr's left operand must be non-null");
  ERROR_IF(right == nullptr, "EqualCmpExpr's right operand must be non-null");

  // TODO(fsaintjacques): Add support for broadcast.
  ERROR_IF(left->type() != right->type(),
           "Compare operator operands must be of same type.");

  return Status::OK();
}

//
// EmptyRelExpr
//

Result<std::shared_ptr<EmptyRelExpr>> EmptyRelExpr::Make(std::shared_ptr<Schema> schema) {
  ERROR_IF(schema == nullptr, "EmptyRelExpr schema must be non-null");
  return std::shared_ptr<EmptyRelExpr>(new EmptyRelExpr(std::move(schema)));
}

//
// ScanRelExpr
//

ScanRelExpr::ScanRelExpr(Catalog::Entry input)
    : RelExpr(input.schema()), input_(std::move(input)) {}

Result<std::shared_ptr<ScanRelExpr>> ScanRelExpr::Make(Catalog::Entry input) {
  return std::shared_ptr<ScanRelExpr>(new ScanRelExpr(std::move(input)));
}

//
// FilterRelExpr
//

Result<std::shared_ptr<FilterRelExpr>> FilterRelExpr::Make(
    std::shared_ptr<Expr> input, std::shared_ptr<Expr> predicate) {
  ERROR_IF(input == nullptr, "FilterRelExpr's input must be non-null.");
  ERROR_IF(!input->type().IsTable(), "FilterRelExpr's input must be a table.");
  ERROR_IF(predicate == nullptr, "FilterRelExpr's predicate must be non-null.");
  ERROR_IF(!predicate->type().IsPredicate(),
           "FilterRelExpr's predicate must be a predicate");

  // TODO(fsaintjacques): check fields referenced in predicate are found in
  // input.

  return std::shared_ptr<FilterRelExpr>(
      new FilterRelExpr(std::move(input), std::move(predicate)));
}

FilterRelExpr::FilterRelExpr(std::shared_ptr<Expr> input, std::shared_ptr<Expr> predicate)
    : UnaryOpExpr(std::move(input)),
      RelExpr(operand()->type().schema()),
      predicate_(std::move(predicate)) {}

#undef ERROR_IF

}  // namespace engine
}  // namespace arrow
