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

ExprType ExprType::Table(std::vector<std::shared_ptr<Field>> fields) {
  return ExprType(arrow::schema(std::move(fields)), Shape::TABLE);
}

ExprType::ExprType(std::shared_ptr<Schema> schema, Shape shape)
    : schema_(std::move(schema)), shape_(shape) {
  DCHECK_EQ(shape, Shape::TABLE);
}

ExprType::ExprType(std::shared_ptr<DataType> type, Shape shape)
    : data_type_(std::move(type)), shape_(shape) {
  DCHECK_NE(shape, Shape::TABLE);
}

ExprType::ExprType(const ExprType& other) : shape_(other.shape()) {
  switch (other.shape()) {
    case SCALAR:
    case ARRAY:
      data_type_ = other.type();
      break;
    case TABLE:
      schema_ = other.schema();
  }
}

ExprType::ExprType(ExprType&& other) : shape_(other.shape()) {
  switch (other.shape()) {
    case SCALAR:
    case ARRAY:
      data_type_ = std::move(other.type());
      break;
    case TABLE:
      schema_ = std::move(other.schema());
  }
}

ExprType::~ExprType() {
  switch (shape()) {
    case SCALAR:
    case ARRAY:
      data_type_.reset();
      break;
    case TABLE:
      schema_.reset();
  }
}

bool ExprType::Equals(const ExprType& other) const {
  if (this == &other) {
    return true;
  }

  if (shape() != other.shape()) {
    return false;
  }

  switch (shape()) {
    case SCALAR:
      return type()->Equals(other.type());
    case ARRAY:
      return type()->Equals(other.type());
    case TABLE:
      return schema()->Equals(other.schema());
    default:
      break;
  }

  return false;
}

Result<ExprType> ExprType::WithType(const std::shared_ptr<DataType>& data_type) const {
  switch (shape()) {
    case SCALAR:
      return ExprType::Scalar(data_type);
    case ARRAY:
      return ExprType::Array(data_type);
    case TABLE:
      return Status::Invalid("Cannot cast a TableType with a DataType");
  }

  return Status::UnknownError("unreachable");
}

Result<ExprType> ExprType::WithSchema(const std::shared_ptr<Schema>& schema) const {
  switch (shape()) {
    case SCALAR:
      return Status::Invalid("Cannot cast a ScalarType with a schema");
    case ARRAY:
      return Status::Invalid("Cannot cast an ArrayType with a schema");
    case TABLE:
      return ExprType::Table(schema);
  }

  return Status::UnknownError("unreachable");
}

Result<ExprType> ExprType::Broadcast(const ExprType& lhs, const ExprType& rhs) {
  if (lhs.IsTable() || rhs.IsTable()) {
    return Status::Invalid("Broadcast operands must not be tables");
  }

  if (!lhs.type()->Equals(rhs.type())) {
    return Status::Invalid("Broadcast operands must be of same type");
  }

  if (lhs.IsArray()) {
    return lhs;
  }

  if (rhs.IsArray()) {
    return rhs;
  }

  return lhs;
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
    case ExprKind::SCALAR_LITERAL:
      return "scalar";
    case ExprKind::FIELD_REFERENCE:
      return "field_ref";
    case ExprKind::COMPARE_OP:
      return "compare_op";
    case ExprKind::EMPTY_REL:
      return "empty_rel";
    case ExprKind::SCAN_REL:
      return "scan_rel";
    case ExprKind::PROJECTION_REL:
      return "projection_rel";
    case ExprKind::FILTER_REL:
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

  bool operator()(const ProjectionRelExpr& rhs) const {
    auto lhs_proj = internal::checked_cast<const ProjectionRelExpr&>(lhs);

    const auto& lhs_exprs = lhs_proj.expressions();
    const auto& rhs_exprs = rhs.expressions();
    if (lhs_exprs.size() != rhs_exprs.size()) {
      return false;
    }

    for (size_t i = 0; i < lhs_exprs.size(); i++) {
      if (!lhs_exprs[i]->Equals(rhs_exprs[i])) {
        return false;
      }
    }

    return true;
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

std::string Expr::ToString() const { return ""; }

//
// ScalarExpr
//

ScalarExpr::ScalarExpr(std::shared_ptr<Scalar> scalar)
    : Expr(SCALAR_LITERAL, ExprType::Scalar(scalar->type)), scalar_(std::move(scalar)) {}

Result<std::shared_ptr<ScalarExpr>> ScalarExpr::Make(std::shared_ptr<Scalar> scalar) {
  ERROR_IF(scalar == nullptr, "ScalarExpr's scalar must be non-null");
  return std::shared_ptr<ScalarExpr>(new ScalarExpr(std::move(scalar)));
}

//
// FieldRefExpr
//

FieldRefExpr::FieldRefExpr(std::shared_ptr<Field> f)
    : Expr(FIELD_REFERENCE, ExprType::Array(f->type())), field_(std::move(f)) {}

Result<std::shared_ptr<FieldRefExpr>> FieldRefExpr::Make(std::shared_ptr<Field> field) {
  ERROR_IF(field == nullptr, "FieldRefExpr's field must be non-null");
  return std::shared_ptr<FieldRefExpr>(new FieldRefExpr(std::move(field)));
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
// ProjectionRelExpr
//

ProjectionRelExpr::ProjectionRelExpr(std::shared_ptr<Expr> input,
                                     std::shared_ptr<Schema> schema,
                                     std::vector<std::shared_ptr<Expr>> expressions)
    : UnaryOpMixin(std::move(input)),
      RelExpr(std::move(schema)),
      expressions_(std::move(expressions)) {}

Result<std::shared_ptr<ProjectionRelExpr>> ProjectionRelExpr::Make(
    std::shared_ptr<Expr> input, std::vector<std::shared_ptr<Expr>> expressions) {
  ERROR_IF(input == nullptr, "ProjectionRelExpr's input must be non-null.");
  ERROR_IF(expressions.empty(), "Must project at least one column.");

  auto n_fields = expressions.size();
  std::vector<std::shared_ptr<Field>> fields;

  for (size_t i = 0; i < n_fields; i++) {
    const auto& expr = expressions[i];
    const auto& expr_type = expr->type();
    ERROR_IF(!expr_type.HasType(), "Expression at position ", i,
             " should not be have a table shape");
    // TODO(fsaintjacques): better name handling. Callers should be able to
    // pass a vector of names.
    fields.push_back(field("expr", expr_type.type()));
  }

  return std::shared_ptr<ProjectionRelExpr>(new ProjectionRelExpr(
      std::move(input), arrow::schema(std::move(fields)), std::move(expressions)));
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

  return std::shared_ptr<FilterRelExpr>(
      new FilterRelExpr(std::move(input), std::move(predicate)));
}

FilterRelExpr::FilterRelExpr(std::shared_ptr<Expr> input, std::shared_ptr<Expr> predicate)
    : UnaryOpMixin(std::move(input)),
      RelExpr(operand()->type().schema()),
      predicate_(std::move(predicate)) {}

#undef ERROR_IF

}  // namespace engine
}  // namespace arrow
