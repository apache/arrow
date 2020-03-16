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

#include "arrow/compute/kernels/sum_internal.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace engine {

//
// ExprType
//

std::string ShapeToString(ExprType::Shape shape) {
  switch (shape) {
    case ExprType::SCALAR:
      return "scalar";
    case ExprType::ARRAY:
      return "array";
    case ExprType::TABLE:
      return "table";
  }

  return "";
}

ExprType ExprType::Scalar(std::shared_ptr<DataType> type) {
  return ExprType(std::move(type), SCALAR);
}

ExprType ExprType::Array(std::shared_ptr<DataType> type) {
  return ExprType(std::move(type), ARRAY);
}

ExprType ExprType::Table(std::shared_ptr<Schema> schema) {
  return ExprType(std::move(schema), TABLE);
}

ExprType ExprType::Table(std::vector<std::shared_ptr<Field>> fields) {
  return ExprType(arrow::schema(std::move(fields)), TABLE);
}

ExprType::ExprType(std::shared_ptr<Schema> schema, Shape shape)
    : schema_(std::move(schema)), shape_(shape) {
  DCHECK_EQ(shape, TABLE);
}

ExprType::ExprType(std::shared_ptr<DataType> type, Shape shape)
    : data_type_(std::move(type)), shape_(shape) {
  DCHECK_NE(shape, TABLE);
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

#define ERROR_IF_TYPE(cond, ErrorType, ...)  \
  do {                                       \
    if (ARROW_PREDICT_FALSE(cond)) {         \
      return Status::ErrorType(__VA_ARGS__); \
    }                                        \
  } while (false)

#define ERROR_IF(cond, ...) ERROR_IF_TYPE(cond, Invalid, __VA_ARGS__)

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
    case ExprKind::AGGREGATE_FN_OP:
      return "aggregate_fn_op";
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
    return lhs_field.index() == rhs.index() &&
           lhs_field.operand()->Equals(*rhs.operand());
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

FieldRefExpr::FieldRefExpr(std::shared_ptr<Expr> input, int index)
    : UnaryOpMixin(std::move(input)),
      Expr(FIELD_REFERENCE,
           ExprType::Array(operand()->type().schema()->field(index)->type())),
      index_(index) {}

Result<std::shared_ptr<FieldRefExpr>> FieldRefExpr::Make(std::shared_ptr<Expr> input,
                                                         int index) {
  ERROR_IF(input == nullptr, "FieldRefExpr's input must be non-null");

  auto expr_type = input->type();
  ERROR_IF(!expr_type.IsTable(), "FieldRefExpr's input must have a table shape, got '",
           ShapeToString(expr_type.shape()), "'");

  auto schema = expr_type.schema();
  ERROR_IF_TYPE(index < 0 || index >= schema->num_fields(), KeyError,
                "FieldRefExpr's index is out of bound, '", index, "' not in range [0, ",
                schema->num_fields(), ")");

  return std::shared_ptr<FieldRefExpr>(new FieldRefExpr(std::move(input), index));
}

Result<std::shared_ptr<FieldRefExpr>> FieldRefExpr::Make(std::shared_ptr<Expr> input,
                                                         std::string field_name) {
  ERROR_IF(input == nullptr, "FieldRefExpr's input must be non-null");

  auto expr_type = input->type();
  ERROR_IF(!expr_type.IsTable(), "FieldRefExpr's input must have a table shape, got '",
           ShapeToString(expr_type.shape()), "'");

  auto schema = expr_type.schema();
  auto field = schema->GetFieldByName(field_name);
  ERROR_IF_TYPE(field == nullptr, KeyError,
                "FieldRefExpr's can't reference with field name '", field_name, "'");

  auto index = schema->GetFieldIndex(field_name);
  ERROR_IF(index == -1, "FieldRefExpr's index by name is invalid.");

  return std::shared_ptr<FieldRefExpr>(new FieldRefExpr(std::move(input), index));
}

//
// CountExpr
//

CountExpr::CountExpr(std::shared_ptr<Expr> input)
    : UnaryOpMixin(std::move(input)),
      AggregateFnExpr(ExprType::Scalar(int64()), AggregateFnKind::COUNT) {}

Result<std::shared_ptr<CountExpr>> CountExpr::Make(std::shared_ptr<Expr> input) {
  ERROR_IF(input == nullptr, "CountExpr's input must be non-null");
  return std::shared_ptr<CountExpr>(new CountExpr(std::move(input)));
}

//
// SumExpr
//

SumExpr::SumExpr(std::shared_ptr<Expr> input)
    : UnaryOpMixin(std::move(input)),
      AggregateFnExpr(
          ExprType::Scalar(arrow::compute::GetAccumulatorType(operand()->type().type())),
          AggregateFnKind::SUM) {}

Result<std::shared_ptr<SumExpr>> SumExpr::Make(std::shared_ptr<Expr> input) {
  ERROR_IF(input == nullptr, "SumExpr's input must be non-null");

  auto expr_type = input->type();
  ERROR_IF(!expr_type.HasType(), "SumExpr's input must be a Scalar or an Array");

  auto type = expr_type.type();
  ERROR_IF(!is_numeric(type->id()), "SumExpr's require an input with numeric type");

  return std::shared_ptr<SumExpr>(new SumExpr(std::move(input)));
}

//
// EmptyRelExpr
//

EmptyRelExpr::EmptyRelExpr(std::shared_ptr<Schema> schema)
    : RelExpr(ExprKind::EMPTY_REL, std::move(schema)) {}

Result<std::shared_ptr<EmptyRelExpr>> EmptyRelExpr::Make(std::shared_ptr<Schema> schema) {
  ERROR_IF(schema == nullptr, "EmptyRelExpr schema must be non-null");
  return std::shared_ptr<EmptyRelExpr>(new EmptyRelExpr(std::move(schema)));
}

//
// ScanRelExpr
//

ScanRelExpr::ScanRelExpr(Catalog::Entry input)
    : RelExpr(ExprKind::SCAN_REL, input.schema()), input_(std::move(input)) {}

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
      RelExpr(ExprKind::PROJECTION_REL, std::move(schema)),
      expressions_(std::move(expressions)) {}

Result<std::shared_ptr<ProjectionRelExpr>> ProjectionRelExpr::Make(
    std::shared_ptr<Expr> input, std::vector<std::shared_ptr<Expr>> expressions) {
  ERROR_IF(input == nullptr, "ProjectionRelExpr's input must be non-null.");
  ERROR_IF(expressions.empty(), "Must project at least one expression.");

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
      RelExpr(ExprKind::FILTER_REL, operand()->type().schema()),
      predicate_(std::move(predicate)) {}

#undef ERROR_IF
#undef ERROR_IF_TYPE

}  // namespace engine
}  // namespace arrow
