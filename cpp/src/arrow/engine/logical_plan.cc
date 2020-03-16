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

#include "arrow/engine/logical_plan.h"

#include <utility>

#include "arrow/engine/expression.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace engine {

//
// LogicalPlan
//

LogicalPlan::LogicalPlan(std::shared_ptr<Expr> root) : root_(std::move(root)) {
  DCHECK_NE(root_, nullptr);
}

const ExprType& LogicalPlan::type() const { return root()->type(); }

bool LogicalPlan::Equals(const LogicalPlan& other) const {
  if (this == &other) {
    return true;
  }

  return root()->Equals(other.root());
}

std::string LogicalPlan::ToString() const { return root_->ToString(); }

//
// LogicalPlanBuilder
//

LogicalPlanBuilder::LogicalPlanBuilder(LogicalPlanBuilderOptions options)
    : catalog_(options.catalog) {}

using ResultExpr = LogicalPlanBuilder::ResultExpr;

#define ERROR_IF_TYPE(cond, ErrorType, ...)  \
  do {                                       \
    if (ARROW_PREDICT_FALSE(cond)) {         \
      return Status::ErrorType(__VA_ARGS__); \
    }                                        \
  } while (false)

#define ERROR_IF(cond, ...) ERROR_IF_TYPE(cond, Invalid, __VA_ARGS__)

//
// Leaf builder.
//

ResultExpr LogicalPlanBuilder::Scalar(const std::shared_ptr<arrow::Scalar>& scalar) {
  return ScalarExpr::Make(scalar);
}

ResultExpr LogicalPlanBuilder::Field(const std::shared_ptr<Expr>& input,
                                     const std::string& field_name) {
  return FieldRefExpr::Make(input, field_name);
}

ResultExpr LogicalPlanBuilder::Field(const std::shared_ptr<Expr>& input,
                                     int field_index) {
  return FieldRefExpr::Make(input, field_index);
}

ResultExpr LogicalPlanBuilder::Compare(CompareKind compare_kind,
                                       const std::shared_ptr<Expr>& lhs,
                                       const std::shared_ptr<Expr>& rhs) {
  switch (compare_kind) {
    case (CompareKind::EQUAL):
      return EqualExpr::Make(lhs, rhs);
    case (CompareKind::NOT_EQUAL):
      return NotEqualExpr::Make(lhs, rhs);
    case (CompareKind::GREATER_THAN):
      return GreaterThanExpr::Make(lhs, rhs);
    case (CompareKind::GREATER_THAN_EQUAL):
      return GreaterThanEqualExpr::Make(lhs, rhs);
    case (CompareKind::LESS_THAN):
      return LessThanExpr::Make(lhs, rhs);
    case (CompareKind::LESS_THAN_EQUAL):
      return LessThanEqualExpr::Make(lhs, rhs);
  }

  ARROW_UNREACHABLE;
}

ResultExpr LogicalPlanBuilder::Equal(const std::shared_ptr<Expr>& lhs,
                                     const std::shared_ptr<Expr>& rhs) {
  return Compare(CompareKind::EQUAL, lhs, rhs);
}

ResultExpr LogicalPlanBuilder::NotEqual(const std::shared_ptr<Expr>& lhs,
                                        const std::shared_ptr<Expr>& rhs) {
  return Compare(CompareKind::NOT_EQUAL, lhs, rhs);
}

ResultExpr LogicalPlanBuilder::GreaterThan(const std::shared_ptr<Expr>& lhs,
                                           const std::shared_ptr<Expr>& rhs) {
  return Compare(CompareKind::GREATER_THAN, lhs, rhs);
}

ResultExpr LogicalPlanBuilder::GreaterThanEqual(const std::shared_ptr<Expr>& lhs,
                                                const std::shared_ptr<Expr>& rhs) {
  return Compare(CompareKind::GREATER_THAN_EQUAL, lhs, rhs);
}

ResultExpr LogicalPlanBuilder::LessThan(const std::shared_ptr<Expr>& lhs,
                                        const std::shared_ptr<Expr>& rhs) {
  return Compare(CompareKind::LESS_THAN, lhs, rhs);
}

ResultExpr LogicalPlanBuilder::LessThanEqual(const std::shared_ptr<Expr>& lhs,
                                             const std::shared_ptr<Expr>& rhs) {
  return Compare(CompareKind::LESS_THAN_EQUAL, lhs, rhs);
}

//
// Count
//

ResultExpr LogicalPlanBuilder::Count(const std::shared_ptr<Expr>& input) {
  return CountExpr::Make(input);
}

ResultExpr LogicalPlanBuilder::Sum(const std::shared_ptr<Expr>& input) {
  return SumExpr::Make(input);
}

//
// Relational
//

ResultExpr LogicalPlanBuilder::Scan(const std::string& table_name) {
  ERROR_IF(catalog_ == nullptr, "Cannot scan from an empty catalog");
  ARROW_ASSIGN_OR_RAISE(auto table, catalog_->Get(table_name));
  return ScanRelExpr::Make(table);
}

ResultExpr LogicalPlanBuilder::Filter(const std::shared_ptr<Expr>& input,
                                      const std::shared_ptr<Expr>& predicate) {
  return FilterRelExpr::Make(input, predicate);
}

ResultExpr LogicalPlanBuilder::Project(
    const std::shared_ptr<Expr>& input,
    const std::vector<std::shared_ptr<Expr>>& expressions) {
  return ProjectionRelExpr::Make(input, expressions);
}

ResultExpr LogicalPlanBuilder::Project(const std::shared_ptr<Expr>& input,
                                       const std::vector<std::string>& column_names) {
  ERROR_IF(input == nullptr, "Input expression can't be null.");
  ERROR_IF(column_names.empty(), "Must have at least one column name.");

  std::vector<std::shared_ptr<Expr>> expressions{column_names.size()};
  for (size_t i = 0; i < column_names.size(); i++) {
    ARROW_ASSIGN_OR_RAISE(expressions[i], Field(input, column_names[i]));
  }

  // TODO(fsaintjacques): preserve field names.
  return Project(input, expressions);
}

ResultExpr LogicalPlanBuilder::Project(const std::shared_ptr<Expr>& input,
                                       const std::vector<int>& column_indices) {
  ERROR_IF(input == nullptr, "Input expression can't be null.");
  ERROR_IF(column_indices.empty(), "Must have at least one column index.");

  std::vector<std::shared_ptr<Expr>> expressions{column_indices.size()};
  for (size_t i = 0; i < column_indices.size(); i++) {
    ARROW_ASSIGN_OR_RAISE(expressions[i], Field(input, column_indices[i]));
  }

  // TODO(fsaintjacques): preserve field names.
  return Project(input, expressions);
}

#undef ERROR_IF
#undef ERROR_IF_TYPE

}  // namespace engine
}  // namespace arrow
