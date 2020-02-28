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
#include "arrow/util/logging.h"

namespace arrow {
namespace engine {

//
// LogicalPlan
//

LogicalPlan::LogicalPlan(std::shared_ptr<Expr> root) : root_(std::move(root)) {
  DCHECK_NE(root_, nullptr);
}

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

#define ERROR_IF(cond, ...)                \
  do {                                     \
    if (ARROW_PREDICT_FALSE(cond)) {       \
      return Status::Invalid(__VA_ARGS__); \
    }                                      \
  } while (false)

//
// Leaf builder.
//

ResultExpr LogicalPlanBuilder::Scalar(const std::shared_ptr<arrow::Scalar>& scalar) {
  return ScalarExpr::Make(scalar);
}

ResultExpr LogicalPlanBuilder::Field(const std::shared_ptr<Expr>& input,
                                     const std::string& field_name) {
  ERROR_IF(input == nullptr, "Input expression must be non-null");

  auto expr_type = input->type();
  ERROR_IF(!expr_type.IsTable(), "Input expression does not have a Table shape.");

  auto field = expr_type.schema()->GetFieldByName(field_name);
  ERROR_IF(field == nullptr, "Cannot reference field '", field_name, "' in schema.");

  return FieldRefExpr::Make(std::move(field));
}

//
// Relational
//

ResultExpr LogicalPlanBuilder::Scan(const std::string& table_name) {
  ERROR_IF(catalog_ == nullptr, "Cannot scan from an empty catalog");
  ARROW_ASSIGN_OR_RAISE(auto table, catalog_->Get(table_name));
  return ScanRelExpr::Make(std::move(table));
}

ResultExpr LogicalPlanBuilder::Filter(const std::shared_ptr<Expr>& input,
                                      const std::shared_ptr<Expr>& predicate) {
  ERROR_IF(input == nullptr, "Input expression can't be null.");
  ERROR_IF(predicate == nullptr, "Predicate expression can't be null.");
  return FilterRelExpr::Make(std::move(input), std::move(predicate));
}

#undef ERROR_IF

}  // namespace engine
}  // namespace arrow
