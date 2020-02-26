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

LogicalPlanBuilder::LogicalPlanBuilder(std::shared_ptr<Catalog> catalog)
    : catalog_(std::move(catalog)) {}

Status LogicalPlanBuilder::Scan(const std::string& table_name) {
  if (catalog_ == nullptr) {
    return Status::Invalid("Cannot scan from an empty catalog");
  }

  ARROW_ASSIGN_OR_RAISE(auto table, catalog_->Get(table_name));
  ARROW_ASSIGN_OR_RAISE(auto scan, ScanRelExpr::Make(std::move(table)));
  return Push(std::move(scan));
}

Status LogicalPlanBuilder::Filter(std::shared_ptr<Expr> predicate) {
  ARROW_ASSIGN_OR_RAISE(auto input, Peek());
  ARROW_ASSIGN_OR_RAISE(auto filter,
                        FilterRelExpr::Make(std::move(input), std::move(predicate)));
  return Push(std::move(filter));
}

Result<std::shared_ptr<LogicalPlan>> LogicalPlanBuilder::Finish() {
  if (stack_.empty()) {
    return Status::Invalid("LogicalPlan is empty, nothing to construct.");
  }

  ARROW_ASSIGN_OR_RAISE(auto root, Pop());
  if (!stack_.empty()) {
    return Status::Invalid("LogicalPlan is ignoring operators left on the stack.");
  }

  return std::make_shared<LogicalPlan>(root);
}

Result<std::shared_ptr<Expr>> LogicalPlanBuilder::Peek() {
  if (stack_.empty()) {
    return Status::Invalid("No Expr left on stack");
  }

  return stack_.top();
}

Result<std::shared_ptr<Expr>> LogicalPlanBuilder::Pop() {
  ARROW_ASSIGN_OR_RAISE(auto top, Peek());
  stack_.pop();
  return top;
}

Status LogicalPlanBuilder::Push(std::shared_ptr<Expr> node) {
  if (node == nullptr) {
    return Status::Invalid(__FUNCTION__, " can't push a nullptr node.");
  }

  stack_.push(std::move(node));
  return Status::OK();
}

}  // namespace engine
}  // namespace arrow
