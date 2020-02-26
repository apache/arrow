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

#pragma once

#include <memory>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/compare.h"
#include "arrow/util/variant.h"

namespace arrow {

namespace dataset {
class Dataset;
}

namespace engine {

class Catalog;
class Expr;

class LogicalPlan : public util::EqualityComparable<LogicalPlan> {
 public:
  explicit LogicalPlan(std::shared_ptr<Expr> root);

  std::shared_ptr<Expr> root() const { return root_; }

  bool Equals(const LogicalPlan& other) const;
  std::string ToString() const;

 private:
  std::shared_ptr<Expr> root_;
};

class LogicalPlanBuilder {
 public:
  explicit LogicalPlanBuilder(std::shared_ptr<Catalog> catalog);

  /// \defgroup leaf-nodes Leaf nodes in the logical plan
  /// @{

  // Anonymous values literal.
  Status Scalar(const std::shared_ptr<Scalar>& array);
  Status Array(const std::shared_ptr<Array>& array);
  Status Table(const std::shared_ptr<Table>& table);

  // Named values
  Status Scan(const std::string& table_name);

  /// @}

  Status Filter(std::shared_ptr<Expr> predicate);

  Result<std::shared_ptr<LogicalPlan>> Finish();

 private:
  Status Push(std::shared_ptr<Expr>);
  Result<std::shared_ptr<Expr>> Pop();
  Result<std::shared_ptr<Expr>> Peek();

  std::shared_ptr<Catalog> catalog_;
  std::stack<std::shared_ptr<Expr>> stack_;
};

}  // namespace engine
}  // namespace arrow
