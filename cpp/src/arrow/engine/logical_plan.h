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

struct LogicalPlanBuilderOptions {
  std::shared_ptr<Catalog> catalog;
};

class LogicalPlanBuilder {
 public:
  using ResultExpr = Result<std::shared_ptr<Expr>>;

  explicit LogicalPlanBuilder(LogicalPlanBuilderOptions options = {});

  /// \defgroup leaf-nodes Leaf nodes in the logical plan
  /// @{

  /// \brief Construct a Scalar literal.
  ResultExpr Scalar(const std::shared_ptr<Scalar>& scalar);

  /// \brief References a field by name.
  ResultExpr Field(const std::shared_ptr<Expr>& input, const std::string& field_name);

  /// \brief Scan a Table/Dataset from the Catalog.
  ResultExpr Scan(const std::string& table_name);

  /// @}

  /// \defgroup rel-nodes Relational operator nodes in the logical plan

  ResultExpr Filter(const std::shared_ptr<Expr>& input,
                    const std::shared_ptr<Expr>& predicate);

  /*
  /// \brief Project (mutate) columns with given expressions.
  ResultExpr Project(const std::vector<std::shared_ptr<Expr>>& expressions);
  ResultExpr Mutate(const std::vector<std::shared_ptr<Expr>>& expressions);

  /// \brief Project (select) columns by names.
  ///
  /// This is a simplified version of Project where columns are selected by
  /// names. Duplicate and ordering are preserved.
  ResultExpr Project(const std::vector<std::string>& column_names);

  /// \brief Project (select) columns by indices.
  ///
  /// This is a simplified version of Project where columns are selected by
  /// indices. Duplicate and ordering are preserved.
  ResultExpr Project(const std::vector<int>& column_indices);
  */

  /// @}

 private:
  std::shared_ptr<Catalog> catalog_;
};

}  // namespace engine
}  // namespace arrow
