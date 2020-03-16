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
#include <string>
#include <vector>

#include "arrow/engine/type_fwd.h"
#include "arrow/engine/visibility.h"
#include "arrow/type_fwd.h"
#include "arrow/util/compare.h"

namespace arrow {

namespace dataset {
class Dataset;
}

namespace engine {

class ARROW_EN_EXPORT LogicalPlan : public util::EqualityComparable<LogicalPlan> {
 public:
  explicit LogicalPlan(std::shared_ptr<Expr> root);

  const std::shared_ptr<Expr>& root() const { return root_; }
  const ExprType& type() const;

  bool Equals(const LogicalPlan& other) const;
  std::string ToString() const;

 private:
  std::shared_ptr<Expr> root_;
};

struct LogicalPlanBuilderOptions {
  /// Catalog containing named tables.
  std::shared_ptr<Catalog> catalog;
};

class ARROW_EN_EXPORT LogicalPlanBuilder {
 public:
  using ResultExpr = Result<std::shared_ptr<Expr>>;

  explicit LogicalPlanBuilder(LogicalPlanBuilderOptions options = {});

  /// \defgroup leaf-nodes Leaf nodes in the logical plan
  /// @{

  /// \brief Construct a Scalar literal.
  ResultExpr Scalar(const std::shared_ptr<Scalar>& scalar);

  /// \brief References a field by index.
  ResultExpr Field(const std::shared_ptr<Expr>& input, int field_index);
  /// \brief References a field by name.
  ResultExpr Field(const std::shared_ptr<Expr>& input, const std::string& field_name);

  /// \brief Scan a Table/Dataset from the Catalog.
  ResultExpr Scan(const std::string& table_name);

  /// @}

  /// \defgroup comparator-nodes Comparison operators
  /// @{

  /*
  TODO(fsaintjacques): This.
  ResultExpr Equal(const std::shared_ptr<Expr>& lhs, const std::shared_ptr<Expr>& rhs);
  ResultExpr NotEqual(const std::shared_ptr<Expr>& lhs, const std::shared_ptr<Expr>& rhs);
  ResultExpr GreaterThan(const std::shared_ptr<Expr>& lhs,
                         const std::shared_ptr<Expr>& rhs);
  ResultExpr GreaterEqualThan(const std::shared_ptr<Expr>& lhs,
                              const std::shared_ptr<Expr>& rhs);
  ResultExpr LessThan(const std::shared_ptr<Expr>& lhs, const std::shared_ptr<Expr>& rhs);
  ResultExpr LessEqualThan(const std::shared_ptr<Expr>& lhs,
                           const std::shared_ptr<Expr>& rhs);
  */

  /// @}

  /// \defgroup Aggregate function operators
  /// @{

  /// \brief Count the number of elements in the input.
  ResultExpr Count(const std::shared_ptr<Expr>& input);

  /// \brief Sum the elements of the input.
  ResultExpr Sum(const std::shared_ptr<Expr>& input);

  /// @}

  /// \defgroup rel-nodes Relational operator nodes in the logical plan

  /// \brief Filter rows of a relation with the given predicate.
  ResultExpr Filter(const std::shared_ptr<Expr>& input,
                    const std::shared_ptr<Expr>& predicate);

  /// \brief Project (mutate) columns with given expressions.
  ResultExpr Project(const std::shared_ptr<Expr>& input,
                     const std::vector<std::shared_ptr<Expr>>& expressions);

  /// \brief Project (select) columns by names.
  ///
  /// This is a simplified version of Project where columns are selected by
  /// names. Duplicates and ordering are preserved.
  ResultExpr Project(const std::shared_ptr<Expr>& input,
                     const std::vector<std::string>& column_names);

  /// \brief Project (select) columns by indices.
  ///
  /// This is a simplified version of Project where columns are selected by
  /// indices. Duplicates and ordering are preserved.
  ResultExpr Project(const std::shared_ptr<Expr>& input,
                     const std::vector<int>& column_indices);

  /// @}

 private:
  std::shared_ptr<Catalog> catalog_;
};

}  // namespace engine
}  // namespace arrow
