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

#include "arrow/util/visibility.h"

#include "arrow/compute/expr/semantic_type.h"

namespace arrow {

class Status;

namespace compute {

class ExprName;
class ExprVisitor;
class Operation;
class SemType;

/// \brief Base class for all analytic expressions. Expressions may represent
/// data values (scalars, arrays, tables)
class ARROW_EXPORT Expr {
 public:
  virtual ~Expr() = default;

  /// \brief A unique string identifier for the kind of expression
  virtual const std::string& kind() const = 0;

  /// \brief Accept expression visitor
  virtual Status Accept(ExprVisitor* visitor) const = 0;

  /// \brief
  std::shared_ptr<Operation> op() const { return op_; }

  /// \brief The name of the expression, if any. The default is unnamed
  virtual const ExprName& name() const;

 protected:
  /// \brief Instantiate expression from an abstract operation
  /// \param[in] op the operation that generates the expression
  explicit Expr(const std::shared_ptr<Operation>& op);
  std::shared_ptr<Operation> op_;
};

/// \brief Base class for a data-generated expression with a fixed and known
/// type. This includes arrays and scalars
class ARROW_EXPORT ValueExpr : public Expr {
 protected:
  ValueExpr(const std::shared_ptr<Operation>& op,
            const std::shared_ptr<SemType>& type);

  /// \brief The semantic data type of the expression
  std::shared_ptr<SemType> type_;
};

class ARROW_EXPORT ScalarExpr : public ValueExpr {
 public:

};

}  // namespace compute
}  // namespace arrow
