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

#include "arrow/compute/operations/literal.h"

#include <memory>

#include "arrow/compute/expression.h"
#include "arrow/compute/logical_type.h"
#include "arrow/scalar.h"

namespace arrow {
namespace compute {
namespace ops {

Literal::Literal(const std::shared_ptr<Scalar>& value) : value_(value) {}

Status Literal::ToExpr(std::shared_ptr<Expr>* out) const {
  std::shared_ptr<LogicalType> ty;
  RETURN_NOT_OK(LogicalType::FromArrow(*value_->type, &ty));
  return GetScalarExpr(shared_from_this(), ty, out);
}

}  // namespace ops
}  // namespace compute
}  // namespace arrow
