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

#include "arrow/compute/operations/cast.h"

#include <memory>
#include <utility>

#include "arrow/compute/expression.h"
#include "arrow/compute/logical_type.h"
#include "arrow/status.h"

namespace arrow {
namespace compute {
namespace ops {

Cast::Cast(std::shared_ptr<Expr> value, std::shared_ptr<LogicalType> out_type)
    : value_(std::move(value)), out_type_(std::move(out_type)) {}

Status Cast::ToExpr(std::shared_ptr<Expr>* out) const {
  // TODO(wesm): Add reusable type-checking rules
  auto value_ty = type::any();
  if (!value_ty->IsInstance(*value_)) {
    return Status::Invalid("Cast only applies to value expressions");
  }

  // TODO(wesm): implement "shaped like" output type rule like Ibis
  auto op = shared_from_this();
  const auto& value_expr = static_cast<const ValueExpr&>(*value_);
  if (value_expr.rank() == ValueRank::SCALAR) {
    return GetScalarExpr(op, out_type_, out);
  } else {
    return GetArrayExpr(op, out_type_, out);
  }
}

}  // namespace ops
}  // namespace compute
}  // namespace arrow
