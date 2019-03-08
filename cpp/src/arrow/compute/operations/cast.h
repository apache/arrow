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

#include "arrow/compute/operation.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

class LogicalType;

namespace ops {

/// \brief A cast operation creates an expression from a known constant
/// scalar value
class ARROW_EXPORT Cast : public Operation {
 public:
  Cast(std::shared_ptr<Expr> value, std::shared_ptr<LogicalType> out_type);
  Status ToExpr(std::shared_ptr<Expr>* out) const override;

 private:
  std::shared_ptr<Expr> value_;
  std::shared_ptr<LogicalType> out_type_;
};

}  // namespace ops
}  // namespace compute
}  // namespace arrow
