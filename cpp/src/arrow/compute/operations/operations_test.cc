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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

#include "arrow/compute/expression.h"
#include "arrow/compute/logical_type.h"
#include "arrow/compute/operation.h"
#include "arrow/compute/operations/cast.h"
#include "arrow/compute/operations/literal.h"

namespace arrow {
namespace compute {

class DummyOp : public Operation {
 public:
  Status ToExpr(std::shared_ptr<Expr>* out) const override {
    return Status::NotImplemented("NYI");
  }
};

TEST(Literal, Basics) {
  auto val = std::make_shared<DoubleScalar>(3.14159);
  std::shared_ptr<Expr> expr;
  ASSERT_OK(std::make_shared<ops::Literal>(val)->ToExpr(&expr));
  ASSERT_TRUE(InheritsFrom<scalar::Float64>(*expr));
}

TEST(Cast, Basics) {
  auto dummy_op = std::make_shared<DummyOp>();
  auto expr = array::int32(dummy_op);
  std::shared_ptr<Expr> out_expr;
  ASSERT_OK(std::make_shared<ops::Cast>(expr, type::float64())->ToExpr(&out_expr));
  ASSERT_TRUE(InheritsFrom<array::Float64>(*out_expr));
}

}  // namespace compute
}  // namespace arrow
