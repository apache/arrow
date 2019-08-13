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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"

#include "arrow/dataset/api.h"

namespace arrow {
namespace dataset {

TEST(Expressions, Basics) {
  using namespace string_literals;

  auto simplified = ("b"_ == 3).Assume("b"_ > 5 and "b"_ < 10);
  ASSERT_OK(simplified.status());
  ASSERT_EQ(simplified.ValueOrDie()->type(), ExpressionType::SCALAR);
  auto value = internal::checked_cast<const ScalarExpression&>(**simplified).value();
  ASSERT_EQ(value->type->id(), Type::BOOL);
  ASSERT_FALSE(internal::checked_cast<const BooleanScalar&>(*value).value);
}

}  // namespace dataset
}  // namespace arrow
