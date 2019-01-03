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

// Adapted from Apache Impala

#include <gtest/gtest.h>

#include "gandiva/decimal_type_util.h"
#include "tests/test_util.h"

namespace gandiva {

#define DECIMAL_TYPE(p, s) DecimalTypeUtil::MakeType(p, s)

Decimal128TypePtr DoOp(DecimalTypeUtil::Op op, Decimal128TypePtr d1,
                       Decimal128TypePtr d2) {
  Decimal128TypePtr ret_type;
  EXPECT_OK(DecimalTypeUtil::GetResultType(op, {d1, d2}, &ret_type));
  return ret_type;
}

TEST(DecimalResultTypes, Basic) {
  EXPECT_ARROW_TYPE_EQUALS(
      DECIMAL_TYPE(31, 10),
      DoOp(DecimalTypeUtil::kOpAdd, DECIMAL_TYPE(30, 10), DECIMAL_TYPE(30, 10)));

  EXPECT_ARROW_TYPE_EQUALS(
      DECIMAL_TYPE(32, 6),
      DoOp(DecimalTypeUtil::kOpAdd, DECIMAL_TYPE(30, 6), DECIMAL_TYPE(30, 5)));

  EXPECT_ARROW_TYPE_EQUALS(
      DECIMAL_TYPE(38, 9),
      DoOp(DecimalTypeUtil::kOpAdd, DECIMAL_TYPE(30, 10), DECIMAL_TYPE(38, 10)));

  EXPECT_ARROW_TYPE_EQUALS(
      DECIMAL_TYPE(38, 9),
      DoOp(DecimalTypeUtil::kOpAdd, DECIMAL_TYPE(38, 10), DECIMAL_TYPE(38, 38)));

  EXPECT_ARROW_TYPE_EQUALS(
      DECIMAL_TYPE(38, 6),
      DoOp(DecimalTypeUtil::kOpAdd, DECIMAL_TYPE(38, 10), DECIMAL_TYPE(38, 2)));
}

}  // namespace gandiva
