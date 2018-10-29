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

#include "gandiva/decimal_type_sql.h"
#include "tests/test_util.h"

namespace gandiva {

Decimal128TypePtr MakeDecimal(int32_t precision, int32_t scale) {
  return std::make_shared<arrow::Decimal128Type>(precision, scale);
}

Decimal128TypePtr DoOp(DecimalTypeSql::Op op, Decimal128TypePtr d1,
                       Decimal128TypePtr d2) {
  Decimal128TypePtr ret_type;
  auto status = DecimalTypeSql::GetResultType(op, {d1, d2}, &ret_type);
  EXPECT_TRUE(status.ok()) << status.message();
  return ret_type;
}

TEST(DecimalResultTypes, Basic) {
  EXPECT_ARROW_TYPE_EQUALS(
      MakeDecimal(31, 10),
      DoOp(DecimalTypeSql::kOpAdd, MakeDecimal(30, 10), MakeDecimal(30, 10)));

  EXPECT_ARROW_TYPE_EQUALS(
      MakeDecimal(32, 6),
      DoOp(DecimalTypeSql::kOpAdd, MakeDecimal(30, 6), MakeDecimal(30, 5)));

  EXPECT_ARROW_TYPE_EQUALS(
      MakeDecimal(38, 9),
      DoOp(DecimalTypeSql::kOpAdd, MakeDecimal(30, 10), MakeDecimal(38, 10)));

  EXPECT_ARROW_TYPE_EQUALS(
      MakeDecimal(38, 9),
      DoOp(DecimalTypeSql::kOpAdd, MakeDecimal(38, 10), MakeDecimal(38, 38)));

  EXPECT_ARROW_TYPE_EQUALS(
      MakeDecimal(38, 6),
      DoOp(DecimalTypeSql::kOpAdd, MakeDecimal(38, 10), MakeDecimal(38, 2)));
}

}  // namespace gandiva
