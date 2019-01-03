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

#include <gtest/gtest.h>
#include <algorithm>
#include <memory>

#include "arrow/test-util.h"
#include "gandiva/decimal_type_util.h"
#include "gandiva/precompiled/decimal_ops.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

class TestDecimalSql : public ::testing::Test {
 protected:
  static void AddAndVerify(const Decimal128Full& x, const Decimal128Full& y,
                           const Decimal128Full& expected);
};

#define EXPECT_DECIMAL_EQ(x, y, expected, actual)                                    \
  EXPECT_EQ(expected, actual) << (x).ToString() << " + " << (y).ToString()           \
                              << " expected : " << expected.ToString() << " actual " \
                              << actual.ToString()

void TestDecimalSql::AddAndVerify(const Decimal128Full& x, const Decimal128Full& y,
                                  const Decimal128Full& expected) {
  auto t1 = std::make_shared<arrow::Decimal128Type>(x.precision(), x.scale());
  auto t2 = std::make_shared<arrow::Decimal128Type>(y.precision(), y.scale());

  Decimal128TypePtr out_type;
  EXPECT_OK(DecimalTypeUtil::GetResultType(DecimalTypeUtil::kOpAdd, {t1, t2}, &out_type));

  auto out_value = decimalops::Add(x, y, out_type->precision(), out_type->scale());
  EXPECT_DECIMAL_EQ(x, y, expected,
                    Decimal128Full(out_value, out_type->precision(), out_type->scale()));
}

TEST_F(TestDecimalSql, Add) {
  // fast-path
  AddAndVerify(Decimal128Full{"201", 30, 3},   // x
               Decimal128Full{"301", 30, 3},   // y
               Decimal128Full{"502", 31, 3});  // expected

  // max precision
  AddAndVerify(Decimal128Full{"09999999999999999999999999999999000000", 38, 5},  // x
               Decimal128Full{"100", 38, 7},                                     // y
               Decimal128Full{"99999999999999999999999999999990000010", 38, 6});

  // Both -ve
  AddAndVerify(Decimal128Full{"-201", 30, 3},    // x
               Decimal128Full{"-301", 30, 2},    // y
               Decimal128Full{"-3211", 32, 3});  // expected

  // -ve and max precision
  AddAndVerify(Decimal128Full{"-09999999999999999999999999999999000000", 38, 5},  // x
               Decimal128Full{"-100", 38, 7},                                     // y
               Decimal128Full{"-99999999999999999999999999999990000010", 38, 6});
}

}  // namespace gandiva
