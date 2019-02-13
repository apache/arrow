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

#include "arrow/testing/gtest_util.h"
#include "gandiva/decimal_scalar.h"
#include "gandiva/decimal_type_util.h"
#include "gandiva/precompiled/decimal_ops.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

class TestDecimalSql : public ::testing::Test {
 protected:
  static void Verify(DecimalTypeUtil::Op op, const DecimalScalar128& x,
                     const DecimalScalar128& y, const DecimalScalar128& expected);

  void AddAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                    const DecimalScalar128& expected) {
    return Verify(DecimalTypeUtil::kOpAdd, x, y, expected);
  }

  void SubtractAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                         const DecimalScalar128& expected) {
    return Verify(DecimalTypeUtil::kOpSubtract, x, y, expected);
  }
};

#define EXPECT_DECIMAL_EQ(x, y, expected, actual)                                    \
  EXPECT_EQ(expected, actual) << (x).ToString() << " + " << (y).ToString()           \
                              << " expected : " << expected.ToString() << " actual " \
                              << actual.ToString()

void TestDecimalSql::Verify(DecimalTypeUtil::Op op, const DecimalScalar128& x,
                            const DecimalScalar128& y, const DecimalScalar128& expected) {
  auto t1 = std::make_shared<arrow::Decimal128Type>(x.precision(), x.scale());
  auto t2 = std::make_shared<arrow::Decimal128Type>(y.precision(), y.scale());

  Decimal128TypePtr out_type;
  EXPECT_OK(DecimalTypeUtil::GetResultType(op, {t1, t2}, &out_type));

  arrow::BasicDecimal128 out_value;
  switch (op) {
    case DecimalTypeUtil::kOpAdd:
      out_value = decimalops::Add(x, y, out_type->precision(), out_type->scale());
      break;

    case DecimalTypeUtil::kOpSubtract:
      out_value = decimalops::Subtract(x, y, out_type->precision(), out_type->scale());
      break;

    default:
      // not implemented.
      ASSERT_FALSE(true);
  }
  EXPECT_DECIMAL_EQ(
      x, y, expected,
      DecimalScalar128(out_value, out_type->precision(), out_type->scale()));
}

TEST_F(TestDecimalSql, Add) {
  // fast-path
  AddAndVerify(DecimalScalar128{"201", 30, 3},   // x
               DecimalScalar128{"301", 30, 3},   // y
               DecimalScalar128{"502", 31, 3});  // expected

  // max precision
  AddAndVerify(DecimalScalar128{"09999999999999999999999999999999000000", 38, 5},  // x
               DecimalScalar128{"100", 38, 7},                                     // y
               DecimalScalar128{"99999999999999999999999999999990000010", 38, 6});

  // Both -ve
  AddAndVerify(DecimalScalar128{"-201", 30, 3},    // x
               DecimalScalar128{"-301", 30, 2},    // y
               DecimalScalar128{"-3211", 32, 3});  // expected

  // -ve and max precision
  AddAndVerify(DecimalScalar128{"-09999999999999999999999999999999000000", 38, 5},  // x
               DecimalScalar128{"-100", 38, 7},                                     // y
               DecimalScalar128{"-99999999999999999999999999999990000010", 38, 6});
}

TEST_F(TestDecimalSql, Subtract) {
  // fast-path
  SubtractAndVerify(DecimalScalar128{"201", 30, 3},    // x
                    DecimalScalar128{"301", 30, 3},    // y
                    DecimalScalar128{"-100", 31, 3});  // expected

  // max precision
  SubtractAndVerify(
      DecimalScalar128{"09999999999999999999999999999999000000", 38, 5},  // x
      DecimalScalar128{"100", 38, 7},                                     // y
      DecimalScalar128{"99999999999999999999999999999989999990", 38, 6});

  // Both -ve
  SubtractAndVerify(DecimalScalar128{"-201", 30, 3},   // x
                    DecimalScalar128{"-301", 30, 2},   // y
                    DecimalScalar128{"2809", 32, 3});  // expected

  // -ve and max precision
  SubtractAndVerify(
      DecimalScalar128{"-09999999999999999999999999999999000000", 38, 5},  // x
      DecimalScalar128{"-100", 38, 7},                                     // y
      DecimalScalar128{"-99999999999999999999999999999989999990", 38, 6});
}

}  // namespace gandiva
