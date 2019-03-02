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
                     const DecimalScalar128& y, const DecimalScalar128& expected_result,
                     bool expected_overflow);

  void AddAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                    const DecimalScalar128& expected_result) {
    // TODO: overflow checks
    return Verify(DecimalTypeUtil::kOpAdd, x, y, expected_result, false);
  }

  void SubtractAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                         const DecimalScalar128& expected_result) {
    // TODO: overflow checks
    return Verify(DecimalTypeUtil::kOpSubtract, x, y, expected_result, false);
  }

  void MultiplyAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                         const DecimalScalar128& expected_result,
                         bool expected_overflow) {
    return Verify(DecimalTypeUtil::kOpMultiply, x, y, expected_result, expected_overflow);
  }

  void MultiplyAndVerifyAllSign(const DecimalScalar128& x, const DecimalScalar128& y,
                                const DecimalScalar128& expected_result,
                                bool expected_overflow);
};

#define EXPECT_DECIMAL_EQ(op, x, y, expected_result, expected_overflow, actual_result, \
                          actual_overflow)                                             \
  {                                                                                    \
    EXPECT_TRUE(expected_overflow == actual_overflow)                                  \
        << op << "(" << (x).ToString() << " and " << (y).ToString() << ")"             \
        << " expected overflow : " << expected_overflow                                \
        << " actual overflow : " << actual_overflow;                                   \
    if (!expected_overflow) {                                                          \
      EXPECT_TRUE(expected_result == actual_result)                                    \
          << op << "(" << (x).ToString() << " and " << (y).ToString() << ")"           \
          << " expected : " << expected_result.ToString()                              \
          << " actual : " << actual_result.ToString();                                 \
    }                                                                                  \
  }

void TestDecimalSql::Verify(DecimalTypeUtil::Op op, const DecimalScalar128& x,
                            const DecimalScalar128& y,
                            const DecimalScalar128& expected_result,
                            bool expected_overflow) {
  auto t1 = std::make_shared<arrow::Decimal128Type>(x.precision(), x.scale());
  auto t2 = std::make_shared<arrow::Decimal128Type>(y.precision(), y.scale());
  bool overflow = false;

  Decimal128TypePtr out_type;
  EXPECT_OK(DecimalTypeUtil::GetResultType(op, {t1, t2}, &out_type));

  arrow::BasicDecimal128 out_value;
  std::string op_name;
  switch (op) {
    case DecimalTypeUtil::kOpAdd:
      op_name = "add";
      out_value = decimalops::Add(x, y, out_type->precision(), out_type->scale());
      break;

    case DecimalTypeUtil::kOpSubtract:
      op_name = "subtract";
      out_value = decimalops::Subtract(x, y, out_type->precision(), out_type->scale());
      break;

    case DecimalTypeUtil::kOpMultiply:
      op_name = "multiply";
      out_value =
          decimalops::Multiply(x, y, out_type->precision(), out_type->scale(), &overflow);
      break;

    default:
      // not implemented.
      ASSERT_FALSE(true);
  }
  EXPECT_DECIMAL_EQ(op_name, x, y, expected_result, expected_overflow,
                    DecimalScalar128(out_value, out_type->precision(), out_type->scale()),
                    overflow);
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

void TestDecimalSql::MultiplyAndVerifyAllSign(const DecimalScalar128& left,
                                              const DecimalScalar128& right,
                                              const DecimalScalar128& expected_output,
                                              bool expected_overflow) {
  // both +ve
  MultiplyAndVerify(left, right, expected_output, expected_overflow);

  // left -ve
  MultiplyAndVerify(-left, right, -expected_output, expected_overflow);

  // right -ve
  MultiplyAndVerify(left, -right, -expected_output, expected_overflow);

  // both -ve
  MultiplyAndVerify(-left, -right, expected_output, expected_overflow);
}

TEST_F(TestDecimalSql, Multiply) {
  const std::string thirty_five_9s(35, '9');
  const std::string thirty_six_9s(36, '9');
  const std::string thirty_eight_9s(38, '9');

  // fast-path : out_precision < 38
  MultiplyAndVerifyAllSign(DecimalScalar128{"201", 10, 3},    // x
                           DecimalScalar128{"301", 10, 2},    // y
                           DecimalScalar128{"60501", 21, 5},  // expected
                           false);                            // overflow

  // right 0
  MultiplyAndVerify(DecimalScalar128{"201", 20, 3},  // x
                    DecimalScalar128{"0", 20, 2},    // y
                    DecimalScalar128{"0", 38, 5},    // expected
                    false);                          // overflow

  // left 0
  MultiplyAndVerify(DecimalScalar128{"0", 20, 3},    // x
                    DecimalScalar128{"301", 20, 2},  // y
                    DecimalScalar128{"0", 38, 5},    // expected
                    false);                          // overflow

  // out_precision == 38, small input values, no trimming of scale (scale <= 6 doesn't
  // get trimmed).
  MultiplyAndVerify(DecimalScalar128{"201", 20, 3},    // x
                    DecimalScalar128{"301", 20, 2},    // y
                    DecimalScalar128{"60501", 38, 5},  // expected
                    false);                            // overflow

  // out_precision == 38, large values, no trimming of scale (scale <= 6 doesn't
  // get trimmed).
  MultiplyAndVerifyAllSign(
      DecimalScalar128{"201", 20, 3},                                     // x
      DecimalScalar128{thirty_five_9s, 35, 2},                            // y
      DecimalScalar128{"20099999999999999999999999999999999799", 38, 5},  // expected
      false);                                                             // overflow

  // out_precision == 38, very large values, no trimming of scale (scale <= 6 doesn't
  // get trimmed). overflow expected.
  MultiplyAndVerifyAllSign(DecimalScalar128{"201", 20, 3},          // x
                           DecimalScalar128{thirty_six_9s, 35, 2},  // y
                           DecimalScalar128{"0", 38, 5},            // expected
                           true);                                   // overflow

  MultiplyAndVerifyAllSign(DecimalScalar128{"201", 20, 3},            // x
                           DecimalScalar128{thirty_eight_9s, 35, 2},  // y
                           DecimalScalar128{"0", 38, 5},              // expected
                           true);                                     // overflow

  // out_precision == 38, small input values, trimming of scale.
  MultiplyAndVerifyAllSign(DecimalScalar128{"201", 20, 5},  // x
                           DecimalScalar128{"301", 20, 5},  // y
                           DecimalScalar128{"61", 38, 7},   // expected
                           false);                          // overflow

  // out_precision == 38, large values, trimming of scale.
  MultiplyAndVerifyAllSign(
      DecimalScalar128{"201", 20, 5},                                 // x
      DecimalScalar128{thirty_five_9s, 35, 5},                        // y
      DecimalScalar128{"2010000000000000000000000000000000", 38, 6},  // expected
      false);                                                         // overflow

  // out_precision == 38, very large values, trimming of scale (requires convert to 256).
  MultiplyAndVerifyAllSign(
      DecimalScalar128{thirty_five_9s, 38, 20},                          // x
      DecimalScalar128{thirty_six_9s, 38, 20},                           // y
      DecimalScalar128{"9999999999999999999999999999999999890", 38, 6},  // expected
      false);                                                            // overflow

  // out_precision == 38, very large values, trimming of scale (requires convert to 256).
  // should cause overflow.
  MultiplyAndVerifyAllSign(DecimalScalar128{thirty_five_9s, 38, 4},  // x
                           DecimalScalar128{thirty_six_9s, 38, 4},   // y
                           DecimalScalar128{"0", 38, 6},             // expected
                           true);                                    // overflow

  // corner cases.
  MultiplyAndVerifyAllSign(
      DecimalScalar128{0, UINT64_MAX, 38, 4},                            // x
      DecimalScalar128{0, UINT64_MAX, 38, 4},                            // y
      DecimalScalar128{"3402823669209384634264811192843491082", 38, 6},  // expected
      false);                                                            // overflow

  MultiplyAndVerifyAllSign(
      DecimalScalar128{0, UINT64_MAX, 38, 4},                            // x
      DecimalScalar128{0, INT64_MAX, 38, 4},                             // y
      DecimalScalar128{"1701411834604692317040171876053197783", 38, 6},  // expected
      false);                                                            // overflow

  MultiplyAndVerifyAllSign(DecimalScalar128{"201", 38, 38},  // x
                           DecimalScalar128{"301", 38, 38},  // y
                           DecimalScalar128{"0", 38, 37},    // expected
                           false);                           // overflow

  MultiplyAndVerifyAllSign(DecimalScalar128{0, UINT64_MAX, 38, 38},  // x
                           DecimalScalar128{0, UINT64_MAX, 38, 38},  // y
                           DecimalScalar128{"0", 38, 37},            // expected
                           false);                                   // overflow

  MultiplyAndVerifyAllSign(
      DecimalScalar128{thirty_five_9s, 38, 38},                       // x
      DecimalScalar128{thirty_six_9s, 38, 38},                        // y
      DecimalScalar128{"100000000000000000000000000000000", 38, 37},  // expected
      false);                                                         // overflow
}

}  // namespace gandiva
