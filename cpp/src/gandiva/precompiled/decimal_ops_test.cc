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
#include "gandiva/execution_context.h"
#include "gandiva/precompiled/decimal_ops.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

const arrow::Decimal128 kThirtyFive9s(std::string(35, '9'));
const arrow::Decimal128 kThirtySix9s(std::string(36, '9'));
const arrow::Decimal128 kThirtyEight9s(std::string(38, '9'));

class TestDecimalSql : public ::testing::Test {
 protected:
  static void Verify(DecimalTypeUtil::Op op, const DecimalScalar128& x,
                     const DecimalScalar128& y, const DecimalScalar128& expected_result,
                     bool expected_overflow);

  static void VerifyAllSign(DecimalTypeUtil::Op op, const DecimalScalar128& left,
                            const DecimalScalar128& right,
                            const DecimalScalar128& expected_output,
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
                                bool expected_overflow) {
    return VerifyAllSign(DecimalTypeUtil::kOpMultiply, x, y, expected_result,
                         expected_overflow);
  }

  void DivideAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                       const DecimalScalar128& expected_result, bool expected_overflow) {
    return Verify(DecimalTypeUtil::kOpDivide, x, y, expected_result, expected_overflow);
  }

  void DivideAndVerifyAllSign(const DecimalScalar128& x, const DecimalScalar128& y,
                              const DecimalScalar128& expected_result,
                              bool expected_overflow) {
    return VerifyAllSign(DecimalTypeUtil::kOpDivide, x, y, expected_result,
                         expected_overflow);
  }

  void ModAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                    const DecimalScalar128& expected_result, bool expected_overflow) {
    return Verify(DecimalTypeUtil::kOpMod, x, y, expected_result, expected_overflow);
  }

  void ModAndVerifyAllSign(const DecimalScalar128& x, const DecimalScalar128& y,
                           const DecimalScalar128& expected_result,
                           bool expected_overflow) {
    return VerifyAllSign(DecimalTypeUtil::kOpMod, x, y, expected_result,
                         expected_overflow);
  }
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
  int64_t context = 0;

  Decimal128TypePtr out_type;
  ARROW_EXPECT_OK(DecimalTypeUtil::GetResultType(op, {t1, t2}, &out_type));

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

    case DecimalTypeUtil::kOpDivide:
      op_name = "divide";
      out_value = decimalops::Divide(context, x, y, out_type->precision(),
                                     out_type->scale(), &overflow);
      break;

    case DecimalTypeUtil::kOpMod:
      op_name = "mod";
      out_value = decimalops::Mod(context, x, y, out_type->precision(), out_type->scale(),
                                  &overflow);
      break;

    default:
      // not implemented.
      ASSERT_FALSE(true);
  }
  EXPECT_DECIMAL_EQ(op_name, x, y, expected_result, expected_overflow,
                    DecimalScalar128(out_value, out_type->precision(), out_type->scale()),
                    overflow);
}

void TestDecimalSql::VerifyAllSign(DecimalTypeUtil::Op op, const DecimalScalar128& left,
                                   const DecimalScalar128& right,
                                   const DecimalScalar128& expected_output,
                                   bool expected_overflow) {
  // both +ve
  Verify(op, left, right, expected_output, expected_overflow);

  // left -ve
  Verify(op, -left, right, -expected_output, expected_overflow);

  if (op == DecimalTypeUtil::kOpMod) {
    // right -ve
    Verify(op, left, -right, expected_output, expected_overflow);

    // both -ve
    Verify(op, -left, -right, -expected_output, expected_overflow);
  } else {
    DCHECK(op == DecimalTypeUtil::kOpMultiply || op == DecimalTypeUtil::kOpDivide);

    // right -ve
    Verify(op, left, -right, -expected_output, expected_overflow);

    // both -ve
    Verify(op, -left, -right, expected_output, expected_overflow);
  }
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

TEST_F(TestDecimalSql, Multiply) {
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
      DecimalScalar128{kThirtyFive9s, 35, 2},                             // y
      DecimalScalar128{"20099999999999999999999999999999999799", 38, 5},  // expected
      false);                                                             // overflow

  // out_precision == 38, very large values, no trimming of scale (scale <= 6 doesn't
  // get trimmed). overflow expected.
  MultiplyAndVerifyAllSign(DecimalScalar128{"201", 20, 3},         // x
                           DecimalScalar128{kThirtySix9s, 35, 2},  // y
                           DecimalScalar128{"0", 38, 5},           // expected
                           true);                                  // overflow

  MultiplyAndVerifyAllSign(DecimalScalar128{"201", 20, 3},           // x
                           DecimalScalar128{kThirtyEight9s, 35, 2},  // y
                           DecimalScalar128{"0", 38, 5},             // expected
                           true);                                    // overflow

  // out_precision == 38, small input values, trimming of scale.
  MultiplyAndVerifyAllSign(DecimalScalar128{"201", 20, 5},  // x
                           DecimalScalar128{"301", 20, 5},  // y
                           DecimalScalar128{"61", 38, 7},   // expected
                           false);                          // overflow

  // out_precision == 38, large values, trimming of scale.
  MultiplyAndVerifyAllSign(
      DecimalScalar128{"201", 20, 5},                                 // x
      DecimalScalar128{kThirtyFive9s, 35, 5},                         // y
      DecimalScalar128{"2010000000000000000000000000000000", 38, 6},  // expected
      false);                                                         // overflow

  // out_precision == 38, very large values, trimming of scale (requires convert to 256).
  MultiplyAndVerifyAllSign(
      DecimalScalar128{kThirtyFive9s, 38, 20},                           // x
      DecimalScalar128{kThirtySix9s, 38, 20},                            // y
      DecimalScalar128{"9999999999999999999999999999999999890", 38, 6},  // expected
      false);                                                            // overflow

  // out_precision == 38, very large values, trimming of scale (requires convert to 256).
  // should cause overflow.
  MultiplyAndVerifyAllSign(DecimalScalar128{kThirtyFive9s, 38, 4},  // x
                           DecimalScalar128{kThirtySix9s, 38, 4},   // y
                           DecimalScalar128{"0", 38, 6},            // expected
                           true);                                   // overflow

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
      DecimalScalar128{kThirtyFive9s, 38, 38},                        // x
      DecimalScalar128{kThirtySix9s, 38, 38},                         // y
      DecimalScalar128{"100000000000000000000000000000000", 38, 37},  // expected
      false);                                                         // overflow
}

TEST_F(TestDecimalSql, Divide) {
  DivideAndVerifyAllSign(DecimalScalar128{"201", 10, 3},             // x
                         DecimalScalar128{"301", 10, 2},             // y
                         DecimalScalar128{"6677740863787", 23, 14},  // expected
                         false);                                     // overflow

  DivideAndVerifyAllSign(DecimalScalar128{"201", 20, 3},                  // x
                         DecimalScalar128{"301", 20, 2},                  // y
                         DecimalScalar128{"667774086378737542", 38, 19},  // expected
                         false);                                          // overflow

  DivideAndVerifyAllSign(DecimalScalar128{"201", 20, 3},          // x
                         DecimalScalar128{kThirtyFive9s, 35, 2},  // y
                         DecimalScalar128{"0", 38, 19},           // expected
                         false);                                  // overflow

  DivideAndVerifyAllSign(
      DecimalScalar128{kThirtyFive9s, 35, 6},                           // x
      DecimalScalar128{"201", 20, 3},                                   // y
      DecimalScalar128{"497512437810945273631840796019900493", 38, 6},  // expected
      false);                                                           // overflow

  DivideAndVerifyAllSign(DecimalScalar128{kThirtyEight9s, 38, 20},  // x
                         DecimalScalar128{kThirtyFive9s, 38, 20},   // y
                         DecimalScalar128{"1000000000", 38, 6},     // expected
                         false);                                    // overflow

  DivideAndVerifyAllSign(DecimalScalar128{"31939128063561476055", 38, 8},  // x
                         DecimalScalar128{"10000", 20, 0},                 // y
                         DecimalScalar128{"3193912806356148", 38, 8},      // expected
                         false);

  // Corner cases
  DivideAndVerifyAllSign(DecimalScalar128{0, UINT64_MAX, 38, 4},  // x
                         DecimalScalar128{0, UINT64_MAX, 38, 4},  // y
                         DecimalScalar128{"1000000", 38, 6},      // expected
                         false);                                  // overflow

  DivideAndVerifyAllSign(DecimalScalar128{0, UINT64_MAX, 38, 4},  // x
                         DecimalScalar128{0, INT64_MAX, 38, 4},   // y
                         DecimalScalar128{"2000000", 38, 6},      // expected
                         false);                                  // overflow

  DivideAndVerifyAllSign(DecimalScalar128{0, UINT64_MAX, 19, 5},            // x
                         DecimalScalar128{0, INT64_MAX, 19, 5},             // y
                         DecimalScalar128{"20000000000000000001", 38, 19},  // expected
                         false);                                            // overflow

  DivideAndVerifyAllSign(DecimalScalar128{kThirtyFive9s, 38, 37},  // x
                         DecimalScalar128{kThirtyFive9s, 38, 38},  // y
                         DecimalScalar128{"10000000", 38, 6},      // expected
                         false);                                   // overflow

  // overflow
  DivideAndVerifyAllSign(DecimalScalar128{kThirtyEight9s, 38, 6},  // x
                         DecimalScalar128{"201", 20, 3},           // y
                         DecimalScalar128{"0", 38, 6},             // expected
                         true);
}

TEST_F(TestDecimalSql, Mod) {
  ModAndVerifyAllSign(DecimalScalar128{"201", 10, 3},  // x
                      DecimalScalar128{"301", 10, 2},  // y
                      DecimalScalar128{"201", 10, 3},  // expected
                      false);                          // overflow

  ModAndVerify(DecimalScalar128{"201", 20, 2},  // x
               DecimalScalar128{"301", 20, 3},  // y
               DecimalScalar128{"204", 20, 3},  // expected
               false);                          // overflow

  ModAndVerifyAllSign(DecimalScalar128{"201", 20, 3},          // x
                      DecimalScalar128{kThirtyFive9s, 35, 2},  // y
                      DecimalScalar128{"201", 20, 3},          // expected
                      false);                                  // overflow

  ModAndVerifyAllSign(DecimalScalar128{kThirtyFive9s, 35, 6},  // x
                      DecimalScalar128{"201", 20, 3},          // y
                      DecimalScalar128{"180999", 23, 6},       // expected
                      false);                                  // overflow

  ModAndVerifyAllSign(DecimalScalar128{kThirtyEight9s, 38, 20},  // x
                      DecimalScalar128{kThirtyFive9s, 38, 21},   // y
                      DecimalScalar128{"9990", 38, 21},          // expected
                      false);                                    // overflow

  ModAndVerifyAllSign(DecimalScalar128{"31939128063561476055", 38, 8},  // x
                      DecimalScalar128{"10000", 20, 0},                 // y
                      DecimalScalar128{"63561476055", 28, 8},           // expected
                      false);

  ModAndVerifyAllSign(DecimalScalar128{0, UINT64_MAX, 38, 4},  // x
                      DecimalScalar128{0, UINT64_MAX, 38, 4},  // y
                      DecimalScalar128{"0", 38, 4},            // expected
                      false);                                  // overflow

  ModAndVerifyAllSign(DecimalScalar128{0, UINT64_MAX, 38, 4},  // x
                      DecimalScalar128{0, INT64_MAX, 38, 4},   // y
                      DecimalScalar128{"1", 38, 4},            // expected
                      false);                                  // overflow
}

TEST_F(TestDecimalSql, DivideByZero) {
  gandiva::ExecutionContext context;
  int32_t result_precision;
  int32_t result_scale;
  bool overflow;

  // divide-by-zero should cause an error.
  context.Reset();
  result_precision = 38;
  result_scale = 19;
  decimalops::Divide(reinterpret_cast<int64>(&context), DecimalScalar128{"201", 20, 3},
                     DecimalScalar128{"0", 20, 2}, result_precision, result_scale,
                     &overflow);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "divide by zero error");

  // divide-by-nonzero should not cause an error.
  context.Reset();
  decimalops::Divide(reinterpret_cast<int64>(&context), DecimalScalar128{"201", 20, 3},
                     DecimalScalar128{"1", 20, 2}, result_precision, result_scale,
                     &overflow);
  EXPECT_FALSE(context.has_error());

  // mod-by-zero should cause an error.
  context.Reset();
  result_precision = 20;
  result_scale = 3;
  decimalops::Mod(reinterpret_cast<int64>(&context), DecimalScalar128{"201", 20, 3},
                  DecimalScalar128{"0", 20, 2}, result_precision, result_scale,
                  &overflow);
  EXPECT_TRUE(context.has_error());
  EXPECT_EQ(context.get_error(), "divide by zero error");

  // mod-by-nonzero should not cause an error.
  context.Reset();
  decimalops::Mod(reinterpret_cast<int64>(&context), DecimalScalar128{"201", 20, 3},
                  DecimalScalar128{"1", 20, 2}, result_precision, result_scale,
                  &overflow);
  EXPECT_FALSE(context.has_error());
}

}  // namespace gandiva
