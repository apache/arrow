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

#include <sstream>

#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "arrow/status.h"

#include "gandiva/decimal_scalar.h"
#include "gandiva/decimal_type_util.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

using arrow::Decimal128;

namespace gandiva {

#define EXPECT_DECIMAL_RESULT(op, x, y, expected, actual)                                \
  EXPECT_EQ(expected, actual) << op << " (" << (x).ToString() << "),(" << (y).ToString() \
                              << ")"                                                     \
                              << " expected : " << (expected).ToString()                 \
                              << " actual : " << (actual).ToString();

DecimalScalar128 decimal_literal(const char* value, int precision, int scale) {
  std::string value_string = std::string(value);
  return DecimalScalar128(value_string, precision, scale);
}

class TestDecimalOps : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

  ArrayPtr MakeDecimalVector(const DecimalScalar128& in);

  void Verify(DecimalTypeUtil::Op, const std::string& function, const DecimalScalar128& x,
              const DecimalScalar128& y, const DecimalScalar128& expected,
              bool use_compute_rules = false, bool verify_failed = false);

  void AddAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                    const DecimalScalar128& expected) {
    Verify(DecimalTypeUtil::kOpAdd, "add", x, y, expected);
  }

  void SubtractAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                         const DecimalScalar128& expected) {
    Verify(DecimalTypeUtil::kOpSubtract, "subtract", x, y, expected);
  }

  void MultiplyAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                         const DecimalScalar128& expected) {
    Verify(DecimalTypeUtil::kOpMultiply, "multiply", x, y, expected);
  }

  void DivideAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                       const DecimalScalar128& expected, bool use_compute_rules = false,
                       bool verify_failed = false) {
    Verify(DecimalTypeUtil::kOpDivide, "divide", x, y, expected, use_compute_rules,
           verify_failed);
  }

  void ModAndVerify(const DecimalScalar128& x, const DecimalScalar128& y,
                    const DecimalScalar128& expected) {
    Verify(DecimalTypeUtil::kOpMod, "mod", x, y, expected);
  }

 protected:
  arrow::MemoryPool* pool_;
};

ArrayPtr TestDecimalOps::MakeDecimalVector(const DecimalScalar128& in) {
  std::vector<arrow::Decimal128> ret;

  Decimal128 decimal_value = in.value();

  auto decimal_type = std::make_shared<arrow::Decimal128Type>(in.precision(), in.scale());
  return MakeArrowArrayDecimal(decimal_type, {decimal_value}, {true});
}

void TestDecimalOps::Verify(DecimalTypeUtil::Op op, const std::string& function,
                            const DecimalScalar128& x, const DecimalScalar128& y,
                            const DecimalScalar128& expected, bool use_compute_rules,
                            bool verify_failed) {
  auto x_type = std::make_shared<arrow::Decimal128Type>(x.precision(), x.scale());
  auto y_type = std::make_shared<arrow::Decimal128Type>(y.precision(), y.scale());
  auto field_x = field("x", x_type);
  auto field_y = field("y", y_type);
  auto schema = arrow::schema({field_x, field_y});

  Decimal128TypePtr output_type;
  auto status = DecimalTypeUtil::GetResultType(op, {x_type, y_type}, &output_type,
                                               use_compute_rules);
  if (verify_failed) {
    ASSERT_NOT_OK(status);
    return;
  } else {
    ARROW_EXPECT_OK(status);
  }

  // output fields
  auto res = field("res", output_type);

  // build expression : x op y
  auto expr = TreeExprBuilder::MakeExpression(function, {field_x, field_y}, res);

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {expr}, TestConfiguration(), &projector);
  ARROW_EXPECT_OK(status);

  // Create a row-batch with some sample data
  auto array_a = MakeDecimalVector(x);
  auto array_b = MakeDecimalVector(y);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, 1 /*num_records*/, {array_a, array_b});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  ARROW_EXPECT_OK(status);

  // Validate results
  auto out_array = dynamic_cast<arrow::Decimal128Array*>(outputs[0].get());
  const Decimal128 out_value(out_array->GetValue(0));

  auto dtype = dynamic_cast<arrow::Decimal128Type*>(out_array->type().get());
  std::string value_string = out_value.ToString(0);
  DecimalScalar128 actual{value_string, dtype->precision(), dtype->scale()};

  EXPECT_DECIMAL_RESULT(function, x, y, expected, actual);
}

TEST_F(TestDecimalOps, TestAdd) {
  // fast-path
  AddAndVerify(decimal_literal("201", 30, 3),   // x
               decimal_literal("301", 30, 3),   // y
               decimal_literal("502", 31, 3));  // expected

  AddAndVerify(decimal_literal("201", 30, 3),    // x
               decimal_literal("301", 30, 2),    // y
               decimal_literal("3211", 32, 3));  // expected

  AddAndVerify(decimal_literal("201", 30, 3),    // x
               decimal_literal("301", 30, 4),    // y
               decimal_literal("2311", 32, 4));  // expected

  // max precision, but no overflow
  AddAndVerify(decimal_literal("201", 38, 3),   // x
               decimal_literal("301", 38, 3),   // y
               decimal_literal("502", 38, 3));  // expected

  AddAndVerify(decimal_literal("201", 38, 3),    // x
               decimal_literal("301", 38, 2),    // y
               decimal_literal("3211", 38, 3));  // expected

  AddAndVerify(decimal_literal("201", 38, 3),    // x
               decimal_literal("301", 38, 4),    // y
               decimal_literal("2311", 38, 4));  // expected

  AddAndVerify(decimal_literal("201", 38, 3),      // x
               decimal_literal("301", 38, 7),      // y
               decimal_literal("201030", 38, 6));  // expected

  AddAndVerify(decimal_literal("1201", 38, 3),   // x
               decimal_literal("1801", 38, 3),   // y
               decimal_literal("3002", 38, 3));  // carry-over from fractional

  // max precision
  AddAndVerify(decimal_literal("09999999999999999999999999999999000000", 38, 5),  // x
               decimal_literal("100", 38, 7),                                     // y
               decimal_literal("99999999999999999999999999999990000010", 38, 6));

  AddAndVerify(decimal_literal("-09999999999999999999999999999999000000", 38, 5),  // x
               decimal_literal("100", 38, 7),                                      // y
               decimal_literal("-99999999999999999999999999999989999990", 38, 6));

  AddAndVerify(decimal_literal("09999999999999999999999999999999000000", 38, 5),  // x
               decimal_literal("-100", 38, 7),                                    // y
               decimal_literal("99999999999999999999999999999989999990", 38, 6));

  AddAndVerify(decimal_literal("-09999999999999999999999999999999000000", 38, 5),  // x
               decimal_literal("-100", 38, 7),                                     // y
               decimal_literal("-99999999999999999999999999999990000010", 38, 6));

  AddAndVerify(decimal_literal("09999999999999999999999999999999999999", 38, 6),  // x
               decimal_literal("89999999999999999999999999999999999999", 38, 7),  // y
               decimal_literal("18999999999999999999999999999999999999", 38, 6));

  // Both -ve
  AddAndVerify(decimal_literal("-201", 30, 3),    // x
               decimal_literal("-301", 30, 2),    // y
               decimal_literal("-3211", 32, 3));  // expected

  AddAndVerify(decimal_literal("-201", 38, 3),    // x
               decimal_literal("-301", 38, 4),    // y
               decimal_literal("-2311", 38, 4));  // expected

  // Mix of +ve and -ve
  AddAndVerify(decimal_literal("-201", 30, 3),   // x
               decimal_literal("301", 30, 2),    // y
               decimal_literal("2809", 32, 3));  // expected

  AddAndVerify(decimal_literal("-201", 38, 3),    // x
               decimal_literal("301", 38, 4),     // y
               decimal_literal("-1709", 38, 4));  // expected

  AddAndVerify(decimal_literal("201", 38, 3),      // x
               decimal_literal("-301", 38, 7),     // y
               decimal_literal("200970", 38, 6));  // expected

  AddAndVerify(decimal_literal("-1901", 38, 4),  // x
               decimal_literal("1801", 38, 4),   // y
               decimal_literal("-100", 38, 4));  // expected

  AddAndVerify(decimal_literal("1801", 38, 4),   // x
               decimal_literal("-1901", 38, 4),  // y
               decimal_literal("-100", 38, 4));  // expected

  // rounding +ve
  AddAndVerify(decimal_literal("1000999", 38, 6),   // x
               decimal_literal("10000999", 38, 7),  // y
               decimal_literal("2001099", 38, 6));

  AddAndVerify(decimal_literal("1000999", 38, 6),   // x
               decimal_literal("10000995", 38, 7),  // y
               decimal_literal("2001099", 38, 6));

  AddAndVerify(decimal_literal("1000999", 38, 6),   // x
               decimal_literal("10000992", 38, 7),  // y
               decimal_literal("2001098", 38, 6));

  // rounding -ve
  AddAndVerify(decimal_literal("-1000999", 38, 6),   // x
               decimal_literal("-10000999", 38, 7),  // y
               decimal_literal("-2001099", 38, 6));

  AddAndVerify(decimal_literal("-1000999", 38, 6),   // x
               decimal_literal("-10000995", 38, 7),  // y
               decimal_literal("-2001099", 38, 6));

  AddAndVerify(decimal_literal("-1000999", 38, 6),   // x
               decimal_literal("-10000992", 38, 7),  // y
               decimal_literal("-2001098", 38, 6));
}

// subtract is a wrapper over add. so, minimal tests are sufficient.
TEST_F(TestDecimalOps, TestSubtract) {
  // fast-path
  SubtractAndVerify(decimal_literal("201", 30, 3),    // x
                    decimal_literal("301", 30, 3),    // y
                    decimal_literal("-100", 31, 3));  // expected

  // max precision
  SubtractAndVerify(
      decimal_literal("09999999999999999999999999999999000000", 38, 5),  // x
      decimal_literal("100", 38, 7),                                     // y
      decimal_literal("99999999999999999999999999999989999990", 38, 6));

  // Mix of +ve and -ve
  SubtractAndVerify(decimal_literal("-201", 30, 3),    // x
                    decimal_literal("301", 30, 2),     // y
                    decimal_literal("-3211", 32, 3));  // expected
}

// Lots of unit tests for multiply/divide/mod in decimal_ops_test.cc. So, keeping these
// basic.
TEST_F(TestDecimalOps, TestMultiply) {
  // fast-path
  MultiplyAndVerify(decimal_literal("201", 10, 3),     // x
                    decimal_literal("301", 10, 2),     // y
                    decimal_literal("60501", 21, 5));  // expected

  // max precision
  MultiplyAndVerify(DecimalScalar128(std::string(35, '9'), 38, 20),  // x
                    DecimalScalar128(std::string(36, '9'), 38, 20),  // x
                    DecimalScalar128("9999999999999999999999999999999999890", 38, 6));
}

TEST_F(TestDecimalOps, TestDivide) {
  // fast-path
  //
  // origin Gandiva's rules
  DivideAndVerify(decimal_literal("201", 10, 3),              // x
                  decimal_literal("301", 10, 2),              // y
                  decimal_literal("6677740863787", 23, 14));  // expected

  // compute module's rules
  DivideAndVerify(decimal_literal("201", 10, 3),           // x
                  decimal_literal("301", 10, 2),           // y
                  decimal_literal("66777408638", 21, 12),  // expected
                  /*use_compute_rules=*/true);

  // max precision beyond 38
  //
  // normally under origin Gandiva rules
  DivideAndVerify(DecimalScalar128(std::string(38, '9'), 38, 20),  // x
                  DecimalScalar128(std::string(35, '9'), 38, 20),  // x
                  DecimalScalar128("1000000000", 38, 6));

  // invalid under compute module's rules
  DivideAndVerify(DecimalScalar128(std::string(38, '9'), 38, 20),  // x
                  DecimalScalar128(std::string(35, '9'), 38, 20),  // x
                  DecimalScalar128(std::string(35, '9'), 0, 0),    // useless expected
                  /*use_compute_rules=*/true, /*verify_failed=*/true);
}

TEST_F(TestDecimalOps, TestMod) {
  ModAndVerify(decimal_literal("201", 20, 2),   // x
               decimal_literal("301", 20, 3),   // y
               decimal_literal("204", 20, 3));  // expected

  ModAndVerify(DecimalScalar128(std::string(38, '9'), 38, 20),  // x
               DecimalScalar128(std::string(35, '9'), 38, 21),  // x
               DecimalScalar128("9990", 38, 21));
}

}  // namespace gandiva
