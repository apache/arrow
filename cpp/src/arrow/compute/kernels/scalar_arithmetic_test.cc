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

#include <algorithm>
#include <cmath>
#include <memory>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/datum.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/math_constants.h"
#include "arrow/util/string.h"

#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {
namespace {

using IntegralTypes = testing::Types<Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
                                     UInt16Type, UInt32Type, UInt64Type>;

using SignedIntegerTypes = testing::Types<Int8Type, Int16Type, Int32Type, Int64Type>;

using UnsignedIntegerTypes =
    testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type>;

// TODO(kszucs): add half-float
using FloatingTypes = testing::Types<FloatType, DoubleType>;

// Assert that all-null-type inputs results in a null-type output.
void AssertNullToNull(const std::string& func_name) {
  SCOPED_TRACE(func_name);
  ASSERT_OK_AND_ASSIGN(auto func, GetFunctionRegistry()->GetFunction(func_name));
  ASSERT_OK_AND_ASSIGN(auto nulls, MakeArrayOfNull(null(), /*length=*/7));
  const auto n = func->arity().num_args;

  {
    std::vector<Datum> args(n, nulls);
    ASSERT_OK_AND_ASSIGN(auto result, CallFunction(func_name, args));
    AssertArraysEqual(*nulls, *result.make_array(), /*verbose=*/true);
  }

  {
    std::vector<Datum> args(n, Datum(std::make_shared<NullScalar>()));
    ASSERT_OK_AND_ASSIGN(auto result, CallFunction(func_name, args));
    AssertScalarsEqual(NullScalar(), *result.scalar(), /*verbose=*/true);
  }
}

template <typename T, typename OptionsType>
class TestBaseUnaryArithmetic : public ::testing::Test {
 protected:
  using ArrowType = T;
  using CType = typename ArrowType::c_type;

  static std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }

  using UnaryFunction =
      std::function<Result<Datum>(const Datum&, OptionsType, ExecContext*)>;

  std::shared_ptr<Scalar> MakeNullScalar() {
    return arrow::MakeNullScalar(type_singleton());
  }

  std::shared_ptr<Scalar> MakeScalar(CType value) {
    return *arrow::MakeScalar(type_singleton(), value);
  }

  void SetUp() override {}

  // (CScalar, CScalar)
  void AssertUnaryOp(UnaryFunction func, CType argument, CType expected) {
    auto arg = MakeScalar(argument);
    auto exp = MakeScalar(expected);
    ASSERT_OK_AND_ASSIGN(auto actual, func(arg, options_, nullptr));
    AssertScalarsApproxEqual(*exp, *actual.scalar(), /*verbose=*/true);
  }

  // (Scalar, Scalar)
  void AssertUnaryOp(UnaryFunction func, const std::shared_ptr<Scalar>& arg,
                     const std::shared_ptr<Scalar>& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, func(arg, options_, nullptr));
    AssertScalarsApproxEqual(*expected, *actual.scalar(), /*verbose=*/true);
  }

  // (JSON, JSON)
  void AssertUnaryOp(UnaryFunction func, const std::string& arg_json,
                     const std::string& expected_json) {
    auto arg = ArrayFromJSON(type_singleton(), arg_json);
    auto expected = ArrayFromJSON(type_singleton(), expected_json);
    AssertUnaryOp(func, arg, expected);
  }

  // (Array, JSON)
  void AssertUnaryOp(UnaryFunction func, const std::shared_ptr<Array>& arg,
                     const std::string& expected_json) {
    const auto expected = ArrayFromJSON(type_singleton(), expected_json);
    AssertUnaryOp(func, arg, expected);
  }

  // (JSON, Array)
  void AssertUnaryOp(UnaryFunction func, const std::string& arg_json,
                     const std::shared_ptr<Array>& expected) {
    auto arg = ArrayFromJSON(type_singleton(), arg_json);
    AssertUnaryOp(func, arg, expected);
  }

  // (Array, Array)
  void AssertUnaryOp(UnaryFunction func, const std::shared_ptr<Array>& arg,
                     const std::shared_ptr<Array>& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, func(arg, options_, nullptr));
    ValidateAndAssertApproxEqual(actual.make_array(), expected);

    // Also check (Scalar, Scalar) operations
    const int64_t length = expected->length();
    for (int64_t i = 0; i < length; ++i) {
      const auto expected_scalar = *expected->GetScalar(i);
      ASSERT_OK_AND_ASSIGN(actual, func(*arg->GetScalar(i), options_, nullptr));
      AssertScalarsApproxEqual(*expected_scalar, *actual.scalar(), /*verbose=*/true,
                               equal_options_);
    }
  }

  // (CScalar, CScalar)
  void AssertUnaryOpRaises(UnaryFunction func, CType argument,
                           const std::string& expected_msg) {
    auto arg = MakeScalar(argument);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr(expected_msg),
                                    func(arg, options_, nullptr));
  }

  void AssertUnaryOpRaises(UnaryFunction func, const std::string& argument,
                           const std::string& expected_msg) {
    auto arg = ArrayFromJSON(type_singleton(), argument);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr(expected_msg),
                                    func(arg, options_, nullptr));
    for (int64_t i = 0; i < arg->length(); i++) {
      ASSERT_OK_AND_ASSIGN(auto scalar, arg->GetScalar(i));
      EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr(expected_msg),
                                      func(scalar, options_, nullptr));
    }
  }

  void AssertUnaryOpNotImplemented(UnaryFunction func, const std::string& argument) {
    auto arg = ArrayFromJSON(type_singleton(), argument);
    const char* expected_msg = "has no kernel matching input types";
    EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented, ::testing::HasSubstr(expected_msg),
                                    func(arg, options_, nullptr));
  }

  void ValidateAndAssertApproxEqual(const std::shared_ptr<Array>& actual,
                                    const std::string& expected) {
    const auto exp = ArrayFromJSON(type_singleton(), expected);
    ValidateAndAssertApproxEqual(actual, exp);
  }

  void ValidateAndAssertApproxEqual(const std::shared_ptr<Array>& actual,
                                    const std::shared_ptr<Array>& expected) {
    ValidateOutput(*actual);
    AssertArraysApproxEqual(*expected, *actual, /*verbose=*/true, equal_options_);
  }

  void SetNansEqual(bool value = true) {
    equal_options_ = equal_options_.nans_equal(value);
  }

  OptionsType options_ = OptionsType();
  EqualOptions equal_options_ = EqualOptions::Defaults().signed_zeros_equal(false);
};

// Subclasses of TestBaseUnaryArithmetic for different FunctionOptions.
template <typename T>
class TestUnaryArithmetic : public TestBaseUnaryArithmetic<T, ArithmeticOptions> {
 protected:
  using Base = TestBaseUnaryArithmetic<T, ArithmeticOptions>;
  using Base::options_;
  void SetOverflowCheck(bool value) { options_.check_overflow = value; }
};

template <typename T>
class TestUnaryArithmeticIntegral : public TestUnaryArithmetic<T> {};

template <typename T>
class TestUnaryArithmeticSigned : public TestUnaryArithmeticIntegral<T> {};

template <typename T>
class TestUnaryArithmeticUnsigned : public TestUnaryArithmeticIntegral<T> {};

template <typename T>
class TestUnaryArithmeticFloating : public TestUnaryArithmetic<T> {};

class TestArithmeticDecimal : public ::testing::Test {
 protected:
  std::vector<std::shared_ptr<DataType>> PositiveScaleTypes() {
    return {decimal128(4, 2), decimal256(4, 2), decimal128(38, 2), decimal256(76, 2)};
  }
  std::vector<std::shared_ptr<DataType>> NegativeScaleTypes() {
    return {decimal128(2, -2), decimal256(2, -2)};
  }

  // Validate that func(*decimals) is the same as
  // func([cast(x, float64) x for x in decimals])
  void CheckDecimalToFloat(const std::string& func, const DatumVector& args) {
    DatumVector floating_args;
    for (const auto& arg : args) {
      if (is_decimal(arg.type()->id())) {
        ASSERT_OK_AND_ASSIGN(auto casted, Cast(arg, float64()));
        floating_args.push_back(casted);
      } else {
        floating_args.push_back(arg);
      }
    }
    ASSERT_OK_AND_ASSIGN(auto expected, CallFunction(func, floating_args));
    ASSERT_OK_AND_ASSIGN(auto actual, CallFunction(func, args));
    AssertDatumsApproxEqual(actual, expected, /*verbose=*/true);
  }

  void CheckRaises(const std::string& func, const DatumVector& args,
                   const std::string& substr, const FunctionOptions* options = nullptr) {
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr(substr),
                                    CallFunction(func, args, options));
  }
};

template <typename T>
class TestBinaryArithmetic : public ::testing::Test {
 protected:
  using ArrowType = T;
  using CType = typename ArrowType::c_type;

  static std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }

  using BinaryFunction = std::function<Result<Datum>(const Datum&, const Datum&,
                                                     ArithmeticOptions, ExecContext*)>;

  void SetUp() override { options_.check_overflow = false; }

  std::shared_ptr<Scalar> MakeNullScalar() {
    return arrow::MakeNullScalar(type_singleton());
  }

  std::shared_ptr<Scalar> MakeScalar(CType value) {
    return *arrow::MakeScalar(type_singleton(), value);
  }

  // (Scalar, Scalar)
  void AssertBinop(BinaryFunction func, CType lhs, CType rhs, CType expected) {
    auto left = MakeScalar(lhs);
    auto right = MakeScalar(rhs);
    auto exp = MakeScalar(expected);

    ASSERT_OK_AND_ASSIGN(auto actual, func(left, right, options_, nullptr));
    AssertScalarsApproxEqual(*exp, *actual.scalar(), /*verbose=*/true);
  }

  // (Scalar, Array)
  void AssertBinop(BinaryFunction func, CType lhs, const std::string& rhs,
                   const std::string& expected) {
    auto left = MakeScalar(lhs);
    AssertBinop(func, left, rhs, expected);
  }

  // (Scalar, Array)
  void AssertBinop(BinaryFunction func, const std::shared_ptr<Scalar>& left,
                   const std::string& rhs, const std::string& expected) {
    auto right = ArrayFromJSON(type_singleton(), rhs);
    auto exp = ArrayFromJSON(type_singleton(), expected);

    ASSERT_OK_AND_ASSIGN(auto actual, func(left, right, options_, nullptr));
    ValidateAndAssertApproxEqual(actual.make_array(), expected);
  }

  // (Array, Scalar)
  void AssertBinop(BinaryFunction func, const std::string& lhs, CType rhs,
                   const std::string& expected) {
    auto right = MakeScalar(rhs);
    AssertBinop(func, lhs, right, expected);
  }

  // (Array, Scalar) => Array
  void AssertBinop(BinaryFunction func, const std::string& lhs,
                   const std::shared_ptr<Scalar>& right,
                   const std::shared_ptr<Array>& expected) {
    auto left = ArrayFromJSON(type_singleton(), lhs);

    ASSERT_OK_AND_ASSIGN(auto actual, func(left, right, options_, nullptr));
    ValidateAndAssertApproxEqual(actual.make_array(), expected);
  }

  // (Array, Scalar)
  void AssertBinop(BinaryFunction func, const std::string& lhs,
                   const std::shared_ptr<Scalar>& right, const std::string& expected) {
    auto left = ArrayFromJSON(type_singleton(), lhs);
    auto exp = ArrayFromJSON(type_singleton(), expected);

    ASSERT_OK_AND_ASSIGN(auto actual, func(left, right, options_, nullptr));
    ValidateAndAssertApproxEqual(actual.make_array(), expected);
  }

  // (Array, Array)
  void AssertBinop(BinaryFunction func, const std::string& lhs, const std::string& rhs,
                   const std::string& expected) {
    auto left = ArrayFromJSON(type_singleton(), lhs);
    auto right = ArrayFromJSON(type_singleton(), rhs);

    AssertBinop(func, left, right, expected);
  }

  // (Array, Array) => Array
  void AssertBinop(BinaryFunction func, const std::string& lhs, const std::string& rhs,
                   const std::shared_ptr<Array>& expected) {
    auto left = ArrayFromJSON(type_singleton(), lhs);
    auto right = ArrayFromJSON(type_singleton(), rhs);

    AssertBinop(func, left, right, expected);
  }

  // (Array, Array)
  void AssertBinop(BinaryFunction func, const std::shared_ptr<Array>& left,
                   const std::shared_ptr<Array>& right,
                   const std::string& expected_json) {
    const auto expected = ArrayFromJSON(type_singleton(), expected_json);
    AssertBinop(func, left, right, expected);
  }

  void AssertBinop(BinaryFunction func, const std::shared_ptr<Array>& left,
                   const std::shared_ptr<Array>& right,
                   const std::shared_ptr<Array>& expected) {
    ASSERT_OK_AND_ASSIGN(Datum actual, func(left, right, options_, nullptr));
    ValidateAndAssertApproxEqual(actual.make_array(), expected);

    // Also check (Scalar, Scalar) operations
    const int64_t length = expected->length();
    for (int64_t i = 0; i < length; ++i) {
      const auto expected_scalar = *expected->GetScalar(i);
      ASSERT_OK_AND_ASSIGN(
          actual, func(*left->GetScalar(i), *right->GetScalar(i), options_, nullptr));
      AssertScalarsApproxEqual(*expected_scalar, *actual.scalar(), /*verbose=*/true,
                               equal_options_);
    }
  }

  void AssertBinopRaises(BinaryFunction func, const std::string& lhs,
                         const std::string& rhs, const std::string& expected_msg) {
    auto left = ArrayFromJSON(type_singleton(), lhs);
    auto right = ArrayFromJSON(type_singleton(), rhs);

    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr(expected_msg),
                                    func(left, right, options_, nullptr));
  }

  void ValidateAndAssertApproxEqual(const std::shared_ptr<Array>& actual,
                                    const std::string& expected) {
    ValidateAndAssertApproxEqual(actual, ArrayFromJSON(type_singleton(), expected));
  }

  void ValidateAndAssertApproxEqual(const std::shared_ptr<Array>& actual,
                                    const std::shared_ptr<Array>& expected) {
    ValidateOutput(*actual);
    AssertArraysApproxEqual(*expected, *actual, /*verbose=*/true, equal_options_);
  }

  void SetOverflowCheck(bool value = true) { options_.check_overflow = value; }

  void SetNansEqual(bool value = true) {
    this->equal_options_ = equal_options_.nans_equal(value);
  }

  void SetSignedZerosEqual(bool value = true) {
    this->equal_options_ = equal_options_.signed_zeros_equal(value);
  }

  ArithmeticOptions options_ = ArithmeticOptions();
  EqualOptions equal_options_ = EqualOptions::Defaults().signed_zeros_equal(false);
};

template <typename... Elements>
std::string MakeArray(Elements... elements) {
  std::vector<std::string> elements_as_strings = {std::to_string(elements)...};

  std::vector<std::string_view> elements_as_views(sizeof...(Elements));
  std::copy(elements_as_strings.begin(), elements_as_strings.end(),
            elements_as_views.begin());

  return "[" + ::arrow::internal::JoinStrings(elements_as_views, ",") + "]";
}

template <typename T>
class TestBinaryArithmeticIntegral : public TestBinaryArithmetic<T> {};

template <typename T>
class TestBinaryArithmeticSigned : public TestBinaryArithmeticIntegral<T> {};

template <typename T>
class TestBinaryArithmeticUnsigned : public TestBinaryArithmeticIntegral<T> {};

template <typename T>
class TestBinaryArithmeticFloating : public TestBinaryArithmetic<T> {};

template <typename T>
class TestBitWiseArithmetic : public ::testing::Test {
 protected:
  using ArrowType = T;
  using CType = typename ArrowType::c_type;

  static std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }

  void AssertUnaryOp(const std::string& func, const std::vector<uint8_t>& args,
                     const std::vector<uint8_t>& expected) {
    auto input = ExpandByteArray(args);
    auto output = ExpandByteArray(expected);
    ASSERT_OK_AND_ASSIGN(Datum actual, CallFunction(func, {input}));
    ValidateAndAssertEqual(actual.make_array(), output);
    for (int64_t i = 0; i < output->length(); i++) {
      ASSERT_OK_AND_ASSIGN(Datum actual, CallFunction(func, {*input->GetScalar(i)}));
      const auto expected_scalar = *output->GetScalar(i);
      AssertScalarsEqual(*expected_scalar, *actual.scalar(), /*verbose=*/true);
    }
  }

  void AssertBinaryOp(const std::string& func, const std::vector<uint8_t>& arg0,
                      const std::vector<uint8_t>& arg1,
                      const std::vector<uint8_t>& expected) {
    auto input0 = ExpandByteArray(arg0);
    auto input1 = ExpandByteArray(arg1);
    auto output = ExpandByteArray(expected);
    ASSERT_OK_AND_ASSIGN(Datum actual, CallFunction(func, {input0, input1}));
    ValidateAndAssertEqual(actual.make_array(), output);
    for (int64_t i = 0; i < output->length(); i++) {
      ASSERT_OK_AND_ASSIGN(Datum actual, CallFunction(func, {*input0->GetScalar(i),
                                                             *input1->GetScalar(i)}));
      const auto expected_scalar = *output->GetScalar(i);
      AssertScalarsEqual(*expected_scalar, *actual.scalar(), /*verbose=*/true);
    }
  }

  // To make it easier to test different widths, tests give bytes which get repeated to
  // make an array of the actual type
  std::shared_ptr<Array> ExpandByteArray(const std::vector<uint8_t>& values) {
    std::vector<CType> c_values(values.size() + 1);
    for (size_t i = 0; i < values.size(); i++) {
      std::memset(&c_values[i], values[i], sizeof(CType));
    }
    std::vector<bool> valid(values.size() + 1, true);
    valid.back() = false;
    std::shared_ptr<Array> arr;
    ArrayFromVector<ArrowType>(valid, c_values, &arr);
    return arr;
  }

  void ValidateAndAssertEqual(const std::shared_ptr<Array>& actual,
                              const std::shared_ptr<Array>& expected) {
    ASSERT_OK(actual->ValidateFull());
    AssertArraysEqual(*expected, *actual, /*verbose=*/true);
  }
};

TYPED_TEST_SUITE(TestUnaryArithmeticIntegral, IntegralTypes);
TYPED_TEST_SUITE(TestUnaryArithmeticSigned, SignedIntegerTypes);
TYPED_TEST_SUITE(TestUnaryArithmeticUnsigned, UnsignedIntegerTypes);
TYPED_TEST_SUITE(TestUnaryArithmeticFloating, FloatingTypes);

TYPED_TEST_SUITE(TestBinaryArithmeticIntegral, IntegralTypes);
TYPED_TEST_SUITE(TestBinaryArithmeticSigned, SignedIntegerTypes);
TYPED_TEST_SUITE(TestBinaryArithmeticUnsigned, UnsignedIntegerTypes);
TYPED_TEST_SUITE(TestBinaryArithmeticFloating, FloatingTypes);

TYPED_TEST_SUITE(TestBitWiseArithmetic, IntegralTypes);

TYPED_TEST(TestBitWiseArithmetic, BitWiseNot) {
  this->AssertUnaryOp("bit_wise_not", std::vector<uint8_t>{0x00, 0x55, 0xAA, 0xFF},
                      std::vector<uint8_t>{0xFF, 0xAA, 0x55, 0x00});
}

TYPED_TEST(TestBitWiseArithmetic, BitWiseAnd) {
  this->AssertBinaryOp("bit_wise_and", std::vector<uint8_t>{0x00, 0xFF, 0x00, 0xFF},
                       std::vector<uint8_t>{0x00, 0x00, 0xFF, 0xFF},
                       std::vector<uint8_t>{0x00, 0x00, 0x00, 0xFF});
}

TYPED_TEST(TestBitWiseArithmetic, BitWiseOr) {
  this->AssertBinaryOp("bit_wise_or", std::vector<uint8_t>{0x00, 0xFF, 0x00, 0xFF},
                       std::vector<uint8_t>{0x00, 0x00, 0xFF, 0xFF},
                       std::vector<uint8_t>{0x00, 0xFF, 0xFF, 0xFF});
}

TYPED_TEST(TestBitWiseArithmetic, BitWiseXor) {
  this->AssertBinaryOp("bit_wise_xor", std::vector<uint8_t>{0x00, 0xFF, 0x00, 0xFF},
                       std::vector<uint8_t>{0x00, 0x00, 0xFF, 0xFF},
                       std::vector<uint8_t>{0x00, 0xFF, 0xFF, 0x00});
}

TYPED_TEST(TestBinaryArithmeticIntegral, Add) {
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);

    this->AssertBinop(Add, "[]", "[]", "[]");
    this->AssertBinop(Add, "[3, 2, 6]", "[1, 0, 2]", "[4, 2, 8]");
    // Nulls on left side
    this->AssertBinop(Add, "[null, 1, null]", "[3, 4, 5]", "[null, 5, null]");
    this->AssertBinop(Add, "[3, 4, 5]", "[null, 1, null]", "[null, 5, null]");
    // Nulls on both sides
    this->AssertBinop(Add, "[null, 1, 2]", "[3, 4, null]", "[null, 5, null]");
    // All nulls
    this->AssertBinop(Add, "[null]", "[null]", "[null]");

    // Scalar on the left
    this->AssertBinop(Add, 3, "[1, 2]", "[4, 5]");
    this->AssertBinop(Add, 3, "[null, 2]", "[null, 5]");
    this->AssertBinop(Add, this->MakeNullScalar(), "[1, 2]", "[null, null]");
    this->AssertBinop(Add, this->MakeNullScalar(), "[null, 2]", "[null, null]");
    // Scalar on the right
    this->AssertBinop(Add, "[1, 2]", 3, "[4, 5]");
    this->AssertBinop(Add, "[null, 2]", 3, "[null, 5]");
    this->AssertBinop(Add, "[1, 2]", this->MakeNullScalar(), "[null, null]");
    this->AssertBinop(Add, "[null, 2]", this->MakeNullScalar(), "[null, null]");
  }
}

TYPED_TEST(TestBinaryArithmeticIntegral, Sub) {
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);

    this->AssertBinop(Subtract, "[]", "[]", "[]");
    this->AssertBinop(Subtract, "[3, 2, 6]", "[1, 0, 2]", "[2, 2, 4]");
    // Nulls on left side
    this->AssertBinop(Subtract, "[null, 4, null]", "[2, 1, 0]", "[null, 3, null]");
    this->AssertBinop(Subtract, "[5, 4, 3]", "[null, 1, null]", "[null, 3, null]");
    // Nulls on both sides
    this->AssertBinop(Subtract, "[null, 4, 3]", "[2, 1, null]", "[null, 3, null]");
    // All nulls
    this->AssertBinop(Subtract, "[null]", "[null]", "[null]");

    // Scalar on the left
    this->AssertBinop(Subtract, 3, "[1, 2]", "[2, 1]");
    this->AssertBinop(Subtract, 3, "[null, 2]", "[null, 1]");
    this->AssertBinop(Subtract, this->MakeNullScalar(), "[1, 2]", "[null, null]");
    this->AssertBinop(Subtract, this->MakeNullScalar(), "[null, 2]", "[null, null]");
    // Scalar on the right
    this->AssertBinop(Subtract, "[4, 5]", 3, "[1, 2]");
    this->AssertBinop(Subtract, "[null, 5]", 3, "[null, 2]");
    this->AssertBinop(Subtract, "[1, 2]", this->MakeNullScalar(), "[null, null]");
    this->AssertBinop(Subtract, "[null, 2]", this->MakeNullScalar(), "[null, null]");
  }
}

TEST(TestBinaryArithmetic, SubtractTimestamps) {
  random::RandomArrayGenerator rand(kRandomSeed);

  const int64_t length = 100;

  auto lhs = rand.Int64(length, 0, 100000000);
  auto rhs = rand.Int64(length, 0, 100000000);
  auto expected_int64 = (*Subtract(lhs, rhs)).make_array();

  for (auto unit : TimeUnit::values()) {
    auto timestamp_ty = timestamp(unit);
    auto duration_ty = duration(unit);

    auto lhs_timestamp = *lhs->View(timestamp_ty);
    auto rhs_timestamp = *rhs->View(timestamp_ty);

    auto result = (*Subtract(lhs_timestamp, rhs_timestamp)).make_array();
    ASSERT_TRUE(result->type()->Equals(*duration_ty));
    AssertArraysEqual(**result->View(int64()), *expected_int64);
  }
}

TYPED_TEST(TestBinaryArithmeticIntegral, Mul) {
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);

    this->AssertBinop(Multiply, "[]", "[]", "[]");
    this->AssertBinop(Multiply, "[3, 2, 6]", "[1, 0, 2]", "[3, 0, 12]");
    // Nulls on left side
    this->AssertBinop(Multiply, "[null, 2, null]", "[4, 5, 6]", "[null, 10, null]");
    this->AssertBinop(Multiply, "[4, 5, 6]", "[null, 2, null]", "[null, 10, null]");
    // Nulls on both sides
    this->AssertBinop(Multiply, "[null, 2, 3]", "[4, 5, null]", "[null, 10, null]");
    // All nulls
    this->AssertBinop(Multiply, "[null]", "[null]", "[null]");

    // Scalar on the left
    this->AssertBinop(Multiply, 3, "[4, 5]", "[12, 15]");
    this->AssertBinop(Multiply, 3, "[null, 5]", "[null, 15]");
    this->AssertBinop(Multiply, this->MakeNullScalar(), "[1, 2]", "[null, null]");
    this->AssertBinop(Multiply, this->MakeNullScalar(), "[null, 2]", "[null, null]");
    // Scalar on the right
    this->AssertBinop(Multiply, "[4, 5]", 3, "[12, 15]");
    this->AssertBinop(Multiply, "[null, 5]", 3, "[null, 15]");
    this->AssertBinop(Multiply, "[1, 2]", this->MakeNullScalar(), "[null, null]");
    this->AssertBinop(Multiply, "[null, 2]", this->MakeNullScalar(), "[null, null]");
  }
}

TYPED_TEST(TestBinaryArithmeticSigned, Add) {
  this->AssertBinop(Add, "[-7, 6, 5, 4, 3, 2, 1]", "[-6, 5, -4, 3, -2, 1, 0]",
                    "[-13, 11, 1, 7, 1, 3, 1]");
  this->AssertBinop(Add, -1, "[-6, 5, -4, 3, -2, 1, 0]", "[-7, 4, -5, 2, -3, 0, -1]");
  this->AssertBinop(Add, -10, 5, -5);
}

TYPED_TEST(TestBinaryArithmeticSigned, OverflowWraps) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->AssertBinop(Subtract, MakeArray(min, max, min), MakeArray(1, max, max),
                    MakeArray(max, 0, 1));
  this->AssertBinop(Multiply, MakeArray(min, max, max), MakeArray(max, 2, max),
                    MakeArray(min, CType(-2), 1));
}

TYPED_TEST(TestBinaryArithmeticIntegral, OverflowRaises) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->SetOverflowCheck(true);

  this->AssertBinopRaises(Add, MakeArray(min, max, max), MakeArray(CType(-1), 1, max),
                          "overflow");
  this->AssertBinopRaises(Subtract, MakeArray(min, max), MakeArray(1, max), "overflow");
  this->AssertBinopRaises(Subtract, MakeArray(min), MakeArray(max), "overflow");

  this->AssertBinopRaises(Multiply, MakeArray(min, max, max), MakeArray(max, 2, max),
                          "overflow");
}

TYPED_TEST(TestBinaryArithmeticSigned, AddOverflowRaises) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->SetOverflowCheck(true);

  this->AssertBinop(Add, MakeArray(max), MakeArray(-1), MakeArray(max - 1));
  this->AssertBinop(Add, MakeArray(min), MakeArray(1), MakeArray(min + 1));
  this->AssertBinop(Add, MakeArray(-1), MakeArray(2), MakeArray(1));
  this->AssertBinop(Add, MakeArray(1), MakeArray(-2), MakeArray(-1));

  this->AssertBinopRaises(Add, MakeArray(max), MakeArray(1), "overflow");
  this->AssertBinopRaises(Add, MakeArray(min), MakeArray(-1), "overflow");

  // Overflow should not be checked on underlying value slots when output would be null
  auto left = ArrayFromJSON(this->type_singleton(), MakeArray(1, max, min));
  auto right = ArrayFromJSON(this->type_singleton(), MakeArray(1, 1, -1));
  left = TweakValidityBit(left, 1, false);
  right = TweakValidityBit(right, 2, false);
  this->AssertBinop(Add, left, right, "[2, null, null]");
}

TYPED_TEST(TestBinaryArithmeticSigned, SubOverflowRaises) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->SetOverflowCheck(true);

  this->AssertBinop(Subtract, MakeArray(max), MakeArray(1), MakeArray(max - 1));
  this->AssertBinop(Subtract, MakeArray(min), MakeArray(-1), MakeArray(min + 1));
  this->AssertBinop(Subtract, MakeArray(-1), MakeArray(-2), MakeArray(1));
  this->AssertBinop(Subtract, MakeArray(1), MakeArray(2), MakeArray(-1));

  this->AssertBinopRaises(Subtract, MakeArray(max), MakeArray(-1), "overflow");
  this->AssertBinopRaises(Subtract, MakeArray(min), MakeArray(1), "overflow");

  // Overflow should not be checked on underlying value slots when output would be null
  auto left = ArrayFromJSON(this->type_singleton(), MakeArray(2, max, min));
  auto right = ArrayFromJSON(this->type_singleton(), MakeArray(1, -1, 1));
  left = TweakValidityBit(left, 1, false);
  right = TweakValidityBit(right, 2, false);
  this->AssertBinop(Subtract, left, right, "[1, null, null]");
}

TYPED_TEST(TestBinaryArithmeticSigned, MulOverflowRaises) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->SetOverflowCheck(true);

  this->AssertBinop(Multiply, MakeArray(max), MakeArray(-1), MakeArray(min + 1));
  this->AssertBinop(Multiply, MakeArray(max / 2), MakeArray(-2), MakeArray(min + 2));

  this->AssertBinopRaises(Multiply, MakeArray(max), MakeArray(2), "overflow");
  this->AssertBinopRaises(Multiply, MakeArray(max / 2), MakeArray(3), "overflow");
  this->AssertBinopRaises(Multiply, MakeArray(max / 2), MakeArray(-3), "overflow");

  this->AssertBinopRaises(Multiply, MakeArray(min), MakeArray(2), "overflow");
  this->AssertBinopRaises(Multiply, MakeArray(min / 2), MakeArray(3), "overflow");
  this->AssertBinopRaises(Multiply, MakeArray(min), MakeArray(-1), "overflow");
  this->AssertBinopRaises(Multiply, MakeArray(min / 2), MakeArray(-2), "overflow");

  // Overflow should not be checked on underlying value slots when output would be null
  auto left = ArrayFromJSON(this->type_singleton(), MakeArray(2, max, min / 2));
  auto right = ArrayFromJSON(this->type_singleton(), MakeArray(1, 2, 3));
  left = TweakValidityBit(left, 1, false);
  right = TweakValidityBit(right, 2, false);
  this->AssertBinop(Multiply, left, right, "[2, null, null]");
}

TYPED_TEST(TestBinaryArithmeticUnsigned, OverflowWraps) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->SetOverflowCheck(false);
  this->AssertBinop(Add, MakeArray(min, max, max), MakeArray(CType(-1), 1, max),
                    MakeArray(max, min, CType(-2)));

  this->AssertBinop(Subtract, MakeArray(min, max, min), MakeArray(1, max, max),
                    MakeArray(max, 0, 1));

  this->AssertBinop(Multiply, MakeArray(min, max, max), MakeArray(max, 2, max),
                    MakeArray(min, CType(-2), 1));
}

TYPED_TEST(TestBinaryArithmeticSigned, Sub) {
  this->AssertBinop(Subtract, "[0, 1, 2, 3, 4, 5, 6]", "[1, 2, 3, 4, 5, 6, 7]",
                    "[-1, -1, -1, -1, -1, -1, -1]");

  this->AssertBinop(Subtract, "[0, 0, 0, 0, 0, 0, 0]", "[6, 5, 4, 3, 2, 1, 0]",
                    "[-6, -5, -4, -3, -2, -1, 0]");

  this->AssertBinop(Subtract, "[10, 12, 4, 50, 50, 32, 11]", "[2, 0, 6, 1, 5, 3, 4]",
                    "[8, 12, -2, 49, 45, 29, 7]");

  this->AssertBinop(Subtract, "[null, 1, 3, null, 2, 5]", "[1, 4, 2, 5, 0, 3]",
                    "[null, -3, 1, null, 2, 2]");
}

TYPED_TEST(TestBinaryArithmeticSigned, Mul) {
  this->AssertBinop(Multiply, "[-10, 12, 4, 50, -5, 32, 11]", "[-2, 0, -6, 1, 5, 3, 4]",
                    "[20, 0, -24, 50, -25, 96, 44]");
  this->AssertBinop(Multiply, -2, "[-10, 12, 4, 50, -5, 32, 11]",
                    "[20, -24, -8, -100, 10, -64, -22]");
  this->AssertBinop(Multiply, -5, -5, 25);
}

// NOTE: cannot test Inf / -Inf (ARROW-9495)

TYPED_TEST(TestBinaryArithmeticFloating, Add) {
  this->AssertBinop(Add, "[]", "[]", "[]");

  this->AssertBinop(Add, "[1.5, 0.5]", "[2.0, -3]", "[3.5, -2.5]");
  // Nulls on the left
  this->AssertBinop(Add, "[null, 0.5]", "[2.0, -3]", "[null, -2.5]");
  // Nulls on the right
  this->AssertBinop(Add, "[1.5, 0.5]", "[null, -3]", "[null, -2.5]");
  // Nulls on both sides
  this->AssertBinop(Add, "[null, 1.5, 0.5]", "[2.0, -3, null]", "[null, -1.5, null]");

  // Scalar on the left
  this->AssertBinop(Add, -1.5f, "[0.0, 2.0]", "[-1.5, 0.5]");
  this->AssertBinop(Add, -1.5f, "[null, 2.0]", "[null, 0.5]");
  this->AssertBinop(Add, this->MakeNullScalar(), "[0.0, 2.0]", "[null, null]");
  this->AssertBinop(Add, this->MakeNullScalar(), "[null, 2.0]", "[null, null]");
  // Scalar on the right
  this->AssertBinop(Add, "[0.0, 2.0]", -1.5f, "[-1.5, 0.5]");
  this->AssertBinop(Add, "[null, 2.0]", -1.5f, "[null, 0.5]");
  this->AssertBinop(Add, "[0.0, 2.0]", this->MakeNullScalar(), "[null, null]");
  this->AssertBinop(Add, "[null, 2.0]", this->MakeNullScalar(), "[null, null]");
}

TYPED_TEST(TestBinaryArithmeticFloating, Div) {
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    // Empty arrays
    this->AssertBinop(Divide, "[]", "[]", "[]");
    // Ordinary arrays
    this->AssertBinop(Divide, "[3.4, 0.64, 1.28]", "[1, 2, 4]", "[3.4, 0.32, 0.32]");
    // Array with nulls
    this->AssertBinop(Divide, "[null, 1, 3.3, null, 2]", "[1, 4, 2, 5, 0.1]",
                      "[null, 0.25, 1.65, null, 20]");
    // Scalar divides by array
    this->AssertBinop(Divide, 10.0F, "[null, 1, 2.5, null, 2, 5]",
                      "[null, 10, 4, null, 5, 2]");
    // Array divides by scalar
    this->AssertBinop(Divide, "[null, 1, 2.5, null, 2, 5]", 10.0F,
                      "[null, 0.1, 0.25, null, 0.2, 0.5]");
    // Array with infinity
    this->AssertBinop(Divide, "[3.4, Inf, -Inf]", "[1, 2, 3]", "[3.4, Inf, -Inf]");
    // Array with NaN
    this->SetNansEqual(true);
    this->AssertBinop(Divide, "[3.4, NaN, 2.0]", "[1, 2, 2.0]", "[3.4, NaN, 1.0]");
    // Scalar divides by scalar
    this->AssertBinop(Divide, 21.0F, 3.0F, 7.0F);
  }
}

TYPED_TEST(TestBinaryArithmeticIntegral, Div) {
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);

    // Empty arrays
    this->AssertBinop(Divide, "[]", "[]", "[]");
    // Ordinary arrays
    this->AssertBinop(Divide, "[3, 2, 6]", "[1, 1, 2]", "[3, 2, 3]");
    // Array with nulls
    this->AssertBinop(Divide, "[null, 10, 30, null, 20]", "[1, 4, 2, 5, 10]",
                      "[null, 2, 15, null, 2]");
    // Scalar divides by array
    this->AssertBinop(Divide, 33, "[null, 1, 3, null, 2]", "[null, 33, 11, null, 16]");
    // Array divides by scalar
    this->AssertBinop(Divide, "[null, 10, 30, null, 2]", 3, "[null, 3, 10, null, 0]");
    // Scalar divides by scalar
    this->AssertBinop(Divide, 16, 7, 2);
  }
}

TYPED_TEST(TestBinaryArithmeticSigned, Div) {
  // Ordinary arrays
  this->AssertBinop(Divide, "[-3, 2, -6]", "[1, 1, 2]", "[-3, 2, -3]");
  // Array with nulls
  this->AssertBinop(Divide, "[null, 10, 30, null, -20]", "[1, 4, 2, 5, 10]",
                    "[null, 2, 15, null, -2]");
  // Scalar divides by array
  this->AssertBinop(Divide, 33, "[null, -1, -3, null, 2]", "[null, -33, -11, null, 16]");
  // Array divides by scalar
  this->AssertBinop(Divide, "[null, 10, 30, null, 2]", 3, "[null, 3, 10, null, 0]");
  // Scalar divides by scalar
  this->AssertBinop(Divide, -16, -8, 2);
}

TYPED_TEST(TestBinaryArithmeticIntegral, DivideByZero) {
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    this->AssertBinopRaises(Divide, "[3, 2, 6]", "[1, 1, 0]", "divide by zero");
  }
}

TYPED_TEST(TestBinaryArithmeticFloating, DivideByZero) {
  this->SetOverflowCheck(true);
  this->AssertBinopRaises(Divide, "[3.0, 2.0, 6.0]", "[1.0, 1.0, 0.0]", "divide by zero");
  this->AssertBinopRaises(Divide, "[3.0, 2.0, 0.0]", "[1.0, 1.0, 0.0]", "divide by zero");
  this->AssertBinopRaises(Divide, "[3.0, 2.0, -6.0]", "[1.0, 1.0, 0.0]",
                          "divide by zero");

  this->SetOverflowCheck(false);
  this->SetNansEqual(true);
  this->AssertBinop(Divide, "[3.0, 2.0, 6.0]", "[1.0, 1.0, 0.0]", "[3.0, 2.0, Inf]");
  this->AssertBinop(Divide, "[3.0, 2.0, 0.0]", "[1.0, 1.0, 0.0]", "[3.0, 2.0, NaN]");
  this->AssertBinop(Divide, "[3.0, 2.0, -6.0]", "[1.0, 1.0, 0.0]", "[3.0, 2.0, -Inf]");
}

TYPED_TEST(TestBinaryArithmeticSigned, DivideOverflowRaises) {
  using CType = typename TestFixture::CType;
  auto min = std::numeric_limits<CType>::lowest();

  this->SetOverflowCheck(true);
  this->AssertBinopRaises(Divide, MakeArray(min), MakeArray(-1), "overflow");

  this->SetOverflowCheck(false);
  this->AssertBinop(Divide, MakeArray(min), MakeArray(-1), "[0]");
}

TYPED_TEST(TestBinaryArithmeticFloating, Power) {
  using CType = typename TestFixture::CType;
  auto max = std::numeric_limits<CType>::max();
  this->SetNansEqual(true);

  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);

    // Empty arrays
    this->AssertBinop(Power, "[]", "[]", "[]");
    // Ordinary arrays
    this->AssertBinop(Power, "[3.4, 16, 0.64, 1.2, 0]", "[1, 0.5, 2, 4, 0]",
                      "[3.4, 4, 0.4096, 2.0736, 1]");
    // Array with nulls
    this->AssertBinop(Power, "[null, 1, 3.3, null, 2]", "[1, 4, 2, 5, 0.1]",
                      "[null, 1, 10.89, null, 1.07177346]");
    // Scalar exponentiated by array
    this->AssertBinop(Power, 10.0F, "[null, 1, 2.5, null, 2, 5]",
                      "[null, 10, 316.227766017, null, 100, 100000]");
    // Array exponentiated by scalar
    this->AssertBinop(Power, "[null, 1, 2.5, null, 2, 5]", 10.0F,
                      "[null, 1, 9536.74316406, null, 1024, 9765625]");
    // Array with infinity
    this->AssertBinop(Power, "[3.4, Inf, -Inf, 1.1, 100000]", "[1, 2, 3, Inf, 100000]",
                      "[3.4, Inf, -Inf, Inf, Inf]");
    // Array with NaN
    this->AssertBinop(Power, "[3.4, NaN, 2.0]", "[1, 2, 2.0]", "[3.4, NaN, 4.0]");
    // Scalar exponentiated by scalar
    this->AssertBinop(Power, 21.0F, 3.0F, 9261.0F);
    // Divide by zero
    this->AssertBinop(Power, "[0.0, 0.0]", "[-1.0, -3.0]", "[Inf, Inf]");
    // Check overflow behaviour
    this->AssertBinop(Power, max, 10, INFINITY);
  }

  // Edge cases - removing NaNs
  this->AssertBinop(Power, "[1, NaN, 0, null, 1.2, -Inf, Inf, 1.1, 1, 0, 1, 0]",
                    "[NaN, 0, NaN, 1, null, 1, 2, -Inf, Inf, 0, 0, 42]",
                    "[1, 1, NaN, null, null, -Inf, Inf, 0, 1, 1, 1, 0]");
}

TYPED_TEST(TestBinaryArithmeticIntegral, Power) {
  using CType = typename TestFixture::CType;
  auto max = std::numeric_limits<CType>::max();

  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);

    // Empty arrays
    this->AssertBinop(Power, "[]", "[]", "[]");
    // Ordinary arrays
    this->AssertBinop(Power, "[3, 2, 6, 2]", "[1, 1, 2, 0]", "[3, 2, 36, 1]");
    // Array with nulls
    this->AssertBinop(Power, "[null, 2, 3, null, 20]", "[1, 6, 2, 5, 1]",
                      "[null, 64, 9, null, 20]");
    // Scalar exponentiated by array
    this->AssertBinop(Power, 3, "[null, 3, 4, null, 2]", "[null, 27, 81, null, 9]");
    // Array exponentiated by scalar
    this->AssertBinop(Power, "[null, 10, 3, null, 2]", 2, "[null, 100, 9, null, 4]");
    // Scalar exponentiated by scalar
    this->AssertBinop(Power, 4, 3, 64);
    // Edge cases
    this->AssertBinop(Power, "[0, 1, 0]", "[0, 0, 42]", "[1, 1, 0]");
  }

  // Overflow raises
  this->SetOverflowCheck(true);
  this->AssertBinopRaises(Power, MakeArray(max), MakeArray(10), "overflow");
  // Disable overflow check
  this->SetOverflowCheck(false);
  this->AssertBinop(Power, max, 10, 1);
}

TYPED_TEST(TestBinaryArithmeticSigned, Power) {
  using CType = typename TestFixture::CType;
  auto max = std::numeric_limits<CType>::max();

  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);

    // Empty arrays
    this->AssertBinop(Power, "[]", "[]", "[]");
    // Ordinary arrays
    this->AssertBinop(Power, "[-3, 2, -6, 2]", "[3, 1, 2, 0]", "[-27, 2, 36, 1]");
    // Array with nulls
    this->AssertBinop(Power, "[null, 10, 127, null, -20]", "[1, 2, 1, 5, 1]",
                      "[null, 100, 127, null, -20]");
    // Scalar exponentiated by array
    this->AssertBinop(Power, 11, "[null, 1, null, 2]", "[null, 11, null, 121]");
    // Array exponentiated by scalar
    this->AssertBinop(Power, "[null, 1, 3, null, 2]", 3, "[null, 1, 27, null, 8]");
    // Scalar exponentiated by scalar
    this->AssertBinop(Power, 16, 1, 16);
    // Edge cases
    this->AssertBinop(Power, "[1, 0, -1, 2]", "[0, 42, 0, 1]", "[1, 0, 1, 2]");
    // Divide by zero raises
    this->AssertBinopRaises(Power, MakeArray(0), MakeArray(-1),
                            "integers to negative integer powers are not allowed");
  }

  // Overflow raises
  this->SetOverflowCheck(true);
  this->AssertBinopRaises(Power, MakeArray(max), MakeArray(10), "overflow");
  // Disable overflow check
  this->SetOverflowCheck(false);
  this->AssertBinop(Power, max, 10, 1);
}

TYPED_TEST(TestBinaryArithmeticFloating, Sub) {
  this->AssertBinop(Subtract, "[]", "[]", "[]");

  this->AssertBinop(Subtract, "[1.5, 0.5]", "[2.0, -3]", "[-0.5, 3.5]");
  // Nulls on the left
  this->AssertBinop(Subtract, "[null, 0.5]", "[2.0, -3]", "[null, 3.5]");
  // Nulls on the right
  this->AssertBinop(Subtract, "[1.5, 0.5]", "[null, -3]", "[null, 3.5]");
  // Nulls on both sides
  this->AssertBinop(Subtract, "[null, 1.5, 0.5]", "[2.0, -3, null]", "[null, 4.5, null]");

  // Scalar on the left
  this->AssertBinop(Subtract, -1.5f, "[0.0, 2.0]", "[-1.5, -3.5]");
  this->AssertBinop(Subtract, -1.5f, "[null, 2.0]", "[null, -3.5]");
  this->AssertBinop(Subtract, this->MakeNullScalar(), "[0.0, 2.0]", "[null, null]");
  this->AssertBinop(Subtract, this->MakeNullScalar(), "[null, 2.0]", "[null, null]");
  // Scalar on the right
  this->AssertBinop(Subtract, "[0.0, 2.0]", -1.5f, "[1.5, 3.5]");
  this->AssertBinop(Subtract, "[null, 2.0]", -1.5f, "[null, 3.5]");
  this->AssertBinop(Subtract, "[0.0, 2.0]", this->MakeNullScalar(), "[null, null]");
  this->AssertBinop(Subtract, "[null, 2.0]", this->MakeNullScalar(), "[null, null]");
}

TYPED_TEST(TestBinaryArithmeticFloating, Mul) {
  this->AssertBinop(Multiply, "[]", "[]", "[]");

  this->AssertBinop(Multiply, "[1.5, 0.5]", "[2.0, -3]", "[3.0, -1.5]");
  // Nulls on the left
  this->AssertBinop(Multiply, "[null, 0.5]", "[2.0, -3]", "[null, -1.5]");
  // Nulls on the right
  this->AssertBinop(Multiply, "[1.5, 0.5]", "[null, -3]", "[null, -1.5]");
  // Nulls on both sides
  this->AssertBinop(Multiply, "[null, 1.5, 0.5]", "[2.0, -3, null]",
                    "[null, -4.5, null]");

  // Negative zeros
  this->AssertBinop(Multiply, "[0.0, 1.0, -1.0, -0.0]", "[1.0, -0.0, 0.0, -1.0]",
                    "[0.0, -0.0, -0.0, 0.0]");
  this->AssertBinop(Multiply, "[0.0, 1.0, -1.0, null]", "[null, -0.0, 0.0, -1.0]",
                    "[null, -0.0, -0.0, null]");

  // Scalar on the left
  this->AssertBinop(Multiply, -1.5f, "[0.0, 2.0]", "[-0.0, -3.0]");
  this->AssertBinop(Multiply, -1.5f, "[null, 2.0]", "[null, -3.0]");
  this->AssertBinop(Multiply, -0.0f, "[3.0, -2.0]", "[-0.0, 0.0]");
  this->AssertBinop(Multiply, -0.0f, "[null, 2.0]", "[null, -0.0]");
  this->AssertBinop(Multiply, this->MakeNullScalar(), "[0.0, 2.0]", "[null, null]");
  this->AssertBinop(Multiply, this->MakeNullScalar(), "[null, 2.0]", "[null, null]");
  // Scalar on the right
  this->AssertBinop(Multiply, "[0.0, 2.0]", -1.5f, "[-0.0, -3.0]");
  this->AssertBinop(Multiply, "[null, 2.0]", -1.5f, "[null, -3.0]");
  this->AssertBinop(Multiply, "[3.0, -2.0]", -0.0f, "[-0.0, 0.0]");
  this->AssertBinop(Multiply, "[null, 2.0]", -0.0f, "[null, -0.0]");
  this->AssertBinop(Multiply, "[0.0, 2.0]", this->MakeNullScalar(), "[null, null]");
  this->AssertBinop(Multiply, "[null, 2.0]", this->MakeNullScalar(), "[null, null]");
}

TEST(TestBinaryArithmetic, DispatchBest) {
  for (std::string name : {"add", "subtract", "multiply", "divide", "power"}) {
    for (std::string suffix : {"", "_checked"}) {
      name += suffix;

      CheckDispatchBest(name, {int32(), int32()}, {int32(), int32()});
      CheckDispatchBest(name, {int32(), null()}, {int32(), int32()});
      CheckDispatchBest(name, {null(), int32()}, {int32(), int32()});

      CheckDispatchBest(name, {int32(), int8()}, {int32(), int32()});
      CheckDispatchBest(name, {int32(), int16()}, {int32(), int32()});
      CheckDispatchBest(name, {int32(), int32()}, {int32(), int32()});
      CheckDispatchBest(name, {int32(), int64()}, {int64(), int64()});

      CheckDispatchBest(name, {int32(), uint8()}, {int32(), int32()});
      CheckDispatchBest(name, {int32(), uint16()}, {int32(), int32()});
      CheckDispatchBest(name, {int32(), uint32()}, {int64(), int64()});
      CheckDispatchBest(name, {int32(), uint64()}, {int64(), int64()});

      CheckDispatchBest(name, {uint8(), uint8()}, {uint8(), uint8()});
      CheckDispatchBest(name, {uint8(), uint16()}, {uint16(), uint16()});

      CheckDispatchBest(name, {int32(), float32()}, {float32(), float32()});
      CheckDispatchBest(name, {float32(), int64()}, {float32(), float32()});
      CheckDispatchBest(name, {float64(), int32()}, {float64(), float64()});

      CheckDispatchBest(name, {dictionary(int8(), float64()), float64()},
                        {float64(), float64()});
      CheckDispatchBest(name, {dictionary(int8(), float64()), int16()},
                        {float64(), float64()});
    }
  }

  CheckDispatchBest("atan2", {int32(), float64()}, {float64(), float64()});
  CheckDispatchBest("atan2", {int32(), uint8()}, {float64(), float64()});
  CheckDispatchBest("atan2", {int32(), null()}, {float64(), float64()});
  CheckDispatchBest("atan2", {float32(), float64()}, {float64(), float64()});
  // Integer always promotes to double
  CheckDispatchBest("atan2", {float32(), int8()}, {float64(), float64()});
}

TEST(TestBinaryArithmetic, Null) {
  for (std::string name : {"add", "divide", "logb", "multiply", "power", "shift_left",
                           "shift_right", "subtract", "tan"}) {
    for (std::string suffix : {"", "_checked"}) {
      name += suffix;
      AssertNullToNull(name);
    }
  }

  for (std::string name : {"atan2", "bit_wise_and", "bit_wise_or", "bit_wise_xor"}) {
    AssertNullToNull(name);
  }
}

TEST(TestBinaryArithmetic, AddWithImplicitCasts) {
  CheckScalarBinary("add", ArrayFromJSON(int32(), "[0, 1, 2, null]"),
                    ArrayFromJSON(float64(), "[0.25, 0.5, 0.75, 1.0]"),
                    ArrayFromJSON(float64(), "[0.25, 1.5, 2.75, null]"));

  CheckScalarBinary("add", ArrayFromJSON(int8(), "[-16, 0, 16, null]"),
                    ArrayFromJSON(uint32(), "[3, 4, 5, 7]"),
                    ArrayFromJSON(int64(), "[-13, 4, 21, null]"));

  CheckScalarBinary("add",
                    ArrayFromJSON(dictionary(int32(), int32()), "[8, 6, 3, null, 2]"),
                    ArrayFromJSON(uint32(), "[3, 4, 5, 7, 0]"),
                    ArrayFromJSON(int64(), "[11, 10, 8, null, 2]"));

  CheckScalarBinary("add", ArrayFromJSON(int32(), "[0, 1, 2, null]"),
                    std::make_shared<NullArray>(4),
                    ArrayFromJSON(int32(), "[null, null, null, null]"));

  CheckScalarBinary("add", ArrayFromJSON(dictionary(int32(), int8()), "[0, 1, 2, null]"),
                    ArrayFromJSON(uint32(), "[3, 4, 5, 7]"),
                    ArrayFromJSON(int64(), "[3, 5, 7, null]"));
}

TEST(TestBinaryArithmetic, AddWithImplicitCastsUint64EdgeCase) {
  // int64 is as wide as we can promote
  CheckDispatchBest("add", {int8(), uint64()}, {int64(), int64()});

  // this works sometimes
  CheckScalarBinary("add", ArrayFromJSON(int8(), "[-1]"), ArrayFromJSON(uint64(), "[0]"),
                    ArrayFromJSON(int64(), "[-1]"));

  // ... but it can result in impossible implicit casts in  the presence of uint64, since
  // some uint64 values cannot be cast to int64:
  ASSERT_RAISES(Invalid,
                CallFunction("add", {ArrayFromJSON(int64(), "[-1]"),
                                     ArrayFromJSON(uint64(), "[18446744073709551615]")}));
}

TEST(TestUnaryArithmetic, DispatchBest) {
  // All types (with _checked variant)
  for (std::string name : {"abs"}) {
    for (std::string suffix : {"", "_checked"}) {
      name += suffix;
      for (const auto& ty : {int8(), int16(), int32(), int64(), uint8(), uint16(),
                             uint32(), uint64(), float32(), float64()}) {
        CheckDispatchBest(name, {ty}, {ty});
        CheckDispatchBest(name, {dictionary(int8(), ty)}, {ty});
      }
    }
  }

  // All types
  for (std::string name : {"negate", "sign"}) {
    for (const auto& ty : {int8(), int16(), int32(), int64(), uint8(), uint16(), uint32(),
                           uint64(), float32(), float64()}) {
      CheckDispatchBest(name, {ty}, {ty});
      CheckDispatchBest(name, {dictionary(int8(), ty)}, {ty});
    }
  }

  // Signed types
  for (std::string name : {"negate_checked"}) {
    for (const auto& ty : {int8(), int16(), int32(), int64(), float32(), float64()}) {
      CheckDispatchBest(name, {ty}, {ty});
      CheckDispatchBest(name, {dictionary(int8(), ty)}, {ty});
    }
  }

  // Float types (with _checked variant)
  for (std::string name :
       {"ln", "log2", "log10", "log1p", "sin", "cos", "tan", "asin", "acos"}) {
    for (std::string suffix : {"", "_checked"}) {
      name += suffix;
      for (const auto& ty : {float32(), float64()}) {
        CheckDispatchBest(name, {ty}, {ty});
        CheckDispatchBest(name, {dictionary(int8(), ty)}, {ty});
      }
    }
  }

  // Float types
  for (std::string name : {"atan", "sign", "exp"}) {
    for (const auto& ty : {float32(), float64()}) {
      CheckDispatchBest(name, {ty}, {ty});
      CheckDispatchBest(name, {dictionary(int8(), ty)}, {ty});
    }
  }

  // Integer -> Float64 (with _checked variant)
  for (std::string name :
       {"ln", "log2", "log10", "log1p", "sin", "cos", "tan", "asin", "acos"}) {
    for (std::string suffix : {"", "_checked"}) {
      name += suffix;
      for (const auto& ty :
           {int8(), int16(), int32(), int64(), uint8(), uint16(), uint32(), uint64()}) {
        CheckDispatchBest(name, {ty}, {float64()});
        CheckDispatchBest(name, {dictionary(int8(), ty)}, {float64()});
      }
    }
  }

  // Integer -> Float64
  for (std::string name : {"atan"}) {
    for (const auto& ty :
         {int8(), int16(), int32(), int64(), uint8(), uint16(), uint32(), uint64()}) {
      CheckDispatchBest(name, {ty}, {float64()});
      CheckDispatchBest(name, {dictionary(int8(), ty)}, {float64()});
    }
  }
}

TEST(TestUnaryArithmetic, Null) {
  for (std::string name : {"abs", "acos", "asin", "cos", "ln", "log10", "log1p", "log2",
                           "negate", "sin", "tan"}) {
    for (std::string suffix : {"", "_checked"}) {
      name += suffix;
      AssertNullToNull(name);
    }
  }

  for (std::string name : {"atan", "bit_wise_not", "sign"}) {
    AssertNullToNull(name);
  }
}

TYPED_TEST(TestUnaryArithmeticSigned, Negate) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::min();
  auto max = std::numeric_limits<CType>::max();

  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    // Empty arrays
    this->AssertUnaryOp(Negate, "[]", "[]");
    // Array with nulls
    this->AssertUnaryOp(Negate, "[null]", "[null]");
    this->AssertUnaryOp(Negate, this->MakeNullScalar(), this->MakeNullScalar());
    this->AssertUnaryOp(Negate, "[1, null, -10]", "[-1, null, 10]");
    // Arrays with zeros
    this->AssertUnaryOp(Negate, "[0, 0, -0]", "[0, -0, 0]");
    this->AssertUnaryOp(Negate, 0, -0);
    this->AssertUnaryOp(Negate, -0, 0);
    this->AssertUnaryOp(Negate, 0, 0);
    // Ordinary arrays (positive inputs)
    this->AssertUnaryOp(Negate, "[1, 10, 127]", "[-1, -10, -127]");
    this->AssertUnaryOp(Negate, 1, -1);
    this->AssertUnaryOp(Negate, this->MakeScalar(1), this->MakeScalar(-1));
    // Ordinary arrays (negative inputs)
    this->AssertUnaryOp(Negate, "[-1, -10, -127]", "[1, 10, 127]");
    this->AssertUnaryOp(Negate, -1, 1);
    this->AssertUnaryOp(Negate, MakeArray(-1), "[1]");
    // Min/max (wrap arounds and overflow)
    this->AssertUnaryOp(Negate, max, min + 1);
    if (check_overflow) {
      this->AssertUnaryOpRaises(Negate, MakeArray(min), "overflow");
    } else {
      this->AssertUnaryOp(Negate, min, min);
    }
  }

  // Overflow should not be checked on underlying value slots when output would be null
  this->SetOverflowCheck(true);
  auto arg = ArrayFromJSON(this->type_singleton(), MakeArray(1, max, min));
  arg = TweakValidityBit(arg, 1, false);
  arg = TweakValidityBit(arg, 2, false);
  this->AssertUnaryOp(Negate, arg, "[-1, null, null]");
}

TYPED_TEST(TestUnaryArithmeticUnsigned, Negate) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::min();
  auto max = std::numeric_limits<CType>::max();

  // Empty arrays
  this->AssertUnaryOp(Negate, "[]", "[]");
  // Array with nulls
  this->AssertUnaryOp(Negate, "[null]", "[null]");
  this->AssertUnaryOp(Negate, this->MakeNullScalar(), this->MakeNullScalar());
  // Min/max (wrap around)
  this->AssertUnaryOp(Negate, min, min);
  this->AssertUnaryOp(Negate, max, 1);
  this->AssertUnaryOp(Negate, 1, max);
  // Not implemented kernels
  this->SetOverflowCheck(true);
  this->AssertUnaryOpNotImplemented(Negate, "[0]");
}

TYPED_TEST(TestUnaryArithmeticFloating, Negate) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    // Empty arrays
    this->AssertUnaryOp(Negate, "[]", "[]");
    // Array with nulls
    this->AssertUnaryOp(Negate, "[null]", "[null]");
    this->AssertUnaryOp(Negate, this->MakeNullScalar(), this->MakeNullScalar());
    this->AssertUnaryOp(Negate, "[1.3, null, -10.80]", "[-1.3, null, 10.80]");
    // Arrays with zeros
    this->AssertUnaryOp(Negate, "[0.0, 0.0, -0.0]", "[-0.0, -0.0, 0.0]");
    this->AssertUnaryOp(Negate, 0.0F, -0.0F);
    this->AssertUnaryOp(Negate, -0.0F, 0.0F);
    // Ordinary arrays (positive inputs)
    this->AssertUnaryOp(Negate, "[1.3, 10.80, 12748.001]", "[-1.3, -10.80, -12748.001]");
    this->AssertUnaryOp(Negate, 1.3F, -1.3F);
    this->AssertUnaryOp(Negate, this->MakeScalar(1.3F), this->MakeScalar(-1.3F));
    // Ordinary arrays (negative inputs)
    this->AssertUnaryOp(Negate, "[-1.3, -10.80, -12748.001]", "[1.3, 10.80, 12748.001]");
    this->AssertUnaryOp(Negate, -1.3F, 1.3F);
    this->AssertUnaryOp(Negate, MakeArray(-1.3F), "[1.3]");
    // Arrays with infinites
    this->AssertUnaryOp(Negate, "[Inf, -Inf]", "[-Inf, Inf]");
    // Arrays with NaNs
    this->SetNansEqual(true);
    this->AssertUnaryOp(Negate, "[NaN]", "[NaN]");
    this->AssertUnaryOp(Negate, "[NaN]", "[-NaN]");
    this->AssertUnaryOp(Negate, "[-NaN]", "[NaN]");
    // Min/max
    this->AssertUnaryOp(Negate, min, max);
    this->AssertUnaryOp(Negate, max, min);
  }
}

TYPED_TEST(TestUnaryArithmeticSigned, AbsoluteValue) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::min();
  auto max = std::numeric_limits<CType>::max();

  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    // Empty array
    this->AssertUnaryOp(AbsoluteValue, "[]", "[]");
    // Scalar/arrays with nulls
    this->AssertUnaryOp(AbsoluteValue, "[null]", "[null]");
    this->AssertUnaryOp(AbsoluteValue, "[1, null, -10]", "[1, null, 10]");
    this->AssertUnaryOp(AbsoluteValue, this->MakeNullScalar(), this->MakeNullScalar());
    // Scalar/arrays with zeros
    this->AssertUnaryOp(AbsoluteValue, "[0, -0]", "[0, 0]");
    this->AssertUnaryOp(AbsoluteValue, -0, 0);
    this->AssertUnaryOp(AbsoluteValue, 0, 0);
    // Ordinary scalar/arrays (positive inputs)
    this->AssertUnaryOp(AbsoluteValue, "[1, 10, 127]", "[1, 10, 127]");
    this->AssertUnaryOp(AbsoluteValue, 1, 1);
    this->AssertUnaryOp(AbsoluteValue, this->MakeScalar(1), this->MakeScalar(1));
    // Ordinary scalar/arrays (negative inputs)
    this->AssertUnaryOp(AbsoluteValue, "[-1, -10, -127]", "[1, 10, 127]");
    this->AssertUnaryOp(AbsoluteValue, -1, 1);
    this->AssertUnaryOp(AbsoluteValue, MakeArray(-1), "[1]");
    // Min/max
    this->AssertUnaryOp(AbsoluteValue, max, max);
    if (check_overflow) {
      this->AssertUnaryOpRaises(AbsoluteValue, MakeArray(min), "overflow");
    } else {
      this->AssertUnaryOp(AbsoluteValue, min, min);
    }
  }

  // Overflow should not be checked on underlying value slots when output would be null
  this->SetOverflowCheck(true);
  auto arg = ArrayFromJSON(this->type_singleton(), MakeArray(-1, max, min));
  arg = TweakValidityBit(arg, 1, false);
  arg = TweakValidityBit(arg, 2, false);
  this->AssertUnaryOp(AbsoluteValue, arg, "[1, null, null]");
}

TYPED_TEST(TestUnaryArithmeticUnsigned, AbsoluteValue) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::min();
  auto max = std::numeric_limits<CType>::max();

  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    // Empty arrays
    this->AssertUnaryOp(AbsoluteValue, "[]", "[]");
    // Array with nulls
    this->AssertUnaryOp(AbsoluteValue, "[null]", "[null]");
    this->AssertUnaryOp(AbsoluteValue, this->MakeNullScalar(), this->MakeNullScalar());
    // Ordinary arrays
    this->AssertUnaryOp(AbsoluteValue, "[0, 1, 10, 127]", "[0, 1, 10, 127]");
    // Min/max
    this->AssertUnaryOp(AbsoluteValue, min, min);
    this->AssertUnaryOp(AbsoluteValue, max, max);
  }
}

TYPED_TEST(TestUnaryArithmeticFloating, AbsoluteValue) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    // Empty array
    this->AssertUnaryOp(AbsoluteValue, "[]", "[]");
    // Scalar/arrays with nulls
    this->AssertUnaryOp(AbsoluteValue, "[null]", "[null]");
    this->AssertUnaryOp(AbsoluteValue, "[1.3, null, -10.80]", "[1.3, null, 10.80]");
    this->AssertUnaryOp(AbsoluteValue, this->MakeNullScalar(), this->MakeNullScalar());
    // Scalars/arrays with zeros
    this->AssertUnaryOp(AbsoluteValue, "[0.0, -0.0]", "[0.0, 0.0]");
    this->AssertUnaryOp(AbsoluteValue, -0.0F, 0.0F);
    this->AssertUnaryOp(AbsoluteValue, 0.0F, 0.0F);
    // Ordinary scalars/arrays (positive inputs)
    this->AssertUnaryOp(AbsoluteValue, "[1.3, 10.80, 12748.001]",
                        "[1.3, 10.80, 12748.001]");
    this->AssertUnaryOp(AbsoluteValue, 1.3F, 1.3F);
    this->AssertUnaryOp(AbsoluteValue, this->MakeScalar(1.3F), this->MakeScalar(1.3F));
    // Ordinary scalars/arrays (negative inputs)
    this->AssertUnaryOp(AbsoluteValue, "[-1.3, -10.80, -12748.001]",
                        "[1.3, 10.80, 12748.001]");
    this->AssertUnaryOp(AbsoluteValue, -1.3F, 1.3F);
    this->AssertUnaryOp(AbsoluteValue, MakeArray(-1.3F), "[1.3]");
    // Arrays with infinites
    this->AssertUnaryOp(AbsoluteValue, "[Inf, -Inf]", "[Inf, Inf]");
    // Arrays with NaNs
    this->SetNansEqual(true);
    this->AssertUnaryOp(AbsoluteValue, "[NaN]", "[NaN]");
    this->AssertUnaryOp(AbsoluteValue, "[-NaN]", "[NaN]");
    // Min/max
    this->AssertUnaryOp(AbsoluteValue, min, max);
    this->AssertUnaryOp(AbsoluteValue, max, max);
  }
}

class TestUnaryArithmeticDecimal : public TestArithmeticDecimal {};

TEST_F(TestUnaryArithmeticDecimal, AbsoluteValue) {
  auto max128 = Decimal128::GetMaxValue(38);
  auto max256 = Decimal256::GetMaxValue(76);
  for (const auto& func : {"abs", "abs_checked"}) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"));
      CheckScalar(func, {ArrayFromJSON(ty, R"(["1.00", "-42.15", null])")},
                  ArrayFromJSON(ty, R"(["1.00", "42.15", null])"));
    }
    CheckScalar(
        func,
        ScalarVector{std::make_shared<Decimal128Scalar>(-max128, decimal128(38, 0))},
        std::make_shared<Decimal128Scalar>(max128, decimal128(38, 0)));
    CheckScalar(
        func,
        ScalarVector{std::make_shared<Decimal256Scalar>(-max256, decimal256(76, 0))},
        std::make_shared<Decimal256Scalar>(max256, decimal256(76, 0)));
    for (const auto& ty : NegativeScaleTypes()) {
      CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"));
      CheckScalar(func, {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")},
                  DecimalArrayFromJSON(ty, R"(["12E2", "42E2", null])"));
    }
  }
}

TYPED_TEST(TestUnaryArithmeticUnsigned, Exp) {
  auto exp = [](const Datum& arg, ArithmeticOptions, ExecContext* ctx) {
    return Exp(arg, ctx);
  };
  // Empty arrays
  this->AssertUnaryOp(exp, "[]", ArrayFromJSON(float64(), "[]"));
  // Array with nulls
  this->AssertUnaryOp(exp, "[null]", ArrayFromJSON(float64(), "[null]"));
  this->AssertUnaryOp(exp, this->MakeNullScalar(), arrow::MakeNullScalar(float64()));
  this->AssertUnaryOp(
      exp, "[null, 1, 10]",
      ArrayFromJSON(float64(), "[null, 2.718281828459045, 22026.465794806718]"));
  this->AssertUnaryOp(exp, this->MakeScalar(1),
                      arrow::MakeScalar<double>(2.718281828459045F));
}

TYPED_TEST(TestUnaryArithmeticSigned, Exp) {
  auto exp = [](const Datum& arg, ArithmeticOptions, ExecContext* ctx) {
    return Exp(arg, ctx);
  };
  // Empty arrays
  this->AssertUnaryOp(exp, "[]", ArrayFromJSON(float64(), "[]"));
  // Array with nulls
  this->AssertUnaryOp(exp, "[null]", ArrayFromJSON(float64(), "[null]"));
  this->AssertUnaryOp(exp, this->MakeNullScalar(), arrow::MakeNullScalar(float64()));
  this->AssertUnaryOp(exp, "[-10, -1, null, 1, 10]",
                      ArrayFromJSON(float64(),
                                    "[0.000045399929762484854, 0.36787944117144233, "
                                    "null, 2.718281828459045, 22026.465794806718]"));
  this->AssertUnaryOp(exp, this->MakeScalar(1),
                      arrow::MakeScalar<double>(2.718281828459045F));
}

TYPED_TEST(TestUnaryArithmeticFloating, Exp) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  auto exp = [](const Datum& arg, ArithmeticOptions, ExecContext* ctx) {
    return Exp(arg, ctx);
  };
  // Empty arrays
  this->AssertUnaryOp(exp, "[]", "[]");
  // Array with nulls
  this->AssertUnaryOp(exp, "[null]", "[null]");
  this->AssertUnaryOp(exp, this->MakeNullScalar(), this->MakeNullScalar());
  this->AssertUnaryOp(exp, "[-1.0, null, 10.0]",
                      "[0.36787944117144233, null, 22026.465794806718]");
  // Ordinary arrays (positive, negative, fractional, and zero inputs)
  this->AssertUnaryOp(
      exp, "[-10.0, 0, 0.5, 1.0]",
      "[0.000045399929762484854,1.0,1.6487212707001282,2.718281828459045]");
  this->AssertUnaryOp(exp, 1.3F, 3.6692964926535487F);
  this->AssertUnaryOp(exp, this->MakeScalar(1.3F), this->MakeScalar(3.6692964926535487F));
  // Arrays with infinites
  this->AssertUnaryOp(exp, "[-Inf, Inf]", "[0, Inf]");
  // Arrays with NaNs
  this->SetNansEqual(true);
  this->AssertUnaryOp(exp, "[NaN]", "[NaN]");
  // Min/max
  this->AssertUnaryOp(exp, min, 0.0);
  this->AssertUnaryOp(exp, max, std::numeric_limits<CType>::infinity());
}

TEST_F(TestUnaryArithmeticDecimal, Exp) {
  auto max128 = Decimal128::GetMaxValue(38);
  auto max256 = Decimal256::GetMaxValue(76);
  const auto func = "exp";
  for (const auto& ty : PositiveScaleTypes()) {
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(float64(), "[]"));
    CheckScalar(
        func, {ArrayFromJSON(ty, R"(["-1.00", "10.00", null])")},
        ArrayFromJSON(float64(), "[0.36787944117144233, 22026.465794806718, null]"));
  }
  CheckScalar(func, {std::make_shared<Decimal128Scalar>(max128, decimal128(38, 0))},
              ScalarFromJSON(float64(), "Inf"));
  CheckScalar(func, {std::make_shared<Decimal128Scalar>(-max128, decimal128(38, 0))},
              ScalarFromJSON(float64(), "0"));
  CheckScalar(func, {std::make_shared<Decimal256Scalar>(max256, decimal256(76, 0))},
              ScalarFromJSON(float64(), "Inf"));
  CheckScalar(func, {std::make_shared<Decimal256Scalar>(-max256, decimal256(76, 0))},
              ScalarFromJSON(float64(), "0"));
  for (const auto& ty : NegativeScaleTypes()) {
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(float64(), "[]"));
    CheckScalar(func, {DecimalArrayFromJSON(ty, R"(["12E2", "0", "-42E2", null])")},
                ArrayFromJSON(float64(), "[Inf, 1.0, 0.0, null]"));
  }
}

TEST_F(TestUnaryArithmeticDecimal, Log) {
  std::vector<std::string> unchecked = {"ln", "log2", "log10", "log1p"};
  std::vector<std::string> checked = {"ln_checked", "log2_checked", "log10_checked",
                                      "log1p_checked"};
  std::vector<std::string> all = unchecked;
  all.insert(all.end(), checked.begin(), checked.end());

  for (const auto& func : all) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"(["0.01", "1.00", "4.42", null])")});
    }
    for (const auto& ty : NegativeScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func, {DecimalArrayFromJSON(ty, R"(["12E2", "42E2", null])")});
    }
  }

  for (const auto& func : unchecked) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"(["-2.00"])")});
    }
  }
  for (const auto& func : checked) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckRaises(func, {DecimalArrayFromJSON(ty, R"(["-2.00"])")},
                  "logarithm of negative number");
    }
  }
}

TEST_F(TestUnaryArithmeticDecimal, SquareRoot) {
  std::vector<std::string> funcs = {"sqrt", "sqrt_checked"};
  for (const auto& func : funcs) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckDecimalToFloat(func, {DecimalArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(
          func, {DecimalArrayFromJSON(ty, R"(["4.00", "16.00", "36.00", null])")});
      CheckRaises("sqrt_checked", {DecimalArrayFromJSON(ty, R"(["-2.00"])")},
                  "square root of negative number");
    }
    for (const auto& ty : NegativeScaleTypes()) {
      CheckDecimalToFloat(func, {DecimalArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func,
                          {DecimalArrayFromJSON(ty, R"(["400", "1600", "3600", null])")});
      CheckRaises("sqrt_checked", {DecimalArrayFromJSON(ty, R"(["-400"])")},
                  "square root of negative number");
    }
  }
}

TEST_F(TestUnaryArithmeticDecimal, Negate) {
  auto max128 = Decimal128::GetMaxValue(38);
  auto max256 = Decimal256::GetMaxValue(76);
  for (const auto& func : {"negate", "negate_checked"}) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"));
      CheckScalar(func, {ArrayFromJSON(ty, R"(["0.00", "1.00", "-42.15", null])")},
                  ArrayFromJSON(ty, R"(["0.00", "-1.00", "42.15", null])"));
    }
    CheckScalar(
        func,
        ScalarVector{std::make_shared<Decimal128Scalar>(-max128, decimal128(38, 0))},
        std::make_shared<Decimal128Scalar>(max128, decimal128(38, 0)));
    CheckScalar(
        func, ScalarVector{std::make_shared<Decimal128Scalar>(max128, decimal128(38, 0))},
        std::make_shared<Decimal128Scalar>(-max128, decimal128(38, 0)));
    CheckScalar(
        func,
        ScalarVector{std::make_shared<Decimal256Scalar>(-max256, decimal256(76, 0))},
        std::make_shared<Decimal256Scalar>(max256, decimal256(76, 0)));
    CheckScalar(
        func, ScalarVector{std::make_shared<Decimal256Scalar>(max256, decimal256(76, 0))},
        std::make_shared<Decimal256Scalar>(-max256, decimal256(76, 0)));
    for (const auto& ty : NegativeScaleTypes()) {
      CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"));
      CheckScalar(func, {DecimalArrayFromJSON(ty, R"(["0", "12E2", "-42E2", null])")},
                  DecimalArrayFromJSON(ty, R"(["0", "-12E2", "42E2", null])"));
    }
  }
}

TEST_F(TestUnaryArithmeticDecimal, Sign) {
  auto max128 = Decimal128::GetMaxValue(38);
  auto max256 = Decimal256::GetMaxValue(76);
  const auto func = "sign";
  for (const auto& ty : PositiveScaleTypes()) {
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(int64(), "[]"));
    CheckScalar(func, {ArrayFromJSON(ty, R"(["1.00", "0.00", "-42.15", null])")},
                ArrayFromJSON(int64(), "[1, 0, -1, null]"));
  }
  CheckScalar(func, {std::make_shared<Decimal128Scalar>(max128, decimal128(38, 0))},
              ScalarFromJSON(int64(), "1"));
  CheckScalar(func, {std::make_shared<Decimal128Scalar>(-max128, decimal128(38, 0))},
              ScalarFromJSON(int64(), "-1"));
  CheckScalar(func, {std::make_shared<Decimal256Scalar>(max256, decimal256(76, 0))},
              ScalarFromJSON(int64(), "1"));
  CheckScalar(func, {std::make_shared<Decimal256Scalar>(-max256, decimal256(76, 0))},
              ScalarFromJSON(int64(), "-1"));
  for (const auto& ty : NegativeScaleTypes()) {
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(int64(), "[]"));
    CheckScalar(func, {DecimalArrayFromJSON(ty, R"(["12E2", "0", "-42E2", null])")},
                ArrayFromJSON(int64(), "[1, 0, -1, null]"));
  }
}

TEST_F(TestUnaryArithmeticDecimal, TrigAcos) {
  for (const auto& func : {"acos", "acos_checked"}) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func,
                          {ArrayFromJSON(ty, R"(["0.00", "-1.00", "1.00", null])")});
    }
  }
  for (const auto& ty : NegativeScaleTypes()) {
    CheckDecimalToFloat("acos", {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")});
    CheckRaises("acos_checked", {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")},
                "domain error");
  }
}

TEST_F(TestUnaryArithmeticDecimal, TrigAsin) {
  for (const auto& func : {"asin", "asin_checked"}) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func,
                          {ArrayFromJSON(ty, R"(["0.00", "-1.00", "1.00", null])")});
    }
  }
  for (const auto& ty : NegativeScaleTypes()) {
    CheckDecimalToFloat("asin", {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")});
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("domain error"),
        CallFunction("asin_checked",
                     {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")}));
  }
}

TEST_F(TestUnaryArithmeticDecimal, TrigAtan) {
  const auto func = "atan";
  for (const auto& ty : PositiveScaleTypes()) {
    CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
    CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"(["0.00", "-1.00", "1.00", null])")});
  }
  for (const auto& ty : NegativeScaleTypes()) {
    CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
    CheckDecimalToFloat(func, {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")});
  }
}

TEST_F(TestUnaryArithmeticDecimal, TrigCos) {
  for (const auto& func : {"cos", "cos_checked"}) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func,
                          {ArrayFromJSON(ty, R"(["0.00", "-1.00", "1.00", null])")});
    }
    for (const auto& ty : NegativeScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func, {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")});
    }
  }
}

TEST_F(TestUnaryArithmeticDecimal, TrigSin) {
  for (const auto& func : {"sin", "sin_checked"}) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func,
                          {ArrayFromJSON(ty, R"(["0.00", "-1.00", "1.00", null])")});
    }
    for (const auto& ty : NegativeScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func, {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")});
    }
  }
}

TEST_F(TestUnaryArithmeticDecimal, TrigTan) {
  for (const auto& func : {"tan", "tan_checked"}) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func,
                          {ArrayFromJSON(ty, R"(["0.00", "-1.00", "1.00", null])")});
    }
    for (const auto& ty : NegativeScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func, {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")});
    }
  }
}

class TestBinaryArithmeticDecimal : public TestArithmeticDecimal {};

TEST_F(TestBinaryArithmeticDecimal, DispatchBest) {
  // decimal, floating point
  for (std::string name : {"add", "subtract", "multiply", "divide"}) {
    for (std::string suffix : {"", "_checked"}) {
      name += suffix;

      CheckDispatchBest(name, {decimal128(1, 0), float32()}, {float64(), float64()});
      CheckDispatchBest(name, {decimal256(1, 0), float64()}, {float64(), float64()});
      CheckDispatchBest(name, {float32(), decimal256(1, 0)}, {float64(), float64()});
      CheckDispatchBest(name, {float64(), decimal128(1, 0)}, {float64(), float64()});
    }
  }

  // decimal, decimal -> decimal
  // decimal, integer -> decimal
  for (std::string name : {"add", "subtract"}) {
    for (std::string suffix : {"", "_checked"}) {
      name += suffix;

      CheckDispatchBest(name, {int64(), decimal128(1, 0)},
                        {decimal128(19, 0), decimal128(1, 0)});
      CheckDispatchBest(name, {decimal128(1, 0), int64()},
                        {decimal128(1, 0), decimal128(19, 0)});

      CheckDispatchBest(name, {decimal128(2, 1), decimal128(2, 1)},
                        {decimal128(2, 1), decimal128(2, 1)});
      CheckDispatchBest(name, {decimal256(2, 1), decimal256(2, 1)},
                        {decimal256(2, 1), decimal256(2, 1)});
      CheckDispatchBest(name, {decimal128(2, 1), decimal256(2, 1)},
                        {decimal256(2, 1), decimal256(2, 1)});
      CheckDispatchBest(name, {decimal256(2, 1), decimal128(2, 1)},
                        {decimal256(2, 1), decimal256(2, 1)});

      CheckDispatchBest(name, {decimal128(2, 0), decimal128(2, 1)},
                        {decimal128(3, 1), decimal128(2, 1)});
      CheckDispatchBest(name, {decimal128(2, 1), decimal128(2, 0)},
                        {decimal128(2, 1), decimal128(3, 1)});
    }
  }
  {
    std::string name = "multiply";
    for (std::string suffix : {"", "_checked"}) {
      name += suffix;

      CheckDispatchBest(name, {int64(), decimal128(1, 0)},
                        {decimal128(19, 0), decimal128(1, 0)});
      CheckDispatchBest(name, {decimal128(1, 0), int64()},
                        {decimal128(1, 0), decimal128(19, 0)});

      CheckDispatchBest(name, {decimal128(2, 1), decimal128(2, 1)},
                        {decimal128(2, 1), decimal128(2, 1)});
      CheckDispatchBest(name, {decimal256(2, 1), decimal256(2, 1)},
                        {decimal256(2, 1), decimal256(2, 1)});
      CheckDispatchBest(name, {decimal128(2, 1), decimal256(2, 1)},
                        {decimal256(2, 1), decimal256(2, 1)});
      CheckDispatchBest(name, {decimal256(2, 1), decimal128(2, 1)},
                        {decimal256(2, 1), decimal256(2, 1)});

      CheckDispatchBest(name, {decimal128(2, 0), decimal128(2, 1)},
                        {decimal128(2, 0), decimal128(2, 1)});
      CheckDispatchBest(name, {decimal128(2, 1), decimal128(2, 0)},
                        {decimal128(2, 1), decimal128(2, 0)});
    }
  }
  {
    std::string name = "divide";
    for (std::string suffix : {"", "_checked"}) {
      name += suffix;
      SCOPED_TRACE(name);

      CheckDispatchBest(name, {int64(), decimal128(1, 0)},
                        {decimal128(23, 4), decimal128(1, 0)});
      CheckDispatchBest(name, {decimal128(1, 0), int64()},
                        {decimal128(21, 20), decimal128(19, 0)});

      CheckDispatchBest(name, {decimal128(2, 1), decimal128(2, 1)},
                        {decimal128(6, 5), decimal128(2, 1)});
      CheckDispatchBest(name, {decimal256(2, 1), decimal256(2, 1)},
                        {decimal256(6, 5), decimal256(2, 1)});
      CheckDispatchBest(name, {decimal128(2, 1), decimal256(2, 1)},
                        {decimal256(6, 5), decimal256(2, 1)});
      CheckDispatchBest(name, {decimal256(2, 1), decimal128(2, 1)},
                        {decimal256(6, 5), decimal256(2, 1)});

      CheckDispatchBest(name, {decimal128(2, 0), decimal128(2, 1)},
                        {decimal128(7, 5), decimal128(2, 1)});
      CheckDispatchBest(name, {decimal128(2, 1), decimal128(2, 0)},
                        {decimal128(5, 4), decimal128(2, 0)});
    }
  }
  for (std::string name : {"atan2", "logb", "logb_checked", "power", "power_checked"}) {
    CheckDispatchBest(name, {decimal128(2, 1), decimal128(2, 1)}, {float64(), float64()});
    CheckDispatchBest(name, {decimal256(2, 1), decimal256(2, 1)}, {float64(), float64()});
    CheckDispatchBest(name, {decimal128(2, 1), int64()}, {float64(), float64()});
    CheckDispatchBest(name, {int32(), decimal128(2, 1)}, {float64(), float64()});
    CheckDispatchBest(name, {decimal128(2, 1), float64()}, {float64(), float64()});
    CheckDispatchBest(name, {float32(), decimal128(2, 1)}, {float64(), float64()});
  }
}

// reference result from bc (precsion=100, scale=40)
TEST_F(TestBinaryArithmeticDecimal, AddSubtract) {
  // array array, decimal128
  {
    auto left = ArrayFromJSON(decimal128(30, 3),
                              R"([
        "1.000",
        "-123456789012345678901234567.890",
        "98765432109876543210.987",
        "-999999999999999999999999999.999"
      ])");
    auto right = ArrayFromJSON(decimal128(20, 9),
                               R"([
        "-1.000000000",
        "12345678901.234567890",
        "98765.432101234",
        "-99999999999.999999999"
      ])");
    auto added = ArrayFromJSON(decimal128(37, 9),
                               R"([
      "0.000000000",
      "-123456789012345666555555666.655432110",
      "98765432109876641976.419101234",
      "-1000000000000000099999999999.998999999"
    ])");
    auto subtracted = ArrayFromJSON(decimal128(37, 9),
                                    R"([
      "2.000000000",
      "-123456789012345691246913469.124567890",
      "98765432109876444445.554898766",
      "-999999999999999899999999999.999000001"
    ])");
    CheckScalarBinary("add", left, right, added);
    CheckScalarBinary("subtract", left, right, subtracted);
  }

  // array array, decimal256
  {
    auto left = ArrayFromJSON(decimal256(30, 20),
                              R"([
        "-1.00000000000000000001",
        "1234567890.12345678900000000000",
        "-9876543210.09876543210987654321",
        "9999999999.99999999999999999999"
      ])");
    auto right = ArrayFromJSON(decimal256(30, 10),
                               R"([
        "1.0000000000",
        "-1234567890.1234567890",
        "6789.5432101234",
        "99999999999999999999.9999999999"
      ])");
    auto added = ArrayFromJSON(decimal256(41, 20),
                               R"([
      "-0.00000000000000000001",
      "0.00000000000000000000",
      "-9876536420.55555530870987654321",
      "100000000009999999999.99999999989999999999"
    ])");
    auto subtracted = ArrayFromJSON(decimal256(41, 20),
                                    R"([
      "-2.00000000000000000001",
      "2469135780.24691357800000000000",
      "-9876549999.64197555550987654321",
      "-99999999989999999999.99999999990000000001"
    ])");
    CheckScalarBinary("add", left, right, added);
    CheckScalarBinary("subtract", left, right, subtracted);
  }

  // scalar array
  {
    auto left = ScalarFromJSON(decimal128(6, 1), R"("12345.6")");
    auto right = ArrayFromJSON(decimal128(10, 3),
                               R"(["1.234", "1234.000", "-9876.543", "666.888"])");
    auto added = ArrayFromJSON(decimal128(11, 3),
                               R"(["12346.834", "13579.600", "2469.057", "13012.488"])");
    auto left_sub_right = ArrayFromJSON(
        decimal128(11, 3), R"(["12344.366", "11111.600", "22222.143", "11678.712"])");
    auto right_sub_left = ArrayFromJSON(
        decimal128(11, 3), R"(["-12344.366", "-11111.600", "-22222.143", "-11678.712"])");
    CheckScalarBinary("add", left, right, added);
    CheckScalarBinary("add", right, left, added);
    CheckScalarBinary("subtract", left, right, left_sub_right);
    CheckScalarBinary("subtract", right, left, right_sub_left);
  }

  // scalar scalar
  {
    auto left = ScalarFromJSON(decimal256(3, 0), R"("666")");
    auto right = ScalarFromJSON(decimal256(3, 0), R"("888")");
    auto added = ScalarFromJSON(decimal256(4, 0), R"("1554")");
    auto subtracted = ScalarFromJSON(decimal256(4, 0), R"("-222")");
    CheckScalarBinary("add", left, right, added);
    CheckScalarBinary("subtract", left, right, subtracted);
  }

  // decimal128 decimal256
  {
    auto left = ScalarFromJSON(decimal128(3, 0), R"("666")");
    auto right = ScalarFromJSON(decimal256(3, 0), R"("888")");
    auto added = ScalarFromJSON(decimal256(4, 0), R"("1554")");
    CheckScalarBinary("add", left, right, added);
    CheckScalarBinary("add", right, left, added);
  }

  // decimal float
  {
    auto left = ScalarFromJSON(decimal128(3, 0), R"("666")");
    ASSIGN_OR_ABORT(auto right, arrow::MakeScalar(float64(), 888));
    ASSIGN_OR_ABORT(auto added, arrow::MakeScalar(float64(), 1554));
    CheckScalarBinary("add", left, right, added);
    CheckScalarBinary("add", right, left, added);
  }

  // decimal integer
  {
    auto left = ScalarFromJSON(decimal128(3, 0), R"("666")");
    auto right = ScalarFromJSON(int64(), "888");
    CheckScalarBinary("add", left, right, ScalarFromJSON(decimal128(20, 0), R"("1554")"));
    CheckScalarBinary("subtract", left, right,
                      ScalarFromJSON(decimal128(20, 0), R"("-222")"));
  }

  // failed case: result maybe overflow
  {
    std::shared_ptr<Scalar> left, right;

    left = ScalarFromJSON(decimal128(21, 20), R"("0.12345678901234567890")");
    right = ScalarFromJSON(decimal128(21, 1), R"("1.0")");
    ASSERT_RAISES(Invalid, CallFunction("add", {left, right}));
    ASSERT_RAISES(Invalid, CallFunction("subtract", {left, right}));

    left = ScalarFromJSON(decimal256(75, 0), R"("0")");
    right = ScalarFromJSON(decimal256(2, 1), R"("0.0")");
    ASSERT_RAISES(Invalid, CallFunction("add", {left, right}));
    ASSERT_RAISES(Invalid, CallFunction("subtract", {left, right}));
  }
}

TEST_F(TestBinaryArithmeticDecimal, Multiply) {
  // array array, decimal128
  {
    auto left = ArrayFromJSON(decimal128(20, 10),
                              R"([
        "1234567890.1234567890",
        "-0.0000000001",
        "-9999999999.9999999999"
      ])");
    auto right = ArrayFromJSON(decimal128(13, 3),
                               R"([
        "1234567890.123",
        "0.001",
        "-9999999999.999"
      ])");
    auto expected = ArrayFromJSON(decimal128(34, 13),
                                  R"([
      "1524157875323319737.9870903950470",
      "-0.0000000000001",
      "99999999999989999999.0000000000001"
    ])");
    CheckScalarBinary("multiply", left, right, expected);
  }

  // array array, decimal256
  {
    auto left = ArrayFromJSON(decimal256(30, 3),
                              R"([
        "123456789012345678901234567.890",
        "0.000"
      ])");
    auto right = ArrayFromJSON(decimal256(20, 9),
                               R"([
        "-12345678901.234567890",
        "99999999999.999999999"
      ])");
    auto expected = ArrayFromJSON(decimal256(51, 12),
                                  R"([
      "-1524157875323883675034293577501905199.875019052100",
      "0.000000000000"
    ])");
    CheckScalarBinary("multiply", left, right, expected);
  }

  // scalar array
  {
    auto left = ScalarFromJSON(decimal128(3, 2), R"("3.14")");
    auto right = ArrayFromJSON(decimal128(1, 0), R"(["1", "2", "3", "4", "5"])");
    auto expected =
        ArrayFromJSON(decimal128(5, 2), R"(["3.14", "6.28", "9.42", "12.56", "15.70"])");
    CheckScalarBinary("multiply", left, right, expected);
    CheckScalarBinary("multiply", right, left, expected);
  }

  // scalar scalar
  {
    auto left = ScalarFromJSON(decimal128(1, 0), R"("1")");
    auto right = ScalarFromJSON(decimal128(1, 0), R"("1")");
    auto expected = ScalarFromJSON(decimal128(3, 0), R"("1")");
    CheckScalarBinary("multiply", left, right, expected);
  }

  // decimal128 decimal256
  {
    auto left = ScalarFromJSON(decimal128(3, 2), R"("6.66")");
    auto right = ScalarFromJSON(decimal256(3, 1), R"("88.8")");
    auto expected = ScalarFromJSON(decimal256(7, 3), R"("591.408")");
    CheckScalarBinary("multiply", left, right, expected);
    CheckScalarBinary("multiply", right, left, expected);
  }

  // decimal float
  {
    auto left = ScalarFromJSON(decimal128(3, 0), R"("666")");
    ASSIGN_OR_ABORT(auto right, arrow::MakeScalar(float64(), 888));
    ASSIGN_OR_ABORT(auto expected, arrow::MakeScalar(float64(), 591408));
    CheckScalarBinary("multiply", left, right, expected);
    CheckScalarBinary("multiply", right, left, expected);
  }

  // decimal integer
  {
    auto left = ScalarFromJSON(decimal128(3, 0), R"("666")");
    auto right = ScalarFromJSON(int64(), "888");
    auto expected = ScalarFromJSON(decimal128(23, 0), R"("591408")");
    CheckScalarBinary("multiply", left, right, expected);
  }

  // failed case: result maybe overflow
  {
    auto left = ScalarFromJSON(decimal128(20, 0), R"("1")");
    auto right = ScalarFromJSON(decimal128(18, 1), R"("1.0")");
    ASSERT_RAISES(Invalid, CallFunction("multiply", {left, right}));
  }
}

TEST_F(TestBinaryArithmeticDecimal, Divide) {
  // array array, decimal128
  {
    auto left = ArrayFromJSON(decimal128(13, 3), R"(["1234567890.123", "0.001"])");
    auto right = ArrayFromJSON(decimal128(3, 0), R"(["-987", "999"])");
    auto expected =
        ArrayFromJSON(decimal128(17, 7), R"(["-1250828.6627386", "0.0000010"])");
    CheckScalarBinary("divide", left, right, expected);
  }

  // array array, decimal256
  {
    auto left = ArrayFromJSON(decimal256(20, 10),
                              R"(["1234567890.1234567890", "9999999999.9999999999"])");
    auto right = ArrayFromJSON(decimal256(13, 3), R"(["1234567890.123", "0.001"])");
    auto expected = ArrayFromJSON(
        decimal256(34, 21),
        R"(["1.000000000000369999093", "9999999999999.999999900000000000000"])");
    CheckScalarBinary("divide", left, right, expected);
  }

  // scalar array
  {
    auto left = ScalarFromJSON(decimal128(1, 0), R"("1")");
    auto right = ArrayFromJSON(decimal128(1, 0), R"(["1", "2", "3", "4"])");
    auto left_div_right =
        ArrayFromJSON(decimal128(5, 4), R"(["1.0000", "0.5000", "0.3333", "0.2500"])");
    auto right_div_left =
        ArrayFromJSON(decimal128(5, 4), R"(["1.0000", "2.0000", "3.0000", "4.0000"])");
    CheckScalarBinary("divide", left, right, left_div_right);
    CheckScalarBinary("divide", right, left, right_div_left);
  }

  // scalar scalar
  {
    auto left = ScalarFromJSON(decimal256(6, 5), R"("2.71828")");
    auto right = ScalarFromJSON(decimal256(6, 5), R"("3.14159")");
    auto expected = ScalarFromJSON(decimal256(13, 7), R"("0.8652561")");
    CheckScalarBinary("divide", left, right, expected);
  }

  // decimal128 decimal256
  {
    auto left = ScalarFromJSON(decimal256(6, 5), R"("2.71828")");
    auto right = ScalarFromJSON(decimal128(6, 5), R"("3.14159")");
    auto left_div_right = ScalarFromJSON(decimal256(13, 7), R"("0.8652561")");
    auto right_div_left = ScalarFromJSON(decimal256(13, 7), R"("1.1557271")");
    CheckScalarBinary("divide", left, right, left_div_right);
    CheckScalarBinary("divide", right, left, right_div_left);
  }

  // decimal float
  {
    auto left = ScalarFromJSON(decimal128(3, 0), R"("100")");
    ASSIGN_OR_ABORT(auto right, arrow::MakeScalar(float64(), 50));
    ASSIGN_OR_ABORT(auto left_div_right, arrow::MakeScalar(float64(), 2));
    ASSIGN_OR_ABORT(auto right_div_left, arrow::MakeScalar(float64(), 0.5));
    CheckScalarBinary("divide", left, right, left_div_right);
    CheckScalarBinary("divide", right, left, right_div_left);
  }

  // decimal integer
  {
    auto left = ScalarFromJSON(decimal128(3, 0), R"("100")");
    auto right = ScalarFromJSON(int64(), "50");
    auto left_div_right =
        ScalarFromJSON(decimal128(23, 20), R"("2.00000000000000000000")");
    auto right_div_left = ScalarFromJSON(decimal128(23, 4), R"("0.5000")");
    CheckScalarBinary("divide", left, right, left_div_right);
    CheckScalarBinary("divide", right, left, right_div_left);
  }

  // failed case: result maybe overflow
  {
    auto left = ScalarFromJSON(decimal128(20, 20), R"("0.12345678901234567890")");
    auto right = ScalarFromJSON(decimal128(20, 0), R"("12345678901234567890")");
    ASSERT_RAISES(Invalid, CallFunction("divide", {left, right}));
  }

  // failed case: divide by 0
  {
    auto left = ScalarFromJSON(decimal256(1, 0), R"("1")");
    auto right = ScalarFromJSON(decimal256(1, 0), R"("0")");
    ASSERT_RAISES(Invalid, CallFunction("divide", {left, right}));
  }
}

TEST_F(TestBinaryArithmeticDecimal, Atan2) {
  // Decimal arguments promoted to double, sanity check here
  const auto func = "atan2";
  for (const auto& ty : PositiveScaleTypes()) {
    CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])"), ArrayFromJSON(ty, R"([])")});
    CheckDecimalToFloat(
        func, {ArrayFromJSON(ty, R"(["1.00", "10.00", "1.00", "2.00", null])"),
               ArrayFromJSON(ty, R"(["10.00", "10.00", "2.00", "2.00", null])")});
    CheckDecimalToFloat(
        func,
        {ArrayFromJSON(ty, R"(["1.00", "10.00", "1.00", "2.00", null])"),
         ArrayFromJSON(decimal128(4, 2), R"(["10.00", "10.00", "2.00", "2.00", null])")});
    CheckDecimalToFloat(func,
                        {ArrayFromJSON(ty, R"(["1.00", "10.00", "1.00", "2.00", null])"),
                         ScalarFromJSON(int64(), "10")});
    CheckDecimalToFloat(func,
                        {ArrayFromJSON(ty, R"(["1.00", "10.00", "1.00", "2.00", null])"),
                         ScalarFromJSON(float64(), "10")});
    CheckDecimalToFloat(func, {ArrayFromJSON(float64(), "[1, 10, 1, 2, null]"),
                               ScalarFromJSON(ty, R"("10.00")")});
    CheckDecimalToFloat(func, {ArrayFromJSON(int64(), "[1, 10, 1, 2, null]"),
                               ScalarFromJSON(ty, R"("10.00")")});
  }
  for (const auto& ty : NegativeScaleTypes()) {
    CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])"), ArrayFromJSON(ty, R"([])")});
    CheckDecimalToFloat(func, {DecimalArrayFromJSON(ty, R"(["12E2", "42E2", null])"),
                               DecimalArrayFromJSON(ty, R"(["12E2", "42E2", null])")});
    CheckDecimalToFloat(
        func, {DecimalArrayFromJSON(ty, R"(["12E2", "42E2", null])"),
               DecimalArrayFromJSON(decimal128(2, -2), R"(["12E2", "42E2", null])")});
    CheckDecimalToFloat(func, {DecimalArrayFromJSON(ty, R"(["12E2", "42E2", null])"),
                               ScalarFromJSON(int64(), "10")});
  }
}

TEST_F(TestBinaryArithmeticDecimal, Logb) {
  // Decimal arguments promoted to double, sanity check here
  for (const auto& func : {"logb", "logb_checked"}) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])"), ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(
          func, {ArrayFromJSON(ty, R"(["1.00", "10.00", "1.00", "2.00", null])"),
                 ArrayFromJSON(ty, R"(["10.00", "10.00", "2.00", "2.00", null])")});
      CheckDecimalToFloat(
          func, {ArrayFromJSON(ty, R"(["1.00", "10.00", "1.00", "2.00", null])"),
                 ArrayFromJSON(decimal128(4, 2),
                               R"(["10.00", "10.00", "2.00", "2.00", null])")});
      CheckDecimalToFloat(
          func, {ArrayFromJSON(ty, R"(["1.00", "10.00", "1.00", "2.00", null])"),
                 ScalarFromJSON(int64(), "10")});
      CheckDecimalToFloat(
          func, {ArrayFromJSON(ty, R"(["1.00", "10.00", "1.00", "2.00", null])"),
                 ScalarFromJSON(float64(), "10")});
      CheckDecimalToFloat(func, {ArrayFromJSON(float64(), "[1, 10, 1, 2, null]"),
                                 ScalarFromJSON(ty, R"("10.00")")});
      CheckDecimalToFloat(func, {ArrayFromJSON(int64(), "[1, 10, 1, 2, null]"),
                                 ScalarFromJSON(ty, R"("10.00")")});
    }
    for (const auto& ty : NegativeScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])"), ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func, {DecimalArrayFromJSON(ty, R"(["12E2", "42E2", null])"),
                                 DecimalArrayFromJSON(ty, R"(["12E2", "42E2", null])")});
      CheckDecimalToFloat(
          func, {DecimalArrayFromJSON(ty, R"(["12E2", "42E2", null])"),
                 DecimalArrayFromJSON(decimal128(2, -2), R"(["12E2", "42E2", null])")});
      CheckDecimalToFloat(func, {DecimalArrayFromJSON(ty, R"(["12E2", "42E2", null])"),
                                 ScalarFromJSON(int64(), "10")});
    }
  }
}

TEST_F(TestBinaryArithmeticDecimal, Power) {
  // Decimal arguments promoted to double, sanity check here
  for (const auto& func : {"logb", "logb_checked"}) {
    for (const auto& ty : PositiveScaleTypes()) {
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"([])"), ArrayFromJSON(ty, R"([])")});
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"(["1.00", "2.00", null])"),
                                 ArrayFromJSON(ty, R"(["1.23", null, "3.45"])")});
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"(["1.00", "2.00", null])"),
                                 ArrayFromJSON(float64(), R"([1.23, null, 3.45])")});
      CheckDecimalToFloat(func, {ArrayFromJSON(float64(), R"([1.00, 2.00, null])"),
                                 ArrayFromJSON(ty, R"(["1.23", null, "3.45"])")});
      CheckDecimalToFloat(func, {ArrayFromJSON(ty, R"(["1.00", "2.00", null])"),
                                 ArrayFromJSON(int64(), R"([1, null, 3])")});
      CheckDecimalToFloat(func, {ArrayFromJSON(int64(), R"([1, 2, null])"),
                                 ArrayFromJSON(ty, R"(["1.23", null, "3.45"])")});
    }
  }
}

TYPED_TEST(TestBinaryArithmeticIntegral, ShiftLeft) {
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);

    this->AssertBinop(ShiftLeft, "[]", "[]", "[]");
    this->AssertBinop(ShiftLeft, "[0, 1, 2, 3]", "[2, 3, 4, 5]", "[0, 8, 32, 96]");
    // Nulls on one side
    this->AssertBinop(ShiftLeft, "[0, null, 2, 3]", "[2, 3, 4, 5]", "[0, null, 32, 96]");
    this->AssertBinop(ShiftLeft, "[0, 1, 2, 3]", "[2, 3, null, 5]", "[0, 8, null, 96]");
    // Nulls on both sides
    this->AssertBinop(ShiftLeft, "[0, null, 2, 3]", "[2, 3, null, 5]",
                      "[0, null, null, 96]");
    // All nulls
    this->AssertBinop(ShiftLeft, "[null]", "[null]", "[null]");

    // Scalar on the left
    this->AssertBinop(ShiftLeft, 2, "[null, 5]", "[null, 64]");
    this->AssertBinop(ShiftLeft, this->MakeNullScalar(), "[null, 5]", "[null, null]");
    // Scalar on the right
    this->AssertBinop(ShiftLeft, "[null, 5]", 3, "[null, 40]");
    this->AssertBinop(ShiftLeft, "[null, 5]", this->MakeNullScalar(), "[null, null]");
  }
}

TYPED_TEST(TestBinaryArithmeticIntegral, ShiftRight) {
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);

    this->AssertBinop(ShiftRight, "[]", "[]", "[]");
    this->AssertBinop(ShiftRight, "[0, 1, 4, 8]", "[1, 1, 1, 4]", "[0, 0, 2, 0]");
    // Nulls on one side
    this->AssertBinop(ShiftRight, "[0, null, 4, 8]", "[1, 1, 1, 4]", "[0, null, 2, 0]");
    this->AssertBinop(ShiftRight, "[0, 1, 4, 8]", "[1, 1, null, 4]", "[0, 0, null, 0]");
    // Nulls on both sides
    this->AssertBinop(ShiftRight, "[0, null, 4, 8]", "[1, 1, null, 4]",
                      "[0, null, null, 0]");
    // All nulls
    this->AssertBinop(ShiftRight, "[null]", "[null]", "[null]");

    // Scalar on the left
    this->AssertBinop(ShiftRight, 64, "[null, 2, 6]", "[null, 16, 1]");
    this->AssertBinop(ShiftRight, this->MakeNullScalar(), "[null, 2, 6]",
                      "[null, null, null]");
    // Scalar on the right
    this->AssertBinop(ShiftRight, "[null, 3, 96]", 3, "[null, 0, 12]");
    this->AssertBinop(ShiftRight, "[null, 3, 96]", this->MakeNullScalar(),
                      "[null, null, null]");
  }
}

TYPED_TEST(TestBinaryArithmeticSigned, ShiftLeftOverflowRaises) {
  using CType = typename TestFixture::CType;
  const CType bit_width = static_cast<CType>(std::numeric_limits<CType>::digits);
  const CType min = std::numeric_limits<CType>::min();
  this->SetOverflowCheck(true);

  this->AssertBinop(ShiftLeft, "[1]", MakeArray(bit_width - 1),
                    MakeArray(static_cast<CType>(1) << (bit_width - 1)));
  this->AssertBinop(ShiftLeft, "[2]", MakeArray(bit_width - 2),
                    MakeArray(static_cast<CType>(1) << (bit_width - 1)));
  // Shift a bit into the sign bit
  this->AssertBinop(ShiftLeft, "[2]", MakeArray(bit_width - 1), MakeArray(min));
  // Shift a bit past the sign bit
  this->AssertBinop(ShiftLeft, "[4]", MakeArray(bit_width - 1), "[0]");
  this->AssertBinop(ShiftLeft, MakeArray(min), "[1]", "[0]");
  this->AssertBinopRaises(ShiftLeft, "[1, 2]", "[1, -1]",
                          "shift amount must be >= 0 and less than precision of type");
  this->AssertBinopRaises(ShiftLeft, "[1]", MakeArray(bit_width),
                          "shift amount must be >= 0 and less than precision of type");

  this->SetOverflowCheck(false);
  this->AssertBinop(ShiftLeft, "[1, 1]", MakeArray(-1, bit_width), "[1, 1]");
}

TYPED_TEST(TestBinaryArithmeticSigned, ShiftRightOverflowRaises) {
  using CType = typename TestFixture::CType;
  const CType bit_width = static_cast<CType>(std::numeric_limits<CType>::digits);
  const CType max = std::numeric_limits<CType>::max();
  const CType min = std::numeric_limits<CType>::min();
  this->SetOverflowCheck(true);

  this->AssertBinop(ShiftRight, MakeArray(max), MakeArray(bit_width - 1), "[1]");
  this->AssertBinop(ShiftRight, "[-1, -1]", "[1, 5]", "[-1, -1]");
  this->AssertBinop(ShiftRight, MakeArray(min), "[1]", MakeArray(min / 2));
  this->AssertBinopRaises(ShiftRight, "[1, 2]", "[1, -1]",
                          "shift amount must be >= 0 and less than precision of type");
  this->AssertBinopRaises(ShiftRight, "[1]", MakeArray(bit_width),
                          "shift amount must be >= 0 and less than precision of type");

  this->SetOverflowCheck(false);
  this->AssertBinop(ShiftRight, "[1, 1]", MakeArray(-1, bit_width), "[1, 1]");
}

TYPED_TEST(TestBinaryArithmeticUnsigned, ShiftLeftOverflowRaises) {
  using CType = typename TestFixture::CType;
  const CType bit_width = static_cast<CType>(std::numeric_limits<CType>::digits);
  this->SetOverflowCheck(true);

  this->AssertBinop(ShiftLeft, "[1]", MakeArray(bit_width - 1),
                    MakeArray(static_cast<CType>(1) << (bit_width - 1)));
  this->AssertBinop(ShiftLeft, "[2]", MakeArray(bit_width - 2),
                    MakeArray(static_cast<CType>(1) << (bit_width - 1)));
  this->AssertBinop(ShiftLeft, "[2]", MakeArray(bit_width - 1), "[0]");
  this->AssertBinop(ShiftLeft, "[4]", MakeArray(bit_width - 1), "[0]");
  this->AssertBinopRaises(ShiftLeft, "[1]", MakeArray(bit_width),
                          "shift amount must be >= 0 and less than precision of type");
}

TYPED_TEST(TestBinaryArithmeticUnsigned, ShiftRightOverflowRaises) {
  using CType = typename TestFixture::CType;
  const CType bit_width = static_cast<CType>(std::numeric_limits<CType>::digits);
  const CType max = std::numeric_limits<CType>::max();
  this->SetOverflowCheck(true);

  this->AssertBinop(ShiftRight, MakeArray(max), MakeArray(bit_width - 1), "[1]");
  this->AssertBinopRaises(ShiftRight, "[1]", MakeArray(bit_width),
                          "shift amount must be >= 0 and less than precision of type");
}

TYPED_TEST(TestUnaryArithmeticFloating, TrigSin) {
  this->SetNansEqual(true);
  this->AssertUnaryOp(Sin, "[Inf, -Inf]", "[NaN, NaN]");
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    this->AssertUnaryOp(Sin, "[]", "[]");
    this->AssertUnaryOp(Sin, "[null, NaN]", "[null, NaN]");
    this->AssertUnaryOp(Sin, MakeArray(0, M_PI_2, M_PI), "[0, 1, 0]");
  }
  this->AssertUnaryOpRaises(Sin, "[Inf, -Inf]", "domain error");
}

TYPED_TEST(TestUnaryArithmeticFloating, TrigCos) {
  this->SetNansEqual(true);
  this->AssertUnaryOp(Cos, "[Inf, -Inf]", "[NaN, NaN]");
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    this->AssertUnaryOp(Cos, "[]", "[]");
    this->AssertUnaryOp(Cos, "[null, NaN]", "[null, NaN]");
    this->AssertUnaryOp(Cos, MakeArray(0, M_PI_2, M_PI), "[1, 0, -1]");
  }
  this->AssertUnaryOpRaises(Cos, "[Inf, -Inf]", "domain error");
}

TYPED_TEST(TestUnaryArithmeticFloating, TrigTan) {
  this->SetNansEqual(true);
  this->AssertUnaryOp(Tan, "[Inf, -Inf]", "[NaN, NaN]");
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    this->AssertUnaryOp(Tan, "[]", "[]");
    this->AssertUnaryOp(Tan, "[null, NaN]", "[null, NaN]");
    // N.B. pi/2 isn't representable exactly -> there are no poles
    // (i.e. tan(pi/2) is merely a large value and not +Inf)
    this->AssertUnaryOp(Tan, MakeArray(0, M_PI), "[0, 0]");
  }
  this->AssertUnaryOpRaises(Tan, "[Inf, -Inf]", "domain error");
}

TYPED_TEST(TestUnaryArithmeticFloating, TrigAsin) {
  this->SetNansEqual(true);
  this->AssertUnaryOp(Asin, "[Inf, -Inf, -2, 2]", "[NaN, NaN, NaN, NaN]");
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    this->AssertUnaryOp(Asin, "[]", "[]");
    this->AssertUnaryOp(Asin, "[null, NaN]", "[null, NaN]");
    this->AssertUnaryOp(Asin, "[0, 1, -1]", MakeArray(0, M_PI_2, -M_PI_2));
  }
  this->AssertUnaryOpRaises(Asin, "[Inf, -Inf, -2, 2]", "domain error");
}

TYPED_TEST(TestUnaryArithmeticFloating, TrigAcos) {
  this->SetNansEqual(true);
  this->AssertUnaryOp(Asin, "[Inf, -Inf, -2, 2]", "[NaN, NaN, NaN, NaN]");
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    this->AssertUnaryOp(Acos, "[]", "[]");
    this->AssertUnaryOp(Acos, "[null, NaN]", "[null, NaN]");
    this->AssertUnaryOp(Acos, "[0, 1, -1]", MakeArray(M_PI_2, 0, M_PI));
  }
  this->AssertUnaryOpRaises(Acos, "[Inf, -Inf, -2, 2]", "domain error");
}

TYPED_TEST(TestUnaryArithmeticFloating, TrigAtan) {
  this->SetNansEqual(true);
  auto atan = [](const Datum& arg, ArithmeticOptions, ExecContext* ctx) {
    return Atan(arg, ctx);
  };
  this->AssertUnaryOp(atan, "[]", "[]");
  this->AssertUnaryOp(atan, "[null, NaN]", "[null, NaN]");
  this->AssertUnaryOp(atan, "[0, 1, -1, Inf, -Inf]",
                      MakeArray(0, M_PI_4, -M_PI_4, M_PI_2, -M_PI_2));
}

TYPED_TEST(TestBinaryArithmeticFloating, TrigAtan2) {
  this->SetNansEqual(true);
  auto atan2 = [](const Datum& y, const Datum& x, ArithmeticOptions, ExecContext* ctx) {
    return Atan2(y, x, ctx);
  };
  this->AssertBinop(atan2, "[]", "[]", "[]");
  this->AssertBinop(atan2, "[0, 0, null, NaN]", "[null, NaN, 0, 0]",
                    "[null, NaN, null, NaN]");
  this->AssertBinop(atan2, "[0, 0, -0.0, 0, -0.0, 0, 1, 0, -1, Inf, -Inf, 0, 0]",
                    "[0, 0, 0, -0.0, -0.0, 1, 0, -1, 0, 0, 0, Inf, -Inf]",
                    MakeArray(0, 0, -0.0, M_PI, -M_PI, 0, M_PI_2, M_PI, -M_PI_2, M_PI_2,
                              -M_PI_2, 0, M_PI));
}

TYPED_TEST(TestUnaryArithmeticIntegral, Trig) {
  // Integer arguments promoted to double, sanity check here
  auto atan = [](const Datum& arg, ArithmeticOptions, ExecContext* ctx) {
    return Atan(arg, ctx);
  };
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    this->AssertUnaryOp(Sin, "[0, 1]",
                        ArrayFromJSON(float64(), "[0, 0.8414709848078965]"));
    this->AssertUnaryOp(Cos, "[0, 1]",
                        ArrayFromJSON(float64(), "[1, 0.5403023058681398]"));
    this->AssertUnaryOp(Tan, "[0, 1]",
                        ArrayFromJSON(float64(), "[0, 1.5574077246549023]"));
    this->AssertUnaryOp(Asin, "[0, 1]", ArrayFromJSON(float64(), MakeArray(0, M_PI_2)));
    this->AssertUnaryOp(Acos, "[0, 1]", ArrayFromJSON(float64(), MakeArray(M_PI_2, 0)));
    this->AssertUnaryOp(atan, "[0, 1]", ArrayFromJSON(float64(), MakeArray(0, M_PI_4)));
  }
}

TYPED_TEST(TestBinaryArithmeticIntegral, Trig) {
  // Integer arguments promoted to double, sanity check here
  auto ty = this->type_singleton();
  auto atan2 = [](const Datum& y, const Datum& x, ArithmeticOptions, ExecContext* ctx) {
    return Atan2(y, x, ctx);
  };
  this->AssertBinop(atan2, ArrayFromJSON(ty, "[0, 1]"), ArrayFromJSON(ty, "[1, 0]"),
                    ArrayFromJSON(float64(), MakeArray(0, M_PI_2)));
}

TYPED_TEST(TestUnaryArithmeticFloating, Log) {
  using CType = typename TestFixture::CType;
  this->SetNansEqual(true);
  auto min_val = std::numeric_limits<CType>::min();
  auto max_val = std::numeric_limits<CType>::max();
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    this->AssertUnaryOp(Ln, "[1, 2.718281828459045, null, NaN, Inf]",
                        "[0, 1, null, NaN, Inf]");
    // N.B. min() for float types is smallest normal number > 0
    this->AssertUnaryOp(Ln, min_val, std::log(min_val));
    this->AssertUnaryOp(Ln, max_val, std::log(max_val));
    this->AssertUnaryOp(Log10, "[1, 10, null, NaN, Inf]", "[0, 1, null, NaN, Inf]");
    this->AssertUnaryOp(Log10, min_val, std::log10(min_val));
    this->AssertUnaryOp(Log10, max_val, std::log10(max_val));
    this->AssertUnaryOp(Log2, "[1, 2, null, NaN, Inf]", "[0, 1, null, NaN, Inf]");
    this->AssertUnaryOp(Log2, min_val, std::log2(min_val));
    this->AssertUnaryOp(Log2, max_val, std::log2(max_val));
    this->AssertUnaryOp(Log1p, "[0, 1.718281828459045, null, NaN, Inf]",
                        "[0, 1, null, NaN, Inf]");
    this->AssertUnaryOp(Log1p, min_val, std::log1p(min_val));
    this->AssertUnaryOp(Log1p, max_val, std::log1p(max_val));
  }
  this->SetOverflowCheck(false);
  this->AssertUnaryOp(Ln, "[-Inf, -1, 0, Inf]", "[NaN, NaN, -Inf, Inf]");
  this->AssertUnaryOp(Log10, "[-Inf, -1, 0, Inf]", "[NaN, NaN, -Inf, Inf]");
  this->AssertUnaryOp(Log2, "[-Inf, -1, 0, Inf]", "[NaN, NaN, -Inf, Inf]");
  this->AssertUnaryOp(Log1p, "[-Inf, -2, -1, Inf]", "[NaN, NaN, -Inf, Inf]");
  this->SetOverflowCheck(true);
  this->AssertUnaryOpRaises(Ln, "[0]", "logarithm of zero");
  this->AssertUnaryOpRaises(Ln, "[-1]", "logarithm of negative number");
  this->AssertUnaryOpRaises(Ln, "[-Inf]", "logarithm of negative number");

  auto lowest_val = MakeScalar(std::numeric_limits<CType>::lowest());
  // N.B. RapidJSON on some platforms raises "Number too big to be stored in double" so
  // don't bounce through JSON
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("logarithm of negative number"),
                                  Ln(lowest_val, this->options_));
  this->AssertUnaryOpRaises(Log10, "[0]", "logarithm of zero");
  this->AssertUnaryOpRaises(Log10, "[-1]", "logarithm of negative number");
  this->AssertUnaryOpRaises(Log10, "[-Inf]", "logarithm of negative number");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("logarithm of negative number"),
                                  Log10(lowest_val, this->options_));
  this->AssertUnaryOpRaises(Log2, "[0]", "logarithm of zero");
  this->AssertUnaryOpRaises(Log2, "[-1]", "logarithm of negative number");
  this->AssertUnaryOpRaises(Log2, "[-Inf]", "logarithm of negative number");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("logarithm of negative number"),
                                  Log2(lowest_val, this->options_));
  this->AssertUnaryOpRaises(Log1p, "[-1]", "logarithm of zero");
  this->AssertUnaryOpRaises(Log1p, "[-2]", "logarithm of negative number");
  this->AssertUnaryOpRaises(Log1p, "[-Inf]", "logarithm of negative number");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("logarithm of negative number"),
                                  Log1p(lowest_val, this->options_));
}

TYPED_TEST(TestUnaryArithmeticIntegral, Log) {
  // Integer arguments promoted to double, sanity check here
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    this->AssertUnaryOp(Ln, "[1, null]", ArrayFromJSON(float64(), "[0, null]"));
    this->AssertUnaryOp(Log10, "[1, 10, null]", ArrayFromJSON(float64(), "[0, 1, null]"));
    this->AssertUnaryOp(Log2, "[1, 2, null]", ArrayFromJSON(float64(), "[0, 1, null]"));
    this->AssertUnaryOp(Log1p, "[0, null]", ArrayFromJSON(float64(), "[0, null]"));
  }
}

TYPED_TEST(TestBinaryArithmeticIntegral, Log) {
  // Integer arguments promoted to double, sanity check here
  this->AssertBinop(Logb, "[1, 10, null]", "[10, 10, null]",
                    ArrayFromJSON(float64(), "[0, 1, null]"));
  this->AssertBinop(Logb, "[1, 2, null]", "[2, 2, null]",
                    ArrayFromJSON(float64(), "[0, 1, null]"));
  this->AssertBinop(Logb, "[10, 100, null]", this->MakeScalar(10),
                    ArrayFromJSON(float64(), "[1, 2, null]"));
}

TYPED_TEST(TestBinaryArithmeticFloating, Log) {
  using CType = typename TestFixture::CType;
  this->SetNansEqual(true);
  auto min_val = std::numeric_limits<CType>::min();
  auto max_val = std::numeric_limits<CType>::max();
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    // N.B. min() for float types is smallest normal number > 0
    this->AssertBinop(Logb, "[1, 10, null, NaN, Inf]", "[100, 10, null, 2, 10]",
                      "[0, 1, null, NaN, Inf]");
    this->AssertBinop(Logb, min_val, 10,
                      static_cast<CType>(std::log(min_val) / std::log(10)));
    this->AssertBinop(Logb, max_val, 10,
                      static_cast<CType>(std::log(max_val) / std::log(10)));
  }
  this->AssertBinop(Logb, "[1.0, 10.0, null]", "[10.0, 10.0, null]", "[0.0, 1.0, null]");
  this->AssertBinop(Logb, "[1.0, 2.0, null]", "[2.0, 2.0, null]", "[0.0, 1.0, null]");
  this->AssertBinop(Logb, "[10.0, 100.0, 1000.0, null]", this->MakeScalar(10),
                    "[1.0, 2.0, 3.0, null]");
  this->AssertBinop(Logb, "[1, 2, 4, 8]", this->MakeScalar(0.25),
                    "[-0.0, -0.5, -1.0, -1.5]");
  this->SetOverflowCheck(false);
  this->AssertBinop(Logb, "[-Inf, -1, 0, Inf]", this->MakeScalar(10),
                    "[NaN, NaN, -Inf, Inf]");
  this->AssertBinop(Logb, "[-Inf, -1, 0, Inf]", this->MakeScalar(2),
                    "[NaN, NaN, -Inf, Inf]");
  this->AssertBinop(Logb, "[-Inf, -1, 0, Inf]", "[2, 10, 0, 0]", "[NaN, NaN, NaN, NaN]");
  this->AssertBinop(Logb, "[-Inf, -1, 0, Inf]", this->MakeScalar(0),
                    "[NaN, NaN, NaN, NaN]");
  this->AssertBinop(Logb, "[-Inf, -2, -1, Inf]", this->MakeScalar(2),
                    "[NaN, NaN, NaN, Inf]");
  this->SetOverflowCheck(true);
  this->AssertBinopRaises(Logb, "[0]", "[2]", "logarithm of zero");
  this->AssertBinopRaises(Logb, "[2]", "[0]", "logarithm of zero");
  this->AssertBinopRaises(Logb, "[-1]", "[2]", "logarithm of negative number");
  this->AssertBinopRaises(Logb, "[-Inf]", "[2]", "logarithm of negative number");
}

TYPED_TEST(TestBinaryArithmeticSigned, Log) {
  // Integer arguments promoted to double, sanity check here
  this->SetNansEqual(true);
  this->SetOverflowCheck(false);
  this->AssertBinop(Logb, "[-1, 0]", this->MakeScalar(10),
                    ArrayFromJSON(float64(), "[NaN, -Inf]"));
  this->AssertBinop(Logb, "[-1, 0]", this->MakeScalar(2),
                    ArrayFromJSON(float64(), "[NaN, -Inf]"));
  this->AssertBinop(Logb, "[10, 100]", this->MakeScalar(-1),
                    ArrayFromJSON(float64(), "[NaN, NaN]"));
  this->AssertBinop(Logb, "[-1, 0, null]", this->MakeScalar(-1),
                    ArrayFromJSON(float64(), "[NaN, NaN, null]"));
  // 10**x is negative for x smaller than 1, and tends towards zero when x -> 0
  this->AssertBinop(Logb, "[10, 100]", this->MakeScalar(0),
                    ArrayFromJSON(float64(), "[-0.0, -0.0]"));
  this->SetOverflowCheck(true);
  this->AssertBinopRaises(Logb, "[0]", "[10]", "logarithm of zero");
  this->AssertBinopRaises(Logb, "[-1]", "[10]", "logarithm of negative number");
  this->AssertBinopRaises(Logb, "[10]", "[0]", "logarithm of zero");
  this->AssertBinopRaises(Logb, "[100]", "[-1]", "logarithm of negative number");
}

TYPED_TEST(TestUnaryArithmeticSigned, Log) {
  // Integer arguments promoted to double, sanity check here
  this->SetNansEqual(true);
  this->SetOverflowCheck(false);
  this->AssertUnaryOp(Ln, "[-1, 0]", ArrayFromJSON(float64(), "[NaN, -Inf]"));
  this->AssertUnaryOp(Log10, "[-1, 0]", ArrayFromJSON(float64(), "[NaN, -Inf]"));
  this->AssertUnaryOp(Log2, "[-1, 0]", ArrayFromJSON(float64(), "[NaN, -Inf]"));
  this->AssertUnaryOp(Log1p, "[-2, -1]", ArrayFromJSON(float64(), "[NaN, -Inf]"));
  this->SetOverflowCheck(true);
  this->AssertUnaryOpRaises(Ln, "[0]", "logarithm of zero");
  this->AssertUnaryOpRaises(Ln, "[-1]", "logarithm of negative number");
  this->AssertUnaryOpRaises(Log10, "[0]", "logarithm of zero");
  this->AssertUnaryOpRaises(Log10, "[-1]", "logarithm of negative number");
  this->AssertUnaryOpRaises(Log2, "[0]", "logarithm of zero");
  this->AssertUnaryOpRaises(Log2, "[-1]", "logarithm of negative number");
  this->AssertUnaryOpRaises(Log1p, "[-1]", "logarithm of zero");
  this->AssertUnaryOpRaises(Log1p, "[-2]", "logarithm of negative number");
}

TYPED_TEST(TestUnaryArithmeticIntegral, Sqrt) {
  // Integer arguments promoted to double, sanity check here
  for (auto check_overflow : {false, true}) {
    this->SetOverflowCheck(check_overflow);
    this->AssertUnaryOp(Sqrt, "[1, null]", ArrayFromJSON(float64(), "[1, null]"));
    this->AssertUnaryOp(Sqrt, "[4, null]", ArrayFromJSON(float64(), "[2, null]"));
    this->AssertUnaryOp(Sqrt, "[null, 9]", ArrayFromJSON(float64(), "[null, 3]"));
  }
}

TYPED_TEST(TestUnaryArithmeticFloating, Sqrt) {
  using CType = typename TestFixture::CType;
  this->SetNansEqual(true);
  for (auto check_overflow : {false, true}) {
    const auto min_val = std::numeric_limits<CType>::min();
    this->SetOverflowCheck(check_overflow);
    this->AssertUnaryOp(Sqrt, "[1, 2, null, NaN, Inf]",
                        "[1, 1.414213562, null, NaN, Inf]");
    this->AssertUnaryOp(Sqrt, min_val, static_cast<CType>(std::sqrt(min_val)));
#ifndef __MINGW32__
    // this is problematic and produces a slight difference on MINGW
    const auto max_val = std::numeric_limits<CType>::max();
    this->AssertUnaryOp(Sqrt, max_val, static_cast<CType>(std::sqrt(max_val)));
#endif
  }
  this->AssertUnaryOpRaises(Sqrt, "[-1]", "square root of negative number");
  this->AssertUnaryOpRaises(Sqrt, "[-Inf]", "square root of negative number");
}

TYPED_TEST(TestUnaryArithmeticSigned, Sign) {
  using CType = typename TestFixture::CType;
  auto min = std::numeric_limits<CType>::min();
  auto max = std::numeric_limits<CType>::max();

  auto sign = [](const Datum& arg, ArithmeticOptions, ExecContext* ctx) {
    return Sign(arg, ctx);
  };

  this->AssertUnaryOp(sign, "[]", ArrayFromJSON(int8(), "[]"));
  this->AssertUnaryOp(sign, "[null]", ArrayFromJSON(int8(), "[null]"));
  this->AssertUnaryOp(sign, "[1, null, -10]", ArrayFromJSON(int8(), "[1, null, -1]"));
  this->AssertUnaryOp(sign, "[0]", ArrayFromJSON(int8(), "[0]"));
  this->AssertUnaryOp(sign, "[1, 10, 127]", ArrayFromJSON(int8(), "[1, 1, 1]"));
  this->AssertUnaryOp(sign, "[-1, -10, -127]", ArrayFromJSON(int8(), "[-1, -1, -1]"));
  this->AssertUnaryOp(sign, this->MakeScalar(min), *arrow::MakeScalar(int8(), -1));
  this->AssertUnaryOp(sign, this->MakeScalar(max), *arrow::MakeScalar(int8(), 1));
}

TYPED_TEST(TestUnaryArithmeticUnsigned, Sign) {
  using CType = typename TestFixture::CType;
  auto min = std::numeric_limits<CType>::min();
  auto max = std::numeric_limits<CType>::max();

  auto sign = [](const Datum& arg, ArithmeticOptions, ExecContext* ctx) {
    return Sign(arg, ctx);
  };

  this->AssertUnaryOp(sign, "[]", ArrayFromJSON(int8(), "[]"));
  this->AssertUnaryOp(sign, "[null]", ArrayFromJSON(int8(), "[null]"));
  this->AssertUnaryOp(sign, "[1, null, 10]", ArrayFromJSON(int8(), "[1, null, 1]"));
  this->AssertUnaryOp(sign, "[0]", ArrayFromJSON(int8(), "[0]"));
  this->AssertUnaryOp(sign, "[1, 10, 127]", ArrayFromJSON(int8(), "[1, 1, 1]"));
  this->AssertUnaryOp(sign, this->MakeScalar(min), *arrow::MakeScalar(int8(), 0));
  this->AssertUnaryOp(sign, this->MakeScalar(max), *arrow::MakeScalar(int8(), 1));
}

TYPED_TEST(TestUnaryArithmeticFloating, Sign) {
  using CType = typename TestFixture::CType;
  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->SetNansEqual(true);

  auto sign = [](const Datum& arg, ArithmeticOptions, ExecContext* ctx) {
    return Sign(arg, ctx);
  };

  this->AssertUnaryOp(sign, "[]", "[]");
  this->AssertUnaryOp(sign, "[null]", "[null]");
  this->AssertUnaryOp(sign, "[1.3, null, -10.80]", "[1, null, -1]");
  this->AssertUnaryOp(sign, "[0.0, -0.0]", "[0, 0]");
  this->AssertUnaryOp(sign, "[1.3, 10.80, 12748.001]", "[1, 1, 1]");
  this->AssertUnaryOp(sign, "[-1.3, -10.80, -12748.001]", "[-1, -1, -1]");
  this->AssertUnaryOp(sign, "[Inf, -Inf]", "[1, -1]");
  this->AssertUnaryOp(sign, "[NaN]", "[NaN]");
  this->AssertUnaryOp(sign, this->MakeScalar(min), this->MakeScalar(-1));
  this->AssertUnaryOp(sign, this->MakeScalar(max), this->MakeScalar(1));
}

}  // namespace
}  // namespace compute
}  // namespace arrow
