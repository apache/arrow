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
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/string.h"

#include "arrow/testing/gtest_util.h"

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

template <typename T, typename OptionsType>
class TestBaseUnaryRoundArithmetic : public ::testing::Test {
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

// Subclasses of TestBaseUnaryRoundArithmetic for different FunctionOptions.
template <typename T>
class TestUnaryRoundArithmetic
    : public TestBaseUnaryRoundArithmetic<T, ArithmeticOptions> {
 protected:
  using Base = TestBaseUnaryRoundArithmetic<T, ArithmeticOptions>;
  using Base::options_;

  void SetOverflowCheck(bool value) { options_.check_overflow = value; }
};

template <typename T>
class TestUnaryRoundArithmeticIntegral : public TestUnaryRoundArithmetic<T> {};

template <typename T>
class TestUnaryRoundArithmeticSigned : public TestUnaryRoundArithmeticIntegral<T> {};

template <typename T>
class TestUnaryRoundArithmeticUnsigned : public TestUnaryRoundArithmeticIntegral<T> {};

template <typename T>
class TestUnaryRoundArithmeticFloating : public TestUnaryRoundArithmetic<T> {};

template <typename T>
class TestUnaryRound : public TestBaseUnaryRoundArithmetic<T, RoundOptions> {
 protected:
  using Base = TestBaseUnaryRoundArithmetic<T, RoundOptions>;
  using Base::options_;

  void SetRoundMode(RoundMode value) { options_.round_mode = value; }

  void SetRoundNdigits(int64_t value) { options_.ndigits = value; }
};

template <typename T>
class TestUnaryRoundIntegral : public TestUnaryRound<T> {};

template <typename T>
class TestUnaryRoundSigned : public TestUnaryRoundIntegral<T> {};

template <typename T>
class TestUnaryRoundUnsigned : public TestUnaryRoundIntegral<T> {};

template <typename T>
class TestUnaryRoundFloating : public TestUnaryRound<T> {};

template <typename T, typename OptionsType>
class TestBaseBinaryRoundArithmetic : public ::testing::Test {
 protected:
  using ArrowType = T;
  using CType = typename ArrowType::c_type;

  static std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }

  using BinaryFunction =
      std::function<Result<Datum>(const Datum&, const Datum&, OptionsType, ExecContext*)>;

  std::shared_ptr<Scalar> MakeNullScalar() {
    return arrow::MakeNullScalar(type_singleton());
  }

  std::shared_ptr<Scalar> MakeScalar(CType value) {
    return *arrow::MakeScalar(type_singleton(), value);
  }

  std::shared_ptr<Scalar> MakeInt32Scalar(int32_t value) {
    return *arrow::MakeScalar(int32(), value);
  }

  void SetUp() override {}

  // (CScalar, CScalar)
  void AssertBinaryOp(BinaryFunction func, CType argument, int32_t ndigits,
                      CType expected) {
    auto arg = MakeScalar(argument);
    auto nd = MakeInt32Scalar(ndigits);
    auto exp = MakeScalar(expected);
    ASSERT_OK_AND_ASSIGN(auto actual, func(arg, nd, options_, nullptr));
    AssertScalarsApproxEqual(*exp, *actual.scalar(), /*verbose=*/true);
  }

  // (Array, Scalar)
  void AssertBinaryOp(BinaryFunction func, const std::shared_ptr<Array>& arg,
                      const std::shared_ptr<Scalar>& ndigits,
                      const std::shared_ptr<Array>& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, func(arg, ndigits, options_, nullptr));
    ValidateAndAssertApproxEqual(actual.make_array(), expected);
  }

  // (Scalar, Scalar)
  void AssertBinaryOp(BinaryFunction func, const std::shared_ptr<Scalar>& arg,
                      const std::shared_ptr<Scalar>& ndigits,
                      const std::shared_ptr<Scalar>& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, func(arg, ndigits, options_, nullptr));
    AssertScalarsApproxEqual(*expected, *actual.scalar(), /*verbose=*/true);
  }

  // (JSON, JSON)
  void AssertBinaryOp(BinaryFunction func, const std::string& arg_json, int32_t ndigits,
                      const std::string& expected_json) {
    auto arg = ArrayFromJSON(type_singleton(), arg_json);
    auto nd = MakeInt32Scalar(ndigits);
    ASSERT_OK_AND_ASSIGN(auto nda, MakeArrayFromScalar(*nd, arg->length()));
    auto expected = ArrayFromJSON(type_singleton(), expected_json);
    AssertBinaryOp(func, arg, nda, expected);

    // Also test with ndigits as a scalar.
    AssertBinaryOp(func, arg, nd, expected);
  }

  // (JSON, JSON)
  void AssertBinaryOp(BinaryFunction func, const std::string& arg_json,
                      const std::string& ndigits_json, const std::string& expected_json) {
    auto arg = ArrayFromJSON(type_singleton(), arg_json);
    auto nd = ArrayFromJSON(int32(), ndigits_json);
    auto expected = ArrayFromJSON(type_singleton(), expected_json);
    AssertBinaryOp(func, arg, nd, expected);
  }

  // (Array, JSON)
  void AssertBinaryOp(BinaryFunction func, const std::shared_ptr<Array>& arg,
                      const std::shared_ptr<Array>& ndigits,
                      const std::string& expected_json) {
    const auto expected = ArrayFromJSON(type_singleton(), expected_json);
    AssertBinaryOp(func, arg, ndigits, expected);
  }

  // (JSON, Array)
  void AssertBinaryOp(BinaryFunction func, const std::string& arg_json, int32_t ndigits,
                      const std::shared_ptr<Array>& expected) {
    auto arg = ArrayFromJSON(type_singleton(), arg_json);
    auto nd = MakeInt32Scalar(ndigits);
    ASSERT_OK_AND_ASSIGN(auto nda, MakeArrayFromScalar(*nd, arg->length()));
    AssertBinaryOp(func, arg, nda, expected);
  }

  // (Array, Array)
  void AssertBinaryOp(BinaryFunction func, const std::shared_ptr<Array>& arg,
                      const std::shared_ptr<Array>& ndigits,
                      const std::shared_ptr<Array>& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, func(arg, ndigits, options_, nullptr));
    ValidateAndAssertApproxEqual(actual.make_array(), expected);

    // Also check (Scalar, Scalar) operations
    const int64_t length = expected->length();
    for (int64_t i = 0; i < length; ++i) {
      const auto expected_scalar = *expected->GetScalar(i);
      ASSERT_OK_AND_ASSIGN(
          actual, func(*arg->GetScalar(i), *ndigits->GetScalar(i), options_, nullptr));
      AssertScalarsApproxEqual(*expected_scalar, *actual.scalar(), /*verbose=*/true,
                               equal_options_);
    }
  }

  // (CScalar, CScalar)
  void AssertBinaryOpRaises(BinaryFunction func, CType argument, CType ndigits,
                            const std::string& expected_msg) {
    auto arg = MakeScalar(argument);
    auto nd = MakeInt32Scalar(ndigits);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr(expected_msg),
                                    func(arg, nd, options_, nullptr));
  }

  void AssertBinaryOpRaises(BinaryFunction func, const std::string& argument,
                            const std::string& ndigits, const std::string& expected_msg) {
    auto arg = ArrayFromJSON(type_singleton(), argument);
    auto nd = ArrayFromJSON(int32(), ndigits);
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr(expected_msg),
                                    func(arg, nd, options_, nullptr));
    for (int64_t i = 0; i < arg->length(); i++) {
      ASSERT_OK_AND_ASSIGN(auto scalar, arg->GetScalar(i));
      ASSERT_OK_AND_ASSIGN(auto nscalar, nd->GetScalar(i));
      EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr(expected_msg),
                                      func(scalar, nscalar, options_, nullptr));
    }
  }

  void AssertBinaryOpNotImplemented(BinaryFunction func, const std::string& argument,
                                    const std::string& ndigits) {
    auto arg = ArrayFromJSON(type_singleton(), argument);
    auto nd = ArrayFromJSON(int32(), ndigits);
    const char* expected_msg = "has no kernel matching input types";
    EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented, ::testing::HasSubstr(expected_msg),
                                    func(arg, nd, options_, nullptr));
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

// Subclasses of TestBaseBinaryRoundArithmetic for different FunctionOptions.
template <typename T>
class TestBinaryRoundArithmetic
    : public TestBaseBinaryRoundArithmetic<T, RoundBinaryOptions> {
 protected:
  using Base = TestBaseBinaryRoundArithmetic<T, RoundBinaryOptions>;
  using Base::options_;

  void SetRoundMode(RoundMode value) { options_.round_mode = value; }
};

template <typename T>
class TestBinaryRound : public TestBaseBinaryRoundArithmetic<T, RoundBinaryOptions> {
 protected:
  using Base = TestBaseBinaryRoundArithmetic<T, RoundBinaryOptions>;
  using Base::options_;

  void SetRoundMode(RoundMode value) { options_.round_mode = value; }
};

template <typename T>
class TestBinaryRoundIntegral : public TestBinaryRound<T> {};

template <typename T>
class TestBinaryRoundSigned : public TestBinaryRoundIntegral<T> {};

template <typename T>
class TestBinaryRoundUnsigned : public TestBinaryRoundIntegral<T> {};

template <typename T>
class TestBinaryRoundFloating : public TestBinaryRoundArithmetic<T> {};

template <typename T>
class TestUnaryRoundToMultiple
    : public TestBaseUnaryRoundArithmetic<T, RoundToMultipleOptions> {
 protected:
  using Base = TestBaseUnaryRoundArithmetic<T, RoundToMultipleOptions>;
  using Base::options_;

  void SetRoundMode(RoundMode value) { options_.round_mode = value; }

  void SetRoundMultiple(double value) {
    options_.multiple = std::make_shared<DoubleScalar>(value);
  }
};

template <typename T>
class TestUnaryRoundToMultipleIntegral : public TestUnaryRoundToMultiple<T> {};

template <typename T>
class TestUnaryRoundToMultipleSigned : public TestUnaryRoundToMultipleIntegral<T> {};

template <typename T>
class TestUnaryRoundToMultipleUnsigned : public TestUnaryRoundToMultipleIntegral<T> {};

template <typename T>
class TestUnaryRoundToMultipleFloating : public TestUnaryRoundToMultiple<T> {};

class TestRoundArithmeticDecimal : public ::testing::Test {
 protected:
  static std::vector<std::shared_ptr<DataType>> PositiveScaleTypes() {
    return {decimal128(4, 2), decimal256(4, 2), decimal128(38, 2), decimal256(76, 2)};
  }

  static std::vector<std::shared_ptr<DataType>> NegativeScaleTypes() {
    return {decimal128(2, -2), decimal256(2, -2)};
  }

  // Validate that func(*decimals) is the same as
  // func([cast(x, float64) x for x in decimals])
  static void CheckDecimalToFloat(const std::string& func, const DatumVector& args) {
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
    AssertDatumsApproxEqual(expected, actual, /*verbose=*/true);
  }

  static void CheckRaises(const std::string& func, const DatumVector& args,
                          const std::string& substr,
                          const FunctionOptions* options = nullptr) {
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr(substr),
                                    CallFunction(func, args, options));
  }
};

TYPED_TEST_SUITE(TestUnaryRoundArithmeticIntegral, IntegralTypes);
TYPED_TEST_SUITE(TestUnaryRoundArithmeticSigned, SignedIntegerTypes);
TYPED_TEST_SUITE(TestUnaryRoundArithmeticUnsigned, UnsignedIntegerTypes);
TYPED_TEST_SUITE(TestUnaryRoundArithmeticFloating, FloatingTypes);

TEST(TestUnaryRound, DispatchBestRound) {
  // Integer -> Float64
  for (std::string name : {"floor", "ceil", "trunc", "round", "round_to_multiple"}) {
    for (const auto& ty :
         {int8(), int16(), int32(), int64(), uint8(), uint16(), uint32(), uint64()}) {
      CheckDispatchBest(name, {ty}, {float64()});
      CheckDispatchBest(name, {dictionary(int8(), ty)}, {float64()});
    }
  }
}

class TestUnaryRoundArithmeticDecimal : public TestRoundArithmeticDecimal {};

// Check two modes exhaustively, give all modes a simple test
TEST_F(TestUnaryRoundArithmeticDecimal, Round) {
  const auto func = "round";
  RoundOptions options(2, RoundMode::DOWN);
  for (const auto& ty : {decimal128(4, 3), decimal256(4, 3)}) {
    auto values = ArrayFromJSON(
        ty,
        R"(["1.010", "1.012", "1.015", "1.019", "-1.010", "-1.012", "-1.015", "-1.019", null])");
    options.round_mode = RoundMode::DOWN;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.010", "1.010", "1.010", "1.010", "-1.010", "-1.020", "-1.020", "-1.020", null])"),
        &options);
    options.round_mode = RoundMode::UP;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.010", "1.020", "1.020", "1.020", "-1.010", "-1.010", "-1.010", "-1.010", null])"),
        &options);
    options.round_mode = RoundMode::TOWARDS_ZERO;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.010", "1.010", "1.010", "1.010", "-1.010", "-1.010", "-1.010", "-1.010", null])"),
        &options);
    options.round_mode = RoundMode::TOWARDS_INFINITY;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.010", "1.020", "1.020", "1.020", "-1.010", "-1.020", "-1.020", "-1.020", null])"),
        &options);
    options.round_mode = RoundMode::HALF_DOWN;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.010", "1.010", "1.010", "1.020", "-1.010", "-1.010", "-1.020", "-1.020", null])"),
        &options);
    options.round_mode = RoundMode::HALF_UP;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.010", "1.010", "1.020", "1.020", "-1.010", "-1.010", "-1.010", "-1.020", null])"),
        &options);
    options.round_mode = RoundMode::HALF_TOWARDS_ZERO;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.010", "1.010", "1.010", "1.020", "-1.010", "-1.010", "-1.010", "-1.020", null])"),
        &options);
    options.round_mode = RoundMode::HALF_TOWARDS_INFINITY;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.010", "1.010", "1.020", "1.020", "-1.010", "-1.010", "-1.020", "-1.020", null])"),
        &options);
    options.round_mode = RoundMode::HALF_TO_EVEN;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.010", "1.010", "1.020", "1.020", "-1.010", "-1.010", "-1.020", "-1.020", null])"),
        &options);
    options.round_mode = RoundMode::HALF_TO_ODD;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.010", "1.010", "1.010", "1.020", "-1.010", "-1.010", "-1.010", "-1.020", null])"),
        &options);
  }
}

TEST_F(TestUnaryRoundArithmeticDecimal, RoundTowardsInfinity) {
  const auto func = "round";
  RoundOptions options(0, RoundMode::TOWARDS_INFINITY);
  for (const auto& ty : {decimal128(4, 2), decimal256(4, 2)}) {
    auto values = ArrayFromJSON(
        ty, R"(["1.00", "1.99", "1.01", "-42.00", "-42.99", "-42.15", null])");
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"), &options);
    options.ndigits = 0;
    CheckScalar(
        func, {values},
        ArrayFromJSON(ty,
                      R"(["1.00", "2.00", "2.00", "-42.00", "-43.00", "-43.00", null])"),
        &options);
    options.ndigits = 1;
    CheckScalar(
        func, {values},
        ArrayFromJSON(ty,
                      R"(["1.00", "2.00", "1.10", "-42.00", "-43.00", "-42.20", null])"),
        &options);
    options.ndigits = 2;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = 4;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = 100;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = -1;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty, R"(["10.00", "10.00", "10.00", "-50.00", "-50.00", "-50.00", null])"),
        &options);
    options.ndigits = -2;
    CheckRaises(func, {values}, "Rounding to -2 digits will not fit in precision",
                &options);
    options.ndigits = -1;
    CheckRaises(func, {ArrayFromJSON(ty, R"(["99.99"])")},
                "Rounded value 100.00 does not fit in precision", &options);
  }
  for (const auto& ty : {decimal128(2, -2), decimal256(2, -2)}) {
    auto values = DecimalArrayFromJSON(
        ty, R"(["10E2", "12E2", "18E2", "-10E2", "-12E2", "-18E2", null])");
    options.ndigits = 0;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = 2;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = 100;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = -1;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = -2;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = -3;
    CheckScalar(func, {values},
                DecimalArrayFromJSON(
                    ty, R"(["10E2", "20E2", "20E2", "-10E2", "-20E2", "-20E2", null])"),
                &options);
    options.ndigits = -4;
    CheckRaises(func, {values}, "Rounding to -4 digits will not fit in precision",
                &options);
  }
}

TEST_F(TestUnaryRoundArithmeticDecimal, RoundHalfToEven) {
  const auto func = "round";
  RoundOptions options(0, RoundMode::HALF_TO_EVEN);
  for (const auto& ty : {decimal128(4, 2), decimal256(4, 2)}) {
    auto values = ArrayFromJSON(
        ty,
        R"(["1.00", "5.99", "1.01", "-42.00", "-42.99", "-42.15", "1.50", "2.50", "-5.50", "-2.55", null])");
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"), &options);
    options.ndigits = 0;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.00", "6.00", "1.00", "-42.00", "-43.00", "-42.00", "2.00", "2.00", "-6.00", "-3.00", null])"),
        &options);
    options.ndigits = 1;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["1.00", "6.00", "1.00", "-42.00", "-43.00", "-42.20", "1.50", "2.50", "-5.50", "-2.60", null])"),
        &options);
    options.ndigits = 2;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = 4;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = 100;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = -1;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["0.00", "10.00", "0.00", "-40.00", "-40.00", "-40.00", "0.00", "0.00", "-10.00", "0.00", null])"),
        &options);
    options.ndigits = -2;
    CheckRaises(func, {values}, "Rounding to -2 digits will not fit in precision",
                &options);
    options.ndigits = -1;
    CheckRaises(func, {ArrayFromJSON(ty, R"(["99.99"])")},
                "Rounded value 100.00 does not fit in precision", &options);
  }
  for (const auto& ty : {decimal128(2, -2), decimal256(2, -2)}) {
    auto values = DecimalArrayFromJSON(
        ty,
        R"(["5E2", "10E2", "12E2", "15E2", "18E2", "-10E2", "-12E2", "-15E2", "-18E2", null])");
    options.ndigits = 0;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = 2;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = 100;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = -1;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = -2;
    CheckScalar(func, {values}, values, &options);
    options.ndigits = -3;
    CheckScalar(
        func, {values},
        DecimalArrayFromJSON(
            ty,
            R"(["0", "10E2", "10E2", "20E2", "20E2", "-10E2", "-10E2", "-20E2", "-20E2", null])"),
        &options);
    options.ndigits = -4;
    CheckRaises(func, {values}, "Rounding to -4 digits will not fit in precision",
                &options);
  }
}

TEST_F(TestUnaryRoundArithmeticDecimal, RoundCeil) {
  const auto func = "ceil";
  for (const auto& ty : PositiveScaleTypes()) {
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"));
    CheckScalar(
        func,
        {ArrayFromJSON(
            ty, R"(["1.00", "1.99", "1.01", "-42.00", "-42.99", "-42.15", null])")},
        ArrayFromJSON(ty,
                      R"(["1.00", "2.00", "2.00", "-42.00", "-42.00", "-42.00", null])"));
  }
  for (const auto& ty : {decimal128(4, 2), decimal256(4, 2)}) {
    CheckRaises(func, {ScalarFromJSON(ty, R"("99.99")")},
                "Rounded value 100.00 does not fit in precision of decimal");
    CheckScalar(func, {ScalarFromJSON(ty, R"("-99.99")")},
                ScalarFromJSON(ty, R"("-99.00")"));
  }
  for (const auto& ty : NegativeScaleTypes()) {
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"));
    CheckScalar(func, {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")},
                DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])"));
  }
}

TEST_F(TestUnaryRoundArithmeticDecimal, RoundFloor) {
  const auto func = "floor";
  for (const auto& ty : PositiveScaleTypes()) {
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"));
    CheckScalar(
        func,
        {ArrayFromJSON(
            ty, R"(["1.00", "1.99", "1.01", "-42.00", "-42.99", "-42.15", null])")},
        ArrayFromJSON(ty,
                      R"(["1.00", "1.00", "1.00", "-42.00", "-43.00", "-43.00", null])"));
  }
  for (const auto& ty : {decimal128(4, 2), decimal256(4, 2)}) {
    CheckScalar(func, {ScalarFromJSON(ty, R"("99.99")")},
                ScalarFromJSON(ty, R"("99.00")"));
    CheckRaises(func, {ScalarFromJSON(ty, R"("-99.99")")},
                "Rounded value -100.00 does not fit in precision of decimal");
  }
  for (const auto& ty : NegativeScaleTypes()) {
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"));
    CheckScalar(func, {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")},
                DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])"));
  }
}

TEST_F(TestUnaryRoundArithmeticDecimal, RoundTrunc) {
  const auto func = "trunc";
  for (const auto& ty : PositiveScaleTypes()) {
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"));
    CheckScalar(
        func,
        {ArrayFromJSON(
            ty, R"(["1.00", "1.99", "1.01", "-42.00", "-42.99", "-42.15", null])")},
        ArrayFromJSON(ty,
                      R"(["1.00", "1.00", "1.00", "-42.00", "-42.00", "-42.00", null])"));
  }
  for (const auto& ty : {decimal128(4, 2), decimal256(4, 2)}) {
    CheckScalar(func, {ScalarFromJSON(ty, R"("99.99")")},
                ScalarFromJSON(ty, R"("99.00")"));
    CheckScalar(func, {ScalarFromJSON(ty, R"("-99.99")")},
                ScalarFromJSON(ty, R"("-99.00")"));
  }
  for (const auto& ty : NegativeScaleTypes()) {
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"));
    CheckScalar(func, {DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])")},
                DecimalArrayFromJSON(ty, R"(["12E2", "-42E2", null])"));
  }
}

TEST_F(TestUnaryRoundArithmeticDecimal, RoundToMultiple) {
  const auto func = "round_to_multiple";
  RoundToMultipleOptions options(0, RoundMode::DOWN);
  for (const auto& ty : {decimal128(4, 2), decimal256(4, 2)}) {
    if (ty->id() == Type::DECIMAL128) {
      options.multiple = std::make_shared<Decimal128Scalar>(Decimal128(200), ty);
    } else {
      options.multiple = std::make_shared<Decimal256Scalar>(Decimal256(200), ty);
    }
    auto values = ArrayFromJSON(
        ty,
        R"(["-3.50", "-3.00", "-2.50", "-2.00", "-1.50", "-1.00", "-0.50", "0.00",
            "0.50", "1.00", "1.50", "2.00", "2.50", "3.00", "3.50", null])");
    options.round_mode = RoundMode::DOWN;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["-4.00", "-4.00", "-4.00", "-2.00", "-2.00", "-2.00", "-2.00", "0.00",
            "0.00", "0.00", "0.00", "2.00", "2.00", "2.00", "2.00", null])"),
        &options);
    options.round_mode = RoundMode::UP;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["-2.00", "-2.00", "-2.00", "-2.00", "-0.00", "-0.00", "-0.00", "0.00",
            "2.00", "2.00", "2.00", "2.00", "4.00", "4.00", "4.00", null])"),
        &options);
    options.round_mode = RoundMode::TOWARDS_ZERO;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["-2.00", "-2.00", "-2.00", "-2.00", "-0.00", "-0.00", "-0.00", "0.00",
            "0.00", "0.00", "0.00", "2.00", "2.00", "2.00", "2.00", null])"),
        &options);
    options.round_mode = RoundMode::TOWARDS_INFINITY;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["-4.00", "-4.00", "-4.00", "-2.00", "-2.00", "-2.00", "-2.00", "0.00",
            "2.00", "2.00", "2.00", "2.00", "4.00", "4.00", "4.00", null])"),
        &options);
    options.round_mode = RoundMode::HALF_DOWN;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["-4.00", "-4.00", "-2.00", "-2.00", "-2.00", "-2.00", "-0.00", "0.00",
            "0.00", "0.00", "2.00", "2.00", "2.00", "2.00", "4.00", null])"),
        &options);
    options.round_mode = RoundMode::HALF_UP;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["-4.00", "-2.00", "-2.00", "-2.00", "-2.00", "-0.00", "-0.00", "0.00",
            "0.00", "2.00", "2.00", "2.00", "2.00", "4.00", "4.00", null])"),
        &options);
    options.round_mode = RoundMode::HALF_TOWARDS_ZERO;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["-4.00", "-2.00", "-2.00", "-2.00", "-2.00", "-0.00", "-0.00", "0.00",
            "0.00", "0.00", "2.00", "2.00", "2.00", "2.00", "4.00", null])"),
        &options);
    options.round_mode = RoundMode::HALF_TOWARDS_INFINITY;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["-4.00", "-4.00", "-2.00", "-2.00", "-2.00", "-2.00", "-0.00", "0.00",
            "0.00", "2.00", "2.00", "2.00", "2.00", "4.00", "4.00", null])"),
        &options);
    options.round_mode = RoundMode::HALF_TO_EVEN;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["-4.00", "-4.00", "-2.00", "-2.00", "-2.00", "-0.00", "-0.00", "0.00",
            "0.00", "0.00", "2.00", "2.00", "2.00", "4.00", "4.00", null])"),
        &options);
    options.round_mode = RoundMode::HALF_TO_ODD;
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["-4.00", "-2.00", "-2.00", "-2.00", "-2.00", "-2.00", "-0.00", "0.00",
            "0.00", "2.00", "2.00", "2.00", "2.00", "2.00", "4.00", null])"),
        &options);
  }
}

TEST_F(TestUnaryRoundArithmeticDecimal, RoundToMultipleTowardsInfinity) {
  const auto func = "round_to_multiple";
  RoundToMultipleOptions options(0, RoundMode::TOWARDS_INFINITY);
  auto set_multiple = [&](const std::shared_ptr<DataType>& ty, int64_t value) {
    if (ty->id() == Type::DECIMAL128) {
      options.multiple = std::make_shared<Decimal128Scalar>(Decimal128(value), ty);
    } else {
      options.multiple = std::make_shared<Decimal256Scalar>(Decimal256(value), ty);
    }
  };
  for (const auto& ty : {decimal128(4, 2), decimal256(4, 2)}) {
    auto values = ArrayFromJSON(
        ty, R"(["1.00", "1.99", "1.01", "-42.00", "-42.99", "-42.15", null])");
    set_multiple(ty, 25);
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"), &options);
    CheckScalar(
        func, {values},
        ArrayFromJSON(ty,
                      R"(["1.00", "2.00", "1.25", "-42.00", "-43.00", "-42.25", null])"),
        &options);
    set_multiple(ty, 1);
    CheckScalar(func, {values}, values, &options);
    set_multiple(decimal128(2, 0), 2);
    CheckScalar(
        func, {values},
        ArrayFromJSON(ty,
                      R"(["2.00", "2.00", "2.00", "-42.00", "-44.00", "-44.00", null])"),
        &options);
    set_multiple(ty, 0);
    CheckRaises(func, {values}, "Rounding multiple must be positive", &options);
    options.multiple =
        std::make_shared<Decimal128Scalar>(Decimal128(0), decimal128(4, 2));
    CheckRaises(func, {values}, "Rounding multiple must be positive", &options);
    set_multiple(ty, -10);
    CheckRaises(func, {ArrayFromJSON(ty, R"(["99.99"])")},
                "Rounding multiple must be positive", &options);
    set_multiple(ty, 100);
    CheckRaises(func, {ArrayFromJSON(ty, R"(["99.99"])")},
                "Rounded value 100.00 does not fit in precision", &options);
    options.multiple = std::make_shared<DoubleScalar>(1.0);
    CheckRaises(func, {ArrayFromJSON(ty, R"(["99.99"])")},
                "Rounded value 100.00 does not fit in precision", &options);
    options.multiple = std::make_shared<Decimal128Scalar>(decimal128(3, 0));
    CheckRaises(func, {ArrayFromJSON(ty, R"(["99.99"])")},
                "Rounding multiple must be non-null and valid", &options);
    options.multiple = nullptr;
    CheckRaises(func, {ArrayFromJSON(ty, R"(["99.99"])")},
                "Rounding multiple must be non-null and valid", &options);
  }
  for (const auto& ty : {decimal128(2, -2), decimal256(2, -2)}) {
    auto values = DecimalArrayFromJSON(
        ty, R"(["10E2", "12E2", "18E2", "-10E2", "-12E2", "-18E2", null])");
    set_multiple(ty, 4);
    CheckScalar(func, {values},
                DecimalArrayFromJSON(
                    ty, R"(["12E2", "12E2", "20E2", "-12E2", "-12E2", "-20E2", null])"),
                &options);
    set_multiple(ty, 1);
    CheckScalar(func, {values}, values, &options);
  }
}

TEST_F(TestUnaryRoundArithmeticDecimal, RoundToMultipleHalfToOdd) {
  const auto func = "round_to_multiple";
  RoundToMultipleOptions options(0, RoundMode::HALF_TO_ODD);
  auto set_multiple = [&](const std::shared_ptr<DataType>& ty, int64_t value) {
    if (ty->id() == Type::DECIMAL128) {
      options.multiple = std::make_shared<Decimal128Scalar>(Decimal128(value), ty);
    } else {
      options.multiple = std::make_shared<Decimal256Scalar>(Decimal256(value), ty);
    }
  };
  for (const auto& ty : {decimal128(4, 2), decimal256(4, 2)}) {
    auto values =
        ArrayFromJSON(ty, R"(["-0.38", "-0.37", "-0.25", "-0.13", "-0.12", "0.00",
                "0.12", "0.13", "0.25", "0.37", "0.38", null])");
    // There is no exact halfway point, check what happens
    set_multiple(ty, 25);
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"), &options);
    CheckScalar(func, {values},
                ArrayFromJSON(ty, R"(["-0.50", "-0.25", "-0.25", "-0.25", "-0.00", "0.00",
                              "0.00", "0.25", "0.25", "0.25", "0.50", null])"),
                &options);
    set_multiple(ty, 1);
    CheckScalar(func, {values}, values, &options);
    set_multiple(ty, 24);
    CheckScalar(func, {ArrayFromJSON(ty, R"([])")}, ArrayFromJSON(ty, R"([])"), &options);
    CheckScalar(func, {values},
                ArrayFromJSON(ty, R"(["-0.48", "-0.48", "-0.24", "-0.24", "-0.24", "0.00",
                              "0.24", "0.24", "0.24", "0.48", "0.48", null])"),
                &options);
    set_multiple(decimal128(3, 1), 1);
    CheckScalar(
        func, {values},
        ArrayFromJSON(
            ty,
            R"(["-0.40", "-0.40", "-0.30", "-0.10", "-0.10", "0.00", "0.10", "0.10",
                      "0.30", "0.40", "0.40", null])"),
        &options);
  }
  for (const auto& ty : {decimal128(2, -2), decimal256(2, -2)}) {
    auto values = DecimalArrayFromJSON(
        ty, R"(["10E2", "12E2", "18E2", "-10E2", "-12E2", "-18E2", null])");
    set_multiple(ty, 4);
    CheckScalar(func, {values},
                DecimalArrayFromJSON(
                    ty, R"(["12E2", "12E2", "20E2", "-12E2", "-12E2", "-20E2", null])"),
                &options);
    set_multiple(ty, 5);
    CheckScalar(func, {values},
                DecimalArrayFromJSON(
                    ty, R"(["10E2", "10E2", "20E2", "-10E2", "-10E2", "-20E2", null])"),
                &options);
    set_multiple(ty, 1);
    CheckScalar(func, {values}, values, &options);
  }
}

TYPED_TEST_SUITE(TestUnaryRoundIntegral, IntegralTypes);
TYPED_TEST_SUITE(TestUnaryRoundSigned, SignedIntegerTypes);
TYPED_TEST_SUITE(TestUnaryRoundUnsigned, UnsignedIntegerTypes);
TYPED_TEST_SUITE(TestUnaryRoundFloating, FloatingTypes);

const std::vector<RoundMode> kRoundModes{
    RoundMode::DOWN,
    RoundMode::UP,
    RoundMode::TOWARDS_ZERO,
    RoundMode::TOWARDS_INFINITY,
    RoundMode::HALF_DOWN,
    RoundMode::HALF_UP,
    RoundMode::HALF_TOWARDS_ZERO,
    RoundMode::HALF_TOWARDS_INFINITY,
    RoundMode::HALF_TO_EVEN,
    RoundMode::HALF_TO_ODD,
};

TYPED_TEST(TestUnaryRoundSigned, Round) {
  // Test different rounding modes for integer rounding
  std::string values("[0, 1, -13, -50, 115]");
  this->SetRoundNdigits(0);
  for (const auto& round_mode : kRoundModes) {
    this->SetRoundMode(round_mode);
    this->AssertUnaryOp(Round, values, ArrayFromJSON(float64(), values));
  }

  // Test different round N-digits for nearest rounding mode
  std::vector<std::pair<int64_t, std::string>> ndigits_and_expected{{
      {-2, "[0.0, 0.0, -0.0, -100, 100]"},
      {-1, "[0.0, 0.0, -10, -50, 120]"},
      {0, values},
      {1, values},
      {2, values},
  }};
  this->SetRoundMode(RoundMode::HALF_TOWARDS_INFINITY);
  for (const auto& pair : ndigits_and_expected) {
    this->SetRoundNdigits(pair.first);
    this->AssertUnaryOp(Round, values, ArrayFromJSON(float64(), pair.second));
  }
}

TYPED_TEST(TestUnaryRoundUnsigned, Round) {
  // Test different rounding modes for integer rounding
  std::string values("[0, 1, 13, 50, 115]");
  this->SetRoundNdigits(0);
  for (const auto& round_mode : kRoundModes) {
    this->SetRoundMode(round_mode);
    this->AssertUnaryOp(Round, values, ArrayFromJSON(float64(), values));
  }

  // Test different round N-digits for nearest rounding mode
  std::vector<std::pair<int64_t, std::string>> ndigits_and_expected{{
      {-2, "[0, 0, 0, 100, 100]"},
      {-1, "[0, 0, 10, 50, 120]"},
      {0, values},
      {1, values},
      {2, values},
  }};
  this->SetRoundMode(RoundMode::HALF_TOWARDS_INFINITY);
  for (const auto& pair : ndigits_and_expected) {
    this->SetRoundNdigits(pair.first);
    this->AssertUnaryOp(Round, values, ArrayFromJSON(float64(), pair.second));
  }
}

TYPED_TEST(TestUnaryRoundFloating, Round) {
  this->SetNansEqual(true);

  // Test different rounding modes
  std::string values("[3.2, 3.5, 3.7, 4.5, -3.2, -3.5, -3.7]");
  std::vector<std::pair<RoundMode, std::string>> rmode_and_expected{{
      {RoundMode::DOWN, "[3, 3, 3, 4, -4, -4, -4]"},
      {RoundMode::UP, "[4, 4, 4, 5, -3, -3, -3]"},
      {RoundMode::TOWARDS_ZERO, "[3, 3, 3, 4, -3, -3, -3]"},
      {RoundMode::TOWARDS_INFINITY, "[4, 4, 4, 5, -4, -4, -4]"},
      {RoundMode::HALF_DOWN, "[3, 3, 4, 4, -3, -4, -4]"},
      {RoundMode::HALF_UP, "[3, 4, 4, 5, -3, -3, -4]"},
      {RoundMode::HALF_TOWARDS_ZERO, "[3, 3, 4, 4, -3, -3, -4]"},
      {RoundMode::HALF_TOWARDS_INFINITY, "[3, 4, 4, 5, -3, -4, -4]"},
      {RoundMode::HALF_TO_EVEN, "[3, 4, 4, 4, -3, -4, -4]"},
      {RoundMode::HALF_TO_ODD, "[3, 3, 4, 5, -3, -3, -4]"},
  }};
  this->SetRoundNdigits(0);
  for (const auto& pair : rmode_and_expected) {
    this->SetRoundMode(pair.first);
    this->AssertUnaryOp(Round, "[]", "[]");
    this->AssertUnaryOp(Round, "[null, 0, Inf, -Inf, NaN, -NaN]",
                        "[null, 0, Inf, -Inf, NaN, -NaN]");
    this->AssertUnaryOp(Round, values, pair.second);
  }

  // Test different round N-digits for nearest rounding mode
  values = "[320, 3.5, 3.075, 4.5, -3.212, -35.1234, -3.045]";
  std::vector<std::pair<int64_t, std::string>> ndigits_and_expected{{
      {-2, "[300, 0.0, 0.0, 0.0, -0.0, -0.0, -0.0]"},
      {-1, "[320, 0.0, 0.0, 0.0, -0.0, -40, -0.0]"},
      {0, "[320, 4, 3, 5, -3, -35, -3]"},
      {1, "[320, 3.5, 3.1, 4.5, -3.2, -35.1, -3]"},
      {2, "[320, 3.5, 3.08, 4.5, -3.21, -35.12, -3.05]"},
  }};
  this->SetRoundMode(RoundMode::HALF_TOWARDS_INFINITY);
  for (const auto& pair : ndigits_and_expected) {
    this->SetRoundNdigits(pair.first);
    this->AssertUnaryOp(Round, values, pair.second);
  }
}

TYPED_TEST_SUITE(TestBinaryRoundIntegral, IntegralTypes);
TYPED_TEST_SUITE(TestBinaryRoundSigned, SignedIntegerTypes);
TYPED_TEST_SUITE(TestBinaryRoundUnsigned, UnsignedIntegerTypes);
TYPED_TEST_SUITE(TestBinaryRoundFloating, FloatingTypes);

TYPED_TEST(TestBinaryRoundSigned, Round) {
  // Test different rounding modes for integer rounding
  std::string values("[0, 1, -13, -50, 115]");
  for (const auto& round_mode : kRoundModes) {
    this->SetRoundMode(round_mode);
    this->AssertBinaryOp(RoundBinary, values, 0, ArrayFromJSON(float64(), values));
  }

  // Test different round N-digits for nearest rounding mode
  std::vector<std::pair<int32_t, std::string>> ndigits_and_expected{{
      {-2, "[0.0, 0.0, -0.0, -100, 100]"},
      {-1, "[0.0, 0.0, -10, -50, 120]"},
      {0, values},
      {1, values},
      {2, values},
  }};
  this->SetRoundMode(RoundMode::HALF_TOWARDS_INFINITY);
  for (const auto& pair : ndigits_and_expected) {
    this->AssertBinaryOp(RoundBinary, values, pair.first,
                         ArrayFromJSON(float64(), pair.second));
  }
}

TYPED_TEST(TestBinaryRoundUnsigned, Round) {
  // Test different rounding modes for integer rounding
  std::string values("[0, 1, 13, 50, 115]");
  for (const auto& round_mode : kRoundModes) {
    this->SetRoundMode(round_mode);
    this->AssertBinaryOp(RoundBinary, values, 0, ArrayFromJSON(float64(), values));
  }

  // Test different round N-digits for nearest rounding mode
  std::vector<std::pair<int32_t, std::string>> ndigits_and_expected{{
      {-2, "[0, 0, 0, 100, 100]"},
      {-1, "[0, 0, 10, 50, 120]"},
      {0, values},
      {1, values},
      {2, values},
  }};
  this->SetRoundMode(RoundMode::HALF_TOWARDS_INFINITY);
  for (const auto& pair : ndigits_and_expected) {
    this->AssertBinaryOp(RoundBinary, values, pair.first,
                         ArrayFromJSON(float64(), pair.second));
  }
}

TYPED_TEST(TestBinaryRoundFloating, Round) {
  this->SetNansEqual(true);

  // Test different rounding modes
  std::string values("[3.2, 3.5, 3.7, 4.5, -3.2, -3.5, -3.7]");
  std::vector<std::pair<RoundMode, std::string>> rmode_and_expected{{
      {RoundMode::DOWN, "[3, 3, 3, 4, -4, -4, -4]"},
      {RoundMode::UP, "[4, 4, 4, 5, -3, -3, -3]"},
      {RoundMode::TOWARDS_ZERO, "[3, 3, 3, 4, -3, -3, -3]"},
      {RoundMode::TOWARDS_INFINITY, "[4, 4, 4, 5, -4, -4, -4]"},
      {RoundMode::HALF_DOWN, "[3, 3, 4, 4, -3, -4, -4]"},
      {RoundMode::HALF_UP, "[3, 4, 4, 5, -3, -3, -4]"},
      {RoundMode::HALF_TOWARDS_ZERO, "[3, 3, 4, 4, -3, -3, -4]"},
      {RoundMode::HALF_TOWARDS_INFINITY, "[3, 4, 4, 5, -3, -4, -4]"},
      {RoundMode::HALF_TO_EVEN, "[3, 4, 4, 4, -3, -4, -4]"},
      {RoundMode::HALF_TO_ODD, "[3, 3, 4, 5, -3, -3, -4]"},
  }};
  for (const auto& pair : rmode_and_expected) {
    this->SetRoundMode(pair.first);
    this->AssertBinaryOp(RoundBinary, "[]", "[]", "[]");
    this->AssertBinaryOp(RoundBinary, "[null, 0, Inf, -Inf, NaN, -NaN]",
                         "[0, 0, 0, 0, 0, 0]", "[null, 0, Inf, -Inf, NaN, -NaN]");
    this->AssertBinaryOp(RoundBinary, "[null, 0, Inf, -Inf, NaN, -NaN, 12]",
                         "[null, null, null, null, null, null, null]",
                         "[null, null, null, null, null, null, null]");
    this->AssertBinaryOp(RoundBinary, values, 0, pair.second);
  }

  // Test different round N-digits for nearest rounding mode
  values = "[320, 3.5, 3.075, 4.5, -3.212, -35.1234, -3.045]";
  std::vector<std::pair<int32_t, std::string>> ndigits_and_expected{{
      {-2, "[300, 0.0, 0.0, 0.0, -0.0, -0.0, -0.0]"},
      {-1, "[320, 0.0, 0.0, 0.0, -0.0, -40, -0.0]"},
      {0, "[320, 4, 3, 5, -3, -35, -3]"},
      {1, "[320, 3.5, 3.1, 4.5, -3.2, -35.1, -3]"},
      {2, "[320, 3.5, 3.08, 4.5, -3.21, -35.12, -3.05]"},
  }};
  this->SetRoundMode(RoundMode::HALF_TOWARDS_INFINITY);
  for (const auto& pair : ndigits_and_expected) {
    this->AssertBinaryOp(RoundBinary, values, pair.first, pair.second);
  }
}

TYPED_TEST_SUITE(TestUnaryRoundToMultipleIntegral, IntegralTypes);
TYPED_TEST_SUITE(TestUnaryRoundToMultipleSigned, SignedIntegerTypes);
TYPED_TEST_SUITE(TestUnaryRoundToMultipleUnsigned, UnsignedIntegerTypes);
TYPED_TEST_SUITE(TestUnaryRoundToMultipleFloating, FloatingTypes);

TYPED_TEST(TestUnaryRoundToMultipleSigned, RoundToMultiple) {
  // Test different rounding modes for integer rounding
  std::string values("[0, 1, -13, -50, 115]");
  this->SetRoundMultiple(1);
  for (const auto& round_mode : kRoundModes) {
    this->SetRoundMode(round_mode);
    this->AssertUnaryOp(RoundToMultiple, values, ArrayFromJSON(float64(), values));
  }

  // Test different round multiples for nearest rounding mode
  std::vector<std::pair<double, std::string>> multiple_and_expected{{
      {2, "[0.0, 2, -14, -50, 116]"},
      {0.05, "[0.0, 1, -13, -50, 115]"},
      {0.1, values},
      {10, "[0.0, 0.0, -10, -50, 120]"},
      {100, "[0.0, 0.0, -0.0, -100, 100]"},
  }};
  this->SetRoundMode(RoundMode::HALF_TOWARDS_INFINITY);
  for (const auto& pair : multiple_and_expected) {
    this->SetRoundMultiple(pair.first);
    this->AssertUnaryOp(RoundToMultiple, values, ArrayFromJSON(float64(), pair.second));
  }
}

TYPED_TEST(TestUnaryRoundToMultipleUnsigned, RoundToMultiple) {
  // Test different rounding modes for integer rounding
  std::string values("[0, 1, 13, 50, 115]");
  this->SetRoundMultiple(1);
  for (const auto& round_mode : kRoundModes) {
    this->SetRoundMode(round_mode);
    this->AssertUnaryOp(RoundToMultiple, values, ArrayFromJSON(float64(), values));
  }

  // Test different round multiples for nearest rounding mode
  std::vector<std::pair<double, std::string>> multiple_and_expected{{
      {0.05, "[0, 1, 13, 50, 115]"},
      {0.1, values},
      {2, "[0, 2, 14, 50, 116]"},
      {10, "[0, 0, 10, 50, 120]"},
      {100, "[0, 0, 0, 100, 100]"},
  }};
  this->SetRoundMode(RoundMode::HALF_TOWARDS_INFINITY);
  for (const auto& pair : multiple_and_expected) {
    this->SetRoundMultiple(pair.first);
    this->AssertUnaryOp(RoundToMultiple, values, ArrayFromJSON(float64(), pair.second));
  }
}

TYPED_TEST(TestUnaryRoundToMultipleFloating, RoundToMultiple) {
  this->SetNansEqual(true);

  // Test different rounding modes for integer rounding
  std::string values("[3.2, 3.5, 3.7, 4.5, -3.2, -3.5, -3.7]");
  std::vector<std::pair<RoundMode, std::string>> rmode_and_expected{{
      {RoundMode::DOWN, "[3, 3, 3, 4, -4, -4, -4]"},
      {RoundMode::UP, "[4, 4, 4, 5, -3, -3, -3]"},
      {RoundMode::TOWARDS_ZERO, "[3, 3, 3, 4, -3, -3, -3]"},
      {RoundMode::TOWARDS_INFINITY, "[4, 4, 4, 5, -4, -4, -4]"},
      {RoundMode::HALF_DOWN, "[3, 3, 4, 4, -3, -4, -4]"},
      {RoundMode::HALF_UP, "[3, 4, 4, 5, -3, -3, -4]"},
      {RoundMode::HALF_TOWARDS_ZERO, "[3, 3, 4, 4, -3, -3, -4]"},
      {RoundMode::HALF_TOWARDS_INFINITY, "[3, 4, 4, 5, -3, -4, -4]"},
      {RoundMode::HALF_TO_EVEN, "[3, 4, 4, 4, -3, -4, -4]"},
      {RoundMode::HALF_TO_ODD, "[3, 3, 4, 5, -3, -3, -4]"},
  }};
  this->SetRoundMultiple(1);
  for (const auto& pair : rmode_and_expected) {
    this->SetRoundMode(pair.first);
    this->AssertUnaryOp(RoundToMultiple, "[]", "[]");
    this->AssertUnaryOp(RoundToMultiple, "[null, 0, Inf, -Inf, NaN, -NaN]",
                        "[null, 0, Inf, -Inf, NaN, -NaN]");
    this->AssertUnaryOp(RoundToMultiple, values, pair.second);
  }

  // Test different round multiples for nearest rounding mode
  values = "[320, 3.5, 3.075, 4.5, -3.212, -35.1234, -3.045]";
  std::vector<std::pair<double, std::string>> multiple_and_expected{{
      {0.05, "[320, 3.5, 3.1, 4.5, -3.2, -35.1, -3.05]"},
      {0.1, "[320, 3.5, 3.1, 4.5, -3.2, -35.1, -3]"},
      {2, "[320, 4, 4, 4, -4, -36, -4]"},
      {10, "[320, 0.0, 0.0, 0.0, -0.0, -40, -0.0]"},
      {100, "[300, 0.0, 0.0, 0.0, -0.0, -0.0, -0.0]"},
  }};
  this->SetRoundMode(RoundMode::HALF_TOWARDS_INFINITY);
  for (const auto& pair : multiple_and_expected) {
    this->SetRoundMultiple(pair.first);
    this->AssertUnaryOp(RoundToMultiple, values, pair.second);
  }

  this->SetRoundMultiple(-2);
  this->AssertUnaryOpRaises(RoundToMultiple, values,
                            "Rounding multiple must be positive");
}

TYPED_TEST(TestUnaryRoundArithmeticSigned, Floor) {
  auto floor = [](const Datum& arg, const ArithmeticOptions&, ExecContext* ctx) {
    return Floor(arg, ctx);
  };

  this->AssertUnaryOp(floor, "[]", ArrayFromJSON(float64(), "[]"));
  this->AssertUnaryOp(floor, "[null]", ArrayFromJSON(float64(), "[null]"));
  this->AssertUnaryOp(floor, "[1, null, -10]",
                      ArrayFromJSON(float64(), "[1, null, -10]"));
  this->AssertUnaryOp(floor, "[0]", ArrayFromJSON(float64(), "[0]"));
  this->AssertUnaryOp(floor, "[1, 10, 127]", ArrayFromJSON(float64(), "[1, 10, 127]"));
  this->AssertUnaryOp(floor, "[-1, -10, -127]",
                      ArrayFromJSON(float64(), "[-1, -10, -127]"));
}

TYPED_TEST(TestUnaryRoundArithmeticUnsigned, Floor) {
  auto floor = [](const Datum& arg, const ArithmeticOptions&, ExecContext* ctx) {
    return Floor(arg, ctx);
  };

  this->AssertUnaryOp(floor, "[]", ArrayFromJSON(float64(), "[]"));
  this->AssertUnaryOp(floor, "[null]", ArrayFromJSON(float64(), "[null]"));
  this->AssertUnaryOp(floor, "[1, null, 10]", ArrayFromJSON(float64(), "[1, null, 10]"));
  this->AssertUnaryOp(floor, "[0]", ArrayFromJSON(float64(), "[0]"));
  this->AssertUnaryOp(floor, "[1, 10, 127]", ArrayFromJSON(float64(), "[1, 10, 127]"));
}

TYPED_TEST(TestUnaryRoundArithmeticFloating, Floor) {
  using CType = typename TestFixture::CType;
  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->SetNansEqual(true);

  auto floor = [](const Datum& arg, const ArithmeticOptions&, ExecContext* ctx) {
    return Floor(arg, ctx);
  };

  this->AssertUnaryOp(floor, "[]", "[]");
  this->AssertUnaryOp(floor, "[null]", "[null]");
  this->AssertUnaryOp(floor, "[1.3, null, -10.80]", "[1, null, -11]");
  // XXX Python uses math.floor(-0.0) == 0.0, but std::floor() keeps the sign
  this->AssertUnaryOp(floor, "[0.0, -0.0]", "[0.0, -0.0]");
  this->AssertUnaryOp(floor, "[1.3, 10.80, 12748.001]", "[1, 10, 12748]");
  this->AssertUnaryOp(floor, "[-1.3, -10.80, -12748.001]", "[-2, -11, -12749]");
  this->AssertUnaryOp(floor, "[Inf, -Inf]", "[Inf, -Inf]");
  this->AssertUnaryOp(floor, "[NaN]", "[NaN]");
  this->AssertUnaryOp(floor, this->MakeScalar(min), this->MakeScalar(min));
  this->AssertUnaryOp(floor, this->MakeScalar(max), this->MakeScalar(max));
}

TYPED_TEST(TestUnaryRoundArithmeticSigned, Ceil) {
  auto ceil = [](const Datum& arg, const ArithmeticOptions&, ExecContext* ctx) {
    return Ceil(arg, ctx);
  };

  this->AssertUnaryOp(ceil, "[]", ArrayFromJSON(float64(), "[]"));
  this->AssertUnaryOp(ceil, "[null]", ArrayFromJSON(float64(), "[null]"));
  this->AssertUnaryOp(ceil, "[1, null, -10]", ArrayFromJSON(float64(), "[1, null, -10]"));
  this->AssertUnaryOp(ceil, "[0]", ArrayFromJSON(float64(), "[0]"));
  this->AssertUnaryOp(ceil, "[1, 10, 127]", ArrayFromJSON(float64(), "[1, 10, 127]"));
  this->AssertUnaryOp(ceil, "[-1, -10, -127]",
                      ArrayFromJSON(float64(), "[-1, -10, -127]"));
}

TYPED_TEST(TestUnaryRoundArithmeticUnsigned, Ceil) {
  auto ceil = [](const Datum& arg, const ArithmeticOptions&, ExecContext* ctx) {
    return Ceil(arg, ctx);
  };

  this->AssertUnaryOp(ceil, "[]", ArrayFromJSON(float64(), "[]"));
  this->AssertUnaryOp(ceil, "[null]", ArrayFromJSON(float64(), "[null]"));
  this->AssertUnaryOp(ceil, "[1, null, 10]", ArrayFromJSON(float64(), "[1, null, 10]"));
  this->AssertUnaryOp(ceil, "[0]", ArrayFromJSON(float64(), "[0]"));
  this->AssertUnaryOp(ceil, "[1, 10, 127]", ArrayFromJSON(float64(), "[1, 10, 127]"));
}

TYPED_TEST(TestUnaryRoundArithmeticFloating, Ceil) {
  using CType = typename TestFixture::CType;
  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->SetNansEqual(true);

  auto ceil = [](const Datum& arg, const ArithmeticOptions&, ExecContext* ctx) {
    return Ceil(arg, ctx);
  };

  this->AssertUnaryOp(ceil, "[]", "[]");
  this->AssertUnaryOp(ceil, "[null]", "[null]");
  this->AssertUnaryOp(ceil, "[1.3, null, -10.80]", "[2, null, -10]");
  // XXX same comment as Floor above
  this->AssertUnaryOp(ceil, "[0.0, -0.0]", "[0.0, -0.0]");
  this->AssertUnaryOp(ceil, "[1.3, 10.80, 12748.001]", "[2, 11, 12749]");
  this->AssertUnaryOp(ceil, "[-1.3, -10.80, -12748.001]", "[-1, -10, -12748]");
  this->AssertUnaryOp(ceil, "[Inf, -Inf]", "[Inf, -Inf]");
  this->AssertUnaryOp(ceil, "[NaN]", "[NaN]");
  this->AssertUnaryOp(ceil, this->MakeScalar(min), this->MakeScalar(min));
  this->AssertUnaryOp(ceil, this->MakeScalar(max), this->MakeScalar(max));
}

TYPED_TEST(TestUnaryRoundArithmeticSigned, Trunc) {
  auto trunc = [](const Datum& arg, const ArithmeticOptions&, ExecContext* ctx) {
    return Trunc(arg, ctx);
  };

  this->AssertUnaryOp(trunc, "[]", ArrayFromJSON(float64(), "[]"));
  this->AssertUnaryOp(trunc, "[null]", ArrayFromJSON(float64(), "[null]"));
  this->AssertUnaryOp(trunc, "[1, null, -10]",
                      ArrayFromJSON(float64(), "[1, null, -10]"));
  this->AssertUnaryOp(trunc, "[0]", ArrayFromJSON(float64(), "[0]"));
  this->AssertUnaryOp(trunc, "[1, 10, 127]", ArrayFromJSON(float64(), "[1, 10, 127]"));
  this->AssertUnaryOp(trunc, "[-1, -10, -127]",
                      ArrayFromJSON(float64(), "[-1, -10, -127]"));
}

TYPED_TEST(TestUnaryRoundArithmeticUnsigned, Trunc) {
  auto trunc = [](const Datum& arg, const ArithmeticOptions&, ExecContext* ctx) {
    return Trunc(arg, ctx);
  };

  this->AssertUnaryOp(trunc, "[]", ArrayFromJSON(float64(), "[]"));
  this->AssertUnaryOp(trunc, "[null]", ArrayFromJSON(float64(), "[null]"));
  this->AssertUnaryOp(trunc, "[1, null, 10]", ArrayFromJSON(float64(), "[1, null, 10]"));
  this->AssertUnaryOp(trunc, "[0]", ArrayFromJSON(float64(), "[0]"));
  this->AssertUnaryOp(trunc, "[1, 10, 127]", ArrayFromJSON(float64(), "[1, 10, 127]"));
}

TYPED_TEST(TestUnaryRoundArithmeticFloating, Trunc) {
  using CType = typename TestFixture::CType;
  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->SetNansEqual(true);

  auto trunc = [](const Datum& arg, const ArithmeticOptions&, ExecContext* ctx) {
    return Trunc(arg, ctx);
  };

  this->AssertUnaryOp(trunc, "[]", "[]");
  this->AssertUnaryOp(trunc, "[null]", "[null]");
  this->AssertUnaryOp(trunc, "[1.3, null, -10.80]", "[1, null, -10]");
  // XXX same comment as Floor above
  this->AssertUnaryOp(trunc, "[0.0, -0.0]", "[0.0, -0.0]");
  this->AssertUnaryOp(trunc, "[1.3, 10.80, 12748.001]", "[1, 10, 12748]");
  this->AssertUnaryOp(trunc, "[-1.3, -10.80, -12748.001]", "[-1, -10, -12748]");
  this->AssertUnaryOp(trunc, "[Inf, -Inf]", "[Inf, -Inf]");
  this->AssertUnaryOp(trunc, "[NaN]", "[NaN]");
  this->AssertUnaryOp(trunc, this->MakeScalar(min), this->MakeScalar(min));
  this->AssertUnaryOp(trunc, this->MakeScalar(max), this->MakeScalar(max));
}

}  // namespace
}  // namespace compute
}  // namespace arrow
