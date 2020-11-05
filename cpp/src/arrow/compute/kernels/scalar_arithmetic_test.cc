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
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/string.h"

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

std::shared_ptr<Array> TweakValidityBit(const std::shared_ptr<Array>& array,
                                        int64_t index, bool validity) {
  auto data = array->data()->Copy();
  if (data->buffers[0] == nullptr) {
    data->buffers[0] = *AllocateBitmap(data->length);
    BitUtil::SetBitsTo(data->buffers[0]->mutable_data(), 0, data->length, true);
  }
  BitUtil::SetBitTo(data->buffers[0]->mutable_data(), index, validity);
  data->null_count = kUnknownNullCount;
  // Need to return a new array, because Array caches the null bitmap pointer
  return MakeArray(data);
}

template <typename T>
class TestBinaryArithmetic : public TestBase {
 protected:
  using ArrowType = T;
  using CType = typename ArrowType::c_type;

  static std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }

  using BinaryFunction = std::function<Result<Datum>(const Datum&, const Datum&,
                                                     ArithmeticOptions, ExecContext*)>;

  void SetUp() { options_.check_overflow = false; }

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
    AssertScalarsEqual(*exp, *actual.scalar(), /*verbose=*/true);
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

  // (Array, Array)
  void AssertBinop(BinaryFunction func, const std::shared_ptr<Array>& left,
                   const std::shared_ptr<Array>& right,
                   const std::string& expected_json) {
    const auto expected = ArrayFromJSON(type_singleton(), expected_json);
    ASSERT_OK_AND_ASSIGN(Datum actual, func(left, right, options_, nullptr));
    ValidateAndAssertApproxEqual(actual.make_array(), expected);

    // Also check (Scalar, Scalar) operations
    const int64_t length = expected->length();
    for (int64_t i = 0; i < length; ++i) {
      const auto expected_scalar = *expected->GetScalar(i);
      ASSERT_OK_AND_ASSIGN(
          actual, func(*left->GetScalar(i), *right->GetScalar(i), options_, nullptr));
      AssertScalarsEqual(*expected_scalar, *actual.scalar(), /*verbose=*/true,
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
    ASSERT_OK(actual->ValidateFull());
    AssertArraysApproxEqual(*expected, *actual, /*verbose=*/true, equal_options_);
  }

  void SetOverflowCheck(bool value = true) { options_.check_overflow = value; }

  void SetNansEqual(bool value = true) {
    this->equal_options_ = equal_options_.nans_equal(value);
  }

  ArithmeticOptions options_ = ArithmeticOptions();
  EqualOptions equal_options_ = EqualOptions::Defaults();
};

template <typename... Elements>
std::string MakeArray(Elements... elements) {
  std::vector<std::string> elements_as_strings = {std::to_string(elements)...};

  std::vector<util::string_view> elements_as_views(sizeof...(Elements));
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

// InputType - OutputType pairs
using IntegralTypes = testing::Types<Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
                                     UInt16Type, UInt32Type, UInt64Type>;

using SignedIntegerTypes = testing::Types<Int8Type, Int16Type, Int32Type, Int64Type>;

using UnsignedIntegerTypes =
    testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type>;

// TODO(kszucs): add half-float
using FloatingTypes = testing::Types<FloatType, DoubleType>;

TYPED_TEST_SUITE(TestBinaryArithmeticIntegral, IntegralTypes);
TYPED_TEST_SUITE(TestBinaryArithmeticSigned, SignedIntegerTypes);
TYPED_TEST_SUITE(TestBinaryArithmeticUnsigned, UnsignedIntegerTypes);
TYPED_TEST_SUITE(TestBinaryArithmeticFloating, FloatingTypes);

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

  for (auto unit : internal::AllTimeUnits()) {
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

  // Scalar on the left
  this->AssertBinop(Multiply, -1.5f, "[0.0, 2.0]", "[0.0, -3.0]");
  this->AssertBinop(Multiply, -1.5f, "[null, 2.0]", "[null, -3.0]");
  this->AssertBinop(Multiply, this->MakeNullScalar(), "[0.0, 2.0]", "[null, null]");
  this->AssertBinop(Multiply, this->MakeNullScalar(), "[null, 2.0]", "[null, null]");
  // Scalar on the right
  this->AssertBinop(Multiply, "[0.0, 2.0]", -1.5f, "[0.0, -3.0]");
  this->AssertBinop(Multiply, "[null, 2.0]", -1.5f, "[null, -3.0]");
  this->AssertBinop(Multiply, "[0.0, 2.0]", this->MakeNullScalar(), "[null, null]");
  this->AssertBinop(Multiply, "[null, 2.0]", this->MakeNullScalar(), "[null, null]");
}

}  // namespace compute
}  // namespace arrow
