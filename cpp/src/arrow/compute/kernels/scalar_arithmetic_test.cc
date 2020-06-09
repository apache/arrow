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
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/string.h"

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

template <typename ArrowType>
class TestBinaryArithmetics : public TestBase {
 protected:
  using CType = typename ArrowType::c_type;

  static std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }

  using BinaryFunction =
      std::function<Result<Datum>(const Datum&, const Datum&, ExecContext*)>;

  // (Scalar, Scalar)
  void AssertBinop(BinaryFunction func, CType lhs, CType rhs, CType expected) {
    ASSERT_OK_AND_ASSIGN(auto left, MakeScalar(type_singleton(), lhs));
    ASSERT_OK_AND_ASSIGN(auto right, MakeScalar(type_singleton(), rhs));
    ASSERT_OK_AND_ASSIGN(auto exp, MakeScalar(type_singleton(), expected));

    ASSERT_OK_AND_ASSIGN(auto actual, func(left, right, nullptr));
    AssertScalarsEqual(*exp, *actual.scalar(), true);
  }

  // (Scalar, Array)
  void AssertBinop(BinaryFunction func, CType lhs, const std::string& rhs,
                   const std::string& expected) {
    ASSERT_OK_AND_ASSIGN(auto left, MakeScalar(type_singleton(), lhs));
    auto right = ArrayFromJSON(type_singleton(), rhs);
    auto exp = ArrayFromJSON(type_singleton(), expected);

    ASSERT_OK_AND_ASSIGN(auto actual, func(left, right, nullptr));
    ValidateAndAssertApproxEqual(actual.make_array(), expected);
  }

  // (Array, Array)
  void AssertBinop(BinaryFunction func, const std::string& lhs, const std::string& rhs,
                   const std::string& expected) {
    auto left = ArrayFromJSON(type_singleton(), lhs);
    auto right = ArrayFromJSON(type_singleton(), rhs);

    ASSERT_OK_AND_ASSIGN(Datum actual, func(left, right, nullptr));
    ValidateAndAssertApproxEqual(actual.make_array(), expected);
  }

  void ValidateAndAssertApproxEqual(std::shared_ptr<Array> actual,
                                    const std::string& expected) {
    auto exp = ArrayFromJSON(type_singleton(), expected);
    ASSERT_OK(actual->ValidateFull());
    AssertArraysApproxEqual(*exp, *actual);
  }
};

template <typename... Elements>
std::string MakeArray(Elements... elements) {
  std::vector<std::string> elements_as_strings = {std::to_string(elements)...};

  std::vector<util::string_view> elements_as_views(sizeof...(Elements));
  std::copy(elements_as_strings.begin(), elements_as_strings.end(),
            elements_as_views.begin());

  return "[" + internal::JoinStrings(elements_as_views, ",") + "]";
}

template <typename T>
class TestBinaryArithmeticsIntegral : public TestBinaryArithmetics<T> {};

template <typename T>
class TestBinaryArithmeticsSigned : public TestBinaryArithmeticsIntegral<T> {};

template <typename T>
class TestBinaryArithmeticsUnsigned : public TestBinaryArithmeticsIntegral<T> {};

template <typename T>
class TestBinaryArithmeticsFloating : public TestBinaryArithmetics<T> {};

// InputType - OutputType pairs
using IntegralTypes = testing::Types<Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
                                     UInt16Type, UInt32Type, UInt64Type>;

using SignedIntegerTypes = testing::Types<Int8Type, Int16Type, Int32Type, Int64Type>;

using UnsignedIntegerTypes =
    testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type>;

// TODO(kszucs): add half-float
using FloatingTypes = testing::Types<FloatType, DoubleType>;

TYPED_TEST_SUITE(TestBinaryArithmeticsIntegral, IntegralTypes);
TYPED_TEST_SUITE(TestBinaryArithmeticsSigned, SignedIntegerTypes);
TYPED_TEST_SUITE(TestBinaryArithmeticsUnsigned, UnsignedIntegerTypes);
TYPED_TEST_SUITE(TestBinaryArithmeticsFloating, FloatingTypes);

TYPED_TEST(TestBinaryArithmeticsIntegral, Add) {
  this->AssertBinop(arrow::compute::Add, "[]", "[]", "[]");
  this->AssertBinop(arrow::compute::Add, "[null]", "[null]", "[null]");
  this->AssertBinop(arrow::compute::Add, "[3, 2, 6]", "[1, 0, 2]", "[4, 2, 8]");

  this->AssertBinop(arrow::compute::Add, "[1, 2, 3, 4, 5, 6, 7]", "[0, 1, 2, 3, 4, 5, 6]",
                    "[1, 3, 5, 7, 9, 11, 13]");

  this->AssertBinop(arrow::compute::Add, "[10, 12, 4, 50, 50, 32, 11]",
                    "[2, 0, 6, 1, 5, 3, 4]", "[12, 12, 10, 51, 55, 35, 15]");

  this->AssertBinop(arrow::compute::Add, "[null, 1, 3, null, 2, 5]", "[1, 4, 2, 5, 0, 3]",
                    "[null, 5, 5, null, 2, 8]");

  this->AssertBinop(arrow::compute::Add, 10, "[null, 1, 3, null, 2, 5]",
                    "[null, 11, 13, null, 12, 15]");

  this->AssertBinop(arrow::compute::Add, 17, 42, 59);
}

TYPED_TEST(TestBinaryArithmeticsIntegral, Sub) {
  this->AssertBinop(arrow::compute::Subtract, "[]", "[]", "[]");
  this->AssertBinop(arrow::compute::Subtract, "[null]", "[null]", "[null]");
  this->AssertBinop(arrow::compute::Subtract, "[3, 2, 6]", "[1, 0, 2]", "[2, 2, 4]");

  this->AssertBinop(arrow::compute::Subtract, "[1, 2, 3, 4, 5, 6, 7]",
                    "[0, 1, 2, 3, 4, 5, 6]", "[1, 1, 1, 1, 1, 1, 1]");

  this->AssertBinop(arrow::compute::Subtract, 10, "[null, 1, 3, null, 2, 5]",
                    "[null, 9, 7, null, 8, 5]");

  this->AssertBinop(arrow::compute::Subtract, 20, 9, 11);
}

TYPED_TEST(TestBinaryArithmeticsIntegral, Mul) {
  this->AssertBinop(arrow::compute::Multiply, "[]", "[]", "[]");
  this->AssertBinop(arrow::compute::Multiply, "[null]", "[null]", "[null]");
  this->AssertBinop(arrow::compute::Multiply, "[3, 2, 6]", "[1, 0, 2]", "[3, 0, 12]");

  this->AssertBinop(arrow::compute::Multiply, "[1, 2, 3, 4, 5, 6, 7]",
                    "[0, 1, 2, 3, 4, 5, 6]", "[0, 2, 6, 12, 20, 30, 42]");

  this->AssertBinop(arrow::compute::Multiply, "[7, 6, 5, 4, 3, 2, 1]",
                    "[6, 5, 4, 3, 2, 1, 0]", "[42, 30, 20, 12, 6, 2, 0]");

  this->AssertBinop(arrow::compute::Multiply, "[null, 1, 3, null, 2, 5]",
                    "[1, 4, 2, 5, 0, 3]", "[null, 4, 6, null, 0, 15]");

  this->AssertBinop(arrow::compute::Multiply, 3, "[null, 1, 3, null, 2, 5]",
                    "[null, 3, 9, null, 6, 15]");

  this->AssertBinop(arrow::compute::Multiply, 6, 7, 42);
}

TYPED_TEST(TestBinaryArithmeticsSigned, Add) {
  this->AssertBinop(arrow::compute::Add, "[-7, 6, 5, 4, 3, 2, 1]",
                    "[-6, 5, -4, 3, -2, 1, 0]", "[-13, 11, 1, 7, 1, 3, 1]");
  this->AssertBinop(arrow::compute::Add, -1, "[-6, 5, -4, 3, -2, 1, 0]",
                    "[-7, 4, -5, 2, -3, 0, -1]");
  this->AssertBinop(arrow::compute::Add, -10, 5, -5);
}

TYPED_TEST(TestBinaryArithmeticsSigned, OverflowWraps) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->AssertBinop(arrow::compute::Add, MakeArray(min, max, max),
                    MakeArray(CType(-1), 1, max), MakeArray(max, min, CType(-2)));

  this->AssertBinop(arrow::compute::Subtract, MakeArray(min, max, min),
                    MakeArray(1, max, max), MakeArray(max, 0, 1));

  this->AssertBinop(arrow::compute::Multiply, MakeArray(min, max, max),
                    MakeArray(max, 2, max), MakeArray(min, CType(-2), 1));
}

TYPED_TEST(TestBinaryArithmeticsUnsigned, OverflowWraps) {
  using CType = typename TestFixture::CType;

  auto min = std::numeric_limits<CType>::lowest();
  auto max = std::numeric_limits<CType>::max();

  this->AssertBinop(arrow::compute::Add, MakeArray(min, max, max),
                    MakeArray(CType(-1), 1, max), MakeArray(max, min, CType(-2)));

  this->AssertBinop(arrow::compute::Subtract, MakeArray(min, max, min),
                    MakeArray(1, max, max), MakeArray(max, 0, 1));

  this->AssertBinop(arrow::compute::Multiply, MakeArray(min, max, max),
                    MakeArray(max, 2, max), MakeArray(min, CType(-2), 1));
}

TYPED_TEST(TestBinaryArithmeticsSigned, Sub) {
  this->AssertBinop(arrow::compute::Subtract, "[0, 1, 2, 3, 4, 5, 6]",
                    "[1, 2, 3, 4, 5, 6, 7]", "[-1, -1, -1, -1, -1, -1, -1]");

  this->AssertBinop(arrow::compute::Subtract, "[0, 0, 0, 0, 0, 0, 0]",
                    "[6, 5, 4, 3, 2, 1, 0]", "[-6, -5, -4, -3, -2, -1, 0]");

  this->AssertBinop(arrow::compute::Subtract, "[10, 12, 4, 50, 50, 32, 11]",
                    "[2, 0, 6, 1, 5, 3, 4]", "[8, 12, -2, 49, 45, 29, 7]");

  this->AssertBinop(arrow::compute::Subtract, "[null, 1, 3, null, 2, 5]",
                    "[1, 4, 2, 5, 0, 3]", "[null, -3, 1, null, 2, 2]");
}

TYPED_TEST(TestBinaryArithmeticsSigned, Mul) {
  this->AssertBinop(arrow::compute::Multiply, "[-10, 12, 4, 50, -5, 32, 11]",
                    "[-2, 0, -6, 1, 5, 3, 4]", "[20, 0, -24, 50, -25, 96, 44]");
  this->AssertBinop(arrow::compute::Multiply, -2, "[-10, 12, 4, 50, -5, 32, 11]",
                    "[20, -24, -8, -100, 10, -64, -22]");
  this->AssertBinop(arrow::compute::Multiply, -5, -5, 25);
}

TYPED_TEST(TestBinaryArithmeticsFloating, Add) {
  this->AssertBinop(arrow::compute::Add, "[]", "[]", "[]");

  this->AssertBinop(arrow::compute::Add, "[3.4, 2.6, 6.3]", "[1, 0, 2]",
                    "[4.4, 2.6, 8.3]");

  this->AssertBinop(arrow::compute::Add, "[1.1, 2.4, 3.5, 4.3, 5.1, 6.8, 7.3]",
                    "[0, 1, 2, 3, 4, 5, 6]", "[1.1, 3.4, 5.5, 7.3, 9.1, 11.8, 13.3]");

  this->AssertBinop(arrow::compute::Add, "[7, 6, 5, 4, 3, 2, 1]", "[6, 5, 4, 3, 2, 1, 0]",
                    "[13, 11, 9, 7, 5, 3, 1]");

  this->AssertBinop(arrow::compute::Add, "[10.4, 12, 4.2, 50, 50.3, 32, 11]",
                    "[2, 0, 6, 1, 5, 3, 4]", "[12.4, 12, 10.2, 51, 55.3, 35, 15]");

  this->AssertBinop(arrow::compute::Add, "[null, 1, 3.3, null, 2, 5.3]",
                    "[1, 4, 2, 5, 0, 3]", "[null, 5, 5.3, null, 2, 8.3]");

  this->AssertBinop(arrow::compute::Add, 1.1F, "[null, 1, 3.3, null, 2, 5.3]",
                    "[null, 2.1, 4.4, null, 3.1, 6.4]");
}

TYPED_TEST(TestBinaryArithmeticsFloating, Sub) {
  this->AssertBinop(arrow::compute::Subtract, "[]", "[]", "[]");

  this->AssertBinop(arrow::compute::Subtract, "[3.4, 2.6, 6.3]", "[1, 0, 2]",
                    "[2.4, 2.6, 4.3]");

  this->AssertBinop(arrow::compute::Subtract, "[1.1, 2.4, 3.5, 4.3, 5.1, 6.8, 7.3]",
                    "[0.1, 1.2, 2.3, 3.4, 4.5, 5.6, 6.7]",
                    "[1.0, 1.2, 1.2, 0.9, 0.6, 1.2, 0.6]");

  this->AssertBinop(arrow::compute::Subtract, "[7, 6, 5, 4, 3, 2, 1]",
                    "[6, 5, 4, 3, 2, 1, 0]", "[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]");

  this->AssertBinop(arrow::compute::Subtract, "[10.4, 12, 4.2, 50, 50.3, 32, 11]",
                    "[2, 0, 6, 1, 5, 3, 4]", "[8.4, 12, -1.8, 49, 45.3, 29, 7]");

  this->AssertBinop(arrow::compute::Subtract, "[null, 1, 3.3, null, 2, 5.3]",
                    "[1, 4, 2, 5, 0, 3]", "[null, -3, 1.3, null, 2, 2.3]");

  this->AssertBinop(arrow::compute::Subtract, 0.1F, "[null, 1, 3.3, null, 2, 5.3]",
                    "[null, -0.9, -3.2, null, -1.9, -5.2]");
}

}  // namespace compute
}  // namespace arrow
