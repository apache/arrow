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

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

template <typename TypePair>
class TestBinaryArithmetics;

template <typename I, typename O>
class TestBinaryArithmetics<std::pair<I, O>> : public TestBase {
 protected:
  using InputType = I;
  using OutputType = O;
  using InputCType = typename I::c_type;
  using OutputCType = typename O::c_type;

  using BinaryFunction = std::function<Result<Datum>(const Datum&, const Datum&, ExecContext*)>;

  static InputCType GetMin() {
    return std::numeric_limits<InputCType>::min();
  }

  static InputCType GetMax() {
    return std::numeric_limits<InputCType>::max();
  }

  std::shared_ptr<Array> MakeInputArray(const std::vector<InputCType>& values) {
    std::shared_ptr<Array> out;
    ArrayFromVector<InputType>(values, &out);
    return out;
  }

  std::shared_ptr<Array> MakeOutputArray(const std::vector<OutputCType>& values) {
    std::shared_ptr<Array> out;
    ArrayFromVector<OutputType>(values, &out);
    return out;
  }

  virtual void AssertBinop(BinaryFunction func,
                           const std::shared_ptr<Array>& lhs,
                           const std::shared_ptr<Array>& rhs,
                           const std::shared_ptr<Array>& expected) {
    ASSERT_OK_AND_ASSIGN(Datum result, func(lhs, rhs, nullptr));
    std::shared_ptr<Array> out = result.make_array();
    ASSERT_OK(out->ValidateFull());
    AssertArraysEqual(*expected, *out);
  }

  virtual void AssertBinop(BinaryFunction func, const std::string& lhs,
                           const std::string& rhs, const std::string& expected) {
    auto input_type = TypeTraits<InputType>::type_singleton();
    auto output_type = TypeTraits<OutputType>::type_singleton();
    AssertBinop(func, ArrayFromJSON(input_type, lhs), ArrayFromJSON(input_type, rhs),
                ArrayFromJSON(output_type, expected));
  }
};

template <typename TypePair>
class TestBinaryArithmeticsIntegral : public TestBinaryArithmetics<TypePair> {};

template <typename TypePair>
class TestBinaryArithmeticsFloating : public TestBinaryArithmetics<TypePair> {};

// InputType - OutputType pairs
using IntegralPairs = testing::Types<
  // std::pair<Int8Type, Int16Type>,
  std::pair<Int16Type, Int32Type>,
  std::pair<Int32Type, Int64Type>,
  std::pair<Int64Type, Int64Type>,
  // std::pair<UInt8Type, UInt16Type>,
  // std::pair<UInt16Type, UInt32Type>,
  std::pair<UInt32Type, UInt64Type>,
  std::pair<UInt64Type, UInt64Type>
>;

// InputType - OutputType pairs
using FloatingPairs = testing::Types<
  // std::pair<HalfFloatType, HalfFloatType>,
  std::pair<FloatType, FloatType>,
  std::pair<DoubleType, DoubleType>
>;

TYPED_TEST_SUITE(TestBinaryArithmeticsIntegral, IntegralPairs);
TYPED_TEST_SUITE(TestBinaryArithmeticsFloating, FloatingPairs);

TYPED_TEST(TestBinaryArithmeticsIntegral, Add) {
  this->AssertBinop(arrow::compute::Add, "[]", "[]", "[]");
  this->AssertBinop(arrow::compute::Add, "[]", "[]", "[]");
  this->AssertBinop(arrow::compute::Add, "[3, 2, 6]", "[1, 0, 2]", "[4, 2, 8]");

  this->AssertBinop(arrow::compute::Add, "[1, 2, 3, 4, 5, 6, 7]",
                    "[0, 1, 2, 3, 4, 5, 6]", "[1, 3, 5, 7, 9, 11, 13]");

  this->AssertBinop(arrow::compute::Add, "[7, 6, 5, 4, 3, 2, 1]",
                    "[6, 5, 4, 3, 2, 1, 0]", "[13, 11, 9, 7, 5, 3, 1]");

  this->AssertBinop(arrow::compute::Add, "[10, 12, 4, 50, 50, 32, 11]",
                    "[2, 0, 6, 1, 5, 3, 4]", "[12, 12, 10, 51, 55, 35, 15]");

  this->AssertBinop(arrow::compute::Add, "[null, 1, 3, null, 2, 5]",
                    "[1, 4, 2, 5, 0, 3]", "[null, 5, 5, null, 2, 8]");
}

// If I uncomment the commented out signed integer pairs above then clang gives me the following
// warning:
//
// ../src/arrow/compute/kernels/scalar_arithmetic_test.cc:150:64: note: insert an explicit cast to silence this issue
//   auto expected = this->MakeOutputArray({1, 12, 14, min + min, max + max});
//                                                                ^~~~~~~~~
//                                                                static_cast<unsigned short>( )
//
// Since I don't really access the types within this typed test, I assume I need a different
// approach?
TYPED_TEST(TestBinaryArithmeticsIntegral, AddCheckExtremes) {
  auto min = this->GetMin();
  auto max = this->GetMax();

  auto left = this->MakeInputArray({1, 2, 3, min, max});
  auto right = this->MakeInputArray({0, 10, 11, min, max});
  auto expected = this->MakeOutputArray({1, 12, 14, min + min, max + max});

  this->AssertBinop(arrow::compute::Add, left, right, expected);
}

TYPED_TEST(TestBinaryArithmeticsFloating, Add) {
  this->AssertBinop(arrow::compute::Add, "[]", "[]", "[]");

  this->AssertBinop(arrow::compute::Add, "[3.4, 2.6, 6.3]", "[1, 0, 2]", "[4.4, 2.6, 8.3]");

  this->AssertBinop(arrow::compute::Add, "[1.1, 2.4, 3.5, 4.3, 5.1, 6.8, 7.3]", "[0, 1, 2, 3, 4, 5, 6]",
                  "[1.1, 3.4, 5.5, 7.3, 9.1, 11.8, 13.3]");

  this->AssertBinop(arrow::compute::Add, "[7, 6, 5, 4, 3, 2, 1]", "[6, 5, 4, 3, 2, 1, 0]",
                  "[13, 11, 9, 7, 5, 3, 1]");

  this->AssertBinop(arrow::compute::Add, "[10.4, 12, 4.2, 50, 50.3, 32, 11]", "[2, 0, 6, 1, 5, 3, 4]",
                  "[12.4, 12, 10.2, 51, 55.3, 35, 15]");

  this->AssertBinop(arrow::compute::Add, "[null, 1, 3.3, null, 2, 5.3]", "[1, 4, 2, 5, 0, 3]",
                  "[null, 5, 5.3, null, 2, 8.3]");
}

}  // namespace compute
}  // namespace arrow
