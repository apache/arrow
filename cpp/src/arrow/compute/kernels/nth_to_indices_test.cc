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

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "arrow/compute/context.h"
#include "arrow/compute/kernels/nth_to_indices.h"
#include "arrow/compute/test_util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace compute {

template <typename ArrayType>
class Comparator {
 public:
  bool operator()(const ArrayType& array, uint64_t lhs, uint64_t rhs) {
    if (array.IsNull(rhs)) return true;
    if (array.IsNull(lhs)) return false;
    return array.Value(lhs) <= array.Value(rhs);
  }
};

template <>
class Comparator<StringArray> {
 public:
  bool operator()(const BinaryArray& array, uint64_t lhs, uint64_t rhs) {
    if (array.IsNull(rhs)) return true;
    if (array.IsNull(lhs)) return false;
    return array.GetView(lhs) <= array.GetView(rhs);
  }
};

template <typename ArrowType>
class TestNthToIndicesKernel : public ComputeFixture, public TestBase {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 private:
  template <typename ArrayType>
  void ValidateNth(const ArrayType& array, int n, UInt64Array& offsets) {
    if (n >= array.length()) {
      for (int i = 0; i < array.length(); ++i) {
        ASSERT_TRUE(offsets.Value(i) == (uint64_t)i);
      }
    } else {
      Comparator<ArrayType> compare;
      uint64_t nth = offsets.Value(n);

      for (int i = 0; i < n; ++i) {
        uint64_t lhs = offsets.Value(i);
        ASSERT_TRUE(compare(array, lhs, nth));
      }
      for (int i = n + 1; i < array.length(); ++i) {
        uint64_t rhs = offsets.Value(i);
        ASSERT_TRUE(compare(array, nth, rhs));
      }
    }
  }

 protected:
  void AssertNthToIndicesArray(const std::shared_ptr<Array> values, int n) {
    std::shared_ptr<Array> offsets;
    ASSERT_OK(arrow::compute::NthToIndices(&this->ctx_, *values, n, &offsets));
    ASSERT_OK(offsets->ValidateFull());
    ValidateNth<ArrayType>(*checked_pointer_cast<ArrayType>(values), n,
                           *checked_pointer_cast<UInt64Array>(offsets));
  }

  void AssertNthToIndicesJson(const std::string& values, int n) {
    auto type = TypeTraits<ArrowType>::type_singleton();
    AssertNthToIndicesArray(ArrayFromJSON(type, values), n);
  }
};

template <typename ArrowType>
class TestNthToIndicesKernelForReal : public TestNthToIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestNthToIndicesKernelForReal, RealArrowTypes);

template <typename ArrowType>
class TestNthToIndicesKernelForIntegral : public TestNthToIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestNthToIndicesKernelForIntegral, IntegralArrowTypes);

template <typename ArrowType>
class TestNthToIndicesKernelForStrings : public TestNthToIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestNthToIndicesKernelForStrings, testing::Types<StringType>);

TYPED_TEST(TestNthToIndicesKernelForReal, NthReal) {
  this->AssertNthToIndicesJson("[null, 1, 3.3, null, 2, 5.3]", 0);
  this->AssertNthToIndicesJson("[null, 1, 3.3, null, 2, 5.3]", 2);
  this->AssertNthToIndicesJson("[null, 1, 3.3, null, 2, 5.3]", 5);
  this->AssertNthToIndicesJson("[null, 1, 3.3, null, 2, 5.3]", 6);
}

TYPED_TEST(TestNthToIndicesKernelForIntegral, NthIntegral) {
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 0);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 2);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 5);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 6);
}

TYPED_TEST(TestNthToIndicesKernelForStrings, NthStrings) {
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 0);
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 2);
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 5);
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 6);
}

template <typename ArrowType>
class TestNthToIndicesKernelRandom : public TestNthToIndicesKernel<ArrowType> {};

using NthToIndicesableTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, StringType>;

class RandomImpl {
 protected:
  random::RandomArrayGenerator generator;

 public:
  explicit RandomImpl(random::SeedType seed) : generator(seed) {}
};

template <typename ArrowType>
class Random : public RandomImpl {
  using CType = typename TypeTraits<ArrowType>::CType;

 public:
  explicit Random(random::SeedType seed) : RandomImpl(seed) {}

  std::shared_ptr<Array> Generate(uint64_t count, double null_prob) {
    return generator.Numeric<ArrowType>(count, std::numeric_limits<CType>::min(),
                                        std::numeric_limits<CType>::max(), null_prob);
  }
};

template <>
class Random<StringType> : public RandomImpl {
 public:
  explicit Random(random::SeedType seed) : RandomImpl(seed) {}

  std::shared_ptr<Array> Generate(uint64_t count, double null_prob) {
    return generator.String(count, 1, 100, null_prob);
  }
};

TYPED_TEST_SUITE(TestNthToIndicesKernelRandom, NthToIndicesableTypes);

TYPED_TEST(TestNthToIndicesKernelRandom, NthRandomValues) {
  Random<TypeParam> rand(0x61549225);
  int length = 100;
  for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
    // Try n from 0 to out of bound
    for (int n = 0; n <= length; ++n) {
      auto array = rand.Generate(length, null_probability);
      this->AssertNthToIndicesArray(array, n);
    }
  }
}

}  // namespace compute
}  // namespace arrow
