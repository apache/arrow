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

#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace compute {

template <typename ArrayType>
class NthComparator {
 public:
  bool operator()(const ArrayType& array, uint64_t lhs, uint64_t rhs) {
    if (array.IsNull(rhs)) return true;
    if (array.IsNull(lhs)) return false;
    return array.GetView(lhs) <= array.GetView(rhs);
  }
};

template <typename ArrayType>
class SortComparator {
 public:
  bool operator()(const ArrayType& array, uint64_t lhs, uint64_t rhs) {
    if (array.IsNull(rhs) && array.IsNull(lhs)) return lhs < rhs;
    if (array.IsNull(rhs)) return true;
    if (array.IsNull(lhs)) return false;
    if (array.GetView(lhs) == array.GetView(rhs)) return lhs < rhs;
    return array.GetView(lhs) < array.GetView(rhs);
  }
};

template <typename ArrowType>
class TestNthToIndices : public TestBase {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 private:
  template <typename ArrayType>
  void Validate(const ArrayType& array, int n, UInt64Array& offsets) {
    if (n >= array.length()) {
      for (int i = 0; i < array.length(); ++i) {
        ASSERT_TRUE(offsets.Value(i) == (uint64_t)i);
      }
    } else {
      NthComparator<ArrayType> compare;
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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets, NthToIndices(*values, n));
    ASSERT_OK(offsets->ValidateFull());
    Validate<ArrayType>(*checked_pointer_cast<ArrayType>(values), n,
                        *checked_pointer_cast<UInt64Array>(offsets));
  }

  void AssertNthToIndicesJson(const std::string& values, int n) {
    AssertNthToIndicesArray(ArrayFromJSON(type_singleton(), values), n);
  }

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

template <typename ArrowType>
class TestNthToIndicesForReal : public TestNthToIndices<ArrowType> {};
TYPED_TEST_SUITE(TestNthToIndicesForReal, RealArrowTypes);

template <typename ArrowType>
class TestNthToIndicesForIntegral : public TestNthToIndices<ArrowType> {};
TYPED_TEST_SUITE(TestNthToIndicesForIntegral, IntegralArrowTypes);

template <typename ArrowType>
class TestNthToIndicesForStrings : public TestNthToIndices<ArrowType> {};
TYPED_TEST_SUITE(TestNthToIndicesForStrings, testing::Types<StringType>);

TYPED_TEST(TestNthToIndicesForReal, NthToIndicesDoesNotProvideDefaultOptions) {
  auto input = ArrayFromJSON(this->type_singleton(), "[null, 1, 3.3, null, 2, 5.3]");
  ASSERT_RAISES(Invalid, CallFunction("partition_nth_indices", {input}));
}

TYPED_TEST(TestNthToIndicesForReal, Real) {
  this->AssertNthToIndicesJson("[null, 1, 3.3, null, 2, 5.3]", 0);
  this->AssertNthToIndicesJson("[null, 1, 3.3, null, 2, 5.3]", 2);
  this->AssertNthToIndicesJson("[null, 1, 3.3, null, 2, 5.3]", 5);
  this->AssertNthToIndicesJson("[null, 1, 3.3, null, 2, 5.3]", 6);
}

TYPED_TEST(TestNthToIndicesForIntegral, Integral) {
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 0);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 2);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 5);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 6);
}

TYPED_TEST(TestNthToIndicesForStrings, Strings) {
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 0);
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 2);
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 5);
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 6);
}

template <typename ArrowType>
class TestNthToIndicesRandom : public TestNthToIndices<ArrowType> {};

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

template <typename ArrowType>
class RandomRange : public RandomImpl {
  using CType = typename TypeTraits<ArrowType>::CType;

 public:
  explicit RandomRange(random::SeedType seed) : RandomImpl(seed) {}

  std::shared_ptr<Array> Generate(uint64_t count, int range, double null_prob) {
    CType min = std::numeric_limits<CType>::min();
    CType max = min + range;
    if (sizeof(CType) < 4 && (range + min) > std::numeric_limits<CType>::max()) {
      max = std::numeric_limits<CType>::max();
    }
    return generator.Numeric<ArrowType>(count, min, max, null_prob);
  }
};

TYPED_TEST_SUITE(TestNthToIndicesRandom, NthToIndicesableTypes);

TYPED_TEST(TestNthToIndicesRandom, RandomValues) {
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

using arrow::internal::checked_pointer_cast;

template <typename ArrowType>
class TestSortToIndicesKernel : public TestBase {
 private:
  void AssertSortToIndicesArrays(const std::shared_ptr<Array> values,
                                 const std::shared_ptr<Array> expected) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> actual, SortToIndices(*values));
    ASSERT_OK(actual->ValidateFull());
    AssertArraysEqual(*expected, *actual);
  }

 protected:
  virtual void AssertSortToIndices(const std::string& values,
                                   const std::string& expected) {
    auto type = TypeTraits<ArrowType>::type_singleton();
    AssertSortToIndicesArrays(ArrayFromJSON(type, values),
                              ArrayFromJSON(uint64(), expected));
  }
};

template <typename ArrowType>
class TestSortToIndicesKernelForReal : public TestSortToIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestSortToIndicesKernelForReal, RealArrowTypes);

template <typename ArrowType>
class TestSortToIndicesKernelForIntegral : public TestSortToIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestSortToIndicesKernelForIntegral, IntegralArrowTypes);

template <typename ArrowType>
class TestSortToIndicesKernelForStrings : public TestSortToIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestSortToIndicesKernelForStrings, testing::Types<StringType>);

TYPED_TEST(TestSortToIndicesKernelForReal, SortReal) {
  this->AssertSortToIndices("[]", "[]");

  this->AssertSortToIndices("[3.4, 2.6, 6.3]", "[1, 0, 2]");

  this->AssertSortToIndices("[1.1, 2.4, 3.5, 4.3, 5.1, 6.8, 7.3]", "[0,1,2,3,4,5,6]");

  this->AssertSortToIndices("[7, 6, 5, 4, 3, 2, 1]", "[6,5,4,3,2,1,0]");

  this->AssertSortToIndices("[10.4, 12, 4.2, 50, 50.3, 32, 11]", "[2,0,6,1,5,3,4]");

  this->AssertSortToIndices("[null, 1, 3.3, null, 2, 5.3]", "[1,4,2,5,0,3]");
}

TYPED_TEST(TestSortToIndicesKernelForIntegral, SortIntegral) {
  this->AssertSortToIndices("[]", "[]");

  this->AssertSortToIndices("[3, 2, 6]", "[1, 0, 2]");

  this->AssertSortToIndices("[1, 2, 3, 4, 5, 6, 7]", "[0,1,2,3,4,5,6]");

  this->AssertSortToIndices("[7, 6, 5, 4, 3, 2, 1]", "[6,5,4,3,2,1,0]");

  this->AssertSortToIndices("[10, 12, 4, 50, 50, 32, 11]", "[2,0,6,1,5,3,4]");

  this->AssertSortToIndices("[null, 1, 3, null, 2, 5]", "[1,4,2,5,0,3]");
}

TYPED_TEST(TestSortToIndicesKernelForStrings, SortStrings) {
  this->AssertSortToIndices("[]", "[]");

  this->AssertSortToIndices(R"(["a", "b", "c"])", "[0, 1, 2]");

  this->AssertSortToIndices(R"(["foo", "bar", "baz"])", "[1,2,0]");

  this->AssertSortToIndices(R"(["testing", "sort", "for", "strings"])", "[2, 1, 3, 0]");
}

template <typename ArrowType>
class TestSortToIndicesKernelForUInt8 : public TestSortToIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestSortToIndicesKernelForUInt8, UInt8Type);

template <typename ArrowType>
class TestSortToIndicesKernelForInt8 : public TestSortToIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestSortToIndicesKernelForInt8, Int8Type);

TYPED_TEST(TestSortToIndicesKernelForUInt8, SortUInt8) {
  this->AssertSortToIndices("[255, null, 0, 255, 10, null, 128, 0]", "[2,7,4,6,0,3,1,5]");
}

TYPED_TEST(TestSortToIndicesKernelForInt8, SortInt8) {
  this->AssertSortToIndices("[null, 10, 127, 0, -128, -128, null]", "[4,5,3,1,2,0,6]");
}

template <typename ArrowType>
class TestSortToIndicesKernelRandom : public TestBase {};

template <typename ArrowType>
class TestSortToIndicesKernelRandomCount : public TestBase {};

template <typename ArrowType>
class TestSortToIndicesKernelRandomCompare : public TestBase {};

using SortToIndicesableTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, StringType>;

template <typename ArrayType>
void ValidateSorted(const ArrayType& array, UInt64Array& offsets) {
  ASSERT_OK(array.ValidateFull());
  SortComparator<ArrayType> compare;
  for (int i = 1; i < array.length(); i++) {
    uint64_t lhs = offsets.Value(i - 1);
    uint64_t rhs = offsets.Value(i);
    ASSERT_TRUE(compare(array, lhs, rhs));
  }
}

TYPED_TEST_SUITE(TestSortToIndicesKernelRandom, SortToIndicesableTypes);

TYPED_TEST(TestSortToIndicesKernelRandom, SortRandomValues) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  Random<TypeParam> rand(0x5487655);
  int times = 5;
  int length = 1000;
  for (int test = 0; test < times; test++) {
    for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
      auto array = rand.Generate(length, null_probability);
      ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets, SortToIndices(*array));
      ValidateSorted<ArrayType>(*checked_pointer_cast<ArrayType>(array),
                                *checked_pointer_cast<UInt64Array>(offsets));
    }
  }
}

// Long array with small value range: counting sort
// - length >= 1024(CountCompareSorter::countsort_min_len_)
// - range  <= 4096(CountCompareSorter::countsort_max_range_)
TYPED_TEST_SUITE(TestSortToIndicesKernelRandomCount, IntegralArrowTypes);

TYPED_TEST(TestSortToIndicesKernelRandomCount, SortRandomValuesCount) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  RandomRange<TypeParam> rand(0x5487656);
  int times = 5;
  int length = 4000;
  int range = 2000;
  for (int test = 0; test < times; test++) {
    for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
      auto array = rand.Generate(length, range, null_probability);
      ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets, SortToIndices(*array));
      ValidateSorted<ArrayType>(*checked_pointer_cast<ArrayType>(array),
                                *checked_pointer_cast<UInt64Array>(offsets));
    }
  }
}

// Long array with big value range: std::stable_sort
TYPED_TEST_SUITE(TestSortToIndicesKernelRandomCompare, IntegralArrowTypes);

TYPED_TEST(TestSortToIndicesKernelRandomCompare, SortRandomValuesCompare) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  Random<TypeParam> rand(0x5487657);
  int times = 5;
  int length = 4000;
  for (int test = 0; test < times; test++) {
    for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
      auto array = rand.Generate(length, null_probability);
      ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets, SortToIndices(*array));
      ValidateSorted<ArrayType>(*checked_pointer_cast<ArrayType>(array),
                                *checked_pointer_cast<UInt64Array>(offsets));
    }
  }
}

}  // namespace compute
}  // namespace arrow
