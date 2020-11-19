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

#include "arrow/array/concatenate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/table.h"
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
    if (is_floating_type<typename ArrayType::TypeClass>::value) {
      if (array.GetView(rhs) != array.GetView(rhs)) return true;
      if (array.GetView(lhs) != array.GetView(lhs)) return false;
    }
    return array.GetView(lhs) <= array.GetView(rhs);
  }
};

template <typename ArrayType>
class SortComparator {
 public:
  bool operator()(const ArrayType& array, SortOrder order, uint64_t lhs, uint64_t rhs) {
    if (array.IsNull(rhs) && array.IsNull(lhs)) return lhs < rhs;
    if (array.IsNull(rhs)) return true;
    if (array.IsNull(lhs)) return false;
    if (is_floating_type<typename ArrayType::TypeClass>::value) {
      const bool lhs_isnan = array.GetView(lhs) != array.GetView(lhs);
      const bool rhs_isnan = array.GetView(rhs) != array.GetView(rhs);
      if (lhs_isnan && rhs_isnan) return lhs < rhs;
      if (rhs_isnan) return true;
      if (lhs_isnan) return false;
    }
    if (array.GetView(lhs) == array.GetView(rhs)) return lhs < rhs;
    if (order == SortOrder::Ascending) {
      return array.GetView(lhs) < array.GetView(rhs);
    } else {
      return array.GetView(lhs) > array.GetView(rhs);
    }
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

  this->AssertNthToIndicesJson("[null, 2, NaN, 3, 1]", 0);
  this->AssertNthToIndicesJson("[null, 2, NaN, 3, 1]", 1);
  this->AssertNthToIndicesJson("[null, 2, NaN, 3, 1]", 2);
  this->AssertNthToIndicesJson("[null, 2, NaN, 3, 1]", 3);
  this->AssertNthToIndicesJson("[null, 2, NaN, 3, 1]", 4);
  this->AssertNthToIndicesJson("[NaN, 2, null, 3, 1]", 3);
  this->AssertNthToIndicesJson("[NaN, 2, null, 3, 1]", 4);
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
class TestArraySortIndicesKernel : public TestBase {
 private:
  void AssertArraysSortIndices(const std::shared_ptr<Array> values, SortOrder order,
                               const std::shared_ptr<Array> expected) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> actual, SortIndices(*values, order));
    ASSERT_OK(actual->ValidateFull());
    AssertArraysEqual(*expected, *actual);
  }

 protected:
  virtual void AssertSortIndices(const std::string& values, SortOrder order,
                                 const std::string& expected) {
    auto type = TypeTraits<ArrowType>::type_singleton();
    AssertArraysSortIndices(ArrayFromJSON(type, values), order,
                            ArrayFromJSON(uint64(), expected));
  }

  virtual void AssertSortIndices(const std::string& values, const std::string& expected) {
    AssertSortIndices(values, SortOrder::Ascending, expected);
  }
};

template <typename ArrowType>
class TestArraySortIndicesKernelForReal : public TestArraySortIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesKernelForReal, RealArrowTypes);

template <typename ArrowType>
class TestArraySortIndicesKernelForIntegral
    : public TestArraySortIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesKernelForIntegral, IntegralArrowTypes);

template <typename ArrowType>
class TestArraySortIndicesKernelForStrings
    : public TestArraySortIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesKernelForStrings, testing::Types<StringType>);

TYPED_TEST(TestArraySortIndicesKernelForReal, SortReal) {
  this->AssertSortIndices("[]", "[]");

  this->AssertSortIndices("[3.4, 2.6, 6.3]", "[1, 0, 2]");
  this->AssertSortIndices("[1.1, 2.4, 3.5, 4.3, 5.1, 6.8, 7.3]", "[0, 1, 2, 3, 4, 5, 6]");
  this->AssertSortIndices("[7, 6, 5, 4, 3, 2, 1]", "[6, 5, 4, 3, 2, 1, 0]");
  this->AssertSortIndices("[10.4, 12, 4.2, 50, 50.3, 32, 11]", "[2, 0, 6, 1, 5, 3, 4]");

  this->AssertSortIndices("[null, 1, 3.3, null, 2, 5.3]", SortOrder::Ascending,
                          "[1, 4, 2, 5, 0, 3]");
  this->AssertSortIndices("[null, 1, 3.3, null, 2, 5.3]", SortOrder::Descending,
                          "[5, 2, 4, 1, 0, 3]");

  this->AssertSortIndices("[3, 4, NaN, 1, 2, null]", SortOrder::Ascending,
                          "[3, 4, 0, 1, 2, 5]");
  this->AssertSortIndices("[3, 4, NaN, 1, 2, null]", SortOrder::Descending,
                          "[1, 0, 4, 3, 2, 5]");
  this->AssertSortIndices("[NaN, 2, NaN, 3, 1]", SortOrder::Ascending, "[4, 1, 3, 0, 2]");
  this->AssertSortIndices("[NaN, 2, NaN, 3, 1]", SortOrder::Descending,
                          "[3, 1, 4, 0, 2]");
  this->AssertSortIndices("[null, NaN, NaN, null]", SortOrder::Ascending, "[1, 2, 0, 3]");
  this->AssertSortIndices("[null, NaN, NaN, null]", SortOrder::Descending,
                          "[1, 2, 0, 3]");
}

TYPED_TEST(TestArraySortIndicesKernelForIntegral, SortIntegral) {
  this->AssertSortIndices("[]", "[]");

  this->AssertSortIndices("[3, 2, 6]", "[1, 0, 2]");
  this->AssertSortIndices("[1, 2, 3, 4, 5, 6, 7]", "[0, 1, 2, 3, 4, 5, 6]");
  this->AssertSortIndices("[7, 6, 5, 4, 3, 2, 1]", "[6, 5, 4, 3, 2, 1, 0]");

  this->AssertSortIndices("[10, 12, 4, 50, 50, 32, 11]", SortOrder::Ascending,
                          "[2, 0, 6, 1, 5, 3, 4]");
  this->AssertSortIndices("[10, 12, 4, 50, 50, 32, 11]", SortOrder::Descending,
                          "[3, 4, 5, 1, 6, 0, 2]");

  this->AssertSortIndices("[null, 1, 3, null, 2, 5]", SortOrder::Ascending,
                          "[1, 4, 2, 5, 0, 3]");
  this->AssertSortIndices("[null, 1, 3, null, 2, 5]", SortOrder::Descending,
                          "[5, 2, 4, 1, 0, 3]");
}

TYPED_TEST(TestArraySortIndicesKernelForStrings, SortStrings) {
  this->AssertSortIndices("[]", "[]");

  this->AssertSortIndices(R"(["a", "b", "c"])", "[0, 1, 2]");
  this->AssertSortIndices(R"(["foo", "bar", "baz"])", "[1,2,0]");
  this->AssertSortIndices(R"(["testing", "sort", "for", "strings"])", "[2, 1, 3, 0]");

  this->AssertSortIndices(R"(["c", "b", "a", "b"])", SortOrder::Ascending,
                          "[2, 1, 3, 0]");
  this->AssertSortIndices(R"(["c", "b", "a", "b"])", SortOrder::Descending,
                          "[0, 1, 3, 2]");
}

template <typename ArrowType>
class TestArraySortIndicesKernelForUInt8 : public TestArraySortIndicesKernel<ArrowType> {
};
TYPED_TEST_SUITE(TestArraySortIndicesKernelForUInt8, UInt8Type);

template <typename ArrowType>
class TestArraySortIndicesKernelForInt8 : public TestArraySortIndicesKernel<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesKernelForInt8, Int8Type);

TYPED_TEST(TestArraySortIndicesKernelForUInt8, SortUInt8) {
  this->AssertSortIndices("[255, null, 0, 255, 10, null, 128, 0]",
                          "[2, 7, 4, 6, 0, 3, 1, 5]");
}

TYPED_TEST(TestArraySortIndicesKernelForInt8, SortInt8) {
  this->AssertSortIndices("[null, 10, 127, 0, -128, -128, null]",
                          "[4, 5, 3, 1, 2, 0, 6]");
}

template <typename ArrowType>
class TestArraySortIndicesKernelRandom : public TestBase {};

template <typename ArrowType>
class TestArraySortIndicesKernelRandomCount : public TestBase {};

template <typename ArrowType>
class TestArraySortIndicesKernelRandomCompare : public TestBase {};

using SortIndicesableTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, StringType>;

template <typename ArrayType>
void ValidateSorted(const ArrayType& array, UInt64Array& offsets, SortOrder order) {
  ASSERT_OK(array.ValidateFull());
  SortComparator<ArrayType> compare;
  for (int i = 1; i < array.length(); i++) {
    uint64_t lhs = offsets.Value(i - 1);
    uint64_t rhs = offsets.Value(i);
    ASSERT_TRUE(compare(array, order, lhs, rhs));
  }
}

TYPED_TEST_SUITE(TestArraySortIndicesKernelRandom, SortIndicesableTypes);

TYPED_TEST(TestArraySortIndicesKernelRandom, SortRandomValues) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  Random<TypeParam> rand(0x5487655);
  int times = 5;
  int length = 1000;
  for (int test = 0; test < times; test++) {
    for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
      for (auto order : {SortOrder::Ascending, SortOrder::Descending}) {
        auto array = rand.Generate(length, null_probability);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets, SortIndices(*array, order));
        ValidateSorted<ArrayType>(*checked_pointer_cast<ArrayType>(array),
                                  *checked_pointer_cast<UInt64Array>(offsets), order);
      }
    }
  }
}

// Long array with small value range: counting sort
// - length >= 1024(CountCompareSorter::countsort_min_len_)
// - range  <= 4096(CountCompareSorter::countsort_max_range_)
TYPED_TEST_SUITE(TestArraySortIndicesKernelRandomCount, IntegralArrowTypes);

TYPED_TEST(TestArraySortIndicesKernelRandomCount, SortRandomValuesCount) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  RandomRange<TypeParam> rand(0x5487656);
  int times = 5;
  int length = 4000;
  int range = 2000;
  for (int test = 0; test < times; test++) {
    for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
      for (auto order : {SortOrder::Ascending, SortOrder::Descending}) {
        auto array = rand.Generate(length, range, null_probability);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets, SortIndices(*array, order));
        ValidateSorted<ArrayType>(*checked_pointer_cast<ArrayType>(array),
                                  *checked_pointer_cast<UInt64Array>(offsets), order);
      }
    }
  }
}

// Long array with big value range: std::stable_sort
TYPED_TEST_SUITE(TestArraySortIndicesKernelRandomCompare, IntegralArrowTypes);

TYPED_TEST(TestArraySortIndicesKernelRandomCompare, SortRandomValuesCompare) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  Random<TypeParam> rand(0x5487657);
  int times = 5;
  int length = 4000;
  for (int test = 0; test < times; test++) {
    for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
      for (auto order : {SortOrder::Ascending, SortOrder::Descending}) {
        auto array = rand.Generate(length, null_probability);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets, SortIndices(*array, order));
        ValidateSorted<ArrayType>(*checked_pointer_cast<ArrayType>(array),
                                  *checked_pointer_cast<UInt64Array>(offsets), order);
      }
    }
  }
}

// Test basic cases for chunked array.
class TestChunkedArraySortIndices : public ::testing::Test {
 protected:
  void AssertSortIndices(const std::shared_ptr<ChunkedArray> chunked_array,
                         SortOrder order, const std::shared_ptr<Array> expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, SortIndices(*chunked_array, order));
    AssertArraysEqual(*expected, *actual);
  }

  void AssertSortIndices(const std::shared_ptr<ChunkedArray> chunked_array,
                         SortOrder order, const std::string expected) {
    AssertSortIndices(chunked_array, order, ArrayFromJSON(uint64(), expected));
  }
};

TEST_F(TestChunkedArraySortIndices, SortNull) {
  auto chunked_array = ChunkedArrayFromJSON(uint8(), {
                                                         "[null, 1]",
                                                         "[3, null, 2]",
                                                         "[1]",
                                                     });
  this->AssertSortIndices(chunked_array, SortOrder::Ascending, "[1, 5, 4, 2, 0, 3]");
}

TEST_F(TestChunkedArraySortIndices, SortNaN) {
  auto chunked_array = ChunkedArrayFromJSON(float32(), {
                                                           "[null, 1]",
                                                           "[3, null, NaN]",
                                                           "[NaN, 1]",
                                                       });
  this->AssertSortIndices(chunked_array, SortOrder::Ascending, "[1, 6, 2, 4, 5, 0, 3]");
}

// Base class for testing against random chunked array.
template <typename Type>
class TestChunkedArrayRandomBase : public TestBase {
 protected:
  // Generates a chunk. This should be implemented in subclasses.
  virtual std::shared_ptr<Array> GenerateArray(int length, double null_probability) = 0;

  // All tests uses this.
  void TestSortIndices(int length) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    // We can use INSTANTIATE_TEST_SUITE_P() instead of using fors in a test.
    for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
      for (auto order : {SortOrder::Ascending, SortOrder::Descending}) {
        for (auto num_chunks : {1, 5, 10}) {
          std::vector<std::shared_ptr<Array>> arrays;
          for (int i = 0; i < num_chunks; ++i) {
            auto array = this->GenerateArray(length, null_probability);
            arrays.push_back(array);
          }
          ASSERT_OK_AND_ASSIGN(auto chunked_array, ChunkedArray::Make(arrays));
          ASSERT_OK_AND_ASSIGN(auto offsets, SortIndices(*chunked_array, order));
          // Concatenates chunks to use existing ValidateSorted() for array.
          ASSERT_OK_AND_ASSIGN(auto concatenated_array, Concatenate(arrays));
          ValidateSorted<ArrayType>(*checked_pointer_cast<ArrayType>(concatenated_array),
                                    *checked_pointer_cast<UInt64Array>(offsets), order);
        }
      }
    }
  }
};

// Long array with big value range: std::stable_sort
template <typename Type>
class TestChunkedArrayRandom : public TestChunkedArrayRandomBase<Type> {
 public:
  void SetUp() { this->rand_.reset(new Random<Type>(0x5487655)); }

  void TearDown() { this->rand_.release(); }

 protected:
  std::shared_ptr<Array> GenerateArray(int length, double null_probability) override {
    return rand_->Generate(length, null_probability);
  }

 private:
  std::unique_ptr<Random<Type>> rand_;
};
TYPED_TEST_SUITE(TestChunkedArrayRandom, SortIndicesableTypes);
TYPED_TEST(TestChunkedArrayRandom, SortIndices) { this->TestSortIndices(4000); }

// Long array with small value range: counting sort
// - length >= 1024(CountCompareSorter::countsort_min_len_)
// - range  <= 4096(CountCompareSorter::countsort_max_range_)
template <typename Type>
class TestChunkedArrayRandomNarrow : public TestChunkedArrayRandomBase<Type> {
 public:
  void SetUp() {
    range_ = 2000;
    rand_.reset(new RandomRange<Type>(0x5487655));
  }

  void TearDown() { rand_.release(); }

 protected:
  std::shared_ptr<Array> GenerateArray(int length, double null_probability) override {
    return rand_->Generate(length, range_, null_probability);
  }

 private:
  int range_;
  std::unique_ptr<RandomRange<Type>> rand_;
};
TYPED_TEST_SUITE(TestChunkedArrayRandomNarrow, IntegralArrowTypes);
TYPED_TEST(TestChunkedArrayRandomNarrow, SortIndices) { this->TestSortIndices(4000); }

// Test basic cases for table.
class TestTableSortIndices : public ::testing::Test {
 protected:
  void AssertSortIndices(const std::shared_ptr<Table> table, const SortOptions& options,
                         const std::shared_ptr<Array> expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, SortIndices(*table, options));
    AssertArraysEqual(*expected, *actual);
  }

  void AssertSortIndices(const std::shared_ptr<Table> table, const SortOptions& options,
                         const std::string expected) {
    AssertSortIndices(table, options, ArrayFromJSON(uint64(), expected));
  }
};

TEST_F(TestTableSortIndices, SortNull) {
  auto table = TableFromJSON(schema({
                                 {field("a", uint8())},
                                 {field("b", uint8())},
                             }),
                             {"["
                              "{\"a\": null, \"b\": 5},"
                              "{\"a\": 1,    \"b\": 3},"
                              "{\"a\": 3,    \"b\": null},"
                              "{\"a\": null, \"b\": null},"
                              "{\"a\": 2,    \"b\": 5},"
                              "{\"a\": 1,    \"b\": 5}"
                              "]"});
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});
  this->AssertSortIndices(table, options, "[5, 1, 4, 2, 0, 3]");
}

TEST_F(TestTableSortIndices, SortNaN) {
  auto table = TableFromJSON(schema({
                                 {field("a", float32())},
                                 {field("b", float32())},
                             }),
                             {"["
                              "{\"a\": null, \"b\": 5},"
                              "{\"a\": 1,    \"b\": 3},"
                              "{\"a\": 3,    \"b\": null},"
                              "{\"a\": null, \"b\": null},"
                              "{\"a\": NaN,  \"b\": null},"
                              "{\"a\": NaN,  \"b\": NaN},"
                              "{\"a\": NaN,  \"b\": 5},"
                              "{\"a\": 1,    \"b\": 5}"
                              "]"});
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});
  this->AssertSortIndices(table, options, "[7, 1, 2, 6, 5, 4, 0, 3]");
}

// For random table tests.
using RandomParam = std::tuple<std::string, double>;
class TestTableSortIndicesRandom : public testing::TestWithParam<RandomParam> {
  // Compares two records in the same table.
  class Comparator : public TypeVisitor {
   public:
    // Returns true if the left record is less or equals to the right
    // record, false otherwise.
    //
    // This supports null and NaN.
    bool operator()(const Table& table, const SortOptions& options, uint64_t lhs,
                    uint64_t rhs) {
      lhs_ = lhs;
      rhs_ = rhs;
      for (const auto& sort_key : options.sort_keys) {
        auto chunked_array = table.GetColumnByName(sort_key.name);
        lhs_array_ = FindTargetArray(chunked_array, lhs, lhs_index_);
        rhs_array_ = FindTargetArray(chunked_array, rhs, rhs_index_);
        if (rhs_array_->IsNull(rhs_index_) && lhs_array_->IsNull(lhs_index_)) continue;
        if (rhs_array_->IsNull(rhs_index_)) return true;
        if (lhs_array_->IsNull(lhs_index_)) return false;
        status_ = lhs_array_->type()->Accept(this);
        if (compared_ == 0) continue;
        if (sort_key.order == SortOrder::Ascending) {
          return compared_ < 0;
        } else {
          return compared_ > 0;
        }
      }
      return lhs < rhs;
    }

    Status status() const { return status_; }

#define VISIT(TYPE)                               \
  Status Visit(const TYPE##Type& type) override { \
    compared_ = CompareType<TYPE##Type>();        \
    return Status::OK();                          \
  }

    VISIT(Int8)
    VISIT(Int16)
    VISIT(Int32)
    VISIT(Int64)
    VISIT(UInt8)
    VISIT(UInt16)
    VISIT(UInt32)
    VISIT(UInt64)
    VISIT(Float)
    VISIT(Double)
    VISIT(String)

#undef VISIT

   private:
    // Finds the target chunk and index in the target chunk from an
    // index in chunked array.
    std::shared_ptr<Array> FindTargetArray(std::shared_ptr<ChunkedArray> chunked_array,
                                           int64_t i, int64_t& chunk_index) {
      int64_t offset = 0;
      for (auto& chunk : chunked_array->chunks()) {
        if (i < offset + chunk->length()) {
          chunk_index = i - offset;
          return chunk;
        }
        offset += chunk->length();
      }
      return nullptr;
    }

    // Compares two values in the same chunked array. Values are never
    // null but may be NaN.
    //
    // Returns true if the left value is less or equals to the right
    // value, false otherwise.
    template <typename Type>
    int CompareType() {
      using ArrayType = typename TypeTraits<Type>::ArrayType;
      auto lhs_value = checked_pointer_cast<ArrayType>(lhs_array_)->GetView(lhs_index_);
      auto rhs_value = checked_pointer_cast<ArrayType>(rhs_array_)->GetView(rhs_index_);
      if (is_floating_type<Type>::value) {
        const bool lhs_isnan = lhs_value != lhs_value;
        const bool rhs_isnan = rhs_value != rhs_value;
        if (lhs_isnan && rhs_isnan) return 0;
        if (rhs_isnan) return 1;
        if (lhs_isnan) return -1;
      }
      if (lhs_value == rhs_value) {
        return 0;
      } else if (lhs_value > rhs_value) {
        return 1;
      } else {
        return -1;
      }
    }

    int64_t lhs_;
    std::shared_ptr<Array> lhs_array_;
    int64_t lhs_index_;
    int64_t rhs_;
    std::shared_ptr<Array> rhs_array_;
    int64_t rhs_index_;
    int compared_;
    Status status_;
  };

 public:
  // Validates the sorted indexes are really sorted.
  void Validate(const Table& table, const SortOptions& options, UInt64Array& offsets) {
    ASSERT_OK(offsets.ValidateFull());
    Comparator comparator;
    for (int i = 1; i < table.num_rows(); i++) {
      uint64_t lhs = offsets.Value(i - 1);
      uint64_t rhs = offsets.Value(i);
      ASSERT_TRUE(comparator(table, options, lhs, rhs));
      ASSERT_OK(comparator.status());
    }
  }
};

TEST_P(TestTableSortIndicesRandom, Sort) {
  auto first_sort_key_name = std::get<0>(GetParam());
  auto null_probability = std::get<1>(GetParam());
  auto seed = 0x61549225;
  std::vector<std::string> column_names = {
      "uint8", "uint16", "uint32", "uint64", "int8",   "int16",
      "int32", "int64",  "float",  "double", "string",
  };
  std::vector<std::shared_ptr<Field>> fields = {
      {field(column_names[0], uint8())},   {field(column_names[1], uint16())},
      {field(column_names[2], uint32())},  {field(column_names[3], uint64())},
      {field(column_names[4], int8())},    {field(column_names[5], int16())},
      {field(column_names[6], int32())},   {field(column_names[7], int64())},
      {field(column_names[8], float32())}, {field(column_names[9], float64())},
      {field(column_names[10], utf8())},
  };
  auto length = 4000;
  std::vector<std::shared_ptr<Array>> columns = {
      Random<UInt8Type>(seed).Generate(length, null_probability),
      Random<UInt16Type>(seed).Generate(length, null_probability),
      Random<UInt32Type>(seed).Generate(length, null_probability),
      Random<UInt64Type>(seed).Generate(length, null_probability),
      Random<Int8Type>(seed).Generate(length, null_probability),
      Random<Int16Type>(seed).Generate(length, null_probability),
      Random<Int32Type>(seed).Generate(length, null_probability),
      Random<Int64Type>(seed).Generate(length, null_probability),
      Random<FloatType>(seed).Generate(length, null_probability),
      Random<DoubleType>(seed).Generate(length, null_probability),
      Random<StringType>(seed).Generate(length, null_probability),
  };
  auto table = Table::Make(schema(fields), columns, length);
  std::default_random_engine engine(seed);
  std::uniform_int_distribution<> distribution(0);
  auto n_sort_keys = 5;
  std::vector<SortKey> sort_keys;
  auto first_sort_key_order =
      (distribution(engine) % 2) == 0 ? SortOrder::Ascending : SortOrder::Descending;
  sort_keys.emplace_back(first_sort_key_name, first_sort_key_order);
  for (int i = 1; i < n_sort_keys; ++i) {
    auto& column_name = column_names[distribution(engine) % column_names.size()];
    auto order =
        (distribution(engine) % 2) == 0 ? SortOrder::Ascending : SortOrder::Descending;
    sort_keys.emplace_back(column_name, order);
  }
  SortOptions options(sort_keys);
  ASSERT_OK_AND_ASSIGN(auto offsets, SortIndices(*table, options));
  Validate(*table, options, *checked_pointer_cast<UInt64Array>(offsets));
}

INSTANTIATE_TEST_SUITE_P(NoNull, TestTableSortIndicesRandom,
                         testing::Combine(testing::Values("uint8", "uint16", "uint32",
                                                          "uint64", "int8", "int16",
                                                          "int32", "int64", "float",
                                                          "double", "string"),
                                          testing::Values(0.0)));

INSTANTIATE_TEST_SUITE_P(MayNull, TestTableSortIndicesRandom,
                         testing::Combine(testing::Values("uint8", "uint16", "uint32",
                                                          "uint64", "int8", "int16",
                                                          "int32", "int64", "float",
                                                          "double", "string"),
                                          testing::Values(0.1, 0.5)));

INSTANTIATE_TEST_SUITE_P(AllNull, TestTableSortIndicesRandom,
                         testing::Combine(testing::Values("uint8", "uint16", "uint32",
                                                          "uint64", "int8", "int16",
                                                          "int32", "int64", "float",
                                                          "double", "string"),
                                          testing::Values(1.0)));

}  // namespace compute
}  // namespace arrow
