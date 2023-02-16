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
#include <functional>
#include <limits>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include <gmock/gmock-matchers.h>

#include "arrow/array/array_decimal.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {

#ifdef ARROW_VALGRIND
using RealArrowTypes = ::testing::Types<FloatType>;

using IntegralArrowTypes = ::testing::Types<UInt32Type>;
using TemporalArrowTypes = ::testing::Types<Date32Type, TimestampType, Time32Type>;

using DecimalArrowTypes = ::testing::Types<Decimal128Type>;
#endif

std::vector<SortOrder> AllOrders() {
  return {SortOrder::Ascending, SortOrder::Descending};
}

std::vector<NullPlacement> AllNullPlacements() {
  return {NullPlacement::AtEnd, NullPlacement::AtStart};
}

std::vector<RankOptions::Tiebreaker> AllTiebreakers() {
  return {RankOptions::Min, RankOptions::Max, RankOptions::First, RankOptions::Dense};
}

std::ostream& operator<<(std::ostream& os, NullPlacement null_placement) {
  os << (null_placement == NullPlacement::AtEnd ? "AtEnd" : "AtStart");
  return os;
}

// ----------------------------------------------------------------------
// Tests for NthToIndices

template <typename ArrayType>
auto GetLogicalValue(const ArrayType& array, uint64_t index)
    -> decltype(array.GetView(index)) {
  return array.GetView(index);
}

Decimal128 GetLogicalValue(const Decimal128Array& array, uint64_t index) {
  return Decimal128(array.Value(index));
}

Decimal256 GetLogicalValue(const Decimal256Array& array, uint64_t index) {
  return Decimal256(array.Value(index));
}

template <typename ArrayType>
struct ThreeWayComparator {
  SortOrder order;
  NullPlacement null_placement;

  int operator()(const ArrayType& array, uint64_t lhs, uint64_t rhs) const {
    return (*this)(array, array, lhs, rhs);
  }

  // Return -1 if L < R, 0 if L == R, 1 if L > R
  int operator()(const ArrayType& left, const ArrayType& right, uint64_t lhs,
                 uint64_t rhs) const {
    const bool lhs_is_null = left.IsNull(lhs);
    const bool rhs_is_null = right.IsNull(rhs);
    if (lhs_is_null && rhs_is_null) return 0;
    if (lhs_is_null) {
      return null_placement == NullPlacement::AtStart ? -1 : 1;
    }
    if (rhs_is_null) {
      return null_placement == NullPlacement::AtStart ? 1 : -1;
    }
    const auto lval = GetLogicalValue(left, lhs);
    const auto rval = GetLogicalValue(right, rhs);
    if (is_floating_type<typename ArrayType::TypeClass>::value) {
      const bool lhs_isnan = lval != lval;
      const bool rhs_isnan = rval != rval;
      if (lhs_isnan && rhs_isnan) return 0;
      if (lhs_isnan) {
        return null_placement == NullPlacement::AtStart ? -1 : 1;
      }
      if (rhs_isnan) {
        return null_placement == NullPlacement::AtStart ? 1 : -1;
      }
    }
    if (lval == rval) return 0;
    if (lval < rval) {
      return order == SortOrder::Ascending ? -1 : 1;
    } else {
      return order == SortOrder::Ascending ? 1 : -1;
    }
  }
};

template <typename ArrayType>
struct NthComparator {
  ThreeWayComparator<ArrayType> three_way;

  explicit NthComparator(NullPlacement null_placement)
      : three_way({SortOrder::Ascending, null_placement}) {}

  // Return true iff L <= R
  bool operator()(const ArrayType& array, uint64_t lhs, uint64_t rhs) const {
    // lhs <= rhs
    return three_way(array, lhs, rhs) <= 0;
  }
};

template <typename ArrayType>
struct SortComparator {
  ThreeWayComparator<ArrayType> three_way;

  explicit SortComparator(SortOrder order, NullPlacement null_placement)
      : three_way({order, null_placement}) {}

  bool operator()(const ArrayType& array, uint64_t lhs, uint64_t rhs) const {
    const int r = three_way(array, lhs, rhs);
    if (r != 0) return r < 0;
    return lhs < rhs;
  }
};

template <typename ArrowType>
class TestNthToIndicesBase : public ::testing::Test {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 protected:
  void Validate(const ArrayType& array, int n, NullPlacement null_placement,
                UInt64Array& offsets) {
    if (n >= array.length()) {
      for (int i = 0; i < array.length(); ++i) {
        ASSERT_TRUE(offsets.Value(i) == static_cast<uint64_t>(i));
      }
    } else {
      NthComparator<ArrayType> compare{null_placement};
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

  void AssertNthToIndicesArray(const std::shared_ptr<Array>& values, int n,
                               NullPlacement null_placement) {
    ARROW_SCOPED_TRACE("n = ", n, ", null_placement = ", null_placement);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets,
                         NthToIndices(*values, PartitionNthOptions(n, null_placement)));
    // null_count field should have been initialized to 0, for convenience
    ASSERT_EQ(offsets->data()->null_count, 0);
    ValidateOutput(*offsets);
    Validate(*checked_pointer_cast<ArrayType>(values), n, null_placement,
             *checked_pointer_cast<UInt64Array>(offsets));
  }

  void AssertNthToIndicesArray(const std::shared_ptr<Array>& values, int n) {
    for (auto null_placement : AllNullPlacements()) {
      AssertNthToIndicesArray(values, n, null_placement);
    }
  }

  void AssertNthToIndicesJson(const std::string& values, int n) {
    AssertNthToIndicesArray(ArrayFromJSON(GetType(), values), n);
  }

  virtual std::shared_ptr<DataType> GetType() = 0;
};

template <typename ArrowType>
class TestNthToIndices : public TestNthToIndicesBase<ArrowType> {
 protected:
  std::shared_ptr<DataType> GetType() override {
    return default_type_instance<ArrowType>();
  }
};

template <typename ArrowType>
class TestNthToIndicesForReal : public TestNthToIndices<ArrowType> {};
TYPED_TEST_SUITE(TestNthToIndicesForReal, RealArrowTypes);

template <typename ArrowType>
class TestNthToIndicesForIntegral : public TestNthToIndices<ArrowType> {};
TYPED_TEST_SUITE(TestNthToIndicesForIntegral, IntegralArrowTypes);

template <typename ArrowType>
class TestNthToIndicesForBool : public TestNthToIndices<ArrowType> {};
TYPED_TEST_SUITE(TestNthToIndicesForBool, ::testing::Types<BooleanType>);

template <typename ArrowType>
class TestNthToIndicesForTemporal : public TestNthToIndices<ArrowType> {};
TYPED_TEST_SUITE(TestNthToIndicesForTemporal, TemporalArrowTypes);

template <typename ArrowType>
class TestNthToIndicesForDecimal : public TestNthToIndicesBase<ArrowType> {
  std::shared_ptr<DataType> GetType() override {
    return std::make_shared<ArrowType>(5, 2);
  }
};
TYPED_TEST_SUITE(TestNthToIndicesForDecimal, DecimalArrowTypes);

template <typename ArrowType>
class TestNthToIndicesForStrings : public TestNthToIndices<ArrowType> {};
TYPED_TEST_SUITE(TestNthToIndicesForStrings, testing::Types<StringType>);

TYPED_TEST(TestNthToIndicesForReal, NthToIndicesDoesNotProvideDefaultOptions) {
  auto input = ArrayFromJSON(this->GetType(), "[null, 1, 3.3, null, 2, 5.3]");
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

  this->AssertNthToIndicesJson("[NaN, 2, NaN, 3, 1]", 0);
  this->AssertNthToIndicesJson("[NaN, 2, NaN, 3, 1]", 1);
  this->AssertNthToIndicesJson("[NaN, 2, NaN, 3, 1]", 2);
  this->AssertNthToIndicesJson("[NaN, 2, NaN, 3, 1]", 3);
  this->AssertNthToIndicesJson("[NaN, 2, NaN, 3, 1]", 4);
}

TYPED_TEST(TestNthToIndicesForIntegral, Integral) {
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 0);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 2);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 5);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 6);
}

TYPED_TEST(TestNthToIndicesForBool, Bool) {
  this->AssertNthToIndicesJson("[null, false, true, null, false, true]", 0);
  this->AssertNthToIndicesJson("[null, false, true, null, false, true]", 2);
  this->AssertNthToIndicesJson("[null, false, true, null, false, true]", 5);
  this->AssertNthToIndicesJson("[null, false, true, null, false, true]", 6);
}

TYPED_TEST(TestNthToIndicesForTemporal, Temporal) {
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 0);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 2);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 5);
  this->AssertNthToIndicesJson("[null, 1, 3, null, 2, 5]", 6);
}

TYPED_TEST(TestNthToIndicesForDecimal, Decimal) {
  const std::string values = R"(["123.45", null, "-123.45", "456.78", "-456.78"])";
  this->AssertNthToIndicesJson(values, 0);
  this->AssertNthToIndicesJson(values, 2);
  this->AssertNthToIndicesJson(values, 4);
  this->AssertNthToIndicesJson(values, 5);
}

TYPED_TEST(TestNthToIndicesForStrings, Strings) {
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 0);
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 2);
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 5);
  this->AssertNthToIndicesJson(R"(["testing", null, "nth", "for", null, "strings"])", 6);
}

TEST(TestNthToIndices, Null) {
  ASSERT_OK_AND_ASSIGN(auto arr, MakeArrayOfNull(null(), 6));
  auto expected = ArrayFromJSON(uint64(), "[0, 1, 2, 3, 4, 5]");
  for (const auto null_placement : AllNullPlacements()) {
    for (const auto n : {0, 1, 2, 3, 4, 5, 6}) {
      ASSERT_OK_AND_ASSIGN(auto actual,
                           NthToIndices(*arr, PartitionNthOptions(n, null_placement)));
      AssertArraysEqual(*expected, *actual, /*verbose=*/true);
    }
  }
}

template <typename ArrowType>
class TestNthToIndicesRandom : public TestNthToIndicesBase<ArrowType> {
 public:
  std::shared_ptr<DataType> GetType() override {
    EXPECT_TRUE(0) << "shouldn't be used";
    return nullptr;
  }
};

#ifdef ARROW_VALGRIND
using NthToIndicesableTypes =
    ::testing::Types<Int16Type, FloatType, Decimal128Type, StringType>;
#else
using NthToIndicesableTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, Decimal128Type,
                     StringType>;
#endif

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

// ----------------------------------------------------------------------
// Basic tests for the "array_sort_indices" function

TEST(ArraySortIndicesFunction, Array) {
  auto arr = ArrayFromJSON(int16(), "[0, 1, null, -3, null, -42, 5]");
  auto expected = ArrayFromJSON(uint64(), "[5, 3, 0, 1, 6, 2, 4]");
  ASSERT_OK_AND_ASSIGN(auto actual, CallFunction("array_sort_indices", {arr}));
  AssertDatumsEqual(expected, actual, /*verbose=*/true);

  ArraySortOptions options{SortOrder::Descending, NullPlacement::AtStart};
  expected = ArrayFromJSON(uint64(), "[2, 4, 6, 1, 0, 3, 5]");
  ASSERT_OK_AND_ASSIGN(actual, CallFunction("array_sort_indices", {arr}, &options));
  AssertDatumsEqual(expected, actual, /*verbose=*/true);
}

TEST(ArraySortIndicesFunction, ChunkedArray) {
  auto arr = ChunkedArrayFromJSON(int16(), {"[0, 1]", "[null, -3, null, -42, 5]"});
  auto expected = ChunkedArrayFromJSON(uint64(), {"[5, 3, 0, 1, 6, 2, 4]"});
  ASSERT_OK_AND_ASSIGN(auto actual, CallFunction("array_sort_indices", {arr}));
  AssertDatumsEqual(expected, actual, /*verbose=*/true);

  ArraySortOptions options{SortOrder::Descending, NullPlacement::AtStart};
  expected = ChunkedArrayFromJSON(uint64(), {"[2, 4, 6, 1, 0, 3, 5]"});
  ASSERT_OK_AND_ASSIGN(actual, CallFunction("array_sort_indices", {arr}, &options));
  AssertDatumsEqual(expected, actual, /*verbose=*/true);
}

// ----------------------------------------------------------------------
// Tests for SortToIndices

template <typename T>
void AssertSortIndices(const std::shared_ptr<T>& input, SortOrder order,
                       NullPlacement null_placement,
                       const std::shared_ptr<Array>& expected) {
  ArraySortOptions options(order, null_placement);
  ASSERT_OK_AND_ASSIGN(auto actual, SortIndices(*input, options));
  ValidateOutput(*actual);
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

template <typename T>
void AssertSortIndices(const std::shared_ptr<T>& input, const SortOptions& options,
                       const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(auto actual, SortIndices(Datum(*input), options));
  ValidateOutput(*actual);
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

template <typename T>
void AssertSortIndices(const std::shared_ptr<T>& input, const SortOptions& options,
                       const std::string& expected) {
  AssertSortIndices(input, options, ArrayFromJSON(uint64(), expected));
}

template <typename T>
void AssertSortIndices(const std::shared_ptr<T>& input, SortOrder order,
                       NullPlacement null_placement, const std::string& expected) {
  AssertSortIndices(input, order, null_placement, ArrayFromJSON(uint64(), expected));
}

void AssertSortIndices(const std::shared_ptr<DataType>& type, const std::string& values,
                       SortOrder order, NullPlacement null_placement,
                       const std::string& expected) {
  AssertSortIndices(ArrayFromJSON(type, values), order, null_placement,
                    ArrayFromJSON(uint64(), expected));
}

class TestArraySortIndicesBase : public ::testing::Test {
 public:
  virtual std::shared_ptr<DataType> type() = 0;

  virtual void AssertSortIndices(const std::string& values, SortOrder order,
                                 NullPlacement null_placement,
                                 const std::string& expected) {
    arrow::compute::AssertSortIndices(this->type(), values, order, null_placement,
                                      expected);
  }

  virtual void AssertSortIndices(const std::string& values, const std::string& expected) {
    AssertSortIndices(values, SortOrder::Ascending, NullPlacement::AtEnd, expected);
  }
};

template <typename ArrowType>
class TestArraySortIndices : public TestArraySortIndicesBase {
 public:
  std::shared_ptr<DataType> type() override {
    // Will choose default parameters for temporal types
    return std::make_shared<ArrowType>();
  }
};

template <typename ArrowType>
class TestArraySortIndicesForReal : public TestArraySortIndices<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesForReal, RealArrowTypes);

template <typename ArrowType>
class TestArraySortIndicesForBool : public TestArraySortIndices<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesForBool, ::testing::Types<BooleanType>);

template <typename ArrowType>
class TestArraySortIndicesForIntegral : public TestArraySortIndices<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesForIntegral, IntegralArrowTypes);

template <typename ArrowType>
class TestArraySortIndicesForTemporal : public TestArraySortIndices<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesForTemporal, TemporalArrowTypes);

using StringSortTestTypes = testing::Types<StringType, LargeStringType>;

template <typename ArrowType>
class TestArraySortIndicesForStrings : public TestArraySortIndices<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesForStrings, StringSortTestTypes);

class TestArraySortIndicesForFixedSizeBinary : public TestArraySortIndicesBase {
 public:
  std::shared_ptr<DataType> type() override { return fixed_size_binary(3); }
};

TYPED_TEST(TestArraySortIndicesForReal, SortReal) {
  for (auto null_placement : AllNullPlacements()) {
    for (auto order : AllOrders()) {
      this->AssertSortIndices("[]", order, null_placement, "[]");
      this->AssertSortIndices("[null, null]", order, null_placement, "[0, 1]");
    }
    this->AssertSortIndices("[3.4, 2.6, 6.3]", SortOrder::Ascending, null_placement,
                            "[1, 0, 2]");
    this->AssertSortIndices("[1.1, 2.4, 3.5, 4.3, 5.1, 6.8, 7.3]", SortOrder::Ascending,
                            null_placement, "[0, 1, 2, 3, 4, 5, 6]");
    this->AssertSortIndices("[7, 6, 5, 4, 3, 2, 1]", SortOrder::Ascending, null_placement,
                            "[6, 5, 4, 3, 2, 1, 0]");
    this->AssertSortIndices("[10.4, 12, 4.2, 50, 50.3, 32, 11]", SortOrder::Ascending,
                            null_placement, "[2, 0, 6, 1, 5, 3, 4]");
  }

  this->AssertSortIndices("[null, 1, 3.3, null, 2, 5.3]", SortOrder::Ascending,
                          NullPlacement::AtEnd, "[1, 4, 2, 5, 0, 3]");
  this->AssertSortIndices("[null, 1, 3.3, null, 2, 5.3]", SortOrder::Ascending,
                          NullPlacement::AtStart, "[0, 3, 1, 4, 2, 5]");
  this->AssertSortIndices("[null, 1, 3.3, null, 2, 5.3]", SortOrder::Descending,
                          NullPlacement::AtEnd, "[5, 2, 4, 1, 0, 3]");
  this->AssertSortIndices("[null, 1, 3.3, null, 2, 5.3]", SortOrder::Descending,
                          NullPlacement::AtStart, "[0, 3, 5, 2, 4, 1]");

  this->AssertSortIndices("[3, 4, NaN, 1, 2, null]", SortOrder::Ascending,
                          NullPlacement::AtEnd, "[3, 4, 0, 1, 2, 5]");
  this->AssertSortIndices("[3, 4, NaN, 1, 2, null]", SortOrder::Ascending,
                          NullPlacement::AtStart, "[5, 2, 3, 4, 0, 1]");
  this->AssertSortIndices("[3, 4, NaN, 1, 2, null]", SortOrder::Descending,
                          NullPlacement::AtEnd, "[1, 0, 4, 3, 2, 5]");
  this->AssertSortIndices("[3, 4, NaN, 1, 2, null]", SortOrder::Descending,
                          NullPlacement::AtStart, "[5, 2, 1, 0, 4, 3]");

  this->AssertSortIndices("[NaN, 2, NaN, 3, 1]", SortOrder::Ascending,
                          NullPlacement::AtEnd, "[4, 1, 3, 0, 2]");
  this->AssertSortIndices("[NaN, 2, NaN, 3, 1]", SortOrder::Ascending,
                          NullPlacement::AtStart, "[0, 2, 4, 1, 3]");
  this->AssertSortIndices("[NaN, 2, NaN, 3, 1]", SortOrder::Descending,
                          NullPlacement::AtEnd, "[3, 1, 4, 0, 2]");
  this->AssertSortIndices("[NaN, 2, NaN, 3, 1]", SortOrder::Descending,
                          NullPlacement::AtStart, "[0, 2, 3, 1, 4]");

  this->AssertSortIndices("[null, NaN, NaN, null]", SortOrder::Ascending,
                          NullPlacement::AtEnd, "[1, 2, 0, 3]");
  this->AssertSortIndices("[null, NaN, NaN, null]", SortOrder::Ascending,
                          NullPlacement::AtStart, "[0, 3, 1, 2]");
  this->AssertSortIndices("[null, NaN, NaN, null]", SortOrder::Descending,
                          NullPlacement::AtEnd, "[1, 2, 0, 3]");
  this->AssertSortIndices("[null, NaN, NaN, null]", SortOrder::Descending,
                          NullPlacement::AtStart, "[0, 3, 1, 2]");
}

TYPED_TEST(TestArraySortIndicesForIntegral, SortIntegral) {
  for (auto null_placement : AllNullPlacements()) {
    for (auto order : AllOrders()) {
      this->AssertSortIndices("[]", order, null_placement, "[]");
      this->AssertSortIndices("[null, null]", order, null_placement, "[0, 1]");
    }
    this->AssertSortIndices("[1, 2, 3, 4, 5, 6, 7]", SortOrder::Ascending, null_placement,
                            "[0, 1, 2, 3, 4, 5, 6]");
    this->AssertSortIndices("[7, 6, 5, 4, 3, 2, 1]", SortOrder::Ascending, null_placement,
                            "[6, 5, 4, 3, 2, 1, 0]");

    this->AssertSortIndices("[10, 12, 4, 50, 50, 32, 11]", SortOrder::Ascending,
                            null_placement, "[2, 0, 6, 1, 5, 3, 4]");
    this->AssertSortIndices("[10, 12, 4, 50, 50, 32, 11]", SortOrder::Descending,
                            null_placement, "[3, 4, 5, 1, 6, 0, 2]");
  }

  // Values with a small range (use a counting sort)
  this->AssertSortIndices("[null, 1, 3, null, 2, 5]", SortOrder::Ascending,
                          NullPlacement::AtEnd, "[1, 4, 2, 5, 0, 3]");
  this->AssertSortIndices("[null, 1, 3, null, 2, 5]", SortOrder::Ascending,
                          NullPlacement::AtStart, "[0, 3, 1, 4, 2, 5]");
  this->AssertSortIndices("[null, 1, 3, null, 2, 5]", SortOrder::Descending,
                          NullPlacement::AtEnd, "[5, 2, 4, 1, 0, 3]");
  this->AssertSortIndices("[null, 1, 3, null, 2, 5]", SortOrder::Descending,
                          NullPlacement::AtStart, "[0, 3, 5, 2, 4, 1]");
}

TYPED_TEST(TestArraySortIndicesForBool, SortBool) {
  for (auto null_placement : AllNullPlacements()) {
    for (auto order : AllOrders()) {
      this->AssertSortIndices("[]", order, null_placement, "[]");
      this->AssertSortIndices("[null, null]", order, null_placement, "[0, 1]");
    }
    this->AssertSortIndices("[true, true, false]", SortOrder::Ascending, null_placement,
                            "[2, 0, 1]");
    this->AssertSortIndices("[false, false,  false, true, true, true, true]",
                            SortOrder::Ascending, null_placement,
                            "[0, 1, 2, 3, 4, 5, 6]");
    this->AssertSortIndices("[true, true, true, true, false, false, false]",
                            SortOrder::Ascending, null_placement,
                            "[4, 5, 6, 0, 1, 2, 3]");

    this->AssertSortIndices("[false, true, false, true, true, false, false]",
                            SortOrder::Ascending, null_placement,
                            "[0, 2, 5, 6, 1, 3, 4]");
    this->AssertSortIndices("[false, true, false, true, true, false, false]",
                            SortOrder::Descending, null_placement,
                            "[1, 3, 4, 0, 2, 5, 6]");
  }

  this->AssertSortIndices("[null, true, false, null, false, true]", SortOrder::Ascending,
                          NullPlacement::AtEnd, "[2, 4, 1, 5, 0, 3]");
  this->AssertSortIndices("[null, true, false, null, false, true]", SortOrder::Ascending,
                          NullPlacement::AtStart, "[0, 3, 2, 4, 1, 5]");
  this->AssertSortIndices("[null, true, false, null, false, true]", SortOrder::Descending,
                          NullPlacement::AtEnd, "[1, 5, 2, 4, 0, 3]");
  this->AssertSortIndices("[null, true, false, null, false, true]", SortOrder::Descending,
                          NullPlacement::AtStart, "[0, 3, 1, 5, 2, 4]");
}

TYPED_TEST(TestArraySortIndicesForTemporal, SortTemporal) {
  for (auto null_placement : AllNullPlacements()) {
    for (auto order : AllOrders()) {
      this->AssertSortIndices("[]", order, null_placement, "[]");
      this->AssertSortIndices("[null, null]", order, null_placement, "[0, 1]");
    }
    this->AssertSortIndices("[3, 2, 6]", SortOrder::Ascending, null_placement,
                            "[1, 0, 2]");
    this->AssertSortIndices("[1, 2, 3, 4, 5, 6, 7]", SortOrder::Ascending, null_placement,
                            "[0, 1, 2, 3, 4, 5, 6]");
    this->AssertSortIndices("[7, 6, 5, 4, 3, 2, 1]", SortOrder::Ascending, null_placement,
                            "[6, 5, 4, 3, 2, 1, 0]");

    this->AssertSortIndices("[10, 12, 4, 50, 50, 32, 11]", SortOrder::Ascending,
                            null_placement, "[2, 0, 6, 1, 5, 3, 4]");
    this->AssertSortIndices("[10, 12, 4, 50, 50, 32, 11]", SortOrder::Descending,
                            null_placement, "[3, 4, 5, 1, 6, 0, 2]");
  }

  this->AssertSortIndices("[null, 1, 3, null, 2, 5]", SortOrder::Ascending,
                          NullPlacement::AtEnd, "[1, 4, 2, 5, 0, 3]");
  this->AssertSortIndices("[null, 1, 3, null, 2, 5]", SortOrder::Ascending,
                          NullPlacement::AtStart, "[0, 3, 1, 4, 2, 5]");
  this->AssertSortIndices("[null, 1, 3, null, 2, 5]", SortOrder::Descending,
                          NullPlacement::AtEnd, "[5, 2, 4, 1, 0, 3]");
  this->AssertSortIndices("[null, 1, 3, null, 2, 5]", SortOrder::Descending,
                          NullPlacement::AtStart, "[0, 3, 5, 2, 4, 1]");
}

TYPED_TEST(TestArraySortIndicesForStrings, SortStrings) {
  for (auto null_placement : AllNullPlacements()) {
    for (auto order : AllOrders()) {
      this->AssertSortIndices("[]", order, null_placement, "[]");
      this->AssertSortIndices("[null, null]", order, null_placement, "[0, 1]");
    }
    this->AssertSortIndices(R"(["a", "b", "c"])", SortOrder::Ascending, null_placement,
                            "[0, 1, 2]");
    this->AssertSortIndices(R"(["foo", "bar", "baz"])", SortOrder::Ascending,
                            null_placement, "[1, 2, 0]");
    this->AssertSortIndices(R"(["testing", "sort", "for", "strings"])",
                            SortOrder::Ascending, null_placement, "[2, 1, 3, 0]");
  }

  const char* input = R"([null, "c", "b", null, "a", "b"])";
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtEnd,
                          "[4, 2, 5, 1, 0, 3]");
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtStart,
                          "[0, 3, 4, 2, 5, 1]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtEnd,
                          "[1, 2, 5, 4, 0, 3]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtStart,
                          "[0, 3, 1, 2, 5, 4]");
}

TEST_F(TestArraySortIndicesForFixedSizeBinary, SortFixedSizeBinary) {
  for (auto null_placement : AllNullPlacements()) {
    for (auto order : AllOrders()) {
      this->AssertSortIndices("[]", order, null_placement, "[]");
      this->AssertSortIndices("[null, null]", order, null_placement, "[0, 1]");
    }
    this->AssertSortIndices(R"(["def", "abc", "ghi"])", SortOrder::Ascending,
                            null_placement, "[1, 0, 2]");
    this->AssertSortIndices(R"(["def", "abc", "ghi"])", SortOrder::Descending,
                            null_placement, "[2, 0, 1]");
  }

  const char* input = R"([null, "ccc", "bbb", null, "aaa", "bbb"])";
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtEnd,
                          "[4, 2, 5, 1, 0, 3]");
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtStart,
                          "[0, 3, 4, 2, 5, 1]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtEnd,
                          "[1, 2, 5, 4, 0, 3]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtStart,
                          "[0, 3, 1, 2, 5, 4]");
}

template <typename ArrowType>
class TestArraySortIndicesForUInt8 : public TestArraySortIndices<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesForUInt8, UInt8Type);

template <typename ArrowType>
class TestArraySortIndicesForInt8 : public TestArraySortIndices<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesForInt8, Int8Type);

TYPED_TEST(TestArraySortIndicesForUInt8, SortUInt8) {
  const char* input = "[255, null, 0, 255, 10, null, 128, 0]";
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtEnd,
                          "[2, 7, 4, 6, 0, 3, 1, 5]");
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtStart,
                          "[1, 5, 2, 7, 4, 6, 0, 3]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtEnd,
                          "[0, 3, 6, 4, 2, 7, 1, 5]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtStart,
                          "[1, 5, 0, 3, 6, 4, 2, 7]");
}

TYPED_TEST(TestArraySortIndicesForInt8, SortInt8) {
  const char* input = "[127, null, -128, 127, 0, null, 10, -128]";
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtEnd,
                          "[2, 7, 4, 6, 0, 3, 1, 5]");
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtStart,
                          "[1, 5, 2, 7, 4, 6, 0, 3]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtEnd,
                          "[0, 3, 6, 4, 2, 7, 1, 5]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtStart,
                          "[1, 5, 0, 3, 6, 4, 2, 7]");
}

template <typename ArrowType>
class TestArraySortIndicesForInt64 : public TestArraySortIndices<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesForInt64, Int64Type);

TYPED_TEST(TestArraySortIndicesForInt64, SortInt64) {
  // Values with a large range (use a comparison-based sort)
  const char* input =
      "[null, -2000000000000000, 3000000000000000,"
      " null, -1000000000000000, 5000000000000000]";
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtEnd,
                          "[1, 4, 2, 5, 0, 3]");
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtStart,
                          "[0, 3, 1, 4, 2, 5]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtEnd,
                          "[5, 2, 4, 1, 0, 3]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtStart,
                          "[0, 3, 5, 2, 4, 1]");
}

template <typename ArrowType>
class TestArraySortIndicesForDecimal : public TestArraySortIndicesBase {
 public:
  std::shared_ptr<DataType> type() override { return std::make_shared<ArrowType>(5, 2); }
};
TYPED_TEST_SUITE(TestArraySortIndicesForDecimal, DecimalArrowTypes);

TYPED_TEST(TestArraySortIndicesForDecimal, DecimalSortTestTypes) {
  const char* input = R"(["123.45", null, "-123.45", "456.78", "-456.78", null])";
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtEnd,
                          "[4, 2, 0, 3, 1, 5]");
  this->AssertSortIndices(input, SortOrder::Ascending, NullPlacement::AtStart,
                          "[1, 5, 4, 2, 0, 3]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtEnd,
                          "[3, 0, 2, 4, 1, 5]");
  this->AssertSortIndices(input, SortOrder::Descending, NullPlacement::AtStart,
                          "[1, 5, 3, 0, 2, 4]");
}

TEST(TestArraySortIndices, NullType) {
  auto chunked = ChunkedArrayFromJSON(null(), {"[null, null]", "[]", "[null]", "[null]"});
  for (const auto null_placement : AllNullPlacements()) {
    for (const auto order : AllOrders()) {
      AssertSortIndices(null(), "[null, null, null, null]", order, null_placement,
                        "[0, 1, 2, 3]");
      AssertSortIndices(chunked, order, null_placement, "[0, 1, 2, 3]");
    }
  }
}

TEST(TestArraySortIndices, TemporalTypeParameters) {
  std::vector<std::shared_ptr<DataType>> types;
  for (auto unit : {TimeUnit::NANO, TimeUnit::MICRO, TimeUnit::MILLI, TimeUnit::SECOND}) {
    types.push_back(duration(unit));
    types.push_back(timestamp(unit));
    types.push_back(timestamp(unit, "America/Phoenix"));
  }
  types.push_back(time64(TimeUnit::NANO));
  types.push_back(time64(TimeUnit::MICRO));
  types.push_back(time32(TimeUnit::MILLI));
  types.push_back(time32(TimeUnit::SECOND));
  for (const auto& ty : types) {
    for (auto null_placement : AllNullPlacements()) {
      for (auto order : AllOrders()) {
        AssertSortIndices(ty, "[]", order, null_placement, "[]");
        AssertSortIndices(ty, "[null, null]", order, null_placement, "[0, 1]");
      }
      AssertSortIndices(ty, "[3, 2, 6]", SortOrder::Ascending, null_placement,
                        "[1, 0, 2]");
      AssertSortIndices(ty, "[1, 2, 3, 4, 5, 6, 7]", SortOrder::Ascending, null_placement,
                        "[0, 1, 2, 3, 4, 5, 6]");
      AssertSortIndices(ty, "[7, 6, 5, 4, 3, 2, 1]", SortOrder::Ascending, null_placement,
                        "[6, 5, 4, 3, 2, 1, 0]");

      AssertSortIndices(ty, "[10, 12, 4, 50, 50, 32, 11]", SortOrder::Ascending,
                        null_placement, "[2, 0, 6, 1, 5, 3, 4]");
      AssertSortIndices(ty, "[10, 12, 4, 50, 50, 32, 11]", SortOrder::Descending,
                        null_placement, "[3, 4, 5, 1, 6, 0, 2]");
    }
    AssertSortIndices(ty, "[null, 1, 3, null, 2, 5]", SortOrder::Ascending,
                      NullPlacement::AtEnd, "[1, 4, 2, 5, 0, 3]");
    AssertSortIndices(ty, "[null, 1, 3, null, 2, 5]", SortOrder::Ascending,
                      NullPlacement::AtStart, "[0, 3, 1, 4, 2, 5]");
    AssertSortIndices(ty, "[null, 1, 3, null, 2, 5]", SortOrder::Descending,
                      NullPlacement::AtEnd, "[5, 2, 4, 1, 0, 3]");
    AssertSortIndices(ty, "[null, 1, 3, null, 2, 5]", SortOrder::Descending,
                      NullPlacement::AtStart, "[0, 3, 5, 2, 4, 1]");
  }
}

template <typename ArrowType>
class TestArraySortIndicesRandom : public ::testing::Test {};

template <typename ArrowType>
class TestArraySortIndicesRandomCount : public ::testing::Test {};

template <typename ArrowType>
class TestArraySortIndicesRandomCompare : public ::testing::Test {};

#ifdef ARROW_VALGRIND
using SortIndicesableTypes = ::testing::Types<UInt32Type, FloatType, DoubleType,
                                              StringType, Decimal128Type, BooleanType>;
#else
using SortIndicesableTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, StringType,
                     Decimal128Type, BooleanType>;
#endif

template <typename ArrayType>
void ValidateSorted(const ArrayType& array, UInt64Array& offsets, SortOrder order,
                    NullPlacement null_placement) {
  ValidateOutput(array);
  SortComparator<ArrayType> compare{order, null_placement};
  for (int i = 1; i < array.length(); i++) {
    uint64_t lhs = offsets.Value(i - 1);
    uint64_t rhs = offsets.Value(i);
    ASSERT_TRUE(compare(array, lhs, rhs));
  }
}

TYPED_TEST_SUITE(TestArraySortIndicesRandom, SortIndicesableTypes);

TYPED_TEST(TestArraySortIndicesRandom, SortRandomValues) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  Random<TypeParam> rand(0x5487655);
  int times = 5;
  int length = 100;
  for (int test = 0; test < times; test++) {
    for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
      auto array = rand.Generate(length, null_probability);
      for (auto order : AllOrders()) {
        for (auto null_placement : AllNullPlacements()) {
          ArraySortOptions options(order, null_placement);
          ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets,
                               SortIndices(*array, options));
          ValidateSorted<ArrayType>(*checked_pointer_cast<ArrayType>(array),
                                    *checked_pointer_cast<UInt64Array>(offsets), order,
                                    null_placement);
        }
      }
    }
  }
}

// Long array with small value range: counting sort
// - length >= 1024(CountCompareSorter::countsort_min_len_)
// - range  <= 4096(CountCompareSorter::countsort_max_range_)
TYPED_TEST_SUITE(TestArraySortIndicesRandomCount, IntegralArrowTypes);

TYPED_TEST(TestArraySortIndicesRandomCount, SortRandomValuesCount) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  RandomRange<TypeParam> rand(0x5487656);
  int times = 5;
  int length = 100;
  int range = 2000;
  for (int test = 0; test < times; test++) {
    for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
      auto array = rand.Generate(length, range, null_probability);
      for (auto order : AllOrders()) {
        for (auto null_placement : AllNullPlacements()) {
          ArraySortOptions options(order, null_placement);
          ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets,
                               SortIndices(*array, options));
          ValidateSorted<ArrayType>(*checked_pointer_cast<ArrayType>(array),
                                    *checked_pointer_cast<UInt64Array>(offsets), order,
                                    null_placement);
        }
      }
    }
  }
}

// Long array with big value range: std::stable_sort
TYPED_TEST_SUITE(TestArraySortIndicesRandomCompare, IntegralArrowTypes);

TYPED_TEST(TestArraySortIndicesRandomCompare, SortRandomValuesCompare) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  Random<TypeParam> rand(0x5487657);
  int times = 5;
  int length = 100;
  for (int test = 0; test < times; test++) {
    for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
      auto array = rand.Generate(length, null_probability);
      for (auto order : AllOrders()) {
        for (auto null_placement : AllNullPlacements()) {
          ArraySortOptions options(order, null_placement);
          ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets,
                               SortIndices(*array, options));
          ValidateSorted<ArrayType>(*checked_pointer_cast<ArrayType>(array),
                                    *checked_pointer_cast<UInt64Array>(offsets), order,
                                    null_placement);
        }
      }
    }
  }
}

// Test basic cases for chunked array.
class TestChunkedArraySortIndices : public ::testing::Test {};

TEST_F(TestChunkedArraySortIndices, Null) {
  auto chunked_array = ChunkedArrayFromJSON(uint8(), {
                                                         "[null, 1]",
                                                         "[3, null, 2]",
                                                         "[1]",
                                                     });
  AssertSortIndices(chunked_array, SortOrder::Ascending, NullPlacement::AtEnd,
                    "[1, 5, 4, 2, 0, 3]");
  AssertSortIndices(chunked_array, SortOrder::Ascending, NullPlacement::AtStart,
                    "[0, 3, 1, 5, 4, 2]");
  AssertSortIndices(chunked_array, SortOrder::Descending, NullPlacement::AtEnd,
                    "[2, 4, 1, 5, 0, 3]");
  AssertSortIndices(chunked_array, SortOrder::Descending, NullPlacement::AtStart,
                    "[0, 3, 2, 4, 1, 5]");
}

TEST_F(TestChunkedArraySortIndices, NaN) {
  auto chunked_array = ChunkedArrayFromJSON(float32(), {
                                                           "[null, 1]",
                                                           "[3, null, NaN]",
                                                           "[NaN, 1]",
                                                       });
  AssertSortIndices(chunked_array, SortOrder::Ascending, NullPlacement::AtEnd,
                    "[1, 6, 2, 4, 5, 0, 3]");
  AssertSortIndices(chunked_array, SortOrder::Ascending, NullPlacement::AtStart,
                    "[0, 3, 4, 5, 1, 6, 2]");
  AssertSortIndices(chunked_array, SortOrder::Descending, NullPlacement::AtEnd,
                    "[2, 1, 6, 4, 5, 0, 3]");
  AssertSortIndices(chunked_array, SortOrder::Descending, NullPlacement::AtStart,
                    "[0, 3, 4, 5, 2, 1, 6]");
}

// Tests for temporal types
template <typename ArrowType>
class TestChunkedArraySortIndicesForTemporal : public TestChunkedArraySortIndices {
 protected:
  std::shared_ptr<DataType> GetType() { return default_type_instance<ArrowType>(); }
};
TYPED_TEST_SUITE(TestChunkedArraySortIndicesForTemporal, TemporalArrowTypes);

TYPED_TEST(TestChunkedArraySortIndicesForTemporal, NoNull) {
  auto type = this->GetType();
  auto chunked_array = ChunkedArrayFromJSON(type, {
                                                      "[0, 1]",
                                                      "[3, 2, 1]",
                                                      "[5, 0]",
                                                  });
  for (auto null_placement : AllNullPlacements()) {
    AssertSortIndices(chunked_array, SortOrder::Ascending, null_placement,
                      "[0, 6, 1, 4, 3, 2, 5]");
    AssertSortIndices(chunked_array, SortOrder::Descending, null_placement,
                      "[5, 2, 3, 1, 4, 0, 6]");
  }
}

// Tests for decimal types
template <typename ArrowType>
class TestChunkedArraySortIndicesForDecimal : public TestChunkedArraySortIndices {
 protected:
  std::shared_ptr<DataType> GetType() { return std::make_shared<ArrowType>(5, 2); }
};
TYPED_TEST_SUITE(TestChunkedArraySortIndicesForDecimal, DecimalArrowTypes);

TYPED_TEST(TestChunkedArraySortIndicesForDecimal, Basics) {
  auto type = this->GetType();
  auto chunked_array = ChunkedArrayFromJSON(
      type, {R"(["123.45", "-123.45"])", R"([null, "456.78"])", R"(["-456.78", null])"});
  AssertSortIndices(chunked_array, SortOrder::Ascending, NullPlacement::AtEnd,
                    "[4, 1, 0, 3, 2, 5]");
  AssertSortIndices(chunked_array, SortOrder::Ascending, NullPlacement::AtStart,
                    "[2, 5, 4, 1, 0, 3]");
  AssertSortIndices(chunked_array, SortOrder::Descending, NullPlacement::AtEnd,
                    "[3, 0, 1, 4, 2, 5]");
  AssertSortIndices(chunked_array, SortOrder::Descending, NullPlacement::AtStart,
                    "[2, 5, 3, 0, 1, 4]");
}

// Base class for testing against random chunked array.
template <typename Type>
class TestChunkedArrayRandomBase : public ::testing::Test {
 protected:
  // Generates a chunk. This should be implemented in subclasses.
  virtual std::shared_ptr<Array> GenerateArray(int length, double null_probability) = 0;

  // All tests uses this.
  void TestSortIndices(int length) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;

    for (auto null_probability : {0.0, 0.1, 0.5, 0.9, 1.0}) {
      for (auto num_chunks : {1, 2, 5, 10, 40}) {
        std::vector<std::shared_ptr<Array>> arrays;
        for (int i = 0; i < num_chunks; ++i) {
          auto array = this->GenerateArray(length / num_chunks, null_probability);
          arrays.push_back(array);
        }
        ASSERT_OK_AND_ASSIGN(auto chunked_array, ChunkedArray::Make(arrays));
        // Concatenate chunks to use existing ValidateSorted() for array.
        ASSERT_OK_AND_ASSIGN(auto concatenated_array, Concatenate(arrays));

        for (auto order : AllOrders()) {
          for (auto null_placement : AllNullPlacements()) {
            ArraySortOptions options(order, null_placement);
            ASSERT_OK_AND_ASSIGN(auto offsets, SortIndices(*chunked_array, options));
            ValidateSorted<ArrayType>(
                *checked_pointer_cast<ArrayType>(concatenated_array),
                *checked_pointer_cast<UInt64Array>(offsets), order, null_placement);
          }
        }
      }
    }
  }
};

// Long array with big value range: std::stable_sort
template <typename Type>
class TestChunkedArrayRandom : public TestChunkedArrayRandomBase<Type> {
 public:
  void SetUp() override { rand_ = new Random<Type>(0x5487655); }

  void TearDown() override { delete rand_; }

 protected:
  std::shared_ptr<Array> GenerateArray(int length, double null_probability) override {
    return rand_->Generate(length, null_probability);
  }

 private:
  Random<Type>* rand_;
};
TYPED_TEST_SUITE(TestChunkedArrayRandom, SortIndicesableTypes);

TYPED_TEST(TestChunkedArrayRandom, SortIndices) { this->TestSortIndices(1000); }

// Long array with small value range: counting sort
// - length >= 1024(CountCompareSorter::countsort_min_len_)
// - range  <= 4096(CountCompareSorter::countsort_max_range_)
template <typename Type>
class TestChunkedArrayRandomNarrow : public TestChunkedArrayRandomBase<Type> {
 public:
  void SetUp() override {
    range_ = 2000;
    rand_ = new RandomRange<Type>(0x5487655);
  }

  void TearDown() override { delete rand_; }

 protected:
  std::shared_ptr<Array> GenerateArray(int length, double null_probability) override {
    return rand_->Generate(length, range_, null_probability);
  }

 private:
  int range_;
  RandomRange<Type>* rand_;
};
TYPED_TEST_SUITE(TestChunkedArrayRandomNarrow, IntegralArrowTypes);
TYPED_TEST(TestChunkedArrayRandomNarrow, SortIndices) { this->TestSortIndices(1000); }

// Test basic cases for record batch.
class TestRecordBatchSortIndices : public ::testing::Test {};

TEST_F(TestRecordBatchSortIndices, NoNull) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });
  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": 3,    "b": 5},
                                       {"a": 1,    "b": 3},
                                       {"a": 3,    "b": 4},
                                       {"a": 0,    "b": 6},
                                       {"a": 2,    "b": 5},
                                       {"a": 1,    "b": 5},
                                       {"a": 1,    "b": 3}
                                       ])");

  for (auto null_placement : AllNullPlacements()) {
    SortOptions options(
        {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)},
        null_placement);

    AssertSortIndices(batch, options, "[3, 5, 1, 6, 4, 0, 2]");
  }
}

TEST_F(TestRecordBatchSortIndices, Null) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });
  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": null, "b": 5},
                                       {"a": 1,    "b": 3},
                                       {"a": 3,    "b": null},
                                       {"a": null, "b": null},
                                       {"a": 2,    "b": 5},
                                       {"a": 1,    "b": 5},
                                       {"a": 3,    "b": 5}
                                       ])");
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};

  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(batch, options, "[5, 1, 4, 6, 2, 0, 3]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(batch, options, "[3, 0, 5, 1, 4, 2, 6]");
}

TEST_F(TestRecordBatchSortIndices, NaN) {
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });
  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": 3,    "b": 5},
                                       {"a": 1,    "b": NaN},
                                       {"a": 3,    "b": 4},
                                       {"a": 0,    "b": 6},
                                       {"a": NaN,  "b": 5},
                                       {"a": NaN,  "b": NaN},
                                       {"a": NaN,  "b": 5},
                                       {"a": 1,    "b": 5}
                                      ])");
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};

  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(batch, options, "[3, 7, 1, 0, 2, 4, 6, 5]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(batch, options, "[5, 4, 6, 3, 1, 7, 0, 2]");
}

TEST_F(TestRecordBatchSortIndices, NaNAndNull) {
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });
  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": null, "b": 5},
                                       {"a": 1,    "b": 3},
                                       {"a": 3,    "b": null},
                                       {"a": null, "b": null},
                                       {"a": NaN,  "b": null},
                                       {"a": NaN,  "b": NaN},
                                       {"a": NaN,  "b": 5},
                                       {"a": 1,    "b": 5}
                                      ])");
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};

  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(batch, options, "[7, 1, 2, 6, 5, 4, 0, 3]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(batch, options, "[3, 0, 4, 5, 6, 7, 1, 2]");
}

TEST_F(TestRecordBatchSortIndices, Boolean) {
  auto schema = ::arrow::schema({
      {field("a", boolean())},
      {field("b", boolean())},
  });
  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": true,    "b": null},
                                       {"a": false,   "b": null},
                                       {"a": true,    "b": true},
                                       {"a": false,   "b": true},
                                       {"a": true,    "b": false},
                                       {"a": null,    "b": false},
                                       {"a": false,   "b": null},
                                       {"a": null,    "b": true}
                                       ])");
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};

  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(batch, options, "[3, 1, 6, 2, 4, 0, 7, 5]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(batch, options, "[7, 5, 1, 6, 3, 0, 2, 4]");
}

TEST_F(TestRecordBatchSortIndices, MoreTypes) {
  auto schema = ::arrow::schema({
      {field("a", timestamp(TimeUnit::MICRO))},
      {field("b", large_utf8())},
      {field("c", fixed_size_binary(3))},
  });
  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": 3, "b": "05",   "c": "aaa"},
                                       {"a": 1, "b": "031",  "c": "bbb"},
                                       {"a": 3, "b": "05",   "c": "bbb"},
                                       {"a": 0, "b": "0666", "c": "aaa"},
                                       {"a": 2, "b": "05",   "c": "aaa"},
                                       {"a": 1, "b": "05",   "c": "bbb"}
                                       ])");
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending),
                                       SortKey("c", SortOrder::Ascending)};

  for (auto null_placement : AllNullPlacements()) {
    SortOptions options(sort_keys, null_placement);
    AssertSortIndices(batch, options, "[3, 5, 1, 4, 0, 2]");
  }
}

TEST_F(TestRecordBatchSortIndices, Decimal) {
  auto schema = ::arrow::schema({
      {field("a", decimal128(3, 1))},
      {field("b", decimal256(4, 2))},
  });
  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": "12.3", "b": "12.34"},
                                       {"a": "45.6", "b": "12.34"},
                                       {"a": "12.3", "b": "-12.34"},
                                       {"a": "-12.3", "b": null},
                                       {"a": "-12.3", "b": "-45.67"}
                                       ])");
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};

  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(batch, options, "[4, 3, 0, 2, 1]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(batch, options, "[3, 4, 0, 2, 1]");
}

TEST_F(TestRecordBatchSortIndices, NullType) {
  auto schema = arrow::schema({
      field("a", null()),
      field("b", int32()),
      field("c", int32()),
      field("d", int32()),
      field("e", int32()),
      field("f", int32()),
      field("g", int32()),
      field("h", int32()),
      field("i", null()),
  });
  auto batch = RecordBatchFromJSON(schema, R"([
    {"a": null, "b": 5, "c": 0, "d": 0, "e": 1, "f": 2, "g": 3, "h": 4, "i": null},
    {"a": null, "b": 5, "c": 1, "d": 0, "e": 1, "f": 2, "g": 3, "h": 4, "i": null},
    {"a": null, "b": 2, "c": 2, "d": 0, "e": 1, "f": 2, "g": 3, "h": 4, "i": null},
    {"a": null, "b": 4, "c": 3, "d": 0, "e": 1, "f": 2, "g": 3, "h": 4, "i": null}
])");
  for (const auto null_placement : AllNullPlacements()) {
    for (const auto order : AllOrders()) {
      // Uses radix sorter
      AssertSortIndices(batch,
                        SortOptions(
                            {
                                SortKey("a", order),
                                SortKey("i", order),
                            },
                            null_placement),
                        "[0, 1, 2, 3]");
      AssertSortIndices(batch,
                        SortOptions(
                            {
                                SortKey("a", order),
                                SortKey("b", SortOrder::Ascending),
                                SortKey("i", order),
                            },
                            null_placement),
                        "[2, 3, 0, 1]");
      // Uses multiple-key sorter
      AssertSortIndices(batch,
                        SortOptions(
                            {
                                SortKey("a", order),
                                SortKey("b", SortOrder::Ascending),
                                SortKey("c", SortOrder::Ascending),
                                SortKey("d", SortOrder::Ascending),
                                SortKey("e", SortOrder::Ascending),
                                SortKey("f", SortOrder::Ascending),
                                SortKey("g", SortOrder::Ascending),
                                SortKey("h", SortOrder::Ascending),
                                SortKey("i", order),
                            },
                            null_placement),
                        "[2, 3, 0, 1]");
    }
  }
}

TEST_F(TestRecordBatchSortIndices, DuplicateSortKeys) {
  // ARROW-14073: only the first occurrence of a given sort column is taken
  // into account.
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });
  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": null, "b": 5},
                                       {"a": 1,    "b": 3},
                                       {"a": 3,    "b": null},
                                       {"a": null, "b": null},
                                       {"a": NaN,  "b": null},
                                       {"a": NaN,  "b": NaN},
                                       {"a": NaN,  "b": 5},
                                       {"a": 1,    "b": 5}
                                      ])");
  const std::vector<SortKey> sort_keys{
      SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending),
      SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Ascending),
      SortKey("a", SortOrder::Descending)};

  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(batch, options, "[7, 1, 2, 6, 5, 4, 0, 3]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(batch, options, "[3, 0, 4, 5, 6, 7, 1, 2]");
}

// Test basic cases for table.
class TestTableSortIndices : public ::testing::Test {};

TEST_F(TestTableSortIndices, EmptyTable) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};

  auto table = TableFromJSON(schema, {"[]"});
  auto chunked_table = TableFromJSON(schema, {"[]", "[]"});

  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(table, options, "[]");
  AssertSortIndices(chunked_table, options, "[]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[]");
  AssertSortIndices(chunked_table, options, "[]");
}

TEST_F(TestTableSortIndices, EmptySortKeys) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });
  const std::vector<SortKey> sort_keys{};
  const SortOptions options(sort_keys, NullPlacement::AtEnd);

  auto table = TableFromJSON(schema, {R"([{"a": null, "b": 5}])"});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Must specify one or more sort keys"),
      CallFunction("sort_indices", {table}, &options));

  // Several chunks
  table = TableFromJSON(schema, {R"([{"a": null, "b": 5}])", R"([{"a": 0, "b": 6}])"});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Must specify one or more sort keys"),
      CallFunction("sort_indices", {table}, &options));
}

TEST_F(TestTableSortIndices, Null) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};
  std::shared_ptr<Table> table;

  table = TableFromJSON(schema, {R"([{"a": null, "b": 5},
                                     {"a": 1,    "b": 3},
                                     {"a": 3,    "b": null},
                                     {"a": null, "b": null},
                                     {"a": 2,    "b": 5},
                                     {"a": 1,    "b": 5},
                                     {"a": 3,    "b": 5}
                                    ])"});
  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(table, options, "[5, 1, 4, 6, 2, 0, 3]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[3, 0, 5, 1, 4, 2, 6]");

  // Same data, several chunks
  table = TableFromJSON(schema, {R"([{"a": null, "b": 5},
                                     {"a": 1,    "b": 3},
                                     {"a": 3,    "b": null}
                                    ])",
                                 R"([{"a": null, "b": null},
                                     {"a": 2,    "b": 5},
                                     {"a": 1,    "b": 5},
                                     {"a": 3,    "b": 5}
                                    ])"});
  options.null_placement = NullPlacement::AtEnd;
  AssertSortIndices(table, options, "[5, 1, 4, 6, 2, 0, 3]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[3, 0, 5, 1, 4, 2, 6]");
}

TEST_F(TestTableSortIndices, NaN) {
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};
  std::shared_ptr<Table> table;

  table = TableFromJSON(schema, {R"([{"a": 3,    "b": 5},
                                     {"a": 1,    "b": NaN},
                                     {"a": 3,    "b": 4},
                                     {"a": 0,    "b": 6},
                                     {"a": NaN,  "b": 5},
                                     {"a": NaN,  "b": NaN},
                                     {"a": NaN,  "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"});
  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(table, options, "[3, 7, 1, 0, 2, 4, 6, 5]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[5, 4, 6, 3, 1, 7, 0, 2]");

  // Same data, several chunks
  table = TableFromJSON(schema, {R"([{"a": 3,    "b": 5},
                                     {"a": 1,    "b": NaN},
                                     {"a": 3,    "b": 4},
                                     {"a": 0,    "b": 6}
                                    ])",
                                 R"([{"a": NaN,  "b": 5},
                                     {"a": NaN,  "b": NaN},
                                     {"a": NaN,  "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"});
  options.null_placement = NullPlacement::AtEnd;
  AssertSortIndices(table, options, "[3, 7, 1, 0, 2, 4, 6, 5]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[5, 4, 6, 3, 1, 7, 0, 2]");
}

TEST_F(TestTableSortIndices, NaNAndNull) {
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};
  std::shared_ptr<Table> table;

  table = TableFromJSON(schema, {R"([{"a": null, "b": 5},
                                     {"a": 1,    "b": 3},
                                     {"a": 3,    "b": null},
                                     {"a": null, "b": null},
                                     {"a": NaN,  "b": null},
                                     {"a": NaN,  "b": NaN},
                                     {"a": NaN,  "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"});
  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(table, options, "[7, 1, 2, 6, 5, 4, 0, 3]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[3, 0, 4, 5, 6, 7, 1, 2]");

  // Same data, several chunks
  table = TableFromJSON(schema, {R"([{"a": null, "b": 5},
                                     {"a": 1,    "b": 3},
                                     {"a": 3,    "b": null},
                                     {"a": null, "b": null}
                                    ])",
                                 R"([{"a": NaN,  "b": null},
                                     {"a": NaN,  "b": NaN},
                                     {"a": NaN,  "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"});
  options.null_placement = NullPlacement::AtEnd;
  AssertSortIndices(table, options, "[7, 1, 2, 6, 5, 4, 0, 3]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[3, 0, 4, 5, 6, 7, 1, 2]");
}

TEST_F(TestTableSortIndices, Boolean) {
  auto schema = ::arrow::schema({
      {field("a", boolean())},
      {field("b", boolean())},
  });
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};

  auto table = TableFromJSON(schema, {R"([{"a": true,    "b": null},
                                          {"a": false,   "b": null},
                                          {"a": true,    "b": true},
                                          {"a": false,   "b": true}
                                         ])",
                                      R"([{"a": true,    "b": false},
                                          {"a": null,    "b": false},
                                          {"a": false,   "b": null},
                                          {"a": null,    "b": true}
                                         ])"});
  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(table, options, "[3, 1, 6, 2, 4, 0, 7, 5]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[7, 5, 1, 6, 3, 0, 2, 4]");
}

TEST_F(TestTableSortIndices, BinaryLike) {
  auto schema = ::arrow::schema({
      {field("a", large_utf8())},
      {field("b", fixed_size_binary(3))},
  });
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Descending),
                                       SortKey("b", SortOrder::Ascending)};

  auto table = TableFromJSON(schema, {R"([{"a": "one", "b": null},
                                          {"a": "two", "b": "aaa"},
                                          {"a": "three", "b": "bbb"},
                                          {"a": "four", "b": "ccc"}
                                         ])",
                                      R"([{"a": "one", "b": "ddd"},
                                          {"a": "two", "b": "ccc"},
                                          {"a": "three", "b": "bbb"},
                                          {"a": "four", "b": "aaa"}
                                         ])"});
  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(table, options, "[1, 5, 2, 6, 4, 0, 7, 3]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[1, 5, 2, 6, 0, 4, 7, 3]");
}

TEST_F(TestTableSortIndices, Decimal) {
  auto schema = ::arrow::schema({
      {field("a", decimal128(3, 1))},
      {field("b", decimal256(4, 2))},
  });
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};

  auto table = TableFromJSON(schema, {R"([{"a": "12.3", "b": "12.34"},
                                          {"a": "45.6", "b": "12.34"},
                                          {"a": "12.3", "b": "-12.34"}
                                          ])",
                                      R"([{"a": "-12.3", "b": null},
                                          {"a": "-12.3", "b": "-45.67"}
                                          ])"});
  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(table, options, "[4, 3, 0, 2, 1]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[3, 4, 0, 2, 1]");
}

TEST_F(TestTableSortIndices, NullType) {
  auto schema = arrow::schema({
      field("a", null()),
      field("b", int32()),
      field("c", int32()),
      field("d", null()),
  });
  auto table = TableFromJSON(schema, {
                                         R"([
                                             {"a": null, "b": 5, "c": 0, "d": null},
                                             {"a": null, "b": 5, "c": 1, "d": null},
                                             {"a": null, "b": 2, "c": 2, "d": null}
                                         ])",
                                         R"([])",
                                         R"([{"a": null, "b": 4, "c": 3, "d": null}])",
                                     });
  for (const auto null_placement : AllNullPlacements()) {
    for (const auto order : AllOrders()) {
      AssertSortIndices(table,
                        SortOptions(
                            {
                                SortKey("a", order),
                                SortKey("d", order),
                            },
                            null_placement),
                        "[0, 1, 2, 3]");
      AssertSortIndices(table,
                        SortOptions(
                            {
                                SortKey("a", order),
                                SortKey("b", SortOrder::Ascending),
                                SortKey("d", order),
                            },
                            null_placement),
                        "[2, 3, 0, 1]");
    }
  }
}

TEST_F(TestTableSortIndices, DuplicateSortKeys) {
  // ARROW-14073: only the first occurrence of a given sort column is taken
  // into account.
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });
  const std::vector<SortKey> sort_keys{
      SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending),
      SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Ascending),
      SortKey("a", SortOrder::Descending)};
  std::shared_ptr<Table> table;

  table = TableFromJSON(schema, {R"([{"a": null, "b": 5},
                                     {"a": 1,    "b": 3},
                                     {"a": 3,    "b": null},
                                     {"a": null, "b": null}
                                    ])",
                                 R"([{"a": NaN,  "b": null},
                                     {"a": NaN,  "b": NaN},
                                     {"a": NaN,  "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"});
  SortOptions options(sort_keys, NullPlacement::AtEnd);
  AssertSortIndices(table, options, "[7, 1, 2, 6, 5, 4, 0, 3]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[3, 0, 4, 5, 6, 7, 1, 2]");
}

TEST_F(TestTableSortIndices, HeterogenousChunking) {
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });

  // Same logical data as in "NaNAndNull" test above
  auto col_a =
      ChunkedArrayFromJSON(float32(), {"[null, 1]", "[]", "[3, null, NaN, NaN, NaN, 1]"});
  auto col_b = ChunkedArrayFromJSON(float64(),
                                    {"[5]", "[3, null, null]", "[null, NaN, 5]", "[5]"});
  auto table = Table::Make(schema, {col_a, col_b});

  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});
  AssertSortIndices(table, options, "[7, 1, 2, 6, 5, 4, 0, 3]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[3, 0, 4, 5, 6, 7, 1, 2]");

  options = SortOptions(
      {SortKey("b", SortOrder::Ascending), SortKey("a", SortOrder::Descending)});
  AssertSortIndices(table, options, "[1, 7, 6, 0, 5, 2, 4, 3]");
  options.null_placement = NullPlacement::AtStart;
  AssertSortIndices(table, options, "[3, 4, 2, 5, 1, 0, 6, 7]");
}

// Tests for temporal types
template <typename ArrowType>
class TestTableSortIndicesForTemporal : public TestTableSortIndices {
 protected:
  std::shared_ptr<DataType> GetType() { return default_type_instance<ArrowType>(); }
};
TYPED_TEST_SUITE(TestTableSortIndicesForTemporal, TemporalArrowTypes);

TYPED_TEST(TestTableSortIndicesForTemporal, NoNull) {
  auto type = this->GetType();
  const std::vector<SortKey> sort_keys{SortKey("a", SortOrder::Ascending),
                                       SortKey("b", SortOrder::Descending)};
  auto table = TableFromJSON(schema({
                                 {field("a", type)},
                                 {field("b", type)},
                             }),
                             {R"([{"a": 0, "b": 5},
                                  {"a": 1, "b": 3},
                                  {"a": 3, "b": 0},
                                  {"a": 2, "b": 1},
                                  {"a": 1, "b": 3},
                                  {"a": 5, "b": 0},
                                  {"a": 0, "b": 4},
                                  {"a": 1, "b": 2}
                                 ])"});
  for (auto null_placement : AllNullPlacements()) {
    SortOptions options(sort_keys, null_placement);
    AssertSortIndices(table, options, "[0, 6, 1, 4, 7, 3, 2, 5]");
  }
}

// For random table tests.
using RandomParam = std::tuple<std::string, int, double>;

class TestTableSortIndicesRandom : public testing::TestWithParam<RandomParam> {
  // Compares two records in a column
  class ColumnComparator : public TypeVisitor {
   public:
    ColumnComparator(SortOrder order, NullPlacement null_placement)
        : order_(order), null_placement_(null_placement) {}

    int operator()(const Array& left, const Array& right, uint64_t lhs, uint64_t rhs) {
      left_ = &left;
      right_ = &right;
      lhs_ = lhs;
      rhs_ = rhs;
      ARROW_CHECK_OK(left.type()->Accept(this));
      return compared_;
    }

#define VISIT(TYPE)                               \
  Status Visit(const TYPE##Type& type) override { \
    compared_ = CompareType<TYPE##Type>();        \
    return Status::OK();                          \
  }

    VISIT(Boolean)
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
    VISIT(LargeString)
    VISIT(Decimal128)
    VISIT(Decimal256)

#undef VISIT

    template <typename Type>
    int CompareType() {
      using ArrayType = typename TypeTraits<Type>::ArrayType;
      ThreeWayComparator<ArrayType> three_way{order_, null_placement_};
      return three_way(checked_cast<const ArrayType&>(*left_),
                       checked_cast<const ArrayType&>(*right_), lhs_, rhs_);
    }

    const SortOrder order_;
    const NullPlacement null_placement_;
    const Array* left_;
    const Array* right_;
    uint64_t lhs_;
    uint64_t rhs_;
    int compared_;
  };

  // Compares two records in the same table.
  class Comparator {
   public:
    Comparator(const Table& table, const SortOptions& options) : options_(options) {
      for (const auto& sort_key : options_.sort_keys) {
        DCHECK(!sort_key.target.IsNested());

        if (auto name = sort_key.target.name()) {
          sort_columns_.emplace_back(table.GetColumnByName(*name).get(), sort_key.order);
          continue;
        }

        auto index = sort_key.target.field_path()->indices()[0];
        sort_columns_.emplace_back(table.column(index).get(), sort_key.order);
      }
    }

    // Return true if the left record is less or equals to the right record,
    // false otherwise.
    bool operator()(uint64_t lhs, uint64_t rhs) {
      for (const auto& pair : sort_columns_) {
        ColumnComparator comparator(pair.second, options_.null_placement);
        const auto& chunked_array = *pair.first;
        int64_t lhs_index = 0, rhs_index = 0;
        const Array* lhs_array = FindTargetArray(chunked_array, lhs, &lhs_index);
        const Array* rhs_array = FindTargetArray(chunked_array, rhs, &rhs_index);
        int compared = comparator(*lhs_array, *rhs_array, lhs_index, rhs_index);
        if (compared != 0) {
          return compared < 0;
        }
      }
      return lhs < rhs;
    }

    // Find the target chunk and index in the target chunk from an
    // index in chunked array.
    const Array* FindTargetArray(const ChunkedArray& chunked_array, int64_t i,
                                 int64_t* chunk_index) {
      int64_t offset = 0;
      for (const auto& chunk : chunked_array.chunks()) {
        if (i < offset + chunk->length()) {
          *chunk_index = i - offset;
          return chunk.get();
        }
        offset += chunk->length();
      }
      return nullptr;
    }

    const SortOptions& options_;
    std::vector<std::pair<const ChunkedArray*, SortOrder>> sort_columns_;
  };

 public:
  // Validates the sorted indices are really sorted.
  void Validate(const Table& table, const SortOptions& options, UInt64Array& offsets) {
    ValidateOutput(offsets);
    Comparator comparator{table, options};
    for (int i = 1; i < table.num_rows(); i++) {
      uint64_t lhs = offsets.Value(i - 1);
      uint64_t rhs = offsets.Value(i);
      if (!comparator(lhs, rhs)) {
        std::stringstream ss;
        ss << "Rows not ordered at consecutive sort indices:";
        ss << "\nFirst row (index = " << lhs << "): ";
        PrintRow(table, lhs, &ss);
        ss << "\nSecond row (index = " << rhs << "): ";
        PrintRow(table, rhs, &ss);
        FAIL() << ss.str();
      }
    }
  }

  void PrintRow(const Table& table, uint64_t index, std::ostream* os) {
    *os << "{";
    const auto& columns = table.columns();
    for (size_t i = 0; i < columns.size(); ++i) {
      if (i != 0) {
        *os << ", ";
      }
      ASSERT_OK_AND_ASSIGN(auto scal, columns[i]->GetScalar(index));
      *os << scal->ToString();
    }
    *os << "}";
  }
};

TEST_P(TestTableSortIndicesRandom, Sort) {
  const auto first_sort_key_name = std::get<0>(GetParam());
  const auto n_sort_keys = std::get<1>(GetParam());
  const auto null_probability = std::get<2>(GetParam());
  const auto nan_probability = (1.0 - null_probability) / 4;
  const auto seed = 0x61549225;

  ARROW_SCOPED_TRACE("n_sort_keys = ", n_sort_keys);
  ARROW_SCOPED_TRACE("null_probability = ", null_probability);

  ::arrow::random::RandomArrayGenerator rng(seed);

  // Of these, "uint8", "boolean" and "string" should have many duplicates
  const FieldVector fields = {
      {field("uint8", uint8())},
      {field("int16", int16())},
      {field("int32", int32())},
      {field("uint64", uint64())},
      {field("float", float32())},
      {field("boolean", boolean())},
      {field("string", utf8())},
      {field("large_string", large_utf8())},
      {field("decimal128", decimal128(25, 3))},
      {field("decimal256", decimal256(42, 6))},
  };
  const auto schema = ::arrow::schema(fields);
  const int64_t length = 80;

  using ArrayFactory = std::function<std::shared_ptr<Array>(int64_t length)>;

  std::vector<ArrayFactory> column_factories{
      [&](int64_t length) { return rng.UInt8(length, 0, 10, null_probability); },
      [&](int64_t length) {
        return rng.Int16(length, -1000, 12000, /*null_probability=*/0.0);
      },
      [&](int64_t length) {
        return rng.Int32(length, -123456789, 987654321, null_probability);
      },
      [&](int64_t length) {
        return rng.UInt64(length, 1, 1234567890123456789ULL, /*null_probability=*/0.0);
      },
      [&](int64_t length) {
        return rng.Float32(length, -1.0f, 1.0f, null_probability, nan_probability);
      },
      [&](int64_t length) {
        return rng.Boolean(length, /*true_probability=*/0.3, null_probability);
      },
      [&](int64_t length) {
        if (length > 0) {
          return rng.StringWithRepeats(length, /*unique=*/1 + length / 10,
                                       /*min_length=*/5,
                                       /*max_length=*/15, null_probability);
        } else {
          return *MakeArrayOfNull(utf8(), 0);
        }
      },
      [&](int64_t length) {
        return rng.LargeString(length, /*min_length=*/5, /*max_length=*/15,
                               /*null_probability=*/0.0);
      },
      [&](int64_t length) {
        return rng.Decimal128(fields[8]->type(), length, null_probability);
      },
      [&](int64_t length) {
        return rng.Decimal256(fields[9]->type(), length, /*null_probability=*/0.0);
      },
  };

  // Generate random sort keys, making sure no column is included twice
  std::default_random_engine engine(seed);
  std::uniform_int_distribution<> distribution(0);

  auto generate_order = [&]() {
    return (distribution(engine) & 1) ? SortOrder::Ascending : SortOrder::Descending;
  };

  std::vector<SortKey> sort_keys;
  sort_keys.reserve(fields.size());
  for (const auto& field : fields) {
    if (field->name() != first_sort_key_name) {
      sort_keys.emplace_back(field->name(), generate_order());
    }
  }
  std::shuffle(sort_keys.begin(), sort_keys.end(), engine);
  sort_keys.emplace(sort_keys.begin(), first_sort_key_name, generate_order());
  sort_keys.erase(sort_keys.begin() + n_sort_keys, sort_keys.end());
  ASSERT_EQ(sort_keys.size(), n_sort_keys);

  std::stringstream ss;
  for (const auto& sort_key : sort_keys) {
    ss << sort_key.target.ToString()
       << (sort_key.order == SortOrder::Ascending ? " ASC" : " DESC");
    ss << ", ";
  }
  ARROW_SCOPED_TRACE("sort_keys = ", ss.str());

  SortOptions options(sort_keys);

  // Test with different, heterogenous table chunkings
  for (const int64_t max_num_chunks : {1, 3, 15}) {
    ARROW_SCOPED_TRACE("Table sorting: max chunks per column = ", max_num_chunks);
    std::uniform_int_distribution<int64_t> num_chunk_dist(1 + max_num_chunks / 2,
                                                          max_num_chunks);
    ChunkedArrayVector columns;
    columns.reserve(fields.size());

    // Chunk each column independently, and make sure they consist of
    // physically non-contiguous chunks.
    for (const auto& factory : column_factories) {
      const int64_t num_chunks = num_chunk_dist(engine);
      ArrayVector chunks(num_chunks);
      const auto offsets =
          checked_pointer_cast<Int32Array>(rng.Offsets(num_chunks + 1, 0, length));
      for (int64_t i = 0; i < num_chunks; ++i) {
        const auto chunk_len = offsets->Value(i + 1) - offsets->Value(i);
        chunks[i] = factory(chunk_len);
      }
      columns.push_back(std::make_shared<ChunkedArray>(std::move(chunks)));
      ASSERT_EQ(columns.back()->length(), length);
    }

    auto table = Table::Make(schema, std::move(columns));
    for (auto null_placement : AllNullPlacements()) {
      ARROW_SCOPED_TRACE("null_placement = ", null_placement);
      options.null_placement = null_placement;
      ASSERT_OK_AND_ASSIGN(auto offsets, SortIndices(Datum(*table), options));
      Validate(*table, options, *checked_pointer_cast<UInt64Array>(offsets));
    }
  }

  // Also validate RecordBatch sorting
  ARROW_SCOPED_TRACE("Record batch sorting");
  ArrayVector columns;
  columns.reserve(fields.size());
  for (const auto& factory : column_factories) {
    columns.push_back(factory(length));
  }
  auto batch = RecordBatch::Make(schema, length, std::move(columns));
  ASSERT_OK(batch->ValidateFull());
  ASSERT_OK_AND_ASSIGN(auto table, Table::FromRecordBatches(schema, {batch}));

  for (auto null_placement : AllNullPlacements()) {
    ARROW_SCOPED_TRACE("null_placement = ", null_placement);
    options.null_placement = null_placement;
    ASSERT_OK_AND_ASSIGN(auto offsets, SortIndices(Datum(batch), options));
    Validate(*table, options, *checked_pointer_cast<UInt64Array>(offsets));
  }
}

#ifdef ARROW_VALGRIND
static const auto first_sort_keys = testing::Values("uint64");
static const auto num_sort_keys = testing::Values(3);
#else
// Some first keys will have duplicates, others not
static const auto first_sort_keys = testing::Values("uint8", "int16", "uint64", "float",
                                                    "boolean", "string", "decimal128");

// Different numbers of sort keys may trigger different algorithms
static const auto num_sort_keys = testing::Values(1, 3, 7, 9);
#endif

INSTANTIATE_TEST_SUITE_P(NoNull, TestTableSortIndicesRandom,
                         testing::Combine(first_sort_keys, num_sort_keys,
                                          testing::Values(0.0)));

INSTANTIATE_TEST_SUITE_P(SomeNulls, TestTableSortIndicesRandom,
                         testing::Combine(first_sort_keys, num_sort_keys,
                                          testing::Values(0.1, 0.5)));

INSTANTIATE_TEST_SUITE_P(AllNull, TestTableSortIndicesRandom,
                         testing::Combine(first_sort_keys, num_sort_keys,
                                          testing::Values(1.0)));

// ----------------------------------------------------------------------
// Tests for Rank

class TestRank : public ::testing::Test {
 protected:
  // Create several test datums from `array`. One of which is the unmodified Array
  // while the rest are chunked variants based on it.
  void SetInput(const std::shared_ptr<Array>& array) {
    datums_ = {array};
    const int64_t length = array->length();
    for (int64_t num_chunks : {1, 2, 3}) {
      if (num_chunks > length) continue;
      ArrayVector chunks;
      int64_t offset = 0;
      for (int64_t i = 0; i < num_chunks; ++i) {
        auto next_offset = offset + length / num_chunks;
        if (i + 1 == num_chunks) {
          next_offset = length;
        }
        chunks.push_back(array->Slice(offset, next_offset - offset));
        offset = next_offset;
      }
      auto chunked = ChunkedArray::Make(std::move(chunks), array->type()).ValueOrDie();
      DCHECK_EQ(chunked->num_chunks(), num_chunks);
      datums_.push_back(std::move(chunked));
    }
  }

  void SetInput(const std::shared_ptr<ChunkedArray>& chunked_array) {
    datums_ = {chunked_array};
  }

  static void AssertRank(const DatumVector& datums, SortOrder order,
                         NullPlacement null_placement, RankOptions::Tiebreaker tiebreaker,
                         const std::shared_ptr<Array>& expected) {
    const std::vector<SortKey> sort_keys{SortKey("foo", order)};
    RankOptions options(sort_keys, null_placement, tiebreaker);
    ARROW_SCOPED_TRACE("options = ", options.ToString());
    for (const auto& datum : datums) {
      ASSERT_OK_AND_ASSIGN(auto actual, CallFunction("rank", {datum}, &options));
      ValidateOutput(actual);
      AssertDatumsEqual(expected, actual, /*verbose=*/true);
    }
  }

  void AssertRank(SortOrder order, NullPlacement null_placement,
                  RankOptions::Tiebreaker tiebreaker,
                  const std::shared_ptr<Array>& expected) const {
    AssertRank(datums_, order, null_placement, tiebreaker, expected);
  }

  static void AssertRankEmpty(std::shared_ptr<DataType> type, SortOrder order,
                              NullPlacement null_placement,
                              RankOptions::Tiebreaker tiebreaker) {
    AssertRank({ArrayFromJSON(type, "[]")}, order, null_placement, tiebreaker,
               ArrayFromJSON(uint64(), "[]"));
    AssertRank({ArrayFromJSON(type, "[null]")}, order, null_placement, tiebreaker,
               ArrayFromJSON(uint64(), "[1]"));
  }

  void AssertRankSimple(NullPlacement null_placement,
                        RankOptions::Tiebreaker tiebreaker) const {
    auto expected_asc = ArrayFromJSON(uint64(), "[3, 4, 2, 1, 5]");
    AssertRank(SortOrder::Ascending, null_placement, tiebreaker, expected_asc);

    auto expected_desc = ArrayFromJSON(uint64(), "[3, 2, 4, 5, 1]");
    AssertRank(SortOrder::Descending, null_placement, tiebreaker, expected_desc);
  }

  void AssertRankAllTiebreakers() const {
    AssertRank(SortOrder::Ascending, NullPlacement::AtEnd, RankOptions::Min,
               ArrayFromJSON(uint64(), "[3, 1, 4, 6, 4, 6, 1]"));
    AssertRank(SortOrder::Ascending, NullPlacement::AtEnd, RankOptions::Max,
               ArrayFromJSON(uint64(), "[3, 2, 5, 7, 5, 7, 2]"));
    AssertRank(SortOrder::Ascending, NullPlacement::AtEnd, RankOptions::First,
               ArrayFromJSON(uint64(), "[3, 1, 4, 6, 5, 7, 2]"));
    AssertRank(SortOrder::Ascending, NullPlacement::AtEnd, RankOptions::Dense,
               ArrayFromJSON(uint64(), "[2, 1, 3, 4, 3, 4, 1]"));

    AssertRank(SortOrder::Ascending, NullPlacement::AtStart, RankOptions::Min,
               ArrayFromJSON(uint64(), "[5, 3, 6, 1, 6, 1, 3]"));
    AssertRank(SortOrder::Ascending, NullPlacement::AtStart, RankOptions::Max,
               ArrayFromJSON(uint64(), "[5, 4, 7, 2, 7, 2, 4]"));
    AssertRank(SortOrder::Ascending, NullPlacement::AtStart, RankOptions::First,
               ArrayFromJSON(uint64(), "[5, 3, 6, 1, 7, 2, 4]"));
    AssertRank(SortOrder::Ascending, NullPlacement::AtStart, RankOptions::Dense,
               ArrayFromJSON(uint64(), "[3, 2, 4, 1, 4, 1, 2]"));

    AssertRank(SortOrder::Descending, NullPlacement::AtEnd, RankOptions::Min,
               ArrayFromJSON(uint64(), "[3, 4, 1, 6, 1, 6, 4]"));
    AssertRank(SortOrder::Descending, NullPlacement::AtEnd, RankOptions::Max,
               ArrayFromJSON(uint64(), "[3, 5, 2, 7, 2, 7, 5]"));
    AssertRank(SortOrder::Descending, NullPlacement::AtEnd, RankOptions::First,
               ArrayFromJSON(uint64(), "[3, 4, 1, 6, 2, 7, 5]"));
    AssertRank(SortOrder::Descending, NullPlacement::AtEnd, RankOptions::Dense,
               ArrayFromJSON(uint64(), "[2, 3, 1, 4, 1, 4, 3]"));

    AssertRank(SortOrder::Descending, NullPlacement::AtStart, RankOptions::Min,
               ArrayFromJSON(uint64(), "[5, 6, 3, 1, 3, 1, 6]"));
    AssertRank(SortOrder::Descending, NullPlacement::AtStart, RankOptions::Max,
               ArrayFromJSON(uint64(), "[5, 7, 4, 2, 4, 2, 7]"));
    AssertRank(SortOrder::Descending, NullPlacement::AtStart, RankOptions::First,
               ArrayFromJSON(uint64(), "[5, 6, 3, 1, 4, 2, 7]"));
    AssertRank(SortOrder::Descending, NullPlacement::AtStart, RankOptions::Dense,
               ArrayFromJSON(uint64(), "[3, 4, 2, 1, 2, 1, 4]"));
  }

  DatumVector datums_;
};

TEST_F(TestRank, Real) {
  for (auto real_type : ::arrow::FloatingPointTypes()) {
    SetInput(ArrayFromJSON(real_type, "[2.1, 3.2, 1.0, 0.0, 5.5]"));
    for (auto null_placement : AllNullPlacements()) {
      for (auto tiebreaker : AllTiebreakers()) {
        for (auto order : AllOrders()) {
          AssertRankEmpty(real_type, order, null_placement, tiebreaker);
        }

        AssertRankSimple(null_placement, tiebreaker);
      }
    }

    SetInput(ArrayFromJSON(real_type, "[1.2, 0.0, 5.3, null, 5.3, null, 0.0]"));
    AssertRankAllTiebreakers();
  }
}

TEST_F(TestRank, Integral) {
  for (auto integer_type : ::arrow::IntTypes()) {
    SetInput(ArrayFromJSON(integer_type, "[2, 3, 1, 0, 5]"));
    for (auto null_placement : AllNullPlacements()) {
      for (auto tiebreaker : AllTiebreakers()) {
        for (auto order : AllOrders()) {
          AssertRankEmpty(integer_type, order, null_placement, tiebreaker);
        }

        AssertRankSimple(null_placement, tiebreaker);
      }
    }

    SetInput(ArrayFromJSON(integer_type, "[1, 0, 5, null, 5, null, 0]"));
    AssertRankAllTiebreakers();
  }
}

TEST_F(TestRank, Bool) {
  SetInput(ArrayFromJSON(boolean(), "[false, true]"));
  for (auto null_placement : AllNullPlacements()) {
    for (auto tiebreaker : AllTiebreakers()) {
      for (auto order : AllOrders()) {
        AssertRankEmpty(boolean(), order, null_placement, tiebreaker);
      }

      AssertRank(SortOrder::Ascending, null_placement, tiebreaker,
                 ArrayFromJSON(uint64(), "[1, 2]"));
      AssertRank(SortOrder::Descending, null_placement, tiebreaker,
                 ArrayFromJSON(uint64(), "[2, 1]"));
    }
  }

  SetInput(ArrayFromJSON(boolean(), "[true, false, true, null, true, null, false]"));
  AssertRank(SortOrder::Ascending, NullPlacement::AtEnd, RankOptions::Min,
             ArrayFromJSON(uint64(), "[3, 1, 3, 6, 3, 6, 1]"));
  AssertRank(SortOrder::Ascending, NullPlacement::AtEnd, RankOptions::Max,
             ArrayFromJSON(uint64(), "[5, 2, 5, 7, 5, 7, 2]"));
  AssertRank(SortOrder::Ascending, NullPlacement::AtEnd, RankOptions::First,
             ArrayFromJSON(uint64(), "[3, 1, 4, 6, 5, 7, 2]"));
  AssertRank(SortOrder::Ascending, NullPlacement::AtEnd, RankOptions::Dense,
             ArrayFromJSON(uint64(), "[2, 1, 2, 3, 2, 3, 1]"));

  AssertRank(SortOrder::Ascending, NullPlacement::AtStart, RankOptions::Min,
             ArrayFromJSON(uint64(), "[5, 3, 5, 1, 5, 1, 3]"));
  AssertRank(SortOrder::Ascending, NullPlacement::AtStart, RankOptions::Max,
             ArrayFromJSON(uint64(), "[7, 4, 7, 2, 7, 2, 4]"));
  AssertRank(SortOrder::Ascending, NullPlacement::AtStart, RankOptions::First,
             ArrayFromJSON(uint64(), "[5, 3, 6, 1, 7, 2, 4]"));
  AssertRank(SortOrder::Ascending, NullPlacement::AtStart, RankOptions::Dense,
             ArrayFromJSON(uint64(), "[3, 2, 3, 1, 3, 1, 2]"));

  AssertRank(SortOrder::Descending, NullPlacement::AtEnd, RankOptions::Min,
             ArrayFromJSON(uint64(), "[1, 4, 1, 6, 1, 6, 4]"));
  AssertRank(SortOrder::Descending, NullPlacement::AtEnd, RankOptions::Max,
             ArrayFromJSON(uint64(), "[3, 5, 3, 7, 3, 7, 5]"));
  AssertRank(SortOrder::Descending, NullPlacement::AtEnd, RankOptions::First,
             ArrayFromJSON(uint64(), "[1, 4, 2, 6, 3, 7, 5]"));
  AssertRank(SortOrder::Descending, NullPlacement::AtEnd, RankOptions::Dense,
             ArrayFromJSON(uint64(), "[1, 2, 1, 3, 1, 3, 2]"));

  AssertRank(SortOrder::Descending, NullPlacement::AtStart, RankOptions::Min,
             ArrayFromJSON(uint64(), "[3, 6, 3, 1, 3, 1, 6]"));
  AssertRank(SortOrder::Descending, NullPlacement::AtStart, RankOptions::Max,
             ArrayFromJSON(uint64(), "[5, 7, 5, 2, 5, 2, 7]"));
  AssertRank(SortOrder::Descending, NullPlacement::AtStart, RankOptions::First,
             ArrayFromJSON(uint64(), "[3, 6, 4, 1, 5, 2, 7]"));
  AssertRank(SortOrder::Descending, NullPlacement::AtStart, RankOptions::Dense,
             ArrayFromJSON(uint64(), "[2, 3, 2, 1, 2, 1, 3]"));
}

TEST_F(TestRank, Temporal) {
  for (auto temporal_type : ::arrow::TemporalTypes()) {
    SetInput(ArrayFromJSON(temporal_type, "[2, 3, 1, 0, 5]"));
    for (auto null_placement : AllNullPlacements()) {
      for (auto tiebreaker : AllTiebreakers()) {
        for (auto order : AllOrders()) {
          AssertRankEmpty(temporal_type, order, null_placement, tiebreaker);
        }

        AssertRankSimple(null_placement, tiebreaker);
      }
    }

    SetInput(ArrayFromJSON(temporal_type, "[1, 0, 5, null, 5, null, 0]"));
    AssertRankAllTiebreakers();
  }
}

TEST_F(TestRank, String) {
  for (auto string_type : ::arrow::StringTypes()) {
    SetInput(ArrayFromJSON(string_type, R"(["b", "c", "a", "", "d"])"));
    for (auto null_placement : AllNullPlacements()) {
      for (auto tiebreaker : AllTiebreakers()) {
        for (auto order : AllOrders()) {
          AssertRankEmpty(string_type, order, null_placement, tiebreaker);
        }

        AssertRankSimple(null_placement, tiebreaker);
      }
    }

    SetInput(ArrayFromJSON(string_type, R"(["a", "", "e", null, "e", null, ""])"));
    AssertRankAllTiebreakers();
  }
}

TEST_F(TestRank, FixedSizeBinary) {
  auto binary_type = fixed_size_binary(3);
  SetInput(ArrayFromJSON(binary_type, R"(["bbb", "ccc", "aaa", "   ", "ddd"])"));
  for (auto null_placement : AllNullPlacements()) {
    for (auto tiebreaker : AllTiebreakers()) {
      for (auto order : AllOrders()) {
        AssertRankEmpty(binary_type, order, null_placement, tiebreaker);
      }

      AssertRankSimple(null_placement, tiebreaker);
    }
  }

  SetInput(
      ArrayFromJSON(binary_type, R"(["aaa", "   ", "eee", null, "eee", null, "   "])"));
  AssertRankAllTiebreakers();
}

TEST_F(TestRank, EmptyChunks) {
  SetInput(ChunkedArrayFromJSON(int32(), {"[2, 3]", "[]", "[1, 0]", "[]", "[5]"}));
  for (auto null_placement : AllNullPlacements()) {
    for (auto tiebreaker : AllTiebreakers()) {
      AssertRankSimple(null_placement, tiebreaker);
    }
  }
}

}  // namespace compute
}  // namespace arrow
