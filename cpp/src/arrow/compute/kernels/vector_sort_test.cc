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

#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array/array_decimal.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {

// Convert arrow::Type to arrow::DataType. If arrow::Type isn't
// parameter free, this returns an arrow::DataType with the default
// parameter.
template <typename ArrowType>
enable_if_t<TypeTraits<ArrowType>::is_parameter_free, std::shared_ptr<DataType>>
TypeToDataType() {
  return TypeTraits<ArrowType>::type_singleton();
}

template <typename ArrowType>
enable_if_t<std::is_same<ArrowType, TimestampType>::value, std::shared_ptr<DataType>>
TypeToDataType() {
  return timestamp(TimeUnit::MILLI);
}

template <typename ArrowType>
enable_if_t<std::is_same<ArrowType, Time32Type>::value, std::shared_ptr<DataType>>
TypeToDataType() {
  return time32(TimeUnit::MILLI);
}

template <typename ArrowType>
enable_if_t<std::is_same<ArrowType, Time64Type>::value, std::shared_ptr<DataType>>
TypeToDataType() {
  return time64(TimeUnit::NANO);
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
class NthComparator {
 public:
  bool operator()(const ArrayType& array, uint64_t lhs, uint64_t rhs) {
    if (array.IsNull(rhs)) return true;
    if (array.IsNull(lhs)) return false;
    const auto lval = GetLogicalValue(array, lhs);
    const auto rval = GetLogicalValue(array, rhs);
    if (is_floating_type<typename ArrayType::TypeClass>::value) {
      // NaNs ordered after non-NaNs
      if (rval != rval) return true;
      if (lval != lval) return false;
    }
    return lval <= rval;
  }
};

template <typename ArrayType>
class SortComparator {
 public:
  bool operator()(const ArrayType& array, SortOrder order, uint64_t lhs, uint64_t rhs) {
    if (array.IsNull(rhs) && array.IsNull(lhs)) return lhs < rhs;
    if (array.IsNull(rhs)) return true;
    if (array.IsNull(lhs)) return false;
    const auto lval = GetLogicalValue(array, lhs);
    const auto rval = GetLogicalValue(array, rhs);
    if (is_floating_type<typename ArrayType::TypeClass>::value) {
      const bool lhs_isnan = lval != lval;
      const bool rhs_isnan = rval != rval;
      if (lhs_isnan && rhs_isnan) return lhs < rhs;
      if (rhs_isnan) return true;
      if (lhs_isnan) return false;
    }
    if (lval == rval) return lhs < rhs;
    if (order == SortOrder::Ascending) {
      return lval < rval;
    } else {
      return lval > rval;
    }
  }
};

template <typename ArrowType>
class TestNthToIndicesBase : public TestBase {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 protected:
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

  void AssertNthToIndicesArray(const std::shared_ptr<Array> values, int n) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets, NthToIndices(*values, n));
    // null_count field should have been initialized to 0, for convenience
    ASSERT_EQ(offsets->data()->null_count, 0);
    ValidateOutput(*offsets);
    Validate(*checked_pointer_cast<ArrayType>(values), n,
             *checked_pointer_cast<UInt64Array>(offsets));
  }

  void AssertNthToIndicesJson(const std::string& values, int n) {
    AssertNthToIndicesArray(ArrayFromJSON(GetType(), values), n);
  }

  virtual std::shared_ptr<DataType> GetType() = 0;
};

template <typename ArrowType>
class TestNthToIndices : public TestNthToIndicesBase<ArrowType> {
 protected:
  std::shared_ptr<DataType> GetType() override { return TypeToDataType<ArrowType>(); }
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

template <typename ArrowType>
class TestNthToIndicesRandom : public TestNthToIndicesBase<ArrowType> {
 public:
  std::shared_ptr<DataType> GetType() override {
    EXPECT_TRUE(0) << "shouldn't be used";
    return nullptr;
  }
};

using NthToIndicesableTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, Decimal128Type,
                     StringType>;

class RandomImpl {
 protected:
  random::RandomArrayGenerator generator_;
  std::shared_ptr<DataType> type_;

  explicit RandomImpl(random::SeedType seed, std::shared_ptr<DataType> type)
      : generator_(seed), type_(std::move(type)) {}

 public:
  std::shared_ptr<Array> Generate(uint64_t count, double null_prob) {
    return generator_.ArrayOf(type_, count, null_prob);
  }
};

template <typename ArrowType>
class Random : public RandomImpl {
 public:
  explicit Random(random::SeedType seed)
      : RandomImpl(seed, TypeTraits<ArrowType>::type_singleton()) {}
};

template <>
class Random<FloatType> : public RandomImpl {
  using CType = float;

 public:
  explicit Random(random::SeedType seed) : RandomImpl(seed, float32()) {}

  std::shared_ptr<Array> Generate(uint64_t count, double null_prob, double nan_prob = 0) {
    return generator_.Float32(count, std::numeric_limits<CType>::min(),
                              std::numeric_limits<CType>::max(), null_prob, nan_prob);
  }
};

template <>
class Random<DoubleType> : public RandomImpl {
  using CType = double;

 public:
  explicit Random(random::SeedType seed) : RandomImpl(seed, float64()) {}

  std::shared_ptr<Array> Generate(uint64_t count, double null_prob, double nan_prob = 0) {
    return generator_.Float64(count, std::numeric_limits<CType>::min(),
                              std::numeric_limits<CType>::max(), null_prob, nan_prob);
  }
};

template <>
class Random<Decimal128Type> : public RandomImpl {
 public:
  explicit Random(random::SeedType seed,
                  std::shared_ptr<DataType> type = decimal128(18, 5))
      : RandomImpl(seed, std::move(type)) {}
};

template <typename ArrowType>
class RandomRange : public RandomImpl {
  using CType = typename TypeTraits<ArrowType>::CType;

 public:
  explicit RandomRange(random::SeedType seed)
      : RandomImpl(seed, TypeTraits<ArrowType>::type_singleton()) {}

  std::shared_ptr<Array> Generate(uint64_t count, int range, double null_prob) {
    CType min = std::numeric_limits<CType>::min();
    CType max = min + range;
    if (sizeof(CType) < 4 && (range + min) > std::numeric_limits<CType>::max()) {
      max = std::numeric_limits<CType>::max();
    }
    return generator_.Numeric<ArrowType>(count, min, max, null_prob);
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

// ----------------------------------------------------------------------
// Tests for SortToIndices

template <typename T>
void AssertSortIndices(const std::shared_ptr<T>& input, SortOrder order,
                       const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(auto actual, SortIndices(*input, order));
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

// `Options` may be both SortOptions or SortOrder
template <typename T, typename Options>
void AssertSortIndices(const std::shared_ptr<T>& input, Options&& options,
                       const std::string& expected) {
  AssertSortIndices(input, std::forward<Options>(options),
                    ArrayFromJSON(uint64(), expected));
}

class TestArraySortIndicesBase : public TestBase {
 public:
  virtual std::shared_ptr<DataType> type() = 0;

  virtual void AssertSortIndices(const std::string& values, SortOrder order,
                                 const std::string& expected) {
    auto type = this->type();
    arrow::compute::AssertSortIndices(ArrayFromJSON(type, values), order,
                                      ArrayFromJSON(uint64(), expected));
  }

  virtual void AssertSortIndices(const std::string& values, const std::string& expected) {
    AssertSortIndices(values, SortOrder::Ascending, expected);
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

TYPED_TEST(TestArraySortIndicesForIntegral, SortIntegral) {
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

TYPED_TEST(TestArraySortIndicesForBool, SortBool) {
  this->AssertSortIndices("[]", "[]");

  this->AssertSortIndices("[true, true, false]", "[2, 0, 1]");
  this->AssertSortIndices("[false, false,  false, true, true, true, true]",
                          "[0, 1, 2, 3, 4, 5, 6]");
  this->AssertSortIndices("[true, true, true, true, false, false, false]",
                          "[4, 5, 6, 0, 1, 2, 3]");

  this->AssertSortIndices("[false, true, false, true, true, false, false]",
                          SortOrder::Ascending, "[0, 2, 5, 6, 1, 3, 4]");
  this->AssertSortIndices("[false, true, false, true, true, false, false]",
                          SortOrder::Descending, "[1, 3, 4, 0, 2, 5, 6]");

  this->AssertSortIndices("[null, true, false, null, false, true]", SortOrder::Ascending,
                          "[2, 4, 1, 5, 0, 3]");
  this->AssertSortIndices("[null, true, false, null, false, true]", SortOrder::Descending,
                          "[1, 5, 2, 4, 0, 3]");
}

TYPED_TEST(TestArraySortIndicesForTemporal, SortTemporal) {
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

TYPED_TEST(TestArraySortIndicesForStrings, SortStrings) {
  this->AssertSortIndices("[]", "[]");

  this->AssertSortIndices(R"(["a", "b", "c"])", "[0, 1, 2]");
  this->AssertSortIndices(R"(["foo", "bar", "baz"])", "[1,2,0]");
  this->AssertSortIndices(R"(["testing", "sort", "for", "strings"])", "[2, 1, 3, 0]");

  this->AssertSortIndices(R"(["c", "b", "a", "b"])", SortOrder::Ascending,
                          "[2, 1, 3, 0]");
  this->AssertSortIndices(R"(["c", "b", "a", "b"])", SortOrder::Descending,
                          "[0, 1, 3, 2]");
}

TEST_F(TestArraySortIndicesForFixedSizeBinary, SortFixedSizeBinary) {
  this->AssertSortIndices("[]", "[]");

  this->AssertSortIndices(R"(["def", "abc", "ghi"])", "[1, 0, 2]");
  this->AssertSortIndices(R"(["def", "abc", "ghi"])", SortOrder::Descending, "[2, 0, 1]");
}

template <typename ArrowType>
class TestArraySortIndicesForUInt8 : public TestArraySortIndices<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesForUInt8, UInt8Type);

template <typename ArrowType>
class TestArraySortIndicesForInt8 : public TestArraySortIndices<ArrowType> {};
TYPED_TEST_SUITE(TestArraySortIndicesForInt8, Int8Type);

TYPED_TEST(TestArraySortIndicesForUInt8, SortUInt8) {
  this->AssertSortIndices("[255, null, 0, 255, 10, null, 128, 0]",
                          "[2, 7, 4, 6, 0, 3, 1, 5]");
}

TYPED_TEST(TestArraySortIndicesForInt8, SortInt8) {
  this->AssertSortIndices("[null, 10, 127, 0, -128, -128, null]",
                          "[4, 5, 3, 1, 2, 0, 6]");
}

template <typename ArrowType>
class TestArraySortIndicesForDecimal : public TestArraySortIndicesBase {
 public:
  std::shared_ptr<DataType> type() override { return std::make_shared<ArrowType>(5, 2); }
};
TYPED_TEST_SUITE(TestArraySortIndicesForDecimal, DecimalArrowTypes);

TYPED_TEST(TestArraySortIndicesForDecimal, DecimalSortTestTypes) {
  this->AssertSortIndices(R"(["123.45", null, "-123.45", "456.78", "-456.78"])",
                          "[4, 2, 0, 3, 1]");
  this->AssertSortIndices(R"(["123.45", null, "-123.45", "456.78", "-456.78"])",
                          SortOrder::Descending, "[3, 0, 2, 4, 1]");
}

template <typename ArrowType>
class TestArraySortIndicesRandom : public TestBase {};

template <typename ArrowType>
class TestArraySortIndicesRandomCount : public TestBase {};

template <typename ArrowType>
class TestArraySortIndicesRandomCompare : public TestBase {};

using SortIndicesableTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, StringType,
                     Decimal128Type, BooleanType>;

template <typename ArrayType>
void ValidateSorted(const ArrayType& array, UInt64Array& offsets, SortOrder order) {
  ValidateOutput(array);
  SortComparator<ArrayType> compare;
  for (int i = 1; i < array.length(); i++) {
    uint64_t lhs = offsets.Value(i - 1);
    uint64_t rhs = offsets.Value(i);
    ASSERT_TRUE(compare(array, order, lhs, rhs));
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
TYPED_TEST_SUITE(TestArraySortIndicesRandomCount, IntegralArrowTypes);

TYPED_TEST(TestArraySortIndicesRandomCount, SortRandomValuesCount) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  RandomRange<TypeParam> rand(0x5487656);
  int times = 5;
  int length = 100;
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
TYPED_TEST_SUITE(TestArraySortIndicesRandomCompare, IntegralArrowTypes);

TYPED_TEST(TestArraySortIndicesRandomCompare, SortRandomValuesCompare) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  Random<TypeParam> rand(0x5487657);
  int times = 5;
  int length = 100;
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
class TestChunkedArraySortIndices : public ::testing::Test {};

TEST_F(TestChunkedArraySortIndices, Null) {
  auto chunked_array = ChunkedArrayFromJSON(uint8(), {
                                                         "[null, 1]",
                                                         "[3, null, 2]",
                                                         "[1]",
                                                     });
  AssertSortIndices(chunked_array, SortOrder::Ascending, "[1, 5, 4, 2, 0, 3]");
  AssertSortIndices(chunked_array, SortOrder::Descending, "[2, 4, 1, 5, 0, 3]");
}

TEST_F(TestChunkedArraySortIndices, NaN) {
  auto chunked_array = ChunkedArrayFromJSON(float32(), {
                                                           "[null, 1]",
                                                           "[3, null, NaN]",
                                                           "[NaN, 1]",
                                                       });
  AssertSortIndices(chunked_array, SortOrder::Ascending, "[1, 6, 2, 4, 5, 0, 3]");
  AssertSortIndices(chunked_array, SortOrder::Descending, "[2, 1, 6, 4, 5, 0, 3]");
}

// Tests for temporal types
template <typename ArrowType>
class TestChunkedArraySortIndicesForTemporal : public TestChunkedArraySortIndices {
 protected:
  std::shared_ptr<DataType> GetType() { return TypeToDataType<ArrowType>(); }
};
TYPED_TEST_SUITE(TestChunkedArraySortIndicesForTemporal, TemporalArrowTypes);

TYPED_TEST(TestChunkedArraySortIndicesForTemporal, NoNull) {
  auto type = this->GetType();
  auto chunked_array = ChunkedArrayFromJSON(type, {
                                                      "[0, 1]",
                                                      "[3, 2, 1]",
                                                      "[5, 0]",
                                                  });
  AssertSortIndices(chunked_array, SortOrder::Ascending, "[0, 6, 1, 4, 3, 2, 5]");
  AssertSortIndices(chunked_array, SortOrder::Descending, "[5, 2, 3, 1, 4, 0, 6]");
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
  AssertSortIndices(chunked_array, SortOrder::Ascending, "[4, 1, 0, 3, 2, 5]");
  AssertSortIndices(chunked_array, SortOrder::Descending, "[3, 0, 1, 4, 2, 5]");
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
    for (auto null_probability : {0.0, 0.1, 0.5, 0.9, 1.0}) {
      for (auto order : {SortOrder::Ascending, SortOrder::Descending}) {
        for (auto num_chunks : {1, 2, 5, 10, 40}) {
          std::vector<std::shared_ptr<Array>> arrays;
          for (int i = 0; i < num_chunks; ++i) {
            auto array = this->GenerateArray(length / num_chunks, null_probability);
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
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});

  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": 3,    "b": 5},
                                       {"a": 1,    "b": 3},
                                       {"a": 3,    "b": 4},
                                       {"a": 0,    "b": 6},
                                       {"a": 2,    "b": 5},
                                       {"a": 1,    "b": 5},
                                       {"a": 1,    "b": 3}
                                       ])");
  AssertSortIndices(batch, options, "[3, 5, 1, 6, 4, 0, 2]");
}

TEST_F(TestRecordBatchSortIndices, Null) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});

  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": null, "b": 5},
                                       {"a": 1,    "b": 3},
                                       {"a": 3,    "b": null},
                                       {"a": null, "b": null},
                                       {"a": 2,    "b": 5},
                                       {"a": 1,    "b": 5}
                                       ])");
  AssertSortIndices(batch, options, "[5, 1, 4, 2, 0, 3]");
}

TEST_F(TestRecordBatchSortIndices, NaN) {
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});

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
  AssertSortIndices(batch, options, "[3, 7, 1, 0, 2, 4, 6, 5]");
}

TEST_F(TestRecordBatchSortIndices, NaNAndNull) {
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});

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
  AssertSortIndices(batch, options, "[7, 1, 2, 6, 5, 4, 0, 3]");
}

TEST_F(TestRecordBatchSortIndices, Boolean) {
  auto schema = ::arrow::schema({
      {field("a", boolean())},
      {field("b", boolean())},
  });
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});

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
  AssertSortIndices(batch, options, "[3, 1, 6, 2, 4, 0, 7, 5]");
}

TEST_F(TestRecordBatchSortIndices, MoreTypes) {
  auto schema = ::arrow::schema({
      {field("a", timestamp(TimeUnit::MICRO))},
      {field("b", large_utf8())},
      {field("c", fixed_size_binary(3))},
  });
  SortOptions options({SortKey("a", SortOrder::Ascending),
                       SortKey("b", SortOrder::Descending),
                       SortKey("c", SortOrder::Ascending)});

  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": 3, "b": "05",   "c": "aaa"},
                                       {"a": 1, "b": "031",  "c": "bbb"},
                                       {"a": 3, "b": "05",   "c": "bbb"},
                                       {"a": 0, "b": "0666", "c": "aaa"},
                                       {"a": 2, "b": "05",   "c": "aaa"},
                                       {"a": 1, "b": "05",   "c": "bbb"}
                                       ])");
  AssertSortIndices(batch, options, "[3, 5, 1, 4, 0, 2]");
}

TEST_F(TestRecordBatchSortIndices, Decimal) {
  auto schema = ::arrow::schema({
      {field("a", decimal128(3, 1))},
      {field("b", decimal256(4, 2))},
  });
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});

  auto batch = RecordBatchFromJSON(schema,
                                   R"([{"a": "12.3", "b": "12.34"},
                                       {"a": "45.6", "b": "12.34"},
                                       {"a": "12.3", "b": "-12.34"},
                                       {"a": "-12.3", "b": null},
                                       {"a": "-12.3", "b": "-45.67"}
                                       ])");
  AssertSortIndices(batch, options, "[4, 3, 0, 2, 1]");
}

// Test basic cases for table.
class TestTableSortIndices : public ::testing::Test {};

TEST_F(TestTableSortIndices, Null) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});
  std::shared_ptr<Table> table;

  table = TableFromJSON(schema, {R"([{"a": null, "b": 5},
                                     {"a": 1,    "b": 3},
                                     {"a": 3,    "b": null},
                                     {"a": null, "b": null},
                                     {"a": 2,    "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"});
  AssertSortIndices(table, options, "[5, 1, 4, 2, 0, 3]");

  // Same data, several chunks
  table = TableFromJSON(schema, {R"([{"a": null, "b": 5},
                                     {"a": 1,    "b": 3},
                                     {"a": 3,    "b": null}
                                    ])",
                                 R"([{"a": null, "b": null},
                                     {"a": 2,    "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"});
  AssertSortIndices(table, options, "[5, 1, 4, 2, 0, 3]");
}

TEST_F(TestTableSortIndices, NaN) {
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});
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
  AssertSortIndices(table, options, "[3, 7, 1, 0, 2, 4, 6, 5]");

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
  AssertSortIndices(table, options, "[3, 7, 1, 0, 2, 4, 6, 5]");
}

TEST_F(TestTableSortIndices, NaNAndNull) {
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});
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
  AssertSortIndices(table, options, "[7, 1, 2, 6, 5, 4, 0, 3]");

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
  AssertSortIndices(table, options, "[7, 1, 2, 6, 5, 4, 0, 3]");
}

TEST_F(TestTableSortIndices, Boolean) {
  auto schema = ::arrow::schema({
      {field("a", boolean())},
      {field("b", boolean())},
  });
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});
  auto table = TableFromJSON(schema, {R"([{"a": true,    "b": null},
                                       {"a": false,   "b": null},
                                       {"a": true,    "b": true},
                                       {"a": false,   "b": true}])",
                                      R"([{"a": true,    "b": false},
                                       {"a": null,    "b": false},
                                       {"a": false,   "b": null},
                                       {"a": null,    "b": true}
                                       ])"});
  AssertSortIndices(table, options, "[3, 1, 6, 2, 4, 0, 7, 5]");
}

TEST_F(TestTableSortIndices, BinaryLike) {
  auto schema = ::arrow::schema({
      {field("a", large_utf8())},
      {field("b", fixed_size_binary(3))},
  });
  SortOptions options(
      {SortKey("a", SortOrder::Descending), SortKey("b", SortOrder::Ascending)});
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
  AssertSortIndices(table, options, "[1, 5, 2, 6, 4, 0, 7, 3]");
}

TEST_F(TestTableSortIndices, Decimal) {
  auto schema = ::arrow::schema({
      {field("a", decimal128(3, 1))},
      {field("b", decimal256(4, 2))},
  });
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});

  auto table = TableFromJSON(schema, {R"([{"a": "12.3", "b": "12.34"},
                                          {"a": "45.6", "b": "12.34"},
                                          {"a": "12.3", "b": "-12.34"}
                                          ])",
                                      R"([{"a": "-12.3", "b": null},
                                          {"a": "-12.3", "b": "-45.67"}
                                          ])"});
  AssertSortIndices(table, options, "[4, 3, 0, 2, 1]");
}

// Tests for temporal types
template <typename ArrowType>
class TestTableSortIndicesForTemporal : public TestTableSortIndices {
 protected:
  std::shared_ptr<DataType> GetType() { return TypeToDataType<ArrowType>(); }
};
TYPED_TEST_SUITE(TestTableSortIndicesForTemporal, TemporalArrowTypes);

TYPED_TEST(TestTableSortIndicesForTemporal, NoNull) {
  auto type = this->GetType();
  auto table = TableFromJSON(schema({
                                 {field("a", type)},
                                 {field("b", type)},
                             }),
                             {"["
                              "{\"a\": 0, \"b\": 5},"
                              "{\"a\": 1, \"b\": 3},"
                              "{\"a\": 3, \"b\": 0},"
                              "{\"a\": 2, \"b\": 1},"
                              "{\"a\": 1, \"b\": 3},"
                              "{\"a\": 5, \"b\": 0},"
                              "{\"a\": 0, \"b\": 4},"
                              "{\"a\": 1, \"b\": 2}"
                              "]"});
  SortOptions options(
      {SortKey("a", SortOrder::Ascending), SortKey("b", SortOrder::Descending)});
  AssertSortIndices(table, options, "[0, 6, 1, 4, 7, 3, 2, 5]");
}

// For random table tests.
using RandomParam = std::tuple<std::string, double>;
class TestTableSortIndicesRandom : public testing::TestWithParam<RandomParam> {
  // Compares two records in the same table.
  class Comparator : public TypeVisitor {
   public:
    Comparator(const Table& table, const SortOptions& options) : options_(options) {
      for (const auto& sort_key : options_.sort_keys) {
        sort_columns_.emplace_back(table.GetColumnByName(sort_key.name).get(),
                                   sort_key.order);
      }
    }

    // Returns true if the left record is less or equals to the right
    // record, false otherwise.
    //
    // This supports null and NaN.
    bool operator()(uint64_t lhs, uint64_t rhs) {
      lhs_ = lhs;
      rhs_ = rhs;
      for (const auto& pair : sort_columns_) {
        const auto& chunked_array = *pair.first;
        lhs_array_ = FindTargetArray(chunked_array, lhs, &lhs_index_);
        rhs_array_ = FindTargetArray(chunked_array, rhs, &rhs_index_);
        if (rhs_array_->IsNull(rhs_index_) && lhs_array_->IsNull(lhs_index_)) continue;
        if (rhs_array_->IsNull(rhs_index_)) return true;
        if (lhs_array_->IsNull(lhs_index_)) return false;
        status_ = lhs_array_->type()->Accept(this);
        if (compared_ == 0) continue;
        // If either value is NaN, it must sort after the other regardless of order
        if (pair.second == SortOrder::Ascending || lhs_isnan_ || rhs_isnan_) {
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
    VISIT(Decimal128)

#undef VISIT

   private:
    // Finds the target chunk and index in the target chunk from an
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

    // Compares two values in the same chunked array. Values are never
    // null but may be NaN.
    //
    // Returns true if the left value is less or equals to the right
    // value, false otherwise.
    template <typename Type>
    int CompareType() {
      using ArrayType = typename TypeTraits<Type>::ArrayType;
      auto lhs_value =
          GetLogicalValue(checked_cast<const ArrayType&>(*lhs_array_), lhs_index_);
      auto rhs_value =
          GetLogicalValue(checked_cast<const ArrayType&>(*rhs_array_), rhs_index_);
      if (is_floating_type<Type>::value) {
        lhs_isnan_ = lhs_value != lhs_value;
        rhs_isnan_ = rhs_value != rhs_value;
        if (lhs_isnan_ && rhs_isnan_) return 0;
        // NaN is considered greater than non-NaN
        if (rhs_isnan_) return -1;
        if (lhs_isnan_) return 1;
      } else {
        lhs_isnan_ = rhs_isnan_ = false;
      }
      if (lhs_value == rhs_value) {
        return 0;
      } else if (lhs_value > rhs_value) {
        return 1;
      } else {
        return -1;
      }
    }

    const SortOptions& options_;
    std::vector<std::pair<const ChunkedArray*, SortOrder>> sort_columns_;
    int64_t lhs_;
    const Array* lhs_array_;
    int64_t lhs_index_;
    int64_t rhs_;
    const Array* rhs_array_;
    int64_t rhs_index_;
    bool lhs_isnan_, rhs_isnan_;
    int compared_;
    Status status_;
  };

 public:
  // Validates the sorted indexes are really sorted.
  void Validate(const Table& table, const SortOptions& options, UInt64Array& offsets) {
    ValidateOutput(offsets);
    Comparator comparator{table, options};
    for (int i = 1; i < table.num_rows(); i++) {
      uint64_t lhs = offsets.Value(i - 1);
      uint64_t rhs = offsets.Value(i);
      ASSERT_OK(comparator.status());
      ASSERT_TRUE(comparator(lhs, rhs)) << "lhs = " << lhs << ", rhs = " << rhs;
    }
  }
};

TEST_P(TestTableSortIndicesRandom, Sort) {
  const auto first_sort_key_name = std::get<0>(GetParam());
  const auto null_probability = std::get<1>(GetParam());
  const auto seed = 0x61549225;

  const FieldVector fields = {
      {field("uint8", uint8())},   {field("uint16", uint16())},
      {field("uint32", uint32())}, {field("uint64", uint64())},
      {field("int8", int8())},     {field("int16", int16())},
      {field("int32", int32())},   {field("int64", int64())},
      {field("float", float32())}, {field("double", float64())},
      {field("string", utf8())},   {field("decimal128", decimal128(18, 3))},
  };
  const auto length = 200;
  ArrayVector columns = {
      Random<UInt8Type>(seed).Generate(length, null_probability),
      Random<UInt16Type>(seed).Generate(length, 0.0),
      Random<UInt32Type>(seed).Generate(length, null_probability),
      Random<UInt64Type>(seed).Generate(length, 0.0),
      Random<Int8Type>(seed).Generate(length, 0.0),
      Random<Int16Type>(seed).Generate(length, null_probability),
      Random<Int32Type>(seed).Generate(length, 0.0),
      Random<Int64Type>(seed).Generate(length, null_probability),
      Random<FloatType>(seed).Generate(length, null_probability, 1 - null_probability),
      Random<DoubleType>(seed).Generate(length, 0.0, null_probability),
      Random<StringType>(seed).Generate(length, null_probability),
      Random<Decimal128Type>(seed, fields[11]->type()).Generate(length, null_probability),
  };
  const auto table = Table::Make(schema(fields), columns, length);

  // Generate random sort keys
  std::default_random_engine engine(seed);
  std::uniform_int_distribution<> distribution(0);
  const auto n_sort_keys = 7;
  std::vector<SortKey> sort_keys;
  const auto first_sort_key_order =
      (distribution(engine) % 2) == 0 ? SortOrder::Ascending : SortOrder::Descending;
  sort_keys.emplace_back(first_sort_key_name, first_sort_key_order);
  for (int i = 1; i < n_sort_keys; ++i) {
    const auto& field = *fields[distribution(engine) % fields.size()];
    const auto order =
        (distribution(engine) % 2) == 0 ? SortOrder::Ascending : SortOrder::Descending;
    sort_keys.emplace_back(field.name(), order);
  }
  SortOptions options(sort_keys);

  // Test with different table chunkings
  for (const int64_t num_chunks : {1, 2, 20}) {
    TableBatchReader reader(*table);
    reader.set_chunksize((length + num_chunks - 1) / num_chunks);
    ASSERT_OK_AND_ASSIGN(auto chunked_table, Table::FromRecordBatchReader(&reader));
    ASSERT_OK_AND_ASSIGN(auto offsets, SortIndices(Datum(*chunked_table), options));
    Validate(*table, options, *checked_pointer_cast<UInt64Array>(offsets));
  }

  // Also validate RecordBatch sorting
  TableBatchReader reader(*table);
  RecordBatchVector batches;
  ASSERT_OK(reader.ReadAll(&batches));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_OK_AND_ASSIGN(auto offsets, SortIndices(Datum(*batches[0]), options));
  Validate(*table, options, *checked_pointer_cast<UInt64Array>(offsets));
}

static const auto first_sort_keys =
    testing::Values("uint8", "uint16", "uint32", "uint64", "int8", "int16", "int32",
                    "int64", "float", "double", "string", "decimal128");

INSTANTIATE_TEST_SUITE_P(NoNull, TestTableSortIndicesRandom,
                         testing::Combine(first_sort_keys, testing::Values(0.0)));

INSTANTIATE_TEST_SUITE_P(MayNull, TestTableSortIndicesRandom,
                         testing::Combine(first_sort_keys, testing::Values(0.1, 0.5)));

INSTANTIATE_TEST_SUITE_P(AllNull, TestTableSortIndicesRandom,
                         testing::Combine(first_sort_keys, testing::Values(1.0)));

}  // namespace compute
}  // namespace arrow
