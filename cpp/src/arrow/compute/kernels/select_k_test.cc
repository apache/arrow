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

namespace {

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
// Tests for SelectK

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

}  // namespace
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
class TestSelectKBase : public TestBase {
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

  void AssertSelectKArray(const std::shared_ptr<Array> values, int n) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> offsets, TopK(*values, n));
    // null_count field should have been initialized to 0, for convenience
    ASSERT_EQ(offsets->data()->null_count, 0);
    ValidateOutput(*offsets);
    Validate(*checked_pointer_cast<ArrayType>(values), n,
             *checked_pointer_cast<UInt64Array>(offsets));
  }

  void AssertSelectKJson(const std::string& values, int n) {
    AssertSelectKArray(ArrayFromJSON(GetType(), values), n);
  }

  virtual std::shared_ptr<DataType> GetType() = 0;
};

template <typename ArrowType>
class TestSelectK : public TestSelectKBase<ArrowType> {
 protected:
  std::shared_ptr<DataType> GetType() override { return TypeToDataType<ArrowType>(); }
};

template <typename ArrowType>
class TestSelectKForReal : public TestSelectK<ArrowType> {};
TYPED_TEST_SUITE(TestSelectKForReal, RealArrowTypes);

template <typename ArrowType>
class TestSelectKForIntegral : public TestSelectK<ArrowType> {};
TYPED_TEST_SUITE(TestSelectKForIntegral, IntegralArrowTypes);

template <typename ArrowType>
class TestSelectKForBool : public TestSelectK<ArrowType> {};
TYPED_TEST_SUITE(TestSelectKForBool, ::testing::Types<BooleanType>);

template <typename ArrowType>
class TestSelectKForTemporal : public TestSelectK<ArrowType> {};
TYPED_TEST_SUITE(TestSelectKForTemporal, TemporalArrowTypes);

template <typename ArrowType>
class TestSelectKForDecimal : public TestSelectKBase<ArrowType> {
  std::shared_ptr<DataType> GetType() override {
    return std::make_shared<ArrowType>(5, 2);
  }
};
TYPED_TEST_SUITE(TestSelectKForDecimal, DecimalArrowTypes);

template <typename ArrowType>
class TestSelectKForStrings : public TestSelectK<ArrowType> {};
TYPED_TEST_SUITE(TestSelectKForStrings, testing::Types<StringType>);

TYPED_TEST(TestSelectKForReal, SelectKDoesNotProvideDefaultOptions) {
  auto input = ArrayFromJSON(this->GetType(), "[null, 1, 3.3, null, 2, 5.3]");
  ASSERT_RAISES(Invalid, CallFunction("top_k", {input}));
}

TYPED_TEST(TestSelectKForReal, Real) {
  this->AssertSelectKJson("[null, 1, 3.3, null, 2, 5.3]", 0);
  this->AssertSelectKJson("[null, 1, 3.3, null, 2, 5.3]", 2);
  this->AssertSelectKJson("[null, 1, 3.3, null, 2, 5.3]", 5);
  this->AssertSelectKJson("[null, 1, 3.3, null, 2, 5.3]", 6);

  this->AssertSelectKJson("[null, 2, NaN, 3, 1]", 0);
  this->AssertSelectKJson("[null, 2, NaN, 3, 1]", 1);
  this->AssertSelectKJson("[null, 2, NaN, 3, 1]", 2);
  this->AssertSelectKJson("[null, 2, NaN, 3, 1]", 3);
  this->AssertSelectKJson("[null, 2, NaN, 3, 1]", 4);
  this->AssertSelectKJson("[NaN, 2, null, 3, 1]", 3);
  this->AssertSelectKJson("[NaN, 2, null, 3, 1]", 4);
}

TYPED_TEST(TestSelectKForIntegral, Integral) {
  this->AssertSelectKJson("[null, 1, 3, null, 2, 5]", 0);
  this->AssertSelectKJson("[null, 1, 3, null, 2, 5]", 2);
  this->AssertSelectKJson("[null, 1, 3, null, 2, 5]", 5);
  this->AssertSelectKJson("[null, 1, 3, null, 2, 5]", 6);
}

TYPED_TEST(TestSelectKForBool, Bool) {
  this->AssertSelectKJson("[null, false, true, null, false, true]", 0);
  this->AssertSelectKJson("[null, false, true, null, false, true]", 2);
  this->AssertSelectKJson("[null, false, true, null, false, true]", 5);
  this->AssertSelectKJson("[null, false, true, null, false, true]", 6);
}

TYPED_TEST(TestSelectKForTemporal, Temporal) {
  this->AssertSelectKJson("[null, 1, 3, null, 2, 5]", 0);
  this->AssertSelectKJson("[null, 1, 3, null, 2, 5]", 2);
  this->AssertSelectKJson("[null, 1, 3, null, 2, 5]", 5);
  this->AssertSelectKJson("[null, 1, 3, null, 2, 5]", 6);
}

TYPED_TEST(TestSelectKForDecimal, Decimal) {
  const std::string values = R"(["123.45", null, "-123.45", "456.78", "-456.78"])";
  this->AssertSelectKJson(values, 0);
  this->AssertSelectKJson(values, 2);
  this->AssertSelectKJson(values, 4);
  this->AssertSelectKJson(values, 5);
}

TYPED_TEST(TestSelectKForStrings, Strings) {
  this->AssertSelectKJson(R"(["testing", null, "nth", "for", null, "strings"])", 0);
  this->AssertSelectKJson(R"(["testing", null, "nth", "for", null, "strings"])", 2);
  this->AssertSelectKJson(R"(["testing", null, "nth", "for", null, "strings"])", 5);
  this->AssertSelectKJson(R"(["testing", null, "nth", "for", null, "strings"])", 6);
}

template <typename ArrowType>
class TestSelectKRandom : public TestSelectKBase<ArrowType> {
 public:
  std::shared_ptr<DataType> GetType() override {
    EXPECT_TRUE(0) << "shouldn't be used";
    return nullptr;
  }
};

using SelectKableTypes =
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

TYPED_TEST_SUITE(TestSelectKRandom, SelectKableTypes);

TYPED_TEST(TestSelectKRandom, RandomValues) {
  Random<TypeParam> rand(0x61549225);
  int length = 100;
  for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
    // Try n from 0 to out of bound
    for (int n = 0; n <= length; ++n) {
      auto array = rand.Generate(length, null_probability);
      this->AssertSelectKArray(array, n);
    }
  }
}

}  // namespace compute
}  // namespace arrow
