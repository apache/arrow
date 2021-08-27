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
#include <iostream>
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

template <typename ArrayType, SortOrder order>
class SelectKComparator {
 public:
  template <typename Type>
  bool operator()(const Type& lval, const Type& rval) {
    if (is_floating_type<typename ArrayType::TypeClass>::value) {
      // NaNs ordered after non-NaNs
      if (rval != rval) return true;
      if (lval != lval) return false;
    }
    if (order == SortOrder::Ascending) {
      return lval <= rval;
    } else {
      return rval <= lval;
    }
  }
};

template <SortOrder order>
Result<std::shared_ptr<Array>> SelectK(const ChunkedArray& values, int64_t k) {
  if (order == SortOrder::Descending) {
    return TopK(values, k);
  } else {
    return BottomK(values, k);
  }
}

template <SortOrder order>
Result<std::shared_ptr<Array>> SelectK(const Array& values, int64_t k) {
  if (order == SortOrder::Descending) {
    return TopK(values, k);
  } else {
    return BottomK(values, k);
  }
}
template <typename ArrowType>
class TestSelectKBase : public TestBase {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 protected:
  void Validate(const ArrayType& array, int k, ArrayType& select_k, SortOrder order) {
    ASSERT_OK_AND_ASSIGN(auto sorted_indices, SortIndices(array, order));
    ASSERT_OK_AND_ASSIGN(Datum sorted_datum,
                         Take(array, sorted_indices, TakeOptions::NoBoundsCheck()));
    std::shared_ptr<Array> sorted_array_out = sorted_datum.make_array();

    const ArrayType& sorted_array = *checked_pointer_cast<ArrayType>(sorted_array_out);

    if (k < array.length()) {
      for (uint64_t i = 0; i < (uint64_t)select_k.length(); ++i) {
        const auto lval = GetLogicalValue(select_k, i);
        const auto rval = GetLogicalValue(sorted_array, i);
        ASSERT_TRUE(lval == rval);
      }
    }
  }
  template <SortOrder order>
  void AssertSelectKArray(const std::shared_ptr<Array> values, int n) {
    std::shared_ptr<Array> select_k;
    ASSERT_OK_AND_ASSIGN(select_k, SelectK<order>(*values, n));
    ASSERT_EQ(select_k->data()->null_count, 0);
    ValidateOutput(*select_k);
    Validate(*checked_pointer_cast<ArrayType>(values), n,
             *checked_pointer_cast<ArrayType>(select_k), order);
  }

  void AssertTopKArray(const std::shared_ptr<Array> values, int n) {
    AssertSelectKArray<SortOrder::Descending>(values, n);
  }
  void AssertBottomKArray(const std::shared_ptr<Array> values, int n) {
    AssertSelectKArray<SortOrder::Descending>(values, n);
  }

  void AssertSelectKJson(const std::string& values, int n) {
    AssertTopKArray(ArrayFromJSON(GetType(), values), n);
    AssertBottomKArray(ArrayFromJSON(GetType(), values), n);
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

  this->AssertSelectKJson("[2, 4, 5, 7, 8, 0, 9, 1, 3]", 5);
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
      this->AssertTopKArray(array, n);
      this->AssertBottomKArray(array, n);
    }
  }
}

template <SortOrder order>
struct TestSelectKWithChunkedArray : public ::testing::Test {
  TestSelectKWithChunkedArray()
      : sizes_({0, 1, 2, 4, 16, 31, 1234}),
        null_probabilities_({0.0, 0.1, 0.5, 0.9, 1.0}) {}

  void Check(const std::shared_ptr<DataType>& type,
             const std::vector<std::string>& values, int64_t k,
             const std::string& expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(this->DoSelectK(type, values, k, &actual));
    ValidateOutput(actual);

    ASSERT_ARRAYS_EQUAL(*ArrayFromJSON(type, expected), *actual);
  }

  void Check(const std::shared_ptr<DataType>& type,
             const std::shared_ptr<ChunkedArray>& values, int64_t k,
             const std::string& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, SelectK<order>(*values, k));
    ValidateOutput(actual);
    ASSERT_ARRAYS_EQUAL(*ArrayFromJSON(type, expected), *actual);
  }

  Status DoSelectK(const std::shared_ptr<DataType>& type,
                   const std::vector<std::string>& values, int64_t k,
                   std::shared_ptr<Array>* out) {
    ARROW_ASSIGN_OR_RAISE(*out, SelectK<order>(*(ChunkedArrayFromJSON(type, values)), k));
    PrettyPrint(**out, {}, &std::cerr);
    return Status::OK();
  }
  std::vector<int32_t> sizes_;
  std::vector<double> null_probabilities_;
};

struct TestTopKWithChunkedArray
    : public TestSelectKWithChunkedArray<SortOrder::Descending> {};

TEST_F(TestTopKWithChunkedArray, Int32) {
  this->Check(int32(), {"[0, 1, 9]", "[3, 7, 2, 4, 10]"}, 3, "[10, 9, 7]");
  this->Check(int32(), {"[]", "[]"}, 0, "[]");
  this->Check(int32(), {"[]"}, 0, "[]");
}

TEST_F(TestTopKWithChunkedArray, Null) {
  this->Check(int8(), {"[null]", "[8, null]"}, 1, "[8]");

  this->Check(int8(), {"[null]", "[null, null]"}, 0, "[]");
  this->Check(int32(), {"[0, null, 9]", "[3, null, 2, null, 10]"}, 3, "[10, 9, 3]");
  this->Check(int8(), {"[null]", "[]"}, 0, "[]");
}

TEST_F(TestTopKWithChunkedArray, NaN) {
  this->Check(float32(), {"[NaN]", "[8, NaN]"}, 1, "[8]");

  this->Check(float32(), {"[NaN]", "[NaN, NaN]"}, 0, "[]");
  this->Check(float32(), {"[0, NaN, 9]", "[3, NaN, 2, NaN, 10]"}, 3, "[10, 9, 3]");
  this->Check(float32(), {"[NaN]", "[]"}, 0, "[]");
}

struct TestBottomKWithChunkedArray
    : public TestSelectKWithChunkedArray<SortOrder::Ascending> {};

TEST_F(TestBottomKWithChunkedArray, Int8) {
  this->Check(int8(), {"[0, 1, 9]", "[3, 7, 2, 4, 10]"}, 3, "[0, 1, 2]");
  this->Check(int8(), {"[]", "[]"}, 0, "[]");
  this->Check(float32(), {"[]"}, 0, "[]");
}

TEST_F(TestBottomKWithChunkedArray, Null) {
  this->Check(int8(), {"[null]", "[8, null]"}, 1, "[8]");

  this->Check(int8(), {"[null]", "[null, null]"}, 0, "[]");
  this->Check(int32(), {"[0, null, 9]", "[3, null, 2, null, 10]"}, 3, "[0, 2, 3]");
  this->Check(int8(), {"[null]", "[]"}, 0, "[]");
}

TEST_F(TestBottomKWithChunkedArray, NaN) {
  this->Check(float32(), {"[NaN]", "[8, NaN]"}, 1, "[8]");

  this->Check(float32(), {"[NaN]", "[NaN, NaN]"}, 0, "[]");
  this->Check(float32(), {"[0, NaN, 9]", "[3, NaN, 2, NaN, 10]"}, 3, "[0, 2, 3]");
  this->Check(float32(), {"[NaN]", "[]"}, 0, "[]");
}

template <typename ArrowType>
class TestTopKWithChunkedArrayForTemporal : public TestTopKWithChunkedArray {
 protected:
  std::shared_ptr<DataType> GetType() { return TypeToDataType<ArrowType>(); }
};
TYPED_TEST_SUITE(TestTopKWithChunkedArrayForTemporal, TemporalArrowTypes);

TYPED_TEST(TestTopKWithChunkedArrayForTemporal, NoNull) {
  auto type = this->GetType();
  auto chunked_array = ChunkedArrayFromJSON(type, {
                                                      "[0, 1]",
                                                      "[3, 2, 1]",
                                                      "[5, 0]",
                                                  });
  this->Check(type, chunked_array, 3, "[5, 3, 2]");
}

template <typename ArrowType>
class TestBottomKWithChunkedArrayForTemporal : public TestBottomKWithChunkedArray {
 protected:
  std::shared_ptr<DataType> GetType() { return TypeToDataType<ArrowType>(); }
};
TYPED_TEST_SUITE(TestBottomKWithChunkedArrayForTemporal, TemporalArrowTypes);

TYPED_TEST(TestBottomKWithChunkedArrayForTemporal, NoNull) {
  auto type = this->GetType();
  auto chunked_array = ChunkedArrayFromJSON(type, {
                                                      "[0, 1]",
                                                      "[3, 2, 1]",
                                                      "[5, 0]",
                                                  });
  this->Check(type, chunked_array, 3, "[0, 0, 1]");
}

// Tests for decimal types
template <typename ArrowType>
class TestTopKWithChunkedArrayForDecimal : public TestTopKWithChunkedArray {
 protected:
  std::shared_ptr<DataType> GetType() { return std::make_shared<ArrowType>(5, 2); }
};
TYPED_TEST_SUITE(TestTopKWithChunkedArrayForDecimal, DecimalArrowTypes);

TYPED_TEST(TestTopKWithChunkedArrayForDecimal, Basics) {
  auto type = this->GetType();
  auto chunked_array = ChunkedArrayFromJSON(
      type, {R"(["123.45", "-123.45"])", R"([null, "456.78"])", R"(["-456.78",
      null])"});
  this->Check(type, chunked_array, 3, R"(["456.78", "123.45", "-123.45"])");
}

template <typename ArrowType>
class TestBottomKWithChunkedArrayForDecimal : public TestBottomKWithChunkedArray {
 protected:
  std::shared_ptr<DataType> GetType() { return std::make_shared<ArrowType>(5, 2); }
};
TYPED_TEST_SUITE(TestBottomKWithChunkedArrayForDecimal, DecimalArrowTypes);

TYPED_TEST(TestBottomKWithChunkedArrayForDecimal, Basics) {
  auto type = this->GetType();
  auto chunked_array = ChunkedArrayFromJSON(
      type, {R"(["123.45", "-123.45"])", R"([null, "456.78"])", R"(["-456.78",
      null])"});
  this->Check(type, chunked_array, 3, R"(["-456.78", "-123.45", "123.45"])");
}

using SortIndicesableTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, StringType,
                     Decimal128Type, BooleanType>;

template <typename ArrayType, SortOrder order>
void ValidateSelectK(const ArrayType& array) {
  ValidateOutput(array);
  SelectKComparator<ArrayType, order> compare;
  for (int i = 1; i < array.length(); i++) {
    const auto lval = GetLogicalValue(array, i - 1);
    const auto rval = GetLogicalValue(array, i);
    ASSERT_TRUE(compare(lval, rval));
  }
}
// Base class for testing against random chunked array.
template <typename Type, SortOrder order>
struct TestSelectKWithChunkedArrayRandomBase : public ::testing::Test {
  void TestSelectK(int length) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    // We can use INSTANTIATE_TEST_SUITE_P() instead of using fors in a test.
    for (auto null_probability : {0.0, 0.1, 0.5, 0.9, 1.0}) {
      for (auto num_chunks : {1, 2, 5, 10, 40}) {
        std::vector<std::shared_ptr<Array>> arrays;
        for (int i = 0; i < num_chunks; ++i) {
          auto array = this->GenerateArray(length / num_chunks, null_probability);
          arrays.push_back(array);
        }
        ASSERT_OK_AND_ASSIGN(auto chunked_array, ChunkedArray::Make(arrays));
        ASSERT_OK_AND_ASSIGN(auto top_k, SelectK<order>(*chunked_array, 5));
        // Concatenates chunks to use existing ValidateSorted() for array.
        ValidateSelectK<ArrayType, order>(*checked_pointer_cast<ArrayType>(top_k));
      }
    }
  }

  void SetUp() override { rand_ = new Random<Type>(0x5487655); }

  void TearDown() override { delete rand_; }

 protected:
  std::shared_ptr<Array> GenerateArray(int length, double null_probability) {
    return rand_->Generate(length, null_probability);
  }

 private:
  Random<Type>* rand_;
};

// Long array with big value range
template <typename Type>
class TestTopKChunkedArrayRandom
    : public TestSelectKWithChunkedArrayRandomBase<Type, SortOrder::Descending> {};

TYPED_TEST_SUITE(TestTopKChunkedArrayRandom, SortIndicesableTypes);

TYPED_TEST(TestTopKChunkedArrayRandom, TopK) { this->TestSelectK(1000); }

template <typename Type>
class TestBottomKChunkedArrayRandom
    : public TestSelectKWithChunkedArrayRandomBase<Type, SortOrder::Ascending> {};

TYPED_TEST_SUITE(TestBottomKChunkedArrayRandom, SortIndicesableTypes);

TYPED_TEST(TestBottomKChunkedArrayRandom, BottomK) { this->TestSelectK(1000); }

// Test basic cases for record batch.
class TestTopKWithRecordBatch : public ::testing::Test {};

TEST_F(TestTopKWithRecordBatch, NoNull) {
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
  // AssertSortIndices(batch, options, "[3, 5, 1, 6, 4, 0, 2]");
}

}  // namespace compute
}  // namespace arrow
