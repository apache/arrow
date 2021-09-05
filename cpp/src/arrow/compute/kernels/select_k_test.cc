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
template <typename ArrayType>
auto GetLogicalValue(const ArrayType& array, uint64_t index)
    -> decltype(array.GetView(index)) {
  return array.GetView(index);
}

Decimal128 GetLogicalValue(const Decimal128Array& array, uint64_t index) {
  return Decimal128(array.Value(index));
}

}  // namespace

template <typename ArrayType, SortOrder order>
class SelectKComparator {
 public:
  template <typename Type>
  bool operator()(const Type& lval, const Type& rval) {
    if (is_floating_type<typename ArrayType::TypeClass>::value) {
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
Result<std::shared_ptr<Array>> SelectK(const Array& values, int64_t k) {
  if (order == SortOrder::Descending) {
    return TopK(values, k);
  } else {
    return BottomK(values, k);
  }
}
template <SortOrder order>
Result<std::shared_ptr<Array>> SelectK(const ChunkedArray& values, int64_t k) {
  if (order == SortOrder::Descending) {
    return TopK(values, k);
  } else {
    return BottomK(values, k);
  }
}

template <SortOrder order>
Result<std::shared_ptr<RecordBatch>> SelectK(const RecordBatch& values,
                                             const SelectKOptions& options) {
  if (order == SortOrder::Descending) {
    ARROW_ASSIGN_OR_RAISE(auto out, TopK(Datum(values), options.k, options));
    return out.record_batch();
  } else {
    ARROW_ASSIGN_OR_RAISE(auto out, BottomK(Datum(values), options.k, options));
    return out.record_batch();
  }
}

template <SortOrder order>
Result<std::shared_ptr<Table>> SelectK(const Table& values,
                                       const SelectKOptions& options) {
  if (order == SortOrder::Descending) {
    ARROW_ASSIGN_OR_RAISE(auto out, TopK(Datum(values), options.k, options));
    return out.table();
  } else {
    ARROW_ASSIGN_OR_RAISE(auto out, BottomK(Datum(values), options.k, options));
    return out.table();
  }
}

template <typename ArrowType>
class TestSelectKBase : public TestBase {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 protected:
  void Validate(const ArrayType& array, int k, ArrayType& select_k, SortOrder order) {
    ASSERT_OK_AND_ASSIGN(auto sorted_indices, SortIndices(array, order));

    // head(k)
    auto head_k_indices = sorted_indices->Slice(0, select_k.length());

    // sorted_indices
    ASSERT_OK_AND_ASSIGN(Datum sorted_datum,
                         Take(array, head_k_indices, TakeOptions::NoBoundsCheck()));
    std::shared_ptr<Array> sorted_array_out = sorted_datum.make_array();

    const ArrayType& sorted_array = *checked_pointer_cast<ArrayType>(sorted_array_out);

    if (k < array.length()) {
      AssertArraysEqual(sorted_array, select_k);
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
    AssertTopKArray(ArrayFromJSON(type_singleton(), values), n);
    AssertBottomKArray(ArrayFromJSON(type_singleton(), values), n);
  }

  virtual std::shared_ptr<DataType> type_singleton() = 0;
};

template <typename ArrowType>
class TestSelectK : public TestSelectKBase<ArrowType> {
 protected:
  std::shared_ptr<DataType> type_singleton() override {
    return default_type_instance<ArrowType>();
  }
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
  std::shared_ptr<DataType> type_singleton() override {
    return std::make_shared<ArrowType>(5, 2);
  }
};
TYPED_TEST_SUITE(TestSelectKForDecimal, DecimalArrowTypes);

template <typename ArrowType>
class TestSelectKForStrings : public TestSelectK<ArrowType> {};
TYPED_TEST_SUITE(TestSelectKForStrings, testing::Types<StringType>);

TYPED_TEST(TestSelectKForReal, SelectKDoesNotProvideDefaultOptions) {
  auto input = ArrayFromJSON(this->type_singleton(), "[null, 1, 3.3, null, 2, 5.3]");
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
  std::shared_ptr<DataType> type_singleton() override {
    EXPECT_TRUE(0) << "shouldn't be used";
    return nullptr;
  }
};

using NumericBasedTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, Date32Type, Date64Type,
                     TimestampType, Time32Type, Time64Type>;

using SelectKableTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, Decimal128Type,
                     StringType>;

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
    ValidateOutput(*out);
    return Status::OK();
  }
  std::vector<int32_t> sizes_;
  std::vector<double> null_probabilities_;
};

template <typename ArrowType>
struct TestTopKWithChunkedArray
    : public TestSelectKWithChunkedArray<SortOrder::Descending> {
  std::shared_ptr<DataType> type_singleton() {
    return default_type_instance<ArrowType>();
  }
};

TYPED_TEST_SUITE(TestTopKWithChunkedArray, NumericBasedTypes);

TYPED_TEST(TestTopKWithChunkedArray, WithTypedParam) {
  auto type = this->type_singleton();
  this->Check(type, {"[0, 1, 9]", "[3, 7, 2, 4, 10]"}, 3, "[10, 9, 7]");
  this->Check(type, {"[]", "[]"}, 0, "[]");
  this->Check(type, {"[]"}, 0, "[]");
}

TYPED_TEST(TestTopKWithChunkedArray, Null) {
  auto type = this->type_singleton();
  this->Check(type, {"[null]", "[8, null]"}, 1, "[8]");

  // this->Check(type, {"[null]", "[null, null]"}, 0, "[]");
  // this->Check(type, {"[0, null, 9]", "[3, null, 2, null, 10]"}, 3, "[10, 9, 3]");
  // this->Check(type, {"[null]", "[]"}, 0, "[]");
}

TYPED_TEST(TestTopKWithChunkedArray, NaN) {
  this->Check(float32(), {"[NaN]", "[8, NaN]"}, 1, "[8]");
  this->Check(float32(), {"[NaN]", "[NaN, NaN]"}, 0, "[]");
  this->Check(float32(), {"[0, NaN, 9]", "[3, NaN, 2, NaN, 10]"}, 3, "[10, 9, 3]");
  this->Check(float32(), {"[NaN]", "[]"}, 0, "[]");
}
template <typename ArrowType>
struct TestBottomKWithChunkedArray
    : public TestSelectKWithChunkedArray<SortOrder::Ascending> {
  std::shared_ptr<DataType> type_singleton() {
    return default_type_instance<ArrowType>();
  }
};

TYPED_TEST_SUITE(TestBottomKWithChunkedArray, NumericBasedTypes);

TYPED_TEST(TestBottomKWithChunkedArray, Int8) {
  auto type = this->type_singleton();
  this->Check(type, {"[0, 1, 9]", "[3, 7, 2, 4, 10]"}, 3, "[0, 1, 2]");
  this->Check(type, {"[]", "[]"}, 0, "[]");
  this->Check(type, {"[]"}, 0, "[]");
}

TYPED_TEST(TestBottomKWithChunkedArray, Null) {
  auto type = this->type_singleton();
  this->Check(type, {"[null]", "[8, null]"}, 1, "[8]");
  this->Check(type, {"[null]", "[null, null]"}, 0, "[]");
  this->Check(type, {"[0, null, 9]", "[3, null, 2, null, 10]"}, 3, "[0, 2, 3]");
  this->Check(type, {"[null]", "[]"}, 0, "[]");
}

TYPED_TEST(TestBottomKWithChunkedArray, NaN) {
  this->Check(float32(), {"[NaN]", "[8, NaN]"}, 1, "[8]");
  this->Check(float32(), {"[NaN]", "[NaN, NaN]"}, 0, "[]");
  this->Check(float32(), {"[0, NaN, 9]", "[3, NaN, 2, NaN, 10]"}, 3, "[0, 2, 3]");
  this->Check(float32(), {"[NaN]", "[]"}, 0, "[]");
}

// Tests for decimal types
template <typename ArrowType>
class TestTopKWithChunkedArrayForDecimal
    : public TestSelectKWithChunkedArray<SortOrder::Descending> {
 protected:
  std::shared_ptr<DataType> type_singleton() { return std::make_shared<ArrowType>(5, 2); }
};
TYPED_TEST_SUITE(TestTopKWithChunkedArrayForDecimal, DecimalArrowTypes);

TYPED_TEST(TestTopKWithChunkedArrayForDecimal, Basics) {
  auto type = this->type_singleton();
  auto chunked_array = ChunkedArrayFromJSON(
      type, {R"(["123.45", "-123.45"])", R"([null, "456.78"])", R"(["-456.78",
      null])"});
  this->Check(type, chunked_array, 3, R"(["456.78", "123.45", "-123.45"])");
}

template <typename ArrowType>
class TestBottomKWithChunkedArrayForDecimal
    : public TestSelectKWithChunkedArray<SortOrder::Ascending> {
 protected:
  std::shared_ptr<DataType> type_singleton() { return std::make_shared<ArrowType>(5, 2); }
};
TYPED_TEST_SUITE(TestBottomKWithChunkedArrayForDecimal, DecimalArrowTypes);

TYPED_TEST(TestBottomKWithChunkedArrayForDecimal, Basics) {
  auto type = this->type_singleton();
  auto chunked_array = ChunkedArrayFromJSON(
      type, {R"(["123.45", "-123.45"])", R"([null, "456.78"])", R"(["-456.78",
      null])"});
  this->Check(type, chunked_array, 3, R"(["-456.78", "-123.45", "123.45"])");
}

template <typename ArrayType, SortOrder order>
void ValidateSelectK(const ArrayType& array) {
  ValidateOutput(array);
  SelectKComparator<ArrayType, order> compare;
  for (uint64_t i = 1; i < static_cast<uint64_t>(array.length()); i++) {
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

TYPED_TEST_SUITE(TestTopKChunkedArrayRandom, SelectKableTypes);

TYPED_TEST(TestTopKChunkedArrayRandom, TopK) { this->TestSelectK(1000); }

template <typename Type>
class TestBottomKChunkedArrayRandom
    : public TestSelectKWithChunkedArrayRandomBase<Type, SortOrder::Ascending> {};

TYPED_TEST_SUITE(TestBottomKChunkedArrayRandom, SelectKableTypes);

TYPED_TEST(TestBottomKChunkedArrayRandom, BottomK) { this->TestSelectK(1000); }

// Test basic cases for record batch.
template <SortOrder order>
class TestSelectKWithRecordBatch : public ::testing::Test {
 public:
  void Check(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
             const SelectKOptions& options, const std::string& expected_batch) {
    std::shared_ptr<RecordBatch> actual;
    ASSERT_OK(this->DoSelectK(schm, batch_json, options, &actual));
    ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(schm, expected_batch), *actual);
  }

  Status DoSelectK(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
                   const SelectKOptions& options, std::shared_ptr<RecordBatch>* out) {
    auto batch = RecordBatchFromJSON(schm, batch_json);
    ARROW_ASSIGN_OR_RAISE(*out, SelectK<order>(*batch, options));
    ValidateOutput(*out);
    return Status::OK();
  }
};

struct TestTopKWithRecordBatch : TestSelectKWithRecordBatch<SortOrder::Descending> {};

TEST_F(TestTopKWithRecordBatch, NoNull) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });

  auto batch_input = R"([
    {"a": 3,    "b": 5},
    {"a": 30,   "b": 3},
    {"a": 3,    "b": 4},
    {"a": 0,    "b": 6},
    {"a": 20,   "b": 5},
    {"a": 10,   "b": 5},
    {"a": 10,   "b": 3}
  ])";

  auto options = SelectKOptions::TopKDefault();
  options.k = 3;
  options.keys = {"a"};

  auto expected_batch = R"([
    {"a": 30,    "b": 3},
    {"a": 20,    "b": 5},
    {"a": 10,    "b": 5}
  ])";

  Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestTopKWithRecordBatch, Null) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });

  auto batch_input = R"([
    {"a": null,    "b": 5},
    {"a": 30,   "b": 3},
    {"a": null,    "b": 4},
    {"a": null,    "b": 6},
    {"a": 20,   "b": 5},
    {"a": null,   "b": 5},
    {"a": 10,   "b": 3}
  ])";

  auto options = SelectKOptions::TopKDefault();
  options.k = 3;
  options.keys = {"a"};

  auto expected_batch = R"([
    {"a": 30,    "b": 3},
    {"a": 20,    "b": 5},
    {"a": 10,    "b": 3}
  ])";

  Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestTopKWithRecordBatch, OneColumnKey) {
  auto schema = ::arrow::schema({
      {field("country", utf8())},
      {field("population", uint64())},
  });

  auto batch_input =
      R"([{"country": "Italy", "population": 59000000},
        {"country": "France", "population": 65000000},
        {"country": "Malta", "population": 434000},
        {"country": "Maldives", "population": 434000},
        {"country": "Brunei", "population": 434000},
        {"country": "Iceland", "population": 337000},
        {"country": "Nauru", "population": 11300},
        {"country": "Tuvalu", "population": 11300},
        {"country": "Anguilla", "population": 11300},
        {"country": "Montserrat", "population": 5200}
        ])";
  auto options = SelectKOptions::TopKDefault();
  options.keys = {"population"};
  options.k = 3;

  auto expected_batch =
      R"([{"country": "France", "population": 65000000},
         {"country": "Italy", "population": 59000000},
         {"country": "Malta", "population": 434000}
         ])";
  this->Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestTopKWithRecordBatch, MultipleColumnKeys) {
  auto schema = ::arrow::schema({{field("country", utf8())},
                                 {field("population", uint64())},
                                 {field("GDP", uint64())}});

  auto batch_input =
      R"([{"country": "Italy", "population": 59000000, "GDP": 1937894},
        {"country": "France", "population": 65000000, "GDP": 2583560},
        {"country": "Malta", "population": 434000, "GDP": 12011},
        {"country": "Maldives", "population": 434000, "GDP": 4520},
        {"country": "Brunei", "population": 434000, "GDP": 12128},
        {"country": "Iceland", "population": 337000, "GDP": 17036},
        {"country": "Nauru", "population": 337000, "GDP": 182},
        {"country": "Tuvalu", "population": 11300, "GDP": 38},
        {"country": "Anguilla", "population": 11300, "GDP": 311}
        ])";
  auto options = SelectKOptions::TopKDefault();
  options.keys = {"population", "GDP"};
  options.k = 3;

  auto expected_batch =
      R"([{"country": "France", "population": 65000000, "GDP": 2583560},
         {"country": "Italy", "population": 59000000, "GDP": 1937894},
         {"country": "Brunei", "population": 434000, "GDP": 12128}
         ])";
  this->Check(schema, batch_input, options, expected_batch);
}

struct TestBottomKWithRecordBatch : TestSelectKWithRecordBatch<SortOrder::Ascending> {};

TEST_F(TestBottomKWithRecordBatch, NoNull) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });

  auto batch_input = R"([
    {"a": 3,    "b": 5},
    {"a": 30,   "b": 3},
    {"a": 3,    "b": 4},
    {"a": 0,    "b": 6},
    {"a": 20,   "b": 5},
    {"a": 10,   "b": 5},
    {"a": 10,   "b": 3}
  ])";

  auto options = SelectKOptions::TopKDefault();
  options.k = 3;
  options.keys = {"a"};

  auto expected_batch = R"([
    {"a": 0,    "b": 6},
    {"a": 3,    "b": 4},
    {"a": 3,    "b": 5}
  ])";

  Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestBottomKWithRecordBatch, Null) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });

  auto batch_input = R"([
    {"a": null,    "b": 5},
    {"a": 30,   "b": 3},
    {"a": null,    "b": 4},
    {"a": null,    "b": 6},
    {"a": 20,   "b": 5},
    {"a": null,   "b": 5},
    {"a": 10,   "b": 3}
  ])";

  auto options = SelectKOptions::TopKDefault();
  options.k = 3;
  options.keys = {"a"};

  auto expected_batch = R"([
    {"a": 10,    "b": 3},
    {"a": 20,    "b": 5},
    {"a": 30,    "b": 3}
  ])";

  Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestBottomKWithRecordBatch, OneColumnKey) {
  auto schema = ::arrow::schema({
      {field("country", utf8())},
      {field("population", uint64())},
  });

  auto batch_input =
      R"([{"country": "Italy", "population": 59000000},
        {"country": "France", "population": 65000000},
        {"country": "Malta", "population": 434000},
        {"country": "Maldives", "population": 434000},
        {"country": "Brunei", "population": 434000},
        {"country": "Iceland", "population": 337000},
        {"country": "Nauru", "population": 11300},
        {"country": "Tuvalu", "population": 11300},
        {"country": "Anguilla", "population": 11300},
        {"country": "Montserrat", "population": 5200}
        ])";
  auto options = SelectKOptions::TopKDefault();
  options.keys = {"population"};
  options.k = 3;

  auto expected_batch =
      R"([{"country": "Montserrat", "population": 5200},
         {"country": "Anguilla", "population": 11300},
         {"country": "Tuvalu", "population": 11300}
         ])";
  this->Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestBottomKWithRecordBatch, MultipleColumnKeys) {
  auto schema = ::arrow::schema({{field("country", utf8())},
                                 {field("population", uint64())},
                                 {field("GDP", uint64())}});

  auto batch_input =
      R"([{"country": "Italy", "population": 59000000, "GDP": 1937894},
        {"country": "France", "population": 65000000, "GDP": 2583560},
        {"country": "Malta", "population": 434000, "GDP": 12011},
        {"country": "Maldives", "population": 434000, "GDP": 4520},
        {"country": "Brunei", "population": 434000, "GDP": 12128},
        {"country": "Iceland", "population": 337000, "GDP": 17036},
        {"country": "Nauru", "population": 337000, "GDP": 182},
        {"country": "Tuvalu", "population": 11300, "GDP": 38},
        {"country": "Anguilla", "population": 11300, "GDP": 311}
        ])";
  auto options = SelectKOptions::TopKDefault();
  options.keys = {"population", "GDP"};
  options.k = 3;

  auto expected_batch =
      R"([{"country": "Tuvalu", "population": 11300, "GDP": 38},
         {"country": "Anguilla", "population": 11300, "GDP": 311},
         {"country": "Nauru", "population": 337000, "GDP": 182}
         ])";
  this->Check(schema, batch_input, options, expected_batch);
}

// Test basic cases for table.
template <SortOrder order>
struct TestSelectKWithTable : public ::testing::Test {
  void Check(const std::shared_ptr<Schema>& schm,
             const std::vector<std::string>& input_json, const SelectKOptions& options,
             const std::vector<std::string>& expected) {
    std::shared_ptr<Table> actual;
    ASSERT_OK(this->DoSelectK(schm, input_json, options, &actual));
    ASSERT_TABLES_EQUAL(*TableFromJSON(schm, expected), *actual);
  }

  Status DoSelectK(const std::shared_ptr<Schema>& schm,
                   const std::vector<std::string>& input_json,
                   const SelectKOptions& options, std::shared_ptr<Table>* out) {
    auto batch = TableFromJSON(schm, input_json);
    ARROW_ASSIGN_OR_RAISE(*out, SelectK<order>(*batch, options));
    ValidateOutput(*out);
    return Status::OK();
  }
};

struct TestTopKWithTable : TestSelectKWithTable<SortOrder::Descending> {};

TEST_F(TestTopKWithTable, OneColumnKey) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });

  std::vector<std::string> input = {R"([{"a": null, "b": 5},
                                     {"a": 1,    "b": 3},
                                     {"a": 3,    "b": null},
                                     {"a": null, "b": null},
                                     {"a": 2,    "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"};
  auto options = SelectKOptions::TopKDefault();
  options.k = 3;
  options.keys = {"a"};

  std::vector<std::string> expected = {R"([{"a": 3,    "b": null},
                                     {"a": 2,    "b": 5},
                                     {"a": 1,    "b": 3}
                                    ])"};
  Check(schema, input, options, expected);
}

TEST_F(TestTopKWithTable, MultipleColumnKeys) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });
  std::vector<std::string> input = {R"([{"a": null, "b": 5},
                                        {"a": 1,    "b": 3},
                                        {"a": 3,    "b": null}
                                      ])",
                                    R"([{"a": null, "b": null},
                                          {"a": 2,    "b": 5},
                                          {"a": 1,    "b": 5}
                                        ])"};

  auto options = SelectKOptions::TopKDefault();
  options.k = 3;
  options.keys = {"a", "b"};

  std::vector<std::string> expected = {R"([{"a": 3,    "b": null},
                                     {"a": 2,    "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"};
  Check(schema, input, options, expected);
}

}  // namespace compute
}  // namespace arrow
