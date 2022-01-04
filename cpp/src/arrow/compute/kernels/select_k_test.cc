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
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {

template <typename ArrayType, SortOrder order>
class SelectKCompareForResult {
 public:
  template <typename Type>
  bool operator()(const Type& lval, const Type& rval) {
    if (order == SortOrder::Ascending) {
      return lval <= rval;
    } else {
      return rval <= lval;
    }
  }
};

template <SortOrder order>
Result<std::shared_ptr<Array>> SelectK(const Datum& values, int64_t k) {
  if (order == SortOrder::Descending) {
    return SelectKUnstable(values, SelectKOptions::TopKDefault(k));
  } else {
    return SelectKUnstable(values, SelectKOptions::BottomKDefault(k));
  }
}

void ValidateSelectK(const Datum& datum, Array& select_k_indices, SortOrder order,
                     bool stable_sort = false) {
  ASSERT_TRUE(datum.is_arraylike());
  ASSERT_OK_AND_ASSIGN(auto sorted_indices,
                       SortIndices(datum, SortOptions({SortKey("unused", order)})));

  int64_t k = select_k_indices.length();
  // head(k)
  auto head_k_indices = sorted_indices->Slice(0, k);
  if (stable_sort) {
    AssertDatumsEqual(*head_k_indices, select_k_indices);
  } else {
    ASSERT_OK_AND_ASSIGN(auto expected,
                         Take(datum, *head_k_indices, TakeOptions::NoBoundsCheck()));
    ASSERT_OK_AND_ASSIGN(auto actual,
                         Take(datum, select_k_indices, TakeOptions::NoBoundsCheck()));
    AssertDatumsEqual(Datum(expected), Datum(actual));
  }
}

template <typename ArrowType>
class TestSelectKBase : public ::testing::Test {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 protected:
  template <SortOrder order>
  void AssertSelectKArray(const std::shared_ptr<Array> values, int k) {
    std::shared_ptr<Array> select_k;
    ASSERT_OK_AND_ASSIGN(select_k, SelectK<order>(Datum(*values), k));
    ASSERT_EQ(select_k->data()->null_count, 0);
    ValidateOutput(*select_k);
    ValidateSelectK(Datum(*values), *select_k, order);
  }

  void AssertTopKArray(const std::shared_ptr<Array> values, int n) {
    AssertSelectKArray<SortOrder::Descending>(values, n);
  }
  void AssertBottomKArray(const std::shared_ptr<Array> values, int n) {
    AssertSelectKArray<SortOrder::Ascending>(values, n);
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
  ASSERT_RAISES(Invalid, CallFunction("select_k_unstable", {input}));
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
  this->AssertSelectKJson("[100, 4, 2, 7, 8, 3, NaN, 3, 1]", 4);
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

using SelectKableTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, StringType>;

TYPED_TEST_SUITE(TestSelectKRandom, SelectKableTypes);

TYPED_TEST(TestSelectKRandom, RandomValues) {
  Random<TypeParam> rand(0x61549225);
  int length = 100;
  for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
    auto array = rand.Generate(length, null_probability);
    // Try n from 0 to out of bound
    for (int n = 0; n <= length; ++n) {
      this->AssertTopKArray(array, n);
      this->AssertBottomKArray(array, n);
    }
  }
}

// Test basic cases for chunked array

template <typename ArrowType>
struct TestSelectKWithChunkedArray : public ::testing::Test {
  TestSelectKWithChunkedArray() {}

  // Slice `array` into multiple chunks along `offsets`
  ArrayVector Slices(const std::shared_ptr<Array>& array,
                     const std::shared_ptr<Int32Array>& offsets) {
    ArrayVector slices(offsets->length() - 1);
    for (int64_t i = 0; i != static_cast<int64_t>(slices.size()); ++i) {
      slices[i] =
          array->Slice(offsets->Value(i), offsets->Value(i + 1) - offsets->Value(i));
    }
    return slices;
  }

  template <SortOrder order = SortOrder::Descending>
  void AssertSelectK(const std::shared_ptr<ChunkedArray>& chunked_array, int64_t k) {
    ASSERT_OK_AND_ASSIGN(auto select_k_array, SelectK<order>(Datum(*chunked_array), k));
    ValidateSelectK(Datum(*chunked_array), *select_k_array, order);
  }

  void AssertTopK(const std::shared_ptr<ChunkedArray>& chunked_array, int64_t k) {
    AssertSelectK<SortOrder::Descending>(chunked_array, k);
  }
  void AssertBottomK(const std::shared_ptr<ChunkedArray>& chunked_array, int64_t k) {
    AssertSelectK<SortOrder::Ascending>(chunked_array, k);
  }
};

TYPED_TEST_SUITE(TestSelectKWithChunkedArray, SelectKableTypes);

TYPED_TEST(TestSelectKWithChunkedArray, RandomValuesWithSlices) {
  Random<TypeParam> rand(0x61549225);
  int length = 100;
  for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
    // Try n from 0 to out of bound
    auto array = rand.Generate(length, null_probability);
    auto offsets = rand.Offsets(length, 3);
    auto slices = this->Slices(array, offsets);
    ASSERT_OK_AND_ASSIGN(auto chunked_array, ChunkedArray::Make(slices));
    for (int k = 0; k <= length; k += 10) {
      this->AssertTopK(chunked_array, k);
      this->AssertBottomK(chunked_array, k);
    }
  }
}

template <typename ArrayType, SortOrder order>
void ValidateSelectKIndices(const ArrayType& array) {
  ValidateOutput(array);

  SelectKCompareForResult<ArrayType, order> compare;
  for (uint64_t i = 1; i < static_cast<uint64_t>(array.length()); i++) {
    using ArrowType = typename ArrayType::TypeClass;
    using GetView = internal::GetViewType<ArrowType>;

    const auto lval = GetView::LogicalValue(array.GetView(i - 1));
    const auto rval = GetView::LogicalValue(array.GetView(i));
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
        ASSERT_OK_AND_ASSIGN(auto indices, SelectK<order>(Datum(*chunked_array), 5));
        ASSERT_OK_AND_ASSIGN(auto actual, Take(Datum(chunked_array), Datum(indices),
                                               TakeOptions::NoBoundsCheck()));
        ASSERT_OK_AND_ASSIGN(auto sorted_k,
                             Concatenate(actual.chunked_array()->chunks()));

        ValidateSelectKIndices<ArrayType, order>(
            *checked_pointer_cast<ArrayType>(sorted_k));
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

// // Test basic cases for record batch.
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
    ARROW_ASSIGN_OR_RAISE(auto indices, SelectKUnstable(Datum(*batch), options));

    ValidateOutput(*indices);
    ARROW_ASSIGN_OR_RAISE(
        auto select_k, Take(Datum(batch), Datum(indices), TakeOptions::NoBoundsCheck()));
    *out = select_k.record_batch();
    return Status::OK();
  }
};

TEST_F(TestSelectKWithRecordBatch, TopKNoNull) {
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

  auto options = SelectKOptions::TopKDefault(3, {"a"});

  auto expected_batch = R"([
    {"a": 30,    "b": 3},
    {"a": 20,    "b": 5},
    {"a": 10,    "b": 5}
  ])";

  Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestSelectKWithRecordBatch, TopKNull) {
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

  auto options = SelectKOptions::TopKDefault(3, {"a"});

  auto expected_batch = R"([
    {"a": 30,    "b": 3},
    {"a": 20,    "b": 5},
    {"a": 10,    "b": 3}
  ])";

  Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestSelectKWithRecordBatch, TopKOneColumnKey) {
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

  auto options = SelectKOptions::TopKDefault(3, {"population"});

  auto expected_batch =
      R"([{"country": "France", "population": 65000000},
         {"country": "Italy", "population": 59000000},
         {"country": "Malta", "population": 434000}
         ])";
  this->Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestSelectKWithRecordBatch, TopKMultipleColumnKeys) {
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
  auto options = SelectKOptions::TopKDefault(3, {"population", "GDP"});

  auto expected_batch =
      R"([{"country": "France", "population": 65000000, "GDP": 2583560},
         {"country": "Italy", "population": 59000000, "GDP": 1937894},
         {"country": "Brunei", "population": 434000, "GDP": 12128}
         ])";
  this->Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestSelectKWithRecordBatch, BottomKNoNull) {
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

  auto options = SelectKOptions::BottomKDefault(3, {"a"});

  auto expected_batch = R"([
    {"a": 0,    "b": 6},
    {"a": 3,    "b": 4},
    {"a": 3,    "b": 5}
  ])";

  Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestSelectKWithRecordBatch, BottomKNull) {
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

  auto options = SelectKOptions::BottomKDefault(3, {"a"});

  auto expected_batch = R"([
    {"a": 10,    "b": 3},
    {"a": 20,    "b": 5},
    {"a": 30,    "b": 3}
  ])";

  Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestSelectKWithRecordBatch, BottomKOneColumnKey) {
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

  auto options = SelectKOptions::BottomKDefault(3, {"population"});

  auto expected_batch =
      R"([{"country": "Montserrat", "population": 5200},
         {"country": "Anguilla", "population": 11300},
         {"country": "Tuvalu", "population": 11300}
         ])";
  this->Check(schema, batch_input, options, expected_batch);
}

TEST_F(TestSelectKWithRecordBatch, BottomKMultipleColumnKeys) {
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

  auto options = SelectKOptions::BottomKDefault(3, {"population", "GDP"});

  auto expected_batch =
      R"([{"country": "Tuvalu", "population": 11300, "GDP": 38},
         {"country": "Anguilla", "population": 11300, "GDP": 311},
         {"country": "Nauru", "population": 337000, "GDP": 182}
         ])";
  this->Check(schema, batch_input, options, expected_batch);
}

// Test basic cases for table.
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
    auto table = TableFromJSON(schm, input_json);
    ARROW_ASSIGN_OR_RAISE(auto indices, SelectKUnstable(Datum(*table), options));
    ValidateOutput(*indices);

    ARROW_ASSIGN_OR_RAISE(
        auto select_k, Take(Datum(table), Datum(indices), TakeOptions::NoBoundsCheck()));
    *out = select_k.table();
    return Status::OK();
  }
};

TEST_F(TestSelectKWithTable, TopKOneColumnKey) {
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

  auto options = SelectKOptions::TopKDefault(3, {"a"});

  std::vector<std::string> expected = {R"([{"a": 3,    "b": null},
                                     {"a": 2,    "b": 5},
                                     {"a": 1,    "b": 3}
                                    ])"};
  Check(schema, input, options, expected);
}

TEST_F(TestSelectKWithTable, TopKMultipleColumnKeys) {
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

  auto options = SelectKOptions::TopKDefault(3, {"a", "b"});

  std::vector<std::string> expected = {R"([{"a": 3,    "b": null},
                                     {"a": 2,    "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"};
  Check(schema, input, options, expected);
}

TEST_F(TestSelectKWithTable, BottomKOneColumnKey) {
  auto schema = ::arrow::schema({
      {field("a", uint8())},
      {field("b", uint32())},
  });

  std::vector<std::string> input = {R"([{"a": null, "b": 5},
                                     {"a": 0,    "b": 3},
                                     {"a": 3,    "b": null},
                                     {"a": null, "b": null},
                                     {"a": 2,    "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"};

  auto options = SelectKOptions::BottomKDefault(3, {"a"});

  std::vector<std::string> expected = {R"([{"a": 0,    "b": 3},
                                           {"a": 1,    "b": 5},
                                           {"a": 2,    "b": 5}
                                           ])"};
  Check(schema, input, options, expected);
}

TEST_F(TestSelectKWithTable, BottomKMultipleColumnKeys) {
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

  auto options = SelectKOptions::BottomKDefault(3, {"a", "b"});

  std::vector<std::string> expected = {R"([{"a": 1,    "b": 3},
                                     {"a": 1,    "b": 5},
                                     {"a": 2,    "b": 5}
                                    ])"};
  Check(schema, input, options, expected);
}

}  // namespace compute
}  // namespace arrow
