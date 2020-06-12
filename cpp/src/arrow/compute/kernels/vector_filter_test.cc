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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace compute {

using internal::checked_pointer_cast;
using util::string_view;

std::shared_ptr<Array> CoalesceNullToFalse(std::shared_ptr<Array> filter) {
  if (filter->null_count() == 0) {
    return filter;
  }
  const auto& data = *filter->data();
  auto is_true = std::make_shared<BooleanArray>(data.length, data.buffers[1]);
  auto is_valid = std::make_shared<BooleanArray>(data.length, data.buffers[0]);
  EXPECT_OK_AND_ASSIGN(Datum out_datum, arrow::compute::And(is_true, is_valid));
  return out_datum.make_array();
}

template <typename ArrowType>
class TestFilterKernel : public ::testing::Test {
 protected:
  TestFilterKernel() {
    emit_null_.null_selection_behavior = FilterOptions::EMIT_NULL;
    drop_.null_selection_behavior = FilterOptions::DROP;
  }

  void AssertFilter(std::shared_ptr<Array> values, std::shared_ptr<Array> filter,
                    std::shared_ptr<Array> expected) {
    // test with EMIT_NULL
    ASSERT_OK_AND_ASSIGN(Datum out_datum,
                         arrow::compute::Filter(values, filter, emit_null_));
    auto actual = out_datum.make_array();
    ASSERT_OK(actual->ValidateFull());
    AssertArraysEqual(*expected, *actual);

    // test with DROP using EMIT_NULL and a coalesced filter
    auto coalesced_filter = CoalesceNullToFalse(filter);
    ASSERT_OK_AND_ASSIGN(out_datum,
                         arrow::compute::Filter(values, coalesced_filter, emit_null_));
    expected = out_datum.make_array();
    ASSERT_OK_AND_ASSIGN(out_datum, arrow::compute::Filter(values, filter, drop_));
    actual = out_datum.make_array();
    AssertArraysEqual(*expected, *actual);
  }

  void AssertFilter(std::shared_ptr<DataType> type, const std::string& values,
                    const std::string& filter, const std::string& expected) {
    AssertFilter(ArrayFromJSON(type, values), ArrayFromJSON(boolean(), filter),
                 ArrayFromJSON(type, expected));
  }

  void ValidateFilter(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<Array>& filter_boxed) {
    ASSERT_OK_AND_ASSIGN(Datum out_datum,
                         arrow::compute::Filter(values, filter_boxed, emit_null_));
    auto filtered_emit_null = out_datum.make_array();
    ASSERT_OK(filtered_emit_null->ValidateFull());

    ASSERT_OK_AND_ASSIGN(out_datum, arrow::compute::Filter(values, filter_boxed, drop_));
    auto filtered_drop = out_datum.make_array();
    ASSERT_OK(filtered_drop->ValidateFull());

    auto filter = checked_pointer_cast<BooleanArray>(filter_boxed);
    int64_t values_i = 0, emit_null_i = 0, drop_i = 0;
    for (; values_i < values->length(); ++values_i, ++emit_null_i, ++drop_i) {
      if (filter->IsNull(values_i)) {
        ASSERT_LT(emit_null_i, filtered_emit_null->length());
        ASSERT_TRUE(filtered_emit_null->IsNull(emit_null_i));
        // this element was (null) filtered out; don't examine filtered_drop
        --drop_i;
        continue;
      }
      if (!filter->Value(values_i)) {
        // this element was filtered out; don't examine filtered_emit_null
        --emit_null_i;
        --drop_i;
        continue;
      }
      ASSERT_LT(emit_null_i, filtered_emit_null->length());
      ASSERT_LT(drop_i, filtered_drop->length());
      ASSERT_TRUE(
          values->RangeEquals(values_i, values_i + 1, emit_null_i, filtered_emit_null));
      ASSERT_TRUE(values->RangeEquals(values_i, values_i + 1, drop_i, filtered_drop));
    }
    ASSERT_EQ(emit_null_i, filtered_emit_null->length());
    ASSERT_EQ(drop_i, filtered_drop->length());
  }

  FilterOptions emit_null_, drop_;
};

class TestFilterKernelWithNull : public TestFilterKernel<NullType> {
 protected:
  void AssertFilter(const std::string& values, const std::string& filter,
                    const std::string& expected) {
    TestFilterKernel<NullType>::AssertFilter(ArrayFromJSON(null(), values),
                                             ArrayFromJSON(boolean(), filter),
                                             ArrayFromJSON(null(), expected));
  }
};

TEST_F(TestFilterKernelWithNull, FilterNull) {
  this->AssertFilter("[]", "[]", "[]");

  this->AssertFilter("[null, null, null]", "[0, 1, 0]", "[null]");
  this->AssertFilter("[null, null, null]", "[1, 1, 0]", "[null, null]");
}

class TestFilterKernelWithBoolean : public TestFilterKernel<BooleanType> {
 protected:
  void AssertFilter(const std::string& values, const std::string& filter,
                    const std::string& expected) {
    TestFilterKernel<BooleanType>::AssertFilter(ArrayFromJSON(boolean(), values),
                                                ArrayFromJSON(boolean(), filter),
                                                ArrayFromJSON(boolean(), expected));
  }
};

TEST_F(TestFilterKernelWithBoolean, FilterBoolean) {
  this->AssertFilter("[]", "[]", "[]");

  this->AssertFilter("[true, false, true]", "[0, 1, 0]", "[false]");
  this->AssertFilter("[null, false, true]", "[0, 1, 0]", "[false]");
  this->AssertFilter("[true, false, true]", "[null, 1, 0]", "[null, false]");
}

template <typename ArrowType>
class TestFilterKernelWithNumeric : public TestFilterKernel<ArrowType> {
 protected:
  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_SUITE(TestFilterKernelWithNumeric, NumericArrowTypes);
TYPED_TEST(TestFilterKernelWithNumeric, FilterNumeric) {
  auto type = this->type_singleton();
  this->AssertFilter(type, "[]", "[]", "[]");

  this->AssertFilter(type, "[9]", "[0]", "[]");
  this->AssertFilter(type, "[9]", "[1]", "[9]");
  this->AssertFilter(type, "[9]", "[null]", "[null]");
  this->AssertFilter(type, "[null]", "[0]", "[]");
  this->AssertFilter(type, "[null]", "[1]", "[null]");
  this->AssertFilter(type, "[null]", "[null]", "[null]");

  this->AssertFilter(type, "[7, 8, 9]", "[0, 1, 0]", "[8]");
  this->AssertFilter(type, "[7, 8, 9]", "[1, 0, 1]", "[7, 9]");
  this->AssertFilter(type, "[null, 8, 9]", "[0, 1, 0]", "[8]");
  this->AssertFilter(type, "[7, 8, 9]", "[null, 1, 0]", "[null, 8]");
  this->AssertFilter(type, "[7, 8, 9]", "[1, null, 1]", "[7, null, 9]");

  this->AssertFilter(ArrayFromJSON(type, "[7, 8, 9]"),
                     ArrayFromJSON(boolean(), "[0, 1, 1, 1, 0, 1]")->Slice(3, 3),
                     ArrayFromJSON(type, "[7, 9]"));

  ASSERT_RAISES(Invalid,
                arrow::compute::Filter(ArrayFromJSON(type, "[7, 8, 9]"),
                                       ArrayFromJSON(boolean(), "[]"), this->emit_null_));
  ASSERT_RAISES(Invalid,
                arrow::compute::Filter(ArrayFromJSON(type, "[7, 8, 9]"),
                                       ArrayFromJSON(boolean(), "[]"), this->drop_));
}

TYPED_TEST(TestFilterKernelWithNumeric, FilterRandomNumeric) {
  auto rand = random::RandomArrayGenerator(kRandomSeed);
  for (size_t i = 3; i < 10; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    for (auto null_probability : {0.0, 0.01, 0.25, 1.0}) {
      for (auto filter_probability : {0.0, 0.1, 0.5, 1.0}) {
        auto values = rand.Numeric<TypeParam>(length, 0, 127, null_probability);
        auto filter = rand.Boolean(length, filter_probability, null_probability);
        this->ValidateFilter(values, filter);
      }
    }
  }
}

template <typename CType>
using Comparator = bool(CType, CType);

template <typename CType>
Comparator<CType>* GetComparator(CompareOperator op) {
  static Comparator<CType>* cmp[] = {
      // EQUAL
      [](CType l, CType r) { return l == r; },
      // NOT_EQUAL
      [](CType l, CType r) { return l != r; },
      // GREATER
      [](CType l, CType r) { return l > r; },
      // GREATER_EQUAL
      [](CType l, CType r) { return l >= r; },
      // LESS
      [](CType l, CType r) { return l < r; },
      // LESS_EQUAL
      [](CType l, CType r) { return l <= r; },
  };
  return cmp[op];
}

template <typename T, typename Fn, typename CType = typename TypeTraits<T>::CType>
std::shared_ptr<Array> CompareAndFilter(const CType* data, int64_t length, Fn&& fn) {
  std::vector<CType> filtered;
  filtered.reserve(length);
  std::copy_if(data, data + length, std::back_inserter(filtered), std::forward<Fn>(fn));
  std::shared_ptr<Array> filtered_array;
  ArrayFromVector<T, CType>(filtered, &filtered_array);
  return filtered_array;
}

template <typename T, typename CType = typename TypeTraits<T>::CType>
std::shared_ptr<Array> CompareAndFilter(const CType* data, int64_t length, CType val,
                                        CompareOperator op) {
  auto cmp = GetComparator<CType>(op);
  return CompareAndFilter<T>(data, length, [&](CType e) { return cmp(e, val); });
}

template <typename T, typename CType = typename TypeTraits<T>::CType>
std::shared_ptr<Array> CompareAndFilter(const CType* data, int64_t length,
                                        const CType* other, CompareOperator op) {
  auto cmp = GetComparator<CType>(op);
  return CompareAndFilter<T>(data, length, [&](CType e) { return cmp(e, *other++); });
}

TYPED_TEST(TestFilterKernelWithNumeric, CompareScalarAndFilterRandomNumeric) {
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  using CType = typename TypeTraits<TypeParam>::CType;

  auto rand = random::RandomArrayGenerator(kRandomSeed);
  for (size_t i = 3; i < 10; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    // TODO(bkietz) rewrite with some nulls
    auto array =
        checked_pointer_cast<ArrayType>(rand.Numeric<TypeParam>(length, 0, 100, 0));
    CType c_fifty = 50;
    auto fifty = std::make_shared<ScalarType>(c_fifty);
    for (auto op : {EQUAL, NOT_EQUAL, GREATER, LESS_EQUAL}) {
      ASSERT_OK_AND_ASSIGN(Datum selection, arrow::compute::Compare(array, Datum(fifty),
                                                                    CompareOptions(op)));
      ASSERT_OK_AND_ASSIGN(Datum filtered, arrow::compute::Filter(array, selection, {}));
      auto filtered_array = filtered.make_array();
      ASSERT_OK(filtered_array->ValidateFull());
      auto expected =
          CompareAndFilter<TypeParam>(array->raw_values(), array->length(), c_fifty, op);
      ASSERT_ARRAYS_EQUAL(*filtered_array, *expected);
    }
  }
}

TYPED_TEST(TestFilterKernelWithNumeric, CompareArrayAndFilterRandomNumeric) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  auto rand = random::RandomArrayGenerator(kRandomSeed);
  for (size_t i = 3; i < 10; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    auto lhs = checked_pointer_cast<ArrayType>(
        rand.Numeric<TypeParam>(length, 0, 100, /*null_probability=*/0.0));
    auto rhs = checked_pointer_cast<ArrayType>(
        rand.Numeric<TypeParam>(length, 0, 100, /*null_probability=*/0.0));
    for (auto op : {EQUAL, NOT_EQUAL, GREATER, LESS_EQUAL}) {
      ASSERT_OK_AND_ASSIGN(Datum selection,
                           arrow::compute::Compare(lhs, rhs, CompareOptions(op)));
      ASSERT_OK_AND_ASSIGN(Datum filtered, arrow::compute::Filter(lhs, selection, {}));
      auto filtered_array = filtered.make_array();
      ASSERT_OK(filtered_array->ValidateFull());
      auto expected = CompareAndFilter<TypeParam>(lhs->raw_values(), lhs->length(),
                                                  rhs->raw_values(), op);
      ASSERT_ARRAYS_EQUAL(*filtered_array, *expected);
    }
  }
}

TYPED_TEST(TestFilterKernelWithNumeric, ScalarInRangeAndFilterRandomNumeric) {
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  using CType = typename TypeTraits<TypeParam>::CType;

  auto rand = random::RandomArrayGenerator(kRandomSeed);
  for (size_t i = 3; i < 10; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    auto array = checked_pointer_cast<ArrayType>(
        rand.Numeric<TypeParam>(length, 0, 100, /*null_probability=*/0.0));
    CType c_fifty = 50, c_hundred = 100;
    auto fifty = std::make_shared<ScalarType>(c_fifty);
    auto hundred = std::make_shared<ScalarType>(c_hundred);
    ASSERT_OK_AND_ASSIGN(
        Datum greater_than_fifty,
        arrow::compute::Compare(array, Datum(fifty), CompareOptions(GREATER)));
    ASSERT_OK_AND_ASSIGN(
        Datum less_than_hundred,
        arrow::compute::Compare(array, Datum(hundred), CompareOptions(LESS)));
    ASSERT_OK_AND_ASSIGN(Datum selection,
                         arrow::compute::And(greater_than_fifty, less_than_hundred));
    ASSERT_OK_AND_ASSIGN(Datum filtered, arrow::compute::Filter(array, selection, {}));
    auto filtered_array = filtered.make_array();
    ASSERT_OK(filtered_array->ValidateFull());
    auto expected = CompareAndFilter<TypeParam>(
        array->raw_values(), array->length(),
        [&](CType e) { return (e > c_fifty) && (e < c_hundred); });
    ASSERT_ARRAYS_EQUAL(*filtered_array, *expected);
  }
}

using StringTypes =
    ::testing::Types<BinaryType, StringType, LargeBinaryType, LargeStringType>;

template <typename TypeClass>
class TestFilterKernelWithString : public TestFilterKernel<TypeClass> {
 protected:
  std::shared_ptr<DataType> value_type() {
    return TypeTraits<TypeClass>::type_singleton();
  }

  void AssertFilter(const std::string& values, const std::string& filter,
                    const std::string& expected) {
    TestFilterKernel<TypeClass>::AssertFilter(ArrayFromJSON(value_type(), values),
                                              ArrayFromJSON(boolean(), filter),
                                              ArrayFromJSON(value_type(), expected));
  }

  void AssertFilterDictionary(const std::string& dictionary_values,
                              const std::string& dictionary_filter,
                              const std::string& filter,
                              const std::string& expected_filter) {
    auto dict = ArrayFromJSON(value_type(), dictionary_values);
    auto type = dictionary(int8(), value_type());
    ASSERT_OK_AND_ASSIGN(auto values,
                         DictionaryArray::FromArrays(
                             type, ArrayFromJSON(int8(), dictionary_filter), dict));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), expected_filter), dict));
    auto take_filter = ArrayFromJSON(boolean(), filter);
    TestFilterKernel<TypeClass>::AssertFilter(values, take_filter, expected);
  }
};

TYPED_TEST_SUITE(TestFilterKernelWithString, StringTypes);

TYPED_TEST(TestFilterKernelWithString, FilterString) {
  this->AssertFilter(R"(["a", "b", "c"])", "[0, 1, 0]", R"(["b"])");
  this->AssertFilter(R"([null, "b", "c"])", "[0, 1, 0]", R"(["b"])");
  this->AssertFilter(R"(["a", "b", "c"])", "[null, 1, 0]", R"([null, "b"])");
}

TYPED_TEST(TestFilterKernelWithString, FilterDictionary) {
  auto dict = R"(["a", "b", "c", "d", "e"])";
  this->AssertFilterDictionary(dict, "[3, 4, 2]", "[0, 1, 0]", "[4]");
  this->AssertFilterDictionary(dict, "[null, 4, 2]", "[0, 1, 0]", "[4]");
  this->AssertFilterDictionary(dict, "[3, 4, 2]", "[null, 1, 0]", "[null, 4]");
}

class TestFilterKernelWithList : public TestFilterKernel<ListType> {
 public:
};

TEST_F(TestFilterKernelWithList, FilterListInt32) {
  std::string list_json = "[[], [1,2], null, [3]]";
  this->AssertFilter(list(int32()), list_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(list(int32()), list_json, "[0, 1, 1, null]", "[[1,2], null, null]");
  this->AssertFilter(list(int32()), list_json, "[0, 0, 1, null]", "[null, null]");
  this->AssertFilter(list(int32()), list_json, "[1, 0, 0, 1]", "[[], [3]]");
  this->AssertFilter(list(int32()), list_json, "[1, 1, 1, 1]", list_json);
  this->AssertFilter(list(int32()), list_json, "[0, 1, 0, 1]", "[[1,2], [3]]");
}

TEST_F(TestFilterKernelWithList, FilterListListInt32) {
  std::string list_json = R"([
    [],
    [[1], [2, null, 2], []],
    null,
    [[3, null], null]
  ])";
  auto type = list(list(int32()));
  this->AssertFilter(type, list_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(type, list_json, "[0, 1, 1, null]", R"([
    [[1], [2, null, 2], []],
    null,
    null
  ])");
  this->AssertFilter(type, list_json, "[0, 0, 1, null]", "[null, null]");
  this->AssertFilter(type, list_json, "[1, 0, 0, 1]", R"([
    [],
    [[3, null], null]
  ])");
  this->AssertFilter(type, list_json, "[1, 1, 1, 1]", list_json);
  this->AssertFilter(type, list_json, "[0, 1, 0, 1]", R"([
    [[1], [2, null, 2], []],
    [[3, null], null]
  ])");
}

class TestFilterKernelWithLargeList : public TestFilterKernel<LargeListType> {};

TEST_F(TestFilterKernelWithLargeList, FilterListInt32) {
  std::string list_json = "[[], [1,2], null, [3]]";
  this->AssertFilter(large_list(int32()), list_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(large_list(int32()), list_json, "[0, 1, 1, null]",
                     "[[1,2], null, null]");
}

class TestFilterKernelWithFixedSizeList : public TestFilterKernel<FixedSizeListType> {};

TEST_F(TestFilterKernelWithFixedSizeList, FilterFixedSizeListInt32) {
  std::string list_json = "[null, [1, null, 3], [4, 5, 6], [7, 8, null]]";
  this->AssertFilter(fixed_size_list(int32(), 3), list_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(fixed_size_list(int32(), 3), list_json, "[0, 1, 1, null]",
                     "[[1, null, 3], [4, 5, 6], null]");
  this->AssertFilter(fixed_size_list(int32(), 3), list_json, "[0, 0, 1, null]",
                     "[[4, 5, 6], null]");
  this->AssertFilter(fixed_size_list(int32(), 3), list_json, "[1, 1, 1, 1]", list_json);
  this->AssertFilter(fixed_size_list(int32(), 3), list_json, "[0, 1, 0, 1]",
                     "[[1, null, 3], [7, 8, null]]");
}

class TestFilterKernelWithMap : public TestFilterKernel<MapType> {};

TEST_F(TestFilterKernelWithMap, FilterMapStringToInt32) {
  std::string map_json = R"([
    [["joe", 0], ["mark", null]],
    null,
    [["cap", 8]],
    []
  ])";
  this->AssertFilter(map(utf8(), int32()), map_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(map(utf8(), int32()), map_json, "[0, 1, 1, null]", R"([
    null,
    [["cap", 8]],
    null
  ])");
  this->AssertFilter(map(utf8(), int32()), map_json, "[1, 1, 1, 1]", map_json);
  this->AssertFilter(map(utf8(), int32()), map_json, "[0, 1, 0, 1]", "[null, []]");
}

class TestFilterKernelWithStruct : public TestFilterKernel<StructType> {};

TEST_F(TestFilterKernelWithStruct, FilterStruct) {
  auto struct_type = struct_({field("a", int32()), field("b", utf8())});
  auto struct_json = R"([
    null,
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])";
  this->AssertFilter(struct_type, struct_json, "[0, 0, 0, 0]", "[]");
  this->AssertFilter(struct_type, struct_json, "[0, 1, 1, null]", R"([
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    null
  ])");
  this->AssertFilter(struct_type, struct_json, "[1, 1, 1, 1]", struct_json);
  this->AssertFilter(struct_type, struct_json, "[1, 0, 1, 0]", R"([
    null,
    {"a": 2, "b": "hello"}
  ])");
}

class TestFilterKernelWithUnion : public TestFilterKernel<UnionType> {};

TEST_F(TestFilterKernelWithUnion, FilterUnion) {
  for (auto union_ : UnionTypeFactories()) {
    auto union_type = union_({field("a", int32()), field("b", utf8())}, {2, 5});
    auto union_json = R"([
      null,
      [2, 222],
      [5, "hello"],
      [5, "eh"],
      null,
      [2, 111]
    ])";
    this->AssertFilter(union_type, union_json, "[0, 0, 0, 0, 0, 0]", "[]");
    this->AssertFilter(union_type, union_json, "[0, 1, 1, null, 0, 1]", R"([
      [2, 222],
      [5, "hello"],
      null,
      [2, 111]
    ])");
    this->AssertFilter(union_type, union_json, "[1, 0, 1, 0, 1, 0]", R"([
      null,
      [5, "hello"],
      null
    ])");
    this->AssertFilter(union_type, union_json, "[1, 1, 1, 1, 1, 1]", union_json);
  }
}

class TestFilterKernelWithRecordBatch : public TestFilterKernel<RecordBatch> {
 public:
  void AssertFilter(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
                    const std::string& selection, FilterOptions options,
                    const std::string& expected_batch) {
    std::shared_ptr<RecordBatch> actual;

    ASSERT_OK(this->Filter(schm, batch_json, selection, options, &actual));
    ASSERT_OK(actual->ValidateFull());
    ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(schm, expected_batch), *actual);
  }

  Status Filter(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
                const std::string& selection, FilterOptions options,
                std::shared_ptr<RecordBatch>* out) {
    auto batch = RecordBatchFromJSON(schm, batch_json);
    ARROW_ASSIGN_OR_RAISE(
        Datum out_datum,
        arrow::compute::Filter(batch, ArrayFromJSON(boolean(), selection), options));
    *out = out_datum.record_batch();
    return Status::OK();
  }
};

TEST_F(TestFilterKernelWithRecordBatch, FilterRecordBatch) {
  std::vector<std::shared_ptr<Field>> fields = {field("a", int32()), field("b", utf8())};
  auto schm = schema(fields);

  auto batch_json = R"([
    {"a": null, "b": "yo"},
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])";
  for (auto options : {this->emit_null_, this->drop_}) {
    this->AssertFilter(schm, batch_json, "[0, 0, 0, 0]", options, "[]");
    this->AssertFilter(schm, batch_json, "[1, 1, 1, 1]", options, batch_json);
    this->AssertFilter(schm, batch_json, "[1, 0, 1, 0]", options, R"([
      {"a": null, "b": "yo"},
      {"a": 2, "b": "hello"}
    ])");
  }

  this->AssertFilter(schm, batch_json, "[0, 1, 1, null]", this->drop_, R"([
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"}
  ])");

  this->AssertFilter(schm, batch_json, "[0, 1, 1, null]", this->emit_null_, R"([
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": null, "b": null}
  ])");
}

class TestFilterKernelWithChunkedArray : public TestFilterKernel<ChunkedArray> {
 public:
  void AssertFilter(const std::shared_ptr<DataType>& type,
                    const std::vector<std::string>& values, const std::string& filter,
                    const std::vector<std::string>& expected) {
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK(this->FilterWithArray(type, values, filter, &actual));
    ASSERT_OK(actual->ValidateFull());
    AssertChunkedEqual(*ChunkedArrayFromJSON(type, expected), *actual);
  }

  void AssertChunkedFilter(const std::shared_ptr<DataType>& type,
                           const std::vector<std::string>& values,
                           const std::vector<std::string>& filter,
                           const std::vector<std::string>& expected) {
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK(this->FilterWithChunkedArray(type, values, filter, &actual));
    ASSERT_OK(actual->ValidateFull());
    AssertChunkedEqual(*ChunkedArrayFromJSON(type, expected), *actual);
  }

  Status FilterWithArray(const std::shared_ptr<DataType>& type,
                         const std::vector<std::string>& values,
                         const std::string& filter, std::shared_ptr<ChunkedArray>* out) {
    ARROW_ASSIGN_OR_RAISE(Datum out_datum,
                          arrow::compute::Filter(ChunkedArrayFromJSON(type, values),
                                                 ArrayFromJSON(boolean(), filter), {}));
    *out = out_datum.chunked_array();
    return Status::OK();
  }

  Status FilterWithChunkedArray(const std::shared_ptr<DataType>& type,
                                const std::vector<std::string>& values,
                                const std::vector<std::string>& filter,
                                std::shared_ptr<ChunkedArray>* out) {
    ARROW_ASSIGN_OR_RAISE(
        Datum out_datum,
        arrow::compute::Filter(ChunkedArrayFromJSON(type, values),
                               ChunkedArrayFromJSON(boolean(), filter), {}));
    *out = out_datum.chunked_array();
    return Status::OK();
  }
};

TEST_F(TestFilterKernelWithChunkedArray, FilterChunkedArray) {
  this->AssertFilter(int8(), {"[]"}, "[]", {});
  this->AssertChunkedFilter(int8(), {"[]"}, {"[]"}, {});

  this->AssertFilter(int8(), {"[7]", "[8, 9]"}, "[0, 1, 0]", {"[8]"});
  this->AssertChunkedFilter(int8(), {"[7]", "[8, 9]"}, {"[0]", "[1, 0]"}, {"[8]"});
  this->AssertChunkedFilter(int8(), {"[7]", "[8, 9]"}, {"[0, 1]", "[0]"}, {"[8]"});

  std::shared_ptr<ChunkedArray> arr;
  ASSERT_RAISES(
      Invalid, this->FilterWithArray(int8(), {"[7]", "[8, 9]"}, "[0, 1, 0, 1, 1]", &arr));
  ASSERT_RAISES(Invalid, this->FilterWithChunkedArray(int8(), {"[7]", "[8, 9]"},
                                                      {"[0, 1, 0]", "[1, 1]"}, &arr));
}

class TestFilterKernelWithTable : public TestFilterKernel<Table> {
 public:
  void AssertFilter(const std::shared_ptr<Schema>& schm,
                    const std::vector<std::string>& table_json, const std::string& filter,
                    FilterOptions options,
                    const std::vector<std::string>& expected_table) {
    std::shared_ptr<Table> actual;

    ASSERT_OK(this->FilterWithArray(schm, table_json, filter, options, &actual));
    ASSERT_OK(actual->ValidateFull());
    ASSERT_TABLES_EQUAL(*TableFromJSON(schm, expected_table), *actual);
  }

  void AssertChunkedFilter(const std::shared_ptr<Schema>& schm,
                           const std::vector<std::string>& table_json,
                           const std::vector<std::string>& filter, FilterOptions options,
                           const std::vector<std::string>& expected_table) {
    std::shared_ptr<Table> actual;

    ASSERT_OK(this->FilterWithChunkedArray(schm, table_json, filter, options, &actual));
    ASSERT_OK(actual->ValidateFull());
    AssertTablesEqual(*TableFromJSON(schm, expected_table), *actual,
                      /*same_chunk_layout=*/false);
  }

  Status FilterWithArray(const std::shared_ptr<Schema>& schm,
                         const std::vector<std::string>& values,
                         const std::string& filter, FilterOptions options,
                         std::shared_ptr<Table>* out) {
    ARROW_ASSIGN_OR_RAISE(
        Datum out_datum,
        arrow::compute::Filter(TableFromJSON(schm, values),
                               ArrayFromJSON(boolean(), filter), options));
    *out = out_datum.table();
    return Status::OK();
  }

  Status FilterWithChunkedArray(const std::shared_ptr<Schema>& schm,
                                const std::vector<std::string>& values,
                                const std::vector<std::string>& filter,
                                FilterOptions options, std::shared_ptr<Table>* out) {
    ARROW_ASSIGN_OR_RAISE(
        Datum out_datum,
        arrow::compute::Filter(TableFromJSON(schm, values),
                               ChunkedArrayFromJSON(boolean(), filter), options));
    *out = out_datum.table();
    return Status::OK();
  }
};

TEST_F(TestFilterKernelWithTable, FilterTable) {
  std::vector<std::shared_ptr<Field>> fields = {field("a", int32()), field("b", utf8())};
  auto schm = schema(fields);

  std::vector<std::string> table_json = {R"([
      {"a": null, "b": "yo"},
      {"a": 1, "b": ""}
    ])",
                                         R"([
      {"a": 2, "b": "hello"},
      {"a": 4, "b": "eh"}
    ])"};
  for (auto options : {this->emit_null_, this->drop_}) {
    this->AssertFilter(schm, table_json, "[0, 0, 0, 0]", options, {});
    this->AssertChunkedFilter(schm, table_json, {"[0]", "[0, 0, 0]"}, options, {});
    this->AssertFilter(schm, table_json, "[1, 1, 1, 1]", options, table_json);
    this->AssertChunkedFilter(schm, table_json, {"[1]", "[1, 1, 1]"}, options,
                              table_json);
  }

  std::vector<std::string> expected_emit_null = {R"([
    {"a": 1, "b": ""}
  ])",
                                                 R"([
    {"a": 2, "b": "hello"},
    {"a": null, "b": null}
  ])"};
  this->AssertFilter(schm, table_json, "[0, 1, 1, null]", this->emit_null_,
                     expected_emit_null);
  this->AssertChunkedFilter(schm, table_json, {"[0, 1, 1]", "[null]"}, this->emit_null_,
                            expected_emit_null);

  std::vector<std::string> expected_drop = {R"([{"a": 1, "b": ""}])",
                                            R"([{"a": 2, "b": "hello"}])"};
  this->AssertFilter(schm, table_json, "[0, 1, 1, null]", this->drop_, expected_drop);
  this->AssertChunkedFilter(schm, table_json, {"[0, 1, 1]", "[null]"}, this->drop_,
                            expected_drop);
}

}  // namespace compute
}  // namespace arrow
