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
#include <vector>

#include "arrow/compute/context.h"
#include "arrow/compute/kernels/boolean.h"
#include "arrow/compute/kernels/compare.h"
#include "arrow/compute/kernels/filter.h"
#include "arrow/compute/test-util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace compute {

using internal::checked_pointer_cast;
using util::string_view;

constexpr auto kSeed = 0x0ff1ce;

template <typename ArrowType>
class TestFilterKernel : public ComputeFixture, public TestBase {
 protected:
  void AssertFilterArrays(const std::shared_ptr<Array>& values,
                          const std::shared_ptr<Array>& filter,
                          const std::shared_ptr<Array>& expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(arrow::compute::Filter(&this->ctx_, *values, *filter, &actual));
    ASSERT_OK(ValidateArray(*actual));
    AssertArraysEqual(*expected, *actual);
  }

  void AssertFilter(const std::shared_ptr<DataType>& type, const std::string& values,
                    const std::string& filter, const std::string& expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(this->Filter(type, values, filter, &actual));
    ASSERT_OK(ValidateArray(*actual));
    AssertArraysEqual(*ArrayFromJSON(type, expected), *actual);
  }

  Status Filter(const std::shared_ptr<DataType>& type, const std::string& values,
                const std::string& filter, std::shared_ptr<Array>* out) {
    return arrow::compute::Filter(&this->ctx_, *ArrayFromJSON(type, values),
                                  *ArrayFromJSON(boolean(), filter), out);
  }

  void ValidateFilter(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<Array>& filter_boxed) {
    std::shared_ptr<Array> filtered;
    ASSERT_OK(arrow::compute::Filter(&this->ctx_, *values, *filter_boxed, &filtered));
    ASSERT_OK(ValidateArray(*filtered));

    auto filter = checked_pointer_cast<BooleanArray>(filter_boxed);
    int64_t values_i = 0, filtered_i = 0;
    for (; values_i < values->length(); ++values_i, ++filtered_i) {
      if (filter->IsNull(values_i)) {
        ASSERT_LT(filtered_i, filtered->length());
        ASSERT_TRUE(filtered->IsNull(filtered_i));
        continue;
      }
      if (!filter->Value(values_i)) {
        // this element was filtered out; don't examine filtered
        --filtered_i;
        continue;
      }
      ASSERT_LT(filtered_i, filtered->length());
      ASSERT_TRUE(values->RangeEquals(values_i, values_i + 1, filtered_i, filtered));
    }
    ASSERT_EQ(filtered_i, filtered->length());
  }
};

class TestFilterKernelWithNull : public TestFilterKernel<NullType> {
 protected:
  void AssertFilter(const std::string& values, const std::string& filter,
                    const std::string& expected) {
    TestFilterKernel<NullType>::AssertFilter(null(), values, filter, expected);
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
    TestFilterKernel<BooleanType>::AssertFilter(boolean(), values, filter, expected);
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
  void AssertFilter(const std::string& values, const std::string& filter,
                    const std::string& expected) {
    TestFilterKernel<ArrowType>::AssertFilter(type_singleton(), values, filter, expected);
  }

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_CASE(TestFilterKernelWithNumeric, NumericArrowTypes);
TYPED_TEST(TestFilterKernelWithNumeric, FilterNumeric) {
  this->AssertFilter("[]", "[]", "[]");

  this->AssertFilter("[9]", "[0]", "[]");
  this->AssertFilter("[9]", "[1]", "[9]");
  this->AssertFilter("[9]", "[null]", "[null]");
  this->AssertFilter("[null]", "[0]", "[]");
  this->AssertFilter("[null]", "[1]", "[null]");
  this->AssertFilter("[null]", "[null]", "[null]");

  this->AssertFilter("[7, 8, 9]", "[0, 1, 0]", "[8]");
  this->AssertFilter("[7, 8, 9]", "[1, 0, 1]", "[7, 9]");
  this->AssertFilter("[null, 8, 9]", "[0, 1, 0]", "[8]");
  this->AssertFilter("[7, 8, 9]", "[null, 1, 0]", "[null, 8]");
  this->AssertFilter("[7, 8, 9]", "[1, null, 1]", "[7, null, 9]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(Invalid, this->Filter(this->type_singleton(), "[7, 8, 9]", "[]", &arr));
}

TYPED_TEST(TestFilterKernelWithNumeric, FilterRandomNumeric) {
  auto rand = random::RandomArrayGenerator(kSeed);
  for (size_t i = 3; i < 13; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    for (auto null_probability : {0.0, 0.01, 0.25, 1.0}) {
      for (auto filter_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
        auto values = rand.Numeric<TypeParam>(length, 0, 127, null_probability);
        auto filter = rand.Boolean(length, filter_probability, null_probability);
        this->ValidateFilter(values, filter);
      }
    }
  }
}

template <typename CType>
decltype(Comparator<CType, EQUAL>::Compare)* GetComparator(CompareOperator op) {
  using cmp_t = decltype(Comparator<CType, EQUAL>::Compare);
  static cmp_t* cmp[] = {
      Comparator<CType, EQUAL>::Compare,   Comparator<CType, NOT_EQUAL>::Compare,
      Comparator<CType, GREATER>::Compare, Comparator<CType, GREATER_EQUAL>::Compare,
      Comparator<CType, LESS>::Compare,    Comparator<CType, LESS_EQUAL>::Compare,
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

  auto rand = random::RandomArrayGenerator(kSeed);
  for (size_t i = 3; i < 13; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    // TODO(bkietz) rewrite with some nulls
    auto array =
        checked_pointer_cast<ArrayType>(rand.Numeric<TypeParam>(length, 0, 100, 0));
    CType c_fifty = 50;
    auto fifty = std::make_shared<ScalarType>(c_fifty);
    for (auto op : {EQUAL, NOT_EQUAL, GREATER, LESS_EQUAL}) {
      auto options = CompareOptions(op);
      Datum selection, filtered;
      ASSERT_OK(arrow::compute::Compare(&this->ctx_, Datum(array), Datum(fifty), options,
                                        &selection));
      ASSERT_OK(arrow::compute::Filter(&this->ctx_, Datum(array), selection, &filtered));
      auto filtered_array = filtered.make_array();
      ASSERT_OK(ValidateArray(*filtered_array));
      auto expected =
          CompareAndFilter<TypeParam>(array->raw_values(), array->length(), c_fifty, op);
      ASSERT_ARRAYS_EQUAL(*filtered_array, *expected);
    }
  }
}

TYPED_TEST(TestFilterKernelWithNumeric, CompareArrayAndFilterRandomNumeric) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  auto rand = random::RandomArrayGenerator(kSeed);
  for (size_t i = 3; i < 13; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    auto lhs =
        checked_pointer_cast<ArrayType>(rand.Numeric<TypeParam>(length, 0, 100, 0));
    auto rhs =
        checked_pointer_cast<ArrayType>(rand.Numeric<TypeParam>(length, 0, 100, 0));
    for (auto op : {EQUAL, NOT_EQUAL, GREATER, LESS_EQUAL}) {
      auto options = CompareOptions(op);
      Datum selection, filtered;
      ASSERT_OK(arrow::compute::Compare(&this->ctx_, Datum(lhs), Datum(rhs), options,
                                        &selection));
      ASSERT_OK(arrow::compute::Filter(&this->ctx_, Datum(lhs), selection, &filtered));
      auto filtered_array = filtered.make_array();
      ASSERT_OK(ValidateArray(*filtered_array));
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

  auto rand = random::RandomArrayGenerator(kSeed);
  for (size_t i = 3; i < 13; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    auto array =
        checked_pointer_cast<ArrayType>(rand.Numeric<TypeParam>(length, 0, 100, 0));
    CType c_fifty = 50, c_hundred = 100;
    auto fifty = std::make_shared<ScalarType>(c_fifty);
    auto hundred = std::make_shared<ScalarType>(c_hundred);
    Datum greater_than_fifty, less_than_hundred, selection, filtered;
    ASSERT_OK(arrow::compute::Compare(&this->ctx_, Datum(array), Datum(fifty),
                                      CompareOptions(GREATER), &greater_than_fifty));
    ASSERT_OK(arrow::compute::Compare(&this->ctx_, Datum(array), Datum(hundred),
                                      CompareOptions(LESS), &less_than_hundred));
    ASSERT_OK(arrow::compute::And(&this->ctx_, greater_than_fifty, less_than_hundred,
                                  &selection));
    ASSERT_OK(arrow::compute::Filter(&this->ctx_, Datum(array), selection, &filtered));
    auto filtered_array = filtered.make_array();
    ASSERT_OK(ValidateArray(*filtered_array));
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
    TestFilterKernel<TypeClass>::AssertFilter(value_type(), values, filter, expected);
  }
  void AssertFilterDictionary(const std::string& dictionary_values,
                              const std::string& dictionary_filter,
                              const std::string& filter,
                              const std::string& expected_filter) {
    auto dict = ArrayFromJSON(value_type(), dictionary_values);
    auto type = dictionary(int8(), value_type());
    std::shared_ptr<Array> values, actual, expected;
    ASSERT_OK(DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), dictionary_filter),
                                          dict, &values));
    ASSERT_OK(DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), expected_filter),
                                          dict, &expected));
    auto take_filter = ArrayFromJSON(boolean(), filter);
    this->AssertFilterArrays(values, take_filter, expected);
  }
};

TYPED_TEST_CASE(TestFilterKernelWithString, StringTypes);

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

class TestFilterKernelWithList : public TestFilterKernel<ListType> {};

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
  for (auto mode : {UnionMode::SPARSE, UnionMode::DENSE}) {
    auto union_type = union_({field("a", int32()), field("b", utf8())}, {2, 5}, mode);
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

}  // namespace compute
}  // namespace arrow
