// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// returnGegarding copyright ownership.  The ASF licenses this file
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

template <typename ArrowType>
class TestFilterKernel : public ComputeFixture, public TestBase {
 protected:
  void AssertFilterArrays(const std::shared_ptr<Array>& values,
                          const std::shared_ptr<Array>& filter,
                          const std::shared_ptr<Array>& expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(arrow::compute::Filter(&this->ctx_, *values, *filter, &actual));
    AssertArraysEqual(*expected, *actual);
  }
  void AssertFilter(const std::shared_ptr<DataType>& type, const std::string& values,
                    const std::string& filter, const std::string& expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(this->Filter(type, values, filter, &actual));
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
    TestFilterKernel<NullType>::AssertFilter(utf8(), values, filter, expected);
  }
};

TEST_F(TestFilterKernelWithNull, FilterNull) {
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
}

TYPED_TEST(TestFilterKernelWithNumeric, FilterRandomNumeric) {
  auto rand = random::RandomArrayGenerator(0x5416447);
  for (size_t i = 3; i < 13; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
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

template <typename CType>
std::vector<CType> CompareAndFilter(const CType* data, int64_t length, CType val,
                                    CompareOperator op) {
  std::vector<CType> filtered;
  filtered.reserve(length);
  auto cmp = GetComparator<CType>(op);
  std::copy_if(data, data + length, std::back_inserter(filtered),
               [cmp, val](CType e) { return cmp(e, val); });
  return filtered;
}

template <typename CType>
std::vector<CType> CompareAndFilter(const CType* data, int64_t length, const CType* other,
                                    CompareOperator op) {
  std::vector<CType> filtered;
  filtered.reserve(length);
  auto cmp = GetComparator<CType>(op);
  int64_t i = 0;
  std::copy_if(data, data + length, std::back_inserter(filtered),
               [cmp, other, &i](CType e) { return cmp(e, other[i++]); });
  return filtered;
}

TYPED_TEST(TestFilterKernelWithNumeric, CompareScalarAndFilterRandomNumeric) {
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  using CType = typename TypeTraits<TypeParam>::CType;

  auto rand = random::RandomArrayGenerator(0x5416447);
  for (size_t i = 3; i < 13; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
      auto array = checked_pointer_cast<ArrayType>(
          rand.Numeric<TypeParam>(length, 0, 100, null_probability));
      CType c_fifty = 50;
      auto fifty = std::make_shared<ScalarType>(c_fifty);
      for (auto op : {EQUAL, NOT_EQUAL, GREATER, LESS_EQUAL}) {
        auto options = CompareOptions(op);
        Datum selection, filtered;
        ASSERT_OK(arrow::compute::Compare(&this->ctx_, Datum(array), Datum(fifty),
                                          options, &selection));
        ASSERT_OK(
            arrow::compute::Filter(&this->ctx_, Datum(array), selection, &filtered));
        auto expected =
            CompareAndFilter(array->raw_values(), array->length(), c_fifty, op);
      }
    }
  }
}

TYPED_TEST(TestFilterKernelWithNumeric, CompareArrayAndFilterRandomNumeric) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  auto rand = random::RandomArrayGenerator(0x5416447);
  for (size_t i = 3; i < 13; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
      auto lhs = checked_pointer_cast<ArrayType>(
          rand.Numeric<TypeParam>(length, 0, 100, null_probability));
      auto rhs = checked_pointer_cast<ArrayType>(
          rand.Numeric<TypeParam>(length, 0, 100, null_probability));
      for (auto op : {EQUAL, NOT_EQUAL, GREATER, LESS_EQUAL}) {
        auto options = CompareOptions(op);
        Datum selection, filtered;
        ASSERT_OK(arrow::compute::Compare(&this->ctx_, Datum(lhs), Datum(rhs), options,
                                          &selection));
        ASSERT_OK(arrow::compute::Filter(&this->ctx_, Datum(lhs), selection, &filtered));
        auto expected =
            CompareAndFilter(lhs->raw_values(), lhs->length(), rhs->raw_values(), op);
      }
    }
  }
}

class TestFilterKernelWithString : public TestFilterKernel<StringType> {
 protected:
  void AssertFilter(const std::string& values, const std::string& filter,
                    const std::string& expected) {
    TestFilterKernel<StringType>::AssertFilter(utf8(), values, filter, expected);
  }
  void AssertFilterDictionary(const std::string& dictionary_values,
                              const std::string& dictionary_filter,
                              const std::string& filter,
                              const std::string& expected_filter) {
    auto dict = ArrayFromJSON(utf8(), dictionary_values);
    auto type = dictionary(int8(), utf8());
    std::shared_ptr<Array> values, actual, expected;
    ASSERT_OK(DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), dictionary_filter),
                                          dict, &values));
    ASSERT_OK(DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), expected_filter),
                                          dict, &expected));
    auto take_filter = ArrayFromJSON(boolean(), filter);
    this->AssertFilterArrays(values, take_filter, expected);
  }
};

TEST_F(TestFilterKernelWithString, FilterString) {
  this->AssertFilter(R"(["a", "b", "c"])", "[0, 1, 0]", R"(["b"])");
  this->AssertFilter(R"([null, "b", "c"])", "[0, 1, 0]", R"(["b"])");
  this->AssertFilter(R"(["a", "b", "c"])", "[null, 1, 0]", R"([null, "b"])");
}

TEST_F(TestFilterKernelWithString, FilterDictionary) {
  auto dict = R"(["a", "b", "c", "d", "e"])";
  this->AssertFilterDictionary(dict, "[3, 4, 2]", "[0, 1, 0]", "[4]");
  this->AssertFilterDictionary(dict, "[null, 4, 2]", "[0, 1, 0]", "[4]");
  this->AssertFilterDictionary(dict, "[3, 4, 2]", "[null, 1, 0]", "[null, 4]");
}

}  // namespace compute
}  // namespace arrow
