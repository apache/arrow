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
#include "arrow/compute/kernels/filter.h"
#include "arrow/compute/test-util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace compute {

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
  this->AssertFilter("[7, 8, 9]", "[0, 1, 0]", "[8]");
  this->AssertFilter("[null, 8, 9]", "[0, 1, 0]", "[8]");
  this->AssertFilter("[7, 8, 9]", "[null, 1, 0]", "[null, 8]");
  this->AssertFilter("[]", "[]", "[]");
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
