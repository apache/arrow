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
#include "arrow/compute/kernels/take.h"
#include "arrow/compute/test-util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace compute {

using util::string_view;

template <typename ArrowType>
class TestTakeKernel : public ComputeFixture, public TestBase {
 protected:
  void AssertTakeArrays(const std::shared_ptr<Array>& values,
                        const std::shared_ptr<Array>& indices, TakeOptions options,
                        const std::shared_ptr<Array>& expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(arrow::compute::Take(&this->ctx_, *values, *indices, options, &actual));
    AssertArraysEqual(*expected, *actual);
  }
  void AssertTake(const std::shared_ptr<DataType>& type, const std::string& values,
                  const std::string& indices, TakeOptions options,
                  const std::string& expected) {
    std::shared_ptr<Array> actual;

    for (auto index_type : {int8(), uint32()}) {
      ASSERT_OK(this->Take(type, values, index_type, indices, options, &actual));
      AssertArraysEqual(*ArrayFromJSON(type, expected), *actual);
    }
  }
  Status Take(const std::shared_ptr<DataType>& type, const std::string& values,
              const std::shared_ptr<DataType>& index_type, const std::string& indices,
              TakeOptions options, std::shared_ptr<Array>* out) {
    return arrow::compute::Take(&this->ctx_, *ArrayFromJSON(type, values),
                                *ArrayFromJSON(index_type, indices), options, out);
  }
};

class TestTakeKernelWithNull : public TestTakeKernel<NullType> {
 protected:
  void AssertTake(const std::string& values, const std::string& indices,
                  TakeOptions options, const std::string& expected) {
    TestTakeKernel<NullType>::AssertTake(utf8(), values, indices, options, expected);
  }
};

TEST_F(TestTakeKernelWithNull, TakeNull) {
  TakeOptions options;
  this->AssertTake("[null, null, null]", "[0, 1, 0]", options, "[null, null, null]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError, this->Take(null(), "[null, null, null]", int8(), "[0, 9, 0]",
                                       options, &arr));
}

TEST_F(TestTakeKernelWithNull, InvalidIndexType) {
  TakeOptions options;
  std::shared_ptr<Array> arr;
  ASSERT_RAISES(TypeError, this->Take(null(), "[null, null, null]", float32(),
                                      "[0.0, 1.0, 0.1]", options, &arr));
}

class TestTakeKernelWithBoolean : public TestTakeKernel<BooleanType> {
 protected:
  void AssertTake(const std::string& values, const std::string& indices,
                  TakeOptions options, const std::string& expected) {
    TestTakeKernel<BooleanType>::AssertTake(boolean(), values, indices, options,
                                            expected);
  }
};

TEST_F(TestTakeKernelWithBoolean, TakeBoolean) {
  TakeOptions options;
  this->AssertTake("[true, false, true]", "[0, 1, 0]", options, "[true, false, true]");
  this->AssertTake("[null, false, true]", "[0, 1, 0]", options, "[null, false, null]");
  this->AssertTake("[true, false, true]", "[null, 1, 0]", options, "[null, false, true]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError, this->Take(boolean(), "[true, false, true]", int8(),
                                       "[0, 9, 0]", options, &arr));
}

template <typename ArrowType>
class TestTakeKernelWithNumeric : public TestTakeKernel<ArrowType> {
 protected:
  void AssertTake(const std::string& values, const std::string& indices,
                  TakeOptions options, const std::string& expected) {
    TestTakeKernel<ArrowType>::AssertTake(type_singleton(), values, indices, options,
                                          expected);
  }
  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_CASE(TestTakeKernelWithNumeric, NumericArrowTypes);
TYPED_TEST(TestTakeKernelWithNumeric, TakeNumeric) {
  TakeOptions options;
  this->AssertTake("[7, 8, 9]", "[0, 1, 0]", options, "[7, 8, 7]");
  this->AssertTake("[null, 8, 9]", "[0, 1, 0]", options, "[null, 8, null]");
  this->AssertTake("[7, 8, 9]", "[null, 1, 0]", options, "[null, 8, 7]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError, this->Take(this->type_singleton(), "[7, 8, 9]", int8(),
                                       "[0, 9, 0]", options, &arr));
}

class TestTakeKernelWithString : public TestTakeKernel<StringType> {
 protected:
  void AssertTake(const std::string& values, const std::string& indices,
                  TakeOptions options, const std::string& expected) {
    TestTakeKernel<StringType>::AssertTake(utf8(), values, indices, options, expected);
  }
  void AssertTakeDictionary(const std::string& dictionary_values,
                            const std::string& dictionary_indices,
                            const std::string& indices, TakeOptions options,
                            const std::string& expected_indices) {
    auto type = dictionary(int8(), ArrayFromJSON(utf8(), dictionary_values));
    std::shared_ptr<Array> values, actual, expected;
    ASSERT_OK(DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), dictionary_indices),
                                          &values));
    ASSERT_OK(DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), expected_indices),
                                          &expected));
    auto take_indices = ArrayFromJSON(int8(), indices);
    this->AssertTakeArrays(values, take_indices, options, expected);
  }
};

TEST_F(TestTakeKernelWithString, TakeString) {
  TakeOptions options;
  this->AssertTake(R"(["a", "b", "c"])", "[0, 1, 0]", options, R"(["a", "b", "a"])");
  this->AssertTake(R"([null, "b", "c"])", "[0, 1, 0]", options, "[null, \"b\", null]");
  this->AssertTake(R"(["a", "b", "c"])", "[null, 1, 0]", options, R"([null, "b", "a"])");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError, this->Take(utf8(), R"(["a", "b", "c"])", int8(), "[0, 9, 0]",
                                       options, &arr));
}

TEST_F(TestTakeKernelWithString, TakeDictionary) {
  TakeOptions options;
  auto dict = R"(["a", "b", "c", "d", "e"])";
  this->AssertTakeDictionary(dict, "[3, 4, 2]", "[0, 1, 0]", options, "[3, 4, 3]");
  this->AssertTakeDictionary(dict, "[null, 4, 2]", "[0, 1, 0]", options,
                             "[null, 4, null]");
  this->AssertTakeDictionary(dict, "[3, 4, 2]", "[null, 1, 0]", options, "[null, 4, 3]");
}

}  // namespace compute
}  // namespace arrow
