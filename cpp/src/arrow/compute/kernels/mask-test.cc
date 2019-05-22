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
#include "arrow/compute/kernels/mask.h"
#include "arrow/compute/test-util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace compute {

using util::string_view;

template <typename ArrowType>
class TestMaskKernel : public ComputeFixture, public TestBase {
 protected:
  void AssertMaskArrays(const std::shared_ptr<Array>& values,
                        const std::shared_ptr<Array>& mask,
                        const std::shared_ptr<Array>& expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(arrow::compute::Mask(&this->ctx_, *values, *mask, &actual));
    AssertArraysEqual(*expected, *actual);
  }
  void AssertMask(const std::shared_ptr<DataType>& type, const std::string& values,
                  const std::string& mask, const std::string& expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(this->Mask(type, values, mask, &actual));
    AssertArraysEqual(*ArrayFromJSON(type, expected), *actual);
  }
  Status Mask(const std::shared_ptr<DataType>& type, const std::string& values,
              const std::string& mask, std::shared_ptr<Array>* out) {
    return arrow::compute::Mask(&this->ctx_, *ArrayFromJSON(type, values),
                                *ArrayFromJSON(boolean(), mask), out);
  }
};

class TestMaskKernelWithNull : public TestMaskKernel<NullType> {
 protected:
  void AssertMask(const std::string& values, const std::string& mask,
                  const std::string& expected) {
    TestMaskKernel<NullType>::AssertMask(utf8(), values, mask, expected);
  }
};

TEST_F(TestMaskKernelWithNull, MaskNull) {
  this->AssertMask("[null, null, null]", "[0, 1, 0]", "[null]");
  this->AssertMask("[null, null, null]", "[1, 1, 0]", "[null, null]");
}

class TestMaskKernelWithBoolean : public TestMaskKernel<BooleanType> {
 protected:
  void AssertMask(const std::string& values, const std::string& mask,
                  const std::string& expected) {
    TestMaskKernel<BooleanType>::AssertMask(boolean(), values, mask, expected);
  }
};

TEST_F(TestMaskKernelWithBoolean, MaskBoolean) {
  this->AssertMask("[true, false, true]", "[0, 1, 0]", "[false]");
  this->AssertMask("[null, false, true]", "[0, 1, 0]", "[false]");
  this->AssertMask("[true, false, true]", "[null, 1, 0]", "[null, false]");
}

template <typename ArrowType>
class TestMaskKernelWithNumeric : public TestMaskKernel<ArrowType> {
 protected:
  void AssertMask(const std::string& values, const std::string& mask,
                  const std::string& expected) {
    TestMaskKernel<ArrowType>::AssertMask(type_singleton(), values, mask, expected);
  }
  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_CASE(TestMaskKernelWithNumeric, NumericArrowTypes);
TYPED_TEST(TestMaskKernelWithNumeric, MaskNumeric) {
  this->AssertMask("[7, 8, 9]", "[0, 1, 0]", "[8]");
  this->AssertMask("[null, 8, 9]", "[0, 1, 0]", "[8]");
  this->AssertMask("[7, 8, 9]", "[null, 1, 0]", "[null, 8]");
  this->AssertMask("[]", "[]", "[]");
}

class TestMaskKernelWithString : public TestMaskKernel<StringType> {
 protected:
  void AssertMask(const std::string& values, const std::string& mask,
                  const std::string& expected) {
    TestMaskKernel<StringType>::AssertMask(utf8(), values, mask, expected);
  }
  void AssertMaskDictionary(const std::string& dictionary_values,
                            const std::string& dictionary_mask, const std::string& mask,
                            const std::string& expected_mask) {
    auto dict = ArrayFromJSON(utf8(), dictionary_values);
    auto type = dictionary(int8(), utf8());
    std::shared_ptr<Array> values, actual, expected;
    ASSERT_OK(DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), dictionary_mask),
                                          dict, &values));
    ASSERT_OK(DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), expected_mask),
                                          dict, &expected));
    auto take_mask = ArrayFromJSON(boolean(), mask);
    this->AssertMaskArrays(values, take_mask, expected);
  }
};

TEST_F(TestMaskKernelWithString, MaskString) {
  this->AssertMask(R"(["a", "b", "c"])", "[0, 1, 0]", R"(["b"])");
  this->AssertMask(R"([null, "b", "c"])", "[0, 1, 0]", R"(["b"])");
  this->AssertMask(R"(["a", "b", "c"])", "[null, 1, 0]", R"([null, "b"])");
}

TEST_F(TestMaskKernelWithString, MaskDictionary) {
  auto dict = R"(["a", "b", "c", "d", "e"])";
  this->AssertMaskDictionary(dict, "[3, 4, 2]", "[0, 1, 0]", "[4]");
  this->AssertMaskDictionary(dict, "[null, 4, 2]", "[0, 1, 0]", "[4]");
  this->AssertMaskDictionary(dict, "[3, 4, 2]", "[null, 1, 0]", "[null, 4]");
}

}  // namespace compute
}  // namespace arrow
