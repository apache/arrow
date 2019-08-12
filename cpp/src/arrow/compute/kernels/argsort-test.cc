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
#include "arrow/compute/kernels/argsort.h"
#include "arrow/compute/test-util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace compute {

template <typename ArrowType>
class TestArgsortKernel : public ComputeFixture, public TestBase {
 private:
  void AssertArgsortArrays(const std::shared_ptr<Array> values,
                           const std::shared_ptr<Array> expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(arrow::compute::Argsort(&this->ctx_, *values, &actual));
    ASSERT_OK(ValidateArray(*actual));
    AssertArraysEqual(*expected, *actual);
  }

 protected:
  virtual void AssertArgsort(const std::string& values, const std::string& expected) {
    auto type = TypeTraits<ArrowType>::type_singleton();
    AssertArgsortArrays(ArrayFromJSON(type, values), ArrayFromJSON(uint64(), expected));
  }
};

template <typename ArrowType>
class TestArgsortKernelForReal : public TestArgsortKernel<ArrowType> {};
TYPED_TEST_CASE(TestArgsortKernelForReal, RealArrowTypes);

template <typename ArrowType>
class TestArgsortKernelForIntegral : public TestArgsortKernel<ArrowType> {};
TYPED_TEST_CASE(TestArgsortKernelForIntegral, IntegralArrowTypes);

template <typename ArrowType>
class TestArgsortKernelForStrings : public TestArgsortKernel<ArrowType> {};
TYPED_TEST_CASE(TestArgsortKernelForStrings, testing::Types<StringType>);

TYPED_TEST(TestArgsortKernelForReal, SortReal) {
  this->AssertArgsort("[]", "[]");

  this->AssertArgsort("[3.4, 2.6, 6.3]", "[1, 0, 2]");

  this->AssertArgsort("[1.1, 2.4, 3.5, 4.3, 5.1, 6.8, 7.3]", "[0,1,2,3,4,5,6]");

  this->AssertArgsort("[7, 6, 5, 4, 3, 2, 1]", "[6,5,4,3,2,1,0]");

  this->AssertArgsort("[10.4, 12, 4.2, 50, 50.3, 32, 11]", "[2,0,6,1,5,3,4]");

  this->AssertArgsort("[null, 1, 3.3, null, 2, 5.3]", "[1,4,2,5,0,3]");
}

TYPED_TEST(TestArgsortKernelForIntegral, SortIntegral) {
  this->AssertArgsort("[]", "[]");

  this->AssertArgsort("[3, 2, 6]", "[1, 0, 2]");

  this->AssertArgsort("[1, 2, 3, 4, 5, 6, 7]", "[0,1,2,3,4,5,6]");

  this->AssertArgsort("[7, 6, 5, 4, 3, 2, 1]", "[6,5,4,3,2,1,0]");

  this->AssertArgsort("[10, 12, 4, 50, 50, 32, 11]", "[2,0,6,1,5,3,4]");

  this->AssertArgsort("[null, 1, 3, null, 2, 5]", "[1,4,2,5,0,3]");
}

TYPED_TEST(TestArgsortKernelForStrings, SortStrings) {
  this->AssertArgsort("[]", "[]");

  this->AssertArgsort(R"(["a", "b", "c"])", "[0, 1, 2]");

  this->AssertArgsort(R"(["foo", "bar", "baz"])", "[1,2,0]");

  this->AssertArgsort(R"(["testing", "sort", "for", "strings"])", "[2, 1, 3, 0]");
}

template <typename ArrowType>
class TestArgsortKernelRandom : public ComputeFixture, public TestBase {};

using ArgsortableTypes =
    ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                     Int32Type, Int64Type, FloatType, DoubleType, StringType>;

template <typename ArrayType>
class Comparator {
 public:
  bool operator()(const ArrayType& array, uint64_t lhs, uint64_t rhs) {
    if (array.IsNull(rhs) && array.IsNull(lhs)) return lhs < rhs;
    if (array.IsNull(rhs)) return true;
    if (array.IsNull(lhs)) return false;
    return array.Value(lhs) <= array.Value(rhs);
  }
};

template <>
class Comparator<StringArray> {
 public:
  bool operator()(const BinaryArray& array, uint64_t lhs, uint64_t rhs) {
    if (array.IsNull(rhs) && array.IsNull(lhs)) return lhs < rhs;
    if (array.IsNull(rhs)) return true;
    if (array.IsNull(lhs)) return false;
    return array.GetView(lhs) <= array.GetView(rhs);
  }
};

template <typename ArrayType>
void ValidateSorted(const ArrayType& array, UInt64Array& offsets) {
  Comparator<ArrayType> compare;
  for (int i = 1; i < array.length(); i++) {
    uint64_t lhs = offsets.Value(i - 1);
    uint64_t rhs = offsets.Value(i);
    ASSERT_TRUE(compare(array, lhs, rhs));
  }
}

class RandomImpl {
 protected:
  random::RandomArrayGenerator generator;

 public:
  explicit RandomImpl(random::SeedType seed) : generator(seed) {}
};

template <typename ArrowType>
class Random : public RandomImpl {
  using CType = typename TypeTraits<ArrowType>::CType;

 public:
  explicit Random(random::SeedType seed) : RandomImpl(seed) {}

  std::shared_ptr<Array> Generate(uint64_t count, double null_prob) {
    return generator.Numeric<ArrowType>(count, std::numeric_limits<CType>::min(),
                                        std::numeric_limits<CType>::max(), null_prob);
  }
};

template <>
class Random<StringType> : public RandomImpl {
 public:
  explicit Random(random::SeedType seed) : RandomImpl(seed) {}

  std::shared_ptr<Array> Generate(uint64_t count, double null_prob) {
    return generator.String(count, 1, 100, null_prob);
  }
};

TYPED_TEST_CASE(TestArgsortKernelRandom, ArgsortableTypes);

TYPED_TEST(TestArgsortKernelRandom, SortRandomValues) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;

  Random<TypeParam> rand(0x5487655);
  int times = 5;
  int length = 10000;
  for (int test = 0; test < times; test++) {
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
      auto array = rand.Generate(length, null_probability);
      std::shared_ptr<Array> offsets;
      ASSERT_OK(arrow::compute::Argsort(&this->ctx_, *array, &offsets));
      ValidateSorted<ArrayType>(*std::static_pointer_cast<ArrayType>(array),
                                *std::static_pointer_cast<UInt64Array>(offsets));
    }
  }
}

}  // namespace compute
}  // namespace arrow
