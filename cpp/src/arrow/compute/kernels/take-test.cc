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

using internal::checked_cast;
using internal::checked_pointer_cast;
using util::string_view;

constexpr auto kSeed = 0x0ff1ce;

template <typename ArrowType>
class TestTakeKernel : public ComputeFixture, public TestBase {
 protected:
  void AssertTakeArrays(const std::shared_ptr<Array>& values,
                        const std::shared_ptr<Array>& indices,
                        const std::shared_ptr<Array>& expected) {
    std::shared_ptr<Array> actual;
    TakeOptions options;
    ASSERT_OK(arrow::compute::Take(&this->ctx_, *values, *indices, options, &actual));
    AssertArraysEqual(*expected, *actual);
  }
  void AssertTake(const std::shared_ptr<DataType>& type, const std::string& values,
                  const std::string& indices, const std::string& expected) {
    std::shared_ptr<Array> actual;

    for (auto index_type : {int8(), uint32()}) {
      ASSERT_OK(this->Take(type, values, index_type, indices, &actual));
      AssertArraysEqual(*ArrayFromJSON(type, expected), *actual);
    }
  }
  Status Take(const std::shared_ptr<DataType>& type, const std::string& values,
              const std::shared_ptr<DataType>& index_type, const std::string& indices,
              std::shared_ptr<Array>* out) {
    TakeOptions options;
    return arrow::compute::Take(&this->ctx_, *ArrayFromJSON(type, values),
                                *ArrayFromJSON(index_type, indices), options, out);
  }
};

class TestTakeKernelWithNull : public TestTakeKernel<NullType> {
 protected:
  void AssertTake(const std::string& values, const std::string& indices,
                  const std::string& expected) {
    TestTakeKernel<NullType>::AssertTake(utf8(), values, indices, expected);
  }
};

TEST_F(TestTakeKernelWithNull, TakeNull) {
  this->AssertTake("[null, null, null]", "[0, 1, 0]", "[null, null, null]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                this->Take(null(), "[null, null, null]", int8(), "[0, 9, 0]", &arr));
}

TEST_F(TestTakeKernelWithNull, InvalidIndexType) {
  std::shared_ptr<Array> arr;
  ASSERT_RAISES(TypeError, this->Take(null(), "[null, null, null]", float32(),
                                      "[0.0, 1.0, 0.1]", &arr));
}

class TestTakeKernelWithBoolean : public TestTakeKernel<BooleanType> {
 protected:
  void AssertTake(const std::string& values, const std::string& indices,
                  const std::string& expected) {
    TestTakeKernel<BooleanType>::AssertTake(boolean(), values, indices, expected);
  }
};

TEST_F(TestTakeKernelWithBoolean, TakeBoolean) {
  this->AssertTake("[true, false, true]", "[0, 1, 0]", "[true, false, true]");
  this->AssertTake("[null, false, true]", "[0, 1, 0]", "[null, false, null]");
  this->AssertTake("[true, false, true]", "[null, 1, 0]", "[null, false, true]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                this->Take(boolean(), "[true, false, true]", int8(), "[0, 9, 0]", &arr));
}

template <typename ArrowType>
class TestTakeKernelWithNumeric : public TestTakeKernel<ArrowType> {
 protected:
  void AssertTake(const std::string& values, const std::string& indices,
                  const std::string& expected) {
    TestTakeKernel<ArrowType>::AssertTake(type_singleton(), values, indices, expected);
  }
  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
  void ValidateTake(const std::shared_ptr<Array>& values,
                    const std::shared_ptr<Array>& indices_boxed) {
    std::shared_ptr<Array> taken;
    TakeOptions options;
    ASSERT_OK(
        arrow::compute::Take(&this->ctx_, *values, *indices_boxed, options, &taken));
    ASSERT_EQ(indices_boxed->length(), taken->length());

    ASSERT_EQ(indices_boxed->type_id(), Type::INT32);
    auto indices = checked_pointer_cast<Int32Array>(indices_boxed);
    for (int64_t i = 0; i < indices->length(); ++i) {
      if (indices->IsNull(i)) {
        ASSERT_TRUE(taken->IsNull(i));
        continue;
      }
      int32_t taken_index = indices->Value(i);
      ASSERT_TRUE(values->RangeEquals(taken_index, taken_index + 1, i, taken));
    }
  }
};

TYPED_TEST_CASE(TestTakeKernelWithNumeric, NumericArrowTypes);
TYPED_TEST(TestTakeKernelWithNumeric, TakeNumeric) {
  this->AssertTake("[7, 8, 9]", "[0, 1, 0]", "[7, 8, 7]");
  this->AssertTake("[null, 8, 9]", "[0, 1, 0]", "[null, 8, null]");
  this->AssertTake("[7, 8, 9]", "[null, 1, 0]", "[null, 8, 7]");
  this->AssertTake("[null, 8, 9]", "[]", "[]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError, this->Take(this->type_singleton(), "[7, 8, 9]", int8(),
                                       "[0, 9, 0]", &arr));
}

TYPED_TEST(TestTakeKernelWithNumeric, TakeRandomNumeric) {
  auto rand = random::RandomArrayGenerator(kSeed);
  for (size_t i = 3; i < 8; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    for (size_t j = 0; j < 13; j++) {
      const int64_t indices_length = static_cast<int64_t>(1ULL << j);
      for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
        auto values = rand.Numeric<TypeParam>(length, 0, 127, null_probability);
        auto max_index = static_cast<int32_t>(length - 1);
        auto filter = rand.Int32(indices_length, 0, max_index, null_probability);
        this->ValidateTake(values, filter);
      }
    }
  }
}

class TestTakeKernelWithString : public TestTakeKernel<StringType> {
 protected:
  void AssertTake(const std::string& values, const std::string& indices,
                  const std::string& expected) {
    TestTakeKernel<StringType>::AssertTake(utf8(), values, indices, expected);
  }
  void AssertTakeDictionary(const std::string& dictionary_values,
                            const std::string& dictionary_indices,
                            const std::string& indices,
                            const std::string& expected_indices) {
    auto dict = ArrayFromJSON(utf8(), dictionary_values);
    auto type = dictionary(int8(), utf8());
    std::shared_ptr<Array> values, actual, expected;
    ASSERT_OK(DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), dictionary_indices),
                                          dict, &values));
    ASSERT_OK(DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), expected_indices),
                                          dict, &expected));
    auto take_indices = ArrayFromJSON(int8(), indices);
    this->AssertTakeArrays(values, take_indices, expected);
  }
};

TEST_F(TestTakeKernelWithString, TakeString) {
  this->AssertTake(R"(["a", "b", "c"])", "[0, 1, 0]", R"(["a", "b", "a"])");
  this->AssertTake(R"([null, "b", "c"])", "[0, 1, 0]", "[null, \"b\", null]");
  this->AssertTake(R"(["a", "b", "c"])", "[null, 1, 0]", R"([null, "b", "a"])");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                this->Take(utf8(), R"(["a", "b", "c"])", int8(), "[0, 9, 0]", &arr));
}

TEST_F(TestTakeKernelWithString, TakeDictionary) {
  auto dict = R"(["a", "b", "c", "d", "e"])";
  this->AssertTakeDictionary(dict, "[3, 4, 2]", "[0, 1, 0]", "[3, 4, 3]");
  this->AssertTakeDictionary(dict, "[null, 4, 2]", "[0, 1, 0]", "[null, 4, null]");
  this->AssertTakeDictionary(dict, "[3, 4, 2]", "[null, 1, 0]", "[null, 4, 3]");
}

class TestTakeKernelWithList : public TestTakeKernel<ListType> {};

TEST_F(TestTakeKernelWithList, TakeListInt32) {
  std::string list_json = "[[], [1,2], null, [3]]";
  this->AssertTake(list(int32()), list_json, "[]", "[]");
  this->AssertTake(list(int32()), list_json, "[3, 2, 1]", "[[3], null, [1,2]]");
  this->AssertTake(list(int32()), list_json, "[null, 3, 0]", "[null, [3], []]");
  this->AssertTake(list(int32()), list_json, "[null, null]", "[null, null]");
  this->AssertTake(list(int32()), list_json, "[3, 0, 0, 3]", "[[3], [], [], [3]]");
  this->AssertTake(list(int32()), list_json, "[0, 1, 2, 3]", list_json);
  this->AssertTake(list(int32()), list_json, "[0, 0, 0, 0, 0, 0, 1]",
                   "[[], [], [], [], [], [], [1, 2]]");
}

class TestTakeKernelWithFixedSizeList : public TestTakeKernel<FixedSizeListType> {};

TEST_F(TestTakeKernelWithFixedSizeList, TakeFixedSizeListInt32) {
  std::string list_json = "[null, [1, null, 3], [4, 5, 6], [7, 8, null]]";
  this->AssertTake(fixed_size_list(int32(), 3), list_json, "[]", "[]");
  this->AssertTake(fixed_size_list(int32(), 3), list_json, "[3, 2, 1]",
                   "[[7, 8, null], [4, 5, 6], [1, null, 3]]");
  this->AssertTake(fixed_size_list(int32(), 3), list_json, "[null, 2, 0]",
                   "[null, [4, 5, 6], null]");
  this->AssertTake(fixed_size_list(int32(), 3), list_json, "[null, null]",
                   "[null, null]");
  this->AssertTake(fixed_size_list(int32(), 3), list_json, "[3, 0, 0, 3]",
                   "[[7, 8, null], null, null, [7, 8, null]]");
  this->AssertTake(fixed_size_list(int32(), 3), list_json, "[0, 1, 2, 3]", list_json);
  this->AssertTake(
      fixed_size_list(int32(), 3), list_json, "[2, 2, 2, 2, 2, 2, 1]",
      "[[4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [1, null, 3]]");
}

class TestTakeKernelWithMap : public TestTakeKernel<MapType> {};

TEST_F(TestTakeKernelWithMap, TakeMapStringToInt32) {
  std::string map_json = R"([
    [["joe", 0], ["mark", null]],
    null,
    [["cap", 8]],
    []
  ])";
  this->AssertTake(map(utf8(), int32()), map_json, "[]", "[]");
  this->AssertTake(map(utf8(), int32()), map_json, "[3, 1, 3, 1, 3]",
                   "[[], null, [], null, []]");
  this->AssertTake(map(utf8(), int32()), map_json, "[2, 1, null]", R"([
    [["cap", 8]],
    null,
    null
  ])");
  this->AssertTake(map(utf8(), int32()), map_json, "[2, 1, 0]", R"([
    [["cap", 8]],
    null,
    [["joe", 0], ["mark", null]]
  ])");
  this->AssertTake(map(utf8(), int32()), map_json, "[0, 1, 2, 3]", map_json);
  this->AssertTake(map(utf8(), int32()), map_json, "[0, 0, 0, 0, 0, 0, 3]", R"([
    [["joe", 0], ["mark", null]],
    [["joe", 0], ["mark", null]],
    [["joe", 0], ["mark", null]],
    [["joe", 0], ["mark", null]],
    [["joe", 0], ["mark", null]],
    [["joe", 0], ["mark", null]],
    []
  ])");
}

class TestTakeKernelWithStruct : public TestTakeKernel<StructType> {};

TEST_F(TestTakeKernelWithStruct, TakeStruct) {
  auto struct_type = struct_({field("a", int32()), field("b", utf8())});
  auto struct_json = R"([
    null,
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])";
  this->AssertTake(struct_type, struct_json, "[]", "[]");
  this->AssertTake(struct_type, struct_json, "[3, 1, 3, 1, 3]", R"([
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": 4, "b": "eh"}
  ])");
  this->AssertTake(struct_type, struct_json, "[3, 1, 0]", R"([
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    null
  ])");
  this->AssertTake(struct_type, struct_json, "[0, 1, 2, 3]", struct_json);
  this->AssertTake(struct_type, struct_json, "[0, 2, 2, 2, 2, 2, 2]", R"([
    null,
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"}
  ])");
}

}  // namespace compute
}  // namespace arrow
