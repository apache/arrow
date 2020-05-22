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

#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/compute/api.h"
#include "arrow/compute/test_util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace compute {

using internal::checked_cast;
using internal::checked_pointer_cast;
using util::string_view;

void AssertTakeArrays(const std::shared_ptr<Array>& values,
                      const std::shared_ptr<Array>& indices,
                      const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> actual, Take(*values, *indices));
  ASSERT_OK(actual->ValidateFull());
  AssertArraysEqual(*expected, *actual);
}

Status TakeJSON(const std::shared_ptr<DataType>& type, const std::string& values,
                const std::shared_ptr<DataType>& index_type, const std::string& indices,
                std::shared_ptr<Array>* out) {
  return Take(*ArrayFromJSON(type, values), *ArrayFromJSON(index_type, indices))
      .Value(out);
}

void CheckTake(const std::shared_ptr<DataType>& type, const std::string& values,
               const std::string& indices, const std::string& expected) {
  std::shared_ptr<Array> actual;

  for (auto index_type : {int8(), uint32()}) {
    ASSERT_OK(TakeJSON(type, values, index_type, indices, &actual));
    ASSERT_OK(actual->ValidateFull());
    AssertArraysEqual(*ArrayFromJSON(type, expected), *actual);
  }
}

void AssertTakeNull(const std::string& values, const std::string& indices,
                    const std::string& expected) {
  CheckTake(null(), values, indices, expected);
}

void AssertTakeBoolean(const std::string& values, const std::string& indices,
                       const std::string& expected) {
  CheckTake(boolean(), values, indices, expected);
}

template <typename ArrowType>
class TestTakeKernel : public ::testing::Test {};

TEST(TestTakeKernel, TakeNull) {
  AssertTakeNull("[null, null, null]", "[0, 1, 0]", "[null, null, null]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                TakeJSON(null(), "[null, null, null]", int8(), "[0, 9, 0]", &arr));
  ASSERT_RAISES(IndexError,
                TakeJSON(boolean(), "[null, null, null]", int8(), "[0, -1, 0]", &arr));
}

TEST(TestTakeKernel, InvalidIndexType) {
  std::shared_ptr<Array> arr;
  ASSERT_RAISES(NotImplemented, TakeJSON(null(), "[null, null, null]", float32(),
                                         "[0.0, 1.0, 0.1]", &arr));
}

TEST(TestTakeKernel, TakeBoolean) {
  AssertTakeBoolean("[7, 8, 9]", "[]", "[]");
  AssertTakeBoolean("[true, false, true]", "[0, 1, 0]", "[true, false, true]");
  AssertTakeBoolean("[null, false, true]", "[0, 1, 0]", "[null, false, null]");
  AssertTakeBoolean("[true, false, true]", "[null, 1, 0]", "[null, false, true]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                TakeJSON(boolean(), "[true, false, true]", int8(), "[0, 9, 0]", &arr));
  ASSERT_RAISES(IndexError,
                TakeJSON(boolean(), "[true, false, true]", int8(), "[0, -1, 0]", &arr));
}

template <typename ArrowType>
class TestTakeKernelWithNumeric : public TestTakeKernel<ArrowType> {
 protected:
  void AssertTake(const std::string& values, const std::string& indices,
                  const std::string& expected) {
    CheckTake(type_singleton(), values, indices, expected);
  }

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }

  void ValidateTake(const std::shared_ptr<Array>& values,
                    const std::shared_ptr<Array>& indices_boxed) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> taken, Take(*values, *indices_boxed));
    ASSERT_OK(taken->ValidateFull());
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

TYPED_TEST_SUITE(TestTakeKernelWithNumeric, NumericArrowTypes);
TYPED_TEST(TestTakeKernelWithNumeric, TakeNumeric) {
  this->AssertTake("[7, 8, 9]", "[]", "[]");
  this->AssertTake("[7, 8, 9]", "[0, 1, 0]", "[7, 8, 7]");
  this->AssertTake("[null, 8, 9]", "[0, 1, 0]", "[null, 8, null]");
  this->AssertTake("[7, 8, 9]", "[null, 1, 0]", "[null, 8, 7]");
  this->AssertTake("[null, 8, 9]", "[]", "[]");
  this->AssertTake("[7, 8, 9]", "[0, 0, 0, 0, 0, 0, 2]", "[7, 7, 7, 7, 7, 7, 9]");

  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                TakeJSON(this->type_singleton(), "[7, 8, 9]", int8(), "[0, 9, 0]", &arr));
  ASSERT_RAISES(IndexError, TakeJSON(this->type_singleton(), "[7, 8, 9]", int8(),
                                     "[0, -1, 0]", &arr));
}

TYPED_TEST(TestTakeKernelWithNumeric, TakeRandomNumeric) {
  auto rand = random::RandomArrayGenerator(kRandomSeed);
  for (size_t i = 3; i < 8; i++) {
    const int64_t length = static_cast<int64_t>(1ULL << i);
    for (size_t j = 0; j < 13; j++) {
      const int64_t indices_length = static_cast<int64_t>(1ULL << j);
      for (auto null_probability : {0.0, 0.01, 0.25, 1.0}) {
        auto values = rand.Numeric<TypeParam>(length, 0, 127, null_probability);
        auto max_index = static_cast<int32_t>(length - 1);
        auto filter = rand.Int32(indices_length, 0, max_index, null_probability);
        this->ValidateTake(values, filter);
      }
    }
  }
}

template <typename TypeClass>
class TestTakeKernelWithString : public TestTakeKernel<TypeClass> {
 public:
  std::shared_ptr<DataType> value_type() {
    return TypeTraits<TypeClass>::type_singleton();
  }

  void AssertTake(const std::string& values, const std::string& indices,
                  const std::string& expected) {
    CheckTake(value_type(), values, indices, expected);
  }
  void AssertTakeDictionary(const std::string& dictionary_values,
                            const std::string& dictionary_indices,
                            const std::string& indices,
                            const std::string& expected_indices) {
    auto dict = ArrayFromJSON(value_type(), dictionary_values);
    auto type = dictionary(int8(), value_type());
    ASSERT_OK_AND_ASSIGN(auto values,
                         DictionaryArray::FromArrays(
                             type, ArrayFromJSON(int8(), dictionary_indices), dict));
    ASSERT_OK_AND_ASSIGN(
        auto expected,
        DictionaryArray::FromArrays(type, ArrayFromJSON(int8(), expected_indices), dict));
    auto take_indices = ArrayFromJSON(int8(), indices);
    AssertTakeArrays(values, take_indices, expected);
  }
};

TYPED_TEST_SUITE(TestTakeKernelWithString, TestingStringTypes);

TYPED_TEST(TestTakeKernelWithString, TakeString) {
  this->AssertTake(R"(["a", "b", "c"])", "[0, 1, 0]", R"(["a", "b", "a"])");
  this->AssertTake(R"([null, "b", "c"])", "[0, 1, 0]", "[null, \"b\", null]");
  this->AssertTake(R"(["a", "b", "c"])", "[null, 1, 0]", R"([null, "b", "a"])");

  std::shared_ptr<DataType> type = this->value_type();
  std::shared_ptr<Array> arr;
  ASSERT_RAISES(IndexError,
                TakeJSON(type, R"(["a", "b", "c"])", int8(), "[0, 9, 0]", &arr));
  ASSERT_RAISES(IndexError, TakeJSON(type, R"(["a", "b", null, "ddd", "ee"])", int64(),
                                     "[2, 5]", &arr));
}

TYPED_TEST(TestTakeKernelWithString, TakeDictionary) {
  auto dict = R"(["a", "b", "c", "d", "e"])";
  this->AssertTakeDictionary(dict, "[3, 4, 2]", "[0, 1, 0]", "[3, 4, 3]");
  this->AssertTakeDictionary(dict, "[null, 4, 2]", "[0, 1, 0]", "[null, 4, null]");
  this->AssertTakeDictionary(dict, "[3, 4, 2]", "[null, 1, 0]", "[null, 4, 3]");
}

class TestTakeKernelWithList : public TestTakeKernel<ListType> {};

TEST_F(TestTakeKernelWithList, TakeListInt32) {
  std::string list_json = "[[], [1,2], null, [3]]";
  CheckTake(list(int32()), list_json, "[]", "[]");
  CheckTake(list(int32()), list_json, "[3, 2, 1]", "[[3], null, [1,2]]");
  CheckTake(list(int32()), list_json, "[null, 3, 0]", "[null, [3], []]");
  CheckTake(list(int32()), list_json, "[null, null]", "[null, null]");
  CheckTake(list(int32()), list_json, "[3, 0, 0, 3]", "[[3], [], [], [3]]");
  CheckTake(list(int32()), list_json, "[0, 1, 2, 3]", list_json);
  CheckTake(list(int32()), list_json, "[0, 0, 0, 0, 0, 0, 1]",
            "[[], [], [], [], [], [], [1, 2]]");
}

TEST_F(TestTakeKernelWithList, TakeListListInt32) {
  std::string list_json = R"([
    [],
    [[1], [2, null, 2], []],
    null,
    [[3, null], null]
  ])";
  auto type = list(list(int32()));
  CheckTake(type, list_json, "[]", "[]");
  CheckTake(type, list_json, "[3, 2, 1]", R"([
    [[3, null], null],
    null,
    [[1], [2, null, 2], []]
  ])");
  CheckTake(type, list_json, "[null, 3, 0]", R"([
    null,
    [[3, null], null],
    []
  ])");
  CheckTake(type, list_json, "[null, null]", "[null, null]");
  CheckTake(type, list_json, "[3, 0, 0, 3]",
            "[[[3, null], null], [], [], [[3, null], null]]");
  CheckTake(type, list_json, "[0, 1, 2, 3]", list_json);
  CheckTake(type, list_json, "[0, 0, 0, 0, 0, 0, 1]",
            "[[], [], [], [], [], [], [[1], [2, null, 2], []]]");
}

class TestTakeKernelWithLargeList : public TestTakeKernel<LargeListType> {};

TEST_F(TestTakeKernelWithLargeList, TakeLargeListInt32) {
  std::string list_json = "[[], [1,2], null, [3]]";
  CheckTake(large_list(int32()), list_json, "[]", "[]");
  CheckTake(large_list(int32()), list_json, "[null, 1, 2, 0]", "[null, [1,2], null, []]");
}

class TestTakeKernelWithFixedSizeList : public TestTakeKernel<FixedSizeListType> {};

TEST_F(TestTakeKernelWithFixedSizeList, TakeFixedSizeListInt32) {
  std::string list_json = "[null, [1, null, 3], [4, 5, 6], [7, 8, null]]";
  CheckTake(fixed_size_list(int32(), 3), list_json, "[]", "[]");
  CheckTake(fixed_size_list(int32(), 3), list_json, "[3, 2, 1]",
            "[[7, 8, null], [4, 5, 6], [1, null, 3]]");
  CheckTake(fixed_size_list(int32(), 3), list_json, "[null, 2, 0]",
            "[null, [4, 5, 6], null]");
  CheckTake(fixed_size_list(int32(), 3), list_json, "[null, null]", "[null, null]");
  CheckTake(fixed_size_list(int32(), 3), list_json, "[3, 0, 0, 3]",
            "[[7, 8, null], null, null, [7, 8, null]]");
  CheckTake(fixed_size_list(int32(), 3), list_json, "[0, 1, 2, 3]", list_json);
  CheckTake(
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
  CheckTake(map(utf8(), int32()), map_json, "[]", "[]");
  CheckTake(map(utf8(), int32()), map_json, "[3, 1, 3, 1, 3]",
            "[[], null, [], null, []]");
  CheckTake(map(utf8(), int32()), map_json, "[2, 1, null]", R"([
    [["cap", 8]],
    null,
    null
  ])");
  CheckTake(map(utf8(), int32()), map_json, "[2, 1, 0]", R"([
    [["cap", 8]],
    null,
    [["joe", 0], ["mark", null]]
  ])");
  CheckTake(map(utf8(), int32()), map_json, "[0, 1, 2, 3]", map_json);
  CheckTake(map(utf8(), int32()), map_json, "[0, 0, 0, 0, 0, 0, 3]", R"([
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
  CheckTake(struct_type, struct_json, "[]", "[]");
  CheckTake(struct_type, struct_json, "[3, 1, 3, 1, 3]", R"([
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": 4, "b": "eh"}
  ])");
  CheckTake(struct_type, struct_json, "[3, 1, 0]", R"([
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    null
  ])");
  CheckTake(struct_type, struct_json, "[0, 1, 2, 3]", struct_json);
  CheckTake(struct_type, struct_json, "[0, 2, 2, 2, 2, 2, 2]", R"([
    null,
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"}
  ])");
}

class TestTakeKernelWithUnion : public TestTakeKernel<UnionType> {};

TEST_F(TestTakeKernelWithUnion, TakeUnion) {
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
    CheckTake(union_type, union_json, "[]", "[]");
    CheckTake(union_type, union_json, "[3, 1, 3, 1, 3]", R"([
      [5, "eh"],
      [2, 222],
      [5, "eh"],
      [2, 222],
      [5, "eh"]
    ])");
    CheckTake(union_type, union_json, "[4, 2, 1]", R"([
      null,
      [5, "hello"],
      [2, 222]
    ])");
    CheckTake(union_type, union_json, "[0, 1, 2, 3, 4, 5]", union_json);
    CheckTake(union_type, union_json, "[0, 2, 2, 2, 2, 2, 2]", R"([
      null,
      [5, "hello"],
      [5, "hello"],
      [5, "hello"],
      [5, "hello"],
      [5, "hello"],
      [5, "hello"]
    ])");
  }
}

class TestPermutationsWithTake : public TestBase {
 protected:
  void DoTake(const Int16Array& values, const Int16Array& indices,
              std::shared_ptr<Int16Array>* out) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> boxed_out, Take(values, indices));
    ASSERT_OK(boxed_out->ValidateFull());
    *out = checked_pointer_cast<Int16Array>(std::move(boxed_out));
  }

  std::shared_ptr<Int16Array> DoTake(const Int16Array& values,
                                     const Int16Array& indices) {
    std::shared_ptr<Int16Array> out;
    DoTake(values, indices, &out);
    return out;
  }

  std::shared_ptr<Int16Array> DoTakeN(uint64_t n, std::shared_ptr<Int16Array> array) {
    auto power_of_2 = array;
    array = Identity(array->length());
    while (n != 0) {
      if (n & 1) {
        array = DoTake(*array, *power_of_2);
      }
      power_of_2 = DoTake(*power_of_2, *power_of_2);
      n >>= 1;
    }
    return array;
  }

  template <typename Rng>
  void Shuffle(const Int16Array& array, Rng& gen, std::shared_ptr<Int16Array>* shuffled) {
    auto byte_length = array.length() * sizeof(int16_t);
    ASSERT_OK_AND_ASSIGN(auto data, array.values()->CopySlice(0, byte_length));
    auto mutable_data = reinterpret_cast<int16_t*>(data->mutable_data());
    std::shuffle(mutable_data, mutable_data + array.length(), gen);
    shuffled->reset(new Int16Array(array.length(), data));
  }

  template <typename Rng>
  std::shared_ptr<Int16Array> Shuffle(const Int16Array& array, Rng& gen) {
    std::shared_ptr<Int16Array> out;
    Shuffle(array, gen, &out);
    return out;
  }

  void Identity(int64_t length, std::shared_ptr<Int16Array>* identity) {
    Int16Builder identity_builder;
    ASSERT_OK(identity_builder.Resize(length));
    for (int16_t i = 0; i < length; ++i) {
      identity_builder.UnsafeAppend(i);
    }
    ASSERT_OK(identity_builder.Finish(identity));
  }

  std::shared_ptr<Int16Array> Identity(int64_t length) {
    std::shared_ptr<Int16Array> out;
    Identity(length, &out);
    return out;
  }

  std::shared_ptr<Int16Array> Inverse(const std::shared_ptr<Int16Array>& permutation) {
    auto length = static_cast<int16_t>(permutation->length());

    std::vector<bool> cycle_lengths(length + 1, false);
    auto permutation_to_the_i = permutation;
    for (int16_t cycle_length = 1; cycle_length <= length; ++cycle_length) {
      cycle_lengths[cycle_length] = HasTrivialCycle(*permutation_to_the_i);
      permutation_to_the_i = DoTake(*permutation, *permutation_to_the_i);
    }

    uint64_t cycle_to_identity_length = 1;
    for (int16_t cycle_length = length; cycle_length > 1; --cycle_length) {
      if (!cycle_lengths[cycle_length]) {
        continue;
      }
      if (cycle_to_identity_length % cycle_length == 0) {
        continue;
      }
      if (cycle_to_identity_length >
          std::numeric_limits<uint64_t>::max() / cycle_length) {
        // overflow, can't compute Inverse
        return nullptr;
      }
      cycle_to_identity_length *= cycle_length;
    }

    return DoTakeN(cycle_to_identity_length - 1, permutation);
  }

  bool HasTrivialCycle(const Int16Array& permutation) {
    for (int64_t i = 0; i < permutation.length(); ++i) {
      if (permutation.Value(i) == static_cast<int16_t>(i)) {
        return true;
      }
    }
    return false;
  }
};

TEST_F(TestPermutationsWithTake, InvertPermutation) {
  for (auto seed : std::vector<random::SeedType>({0, kRandomSeed, kRandomSeed * 2 - 1})) {
    std::default_random_engine gen(seed);
    for (int16_t length = 0; length < 1 << 10; ++length) {
      auto identity = Identity(length);
      auto permutation = Shuffle(*identity, gen);
      auto inverse = Inverse(permutation);
      if (inverse == nullptr) {
        break;
      }
      ASSERT_TRUE(DoTake(*inverse, *permutation)->Equals(identity));
    }
  }
}

class TestTakeKernelWithRecordBatch : public TestTakeKernel<RecordBatch> {
 public:
  void AssertTake(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
                  const std::string& indices, const std::string& expected_batch) {
    std::shared_ptr<RecordBatch> actual;

    for (auto index_type : {int8(), uint32()}) {
      ASSERT_OK(TakeJSON(schm, batch_json, index_type, indices, &actual));
      ASSERT_OK(actual->ValidateFull());
      ASSERT_BATCHES_EQUAL(*RecordBatchFromJSON(schm, expected_batch), *actual);
    }
  }

  Status TakeJSON(const std::shared_ptr<Schema>& schm, const std::string& batch_json,
                  const std::shared_ptr<DataType>& index_type, const std::string& indices,
                  std::shared_ptr<RecordBatch>* out) {
    auto batch = RecordBatchFromJSON(schm, batch_json);
    return Take(*batch, *ArrayFromJSON(index_type, indices)).Value(out);
  }
};

TEST_F(TestTakeKernelWithRecordBatch, TakeRecordBatch) {
  std::vector<std::shared_ptr<Field>> fields = {field("a", int32()), field("b", utf8())};
  auto schm = schema(fields);

  auto struct_json = R"([
    {"a": null, "b": "yo"},
    {"a": 1, "b": ""},
    {"a": 2, "b": "hello"},
    {"a": 4, "b": "eh"}
  ])";
  this->AssertTake(schm, struct_json, "[]", "[]");
  this->AssertTake(schm, struct_json, "[3, 1, 3, 1, 3]", R"([
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": 4, "b": "eh"}
  ])");
  this->AssertTake(schm, struct_json, "[3, 1, 0]", R"([
    {"a": 4, "b": "eh"},
    {"a": 1, "b": ""},
    {"a": null, "b": "yo"}
  ])");
  this->AssertTake(schm, struct_json, "[0, 1, 2, 3]", struct_json);
  this->AssertTake(schm, struct_json, "[0, 2, 2, 2, 2, 2, 2]", R"([
    {"a": null, "b": "yo"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"},
    {"a": 2, "b": "hello"}
  ])");
}

class TestTakeKernelWithChunkedArray : public TestTakeKernel<ChunkedArray> {
 public:
  void AssertTake(const std::shared_ptr<DataType>& type,
                  const std::vector<std::string>& values, const std::string& indices,
                  const std::vector<std::string>& expected) {
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK(this->TakeWithArray(type, values, indices, &actual));
    ASSERT_OK(actual->ValidateFull());
    AssertChunkedEqual(*ChunkedArrayFromJSON(type, expected), *actual);
  }

  void AssertChunkedTake(const std::shared_ptr<DataType>& type,
                         const std::vector<std::string>& values,
                         const std::vector<std::string>& indices,
                         const std::vector<std::string>& expected) {
    std::shared_ptr<ChunkedArray> actual;
    ASSERT_OK(this->TakeWithChunkedArray(type, values, indices, &actual));
    ASSERT_OK(actual->ValidateFull());
    AssertChunkedEqual(*ChunkedArrayFromJSON(type, expected), *actual);
  }

  Status TakeWithArray(const std::shared_ptr<DataType>& type,
                       const std::vector<std::string>& values, const std::string& indices,
                       std::shared_ptr<ChunkedArray>* out) {
    return Take(*ChunkedArrayFromJSON(type, values), *ArrayFromJSON(int8(), indices))
        .Value(out);
  }

  Status TakeWithChunkedArray(const std::shared_ptr<DataType>& type,
                              const std::vector<std::string>& values,
                              const std::vector<std::string>& indices,
                              std::shared_ptr<ChunkedArray>* out) {
    return Take(*ChunkedArrayFromJSON(type, values),
                *ChunkedArrayFromJSON(int8(), indices))
        .Value(out);
  }
};

TEST_F(TestTakeKernelWithChunkedArray, TakeChunkedArray) {
  this->AssertTake(int8(), {"[]"}, "[]", {"[]"});
  this->AssertChunkedTake(int8(), {"[]"}, {"[]"}, {"[]"});

  this->AssertTake(int8(), {"[7]", "[8, 9]"}, "[0, 1, 0, 2]", {"[7, 8, 7, 9]"});
  this->AssertChunkedTake(int8(), {"[7]", "[8, 9]"}, {"[0, 1, 0]", "[]", "[2]"},
                          {"[7, 8, 7]", "[]", "[9]"});
  this->AssertTake(int8(), {"[7]", "[8, 9]"}, "[2, 1]", {"[9, 8]"});

  std::shared_ptr<ChunkedArray> arr;
  ASSERT_RAISES(IndexError,
                this->TakeWithArray(int8(), {"[7]", "[8, 9]"}, "[0, 5]", &arr));
  ASSERT_RAISES(IndexError, this->TakeWithChunkedArray(int8(), {"[7]", "[8, 9]"},
                                                       {"[0, 1, 0]", "[5, 1]"}, &arr));
}

class TestTakeKernelWithTable : public TestTakeKernel<Table> {
 public:
  void AssertTake(const std::shared_ptr<Schema>& schm,
                  const std::vector<std::string>& table_json, const std::string& filter,
                  const std::vector<std::string>& expected_table) {
    std::shared_ptr<Table> actual;

    ASSERT_OK(this->TakeWithArray(schm, table_json, filter, &actual));
    ASSERT_OK(actual->ValidateFull());
    ASSERT_TABLES_EQUAL(*TableFromJSON(schm, expected_table), *actual);
  }

  void AssertChunkedTake(const std::shared_ptr<Schema>& schm,
                         const std::vector<std::string>& table_json,
                         const std::vector<std::string>& filter,
                         const std::vector<std::string>& expected_table) {
    std::shared_ptr<Table> actual;

    ASSERT_OK(this->TakeWithChunkedArray(schm, table_json, filter, &actual));
    ASSERT_OK(actual->ValidateFull());
    ASSERT_TABLES_EQUAL(*TableFromJSON(schm, expected_table), *actual);
  }

  Status TakeWithArray(const std::shared_ptr<Schema>& schm,
                       const std::vector<std::string>& values, const std::string& indices,
                       std::shared_ptr<Table>* out) {
    return Take(*TableFromJSON(schm, values), *ArrayFromJSON(int8(), indices)).Value(out);
  }

  Status TakeWithChunkedArray(const std::shared_ptr<Schema>& schm,
                              const std::vector<std::string>& values,
                              const std::vector<std::string>& indices,
                              std::shared_ptr<Table>* out) {
    return Take(*TableFromJSON(schm, values), *ChunkedArrayFromJSON(int8(), indices))
        .Value(out);
  }
};

TEST_F(TestTakeKernelWithTable, TakeTable) {
  std::vector<std::shared_ptr<Field>> fields = {field("a", int32()), field("b", utf8())};
  auto schm = schema(fields);

  std::vector<std::string> table_json = {
      "[{\"a\": null, \"b\": \"yo\"},{\"a\": 1, \"b\": \"\"}]",
      "[{\"a\": 2, \"b\": \"hello\"},{\"a\": 4, \"b\": \"eh\"}]"};

  this->AssertTake(schm, table_json, "[]", {"[]"});
  std::vector<std::string> expected_310 = {
      "[{\"a\": 4, \"b\": \"eh\"},{\"a\": 1, \"b\": \"\"},{\"a\": null, \"b\": \"yo\"}]"};
  this->AssertTake(schm, table_json, "[3, 1, 0]", expected_310);
  this->AssertChunkedTake(schm, table_json, {"[0, 1]", "[2, 3]"}, table_json);
}

}  // namespace compute
}  // namespace arrow
