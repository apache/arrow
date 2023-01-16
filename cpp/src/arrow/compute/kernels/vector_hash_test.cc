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

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <locale>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/status.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"

#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"

#include "arrow/ipc/json_simple.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

// ----------------------------------------------------------------------
// Dictionary tests

template <typename T>
void CheckUnique(const std::shared_ptr<T>& input,
                 const std::shared_ptr<Array>& expected) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result, Unique(input));
  ValidateOutput(*result);
  // TODO: We probably shouldn't rely on array ordering.
  ASSERT_ARRAYS_EQUAL(*expected, *result);
}

template <typename Type, typename T>
void CheckUnique(const std::shared_ptr<DataType>& type, const std::vector<T>& in_values,
                 const std::vector<bool>& in_is_valid, const std::vector<T>& out_values,
                 const std::vector<bool>& out_is_valid) {
  std::shared_ptr<Array> input = _MakeArray<Type, T>(type, in_values, in_is_valid);
  std::shared_ptr<Array> expected = _MakeArray<Type, T>(type, out_values, out_is_valid);
  CheckUnique(input, expected);
}

// Check that ValueCounts() accepts a 0-length array with null buffers
void CheckValueCountsNull(const std::shared_ptr<DataType>& type) {
  std::vector<std::shared_ptr<Buffer>> data_buffers(2);
  Datum input;
  input.value =
      ArrayData::Make(type, 0 /* length */, std::move(data_buffers), 0 /* null_count */);

  std::shared_ptr<Array> ex_values = ArrayFromJSON(type, "[]");
  std::shared_ptr<Array> ex_counts = ArrayFromJSON(int64(), "[]");

  ASSERT_OK_AND_ASSIGN(auto result_struct, ValueCounts(input));
  ValidateOutput(*result_struct);
  ASSERT_NE(result_struct->GetFieldByName(kValuesFieldName), nullptr);
  // TODO: We probably shouldn't rely on value ordering.
  ASSERT_ARRAYS_EQUAL(*ex_values, *result_struct->GetFieldByName(kValuesFieldName));
  ASSERT_ARRAYS_EQUAL(*ex_counts, *result_struct->GetFieldByName(kCountsFieldName));
}

template <typename T>
void CheckValueCounts(const std::shared_ptr<T>& input,
                      const std::shared_ptr<Array>& expected_values,
                      const std::shared_ptr<Array>& expected_counts) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result, ValueCounts(input));
  ValidateOutput(*result);
  auto result_struct = std::dynamic_pointer_cast<StructArray>(result);
  ASSERT_EQ(result_struct->num_fields(), 2);
  // TODO: We probably shouldn't rely on value ordering.
  ASSERT_ARRAYS_EQUAL(*expected_values, *result_struct->field(kValuesFieldIndex));
  ASSERT_ARRAYS_EQUAL(*expected_counts, *result_struct->field(kCountsFieldIndex));
}

template <typename Type, typename T>
void CheckValueCounts(const std::shared_ptr<DataType>& type,
                      const std::vector<T>& in_values,
                      const std::vector<bool>& in_is_valid,
                      const std::vector<T>& out_values,
                      const std::vector<bool>& out_is_valid,
                      const std::vector<int64_t>& out_counts) {
  std::vector<bool> all_valids(out_is_valid.size(), true);
  std::shared_ptr<Array> input = _MakeArray<Type, T>(type, in_values, in_is_valid);
  std::shared_ptr<Array> ex_values = _MakeArray<Type, T>(type, out_values, out_is_valid);
  std::shared_ptr<Array> ex_counts =
      _MakeArray<Int64Type, int64_t>(int64(), out_counts, all_valids);

  CheckValueCounts(input, ex_values, ex_counts);
}

void CheckDictEncode(const std::shared_ptr<Array>& input,
                     const std::shared_ptr<Array>& expected_values,
                     const std::shared_ptr<Array>& expected_indices) {
  auto type = dictionary(expected_indices->type(), expected_values->type());
  DictionaryArray expected(type, expected_indices, expected_values);

  ASSERT_OK_AND_ASSIGN(Datum datum_out, DictionaryEncode(input));
  std::shared_ptr<Array> result = MakeArray(datum_out.array());
  ValidateOutput(*result);

  ASSERT_ARRAYS_EQUAL(expected, *result);
}

template <typename Type, typename T>
void CheckDictEncode(const std::shared_ptr<DataType>& type,
                     const std::vector<T>& in_values,
                     const std::vector<bool>& in_is_valid,
                     const std::vector<T>& out_values,
                     const std::vector<bool>& out_is_valid,
                     const std::vector<int32_t>& out_indices) {
  std::shared_ptr<Array> input = _MakeArray<Type, T>(type, in_values, in_is_valid);
  std::shared_ptr<Array> ex_dict = _MakeArray<Type, T>(type, out_values, out_is_valid);
  std::shared_ptr<Array> ex_indices =
      _MakeArray<Int32Type, int32_t>(int32(), out_indices, in_is_valid);
  return CheckDictEncode(input, ex_dict, ex_indices);
}

class TestHashKernel : public ::testing::Test {};

template <typename Type>
class TestHashKernelPrimitive : public ::testing::Test {};

typedef ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type,
                         UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType,
                         Date32Type, Date64Type>
    PrimitiveDictionaries;

TYPED_TEST_SUITE(TestHashKernelPrimitive, PrimitiveDictionaries);

TYPED_TEST(TestHashKernelPrimitive, Unique) {
  using T = typename TypeParam::c_type;
  auto type = TypeTraits<TypeParam>::type_singleton();

  if (type->id() == Type::DATE64) {
    CheckUnique<Date64Type, int64_t>(
        type, {172800000LL, 86400000LL, 172800000LL, 86400000LL},
        {true, false, true, true}, {172800000LL, 0, 86400000LL}, {1, 0, 1});
    CheckUnique<Date64Type, int64_t>(
        type, {172800000LL, 86400000LL, 259200000LL, 86400000LL},
        {false, false, true, true}, {0, 259200000LL, 86400000LL}, {0, 1, 1});

    // Sliced
    CheckUnique(
        ArrayFromJSON(type, "[86400000, 172800000, null, 259200000, 172800000, null]")
            ->Slice(1, 4),
        ArrayFromJSON(type, "[172800000, null, 259200000]"));
  } else {
    CheckUnique<TypeParam, T>(type, {2, 1, 2, 1}, {true, false, true, true}, {2, 0, 1},
                              {1, 0, 1});
    CheckUnique<TypeParam, T>(type, {2, 1, 3, 1}, {false, false, true, true}, {0, 3, 1},
                              {0, 1, 1});

    // Sliced
    CheckUnique(ArrayFromJSON(type, "[1, 2, null, 3, 2, null]")->Slice(1, 4),
                ArrayFromJSON(type, "[2, null, 3]"));
  }
}

TYPED_TEST(TestHashKernelPrimitive, ValueCounts) {
  using T = typename TypeParam::c_type;
  auto type = TypeTraits<TypeParam>::type_singleton();

  if (type->id() == Type::DATE64) {
    CheckValueCounts<Date64Type, int64_t>(
        type,
        {172800000LL, 86400000LL, 172800000LL, 86400000LL, 172800000LL, 259200000LL,
         345600000LL},
        {true, false, true, true, true, true, false},
        {172800000LL, 0, 86400000LL, 259200000LL}, {1, 0, 1, 1}, {3, 2, 1, 1});
    CheckValueCounts<Date64Type, int64_t>(type, {}, {}, {}, {}, {});
    CheckValueCountsNull(type);

    // Sliced
    CheckValueCounts(
        ArrayFromJSON(type, "[86400000, 172800000, null, 259200000, 172800000, null]")
            ->Slice(1, 4),
        ArrayFromJSON(type, "[172800000, null, 259200000]"),
        ArrayFromJSON(int64(), "[2, 1, 1]"));
  } else {
    CheckValueCounts<TypeParam, T>(type, {2, 1, 2, 1, 2, 3, 4},
                                   {true, false, true, true, true, true, false},
                                   {2, 0, 1, 3}, {1, 0, 1, 1}, {3, 2, 1, 1});
    CheckValueCounts<TypeParam, T>(type, {}, {}, {}, {}, {});
    CheckValueCountsNull(type);

    // Sliced
    CheckValueCounts(ArrayFromJSON(type, "[1, 2, null, 3, 2, null]")->Slice(1, 4),
                     ArrayFromJSON(type, "[2, null, 3]"),
                     ArrayFromJSON(int64(), "[2, 1, 1]"));
  }
}

TYPED_TEST(TestHashKernelPrimitive, DictEncode) {
  using T = typename TypeParam::c_type;
  auto type = TypeTraits<TypeParam>::type_singleton();

  if (type->id() == Type::DATE64) {
    CheckDictEncode<Date64Type, int64_t>(
        type,
        {172800000LL, 86400000LL, 172800000LL, 86400000LL, 172800000LL, 345600000LL},
        {true, false, true, true, true, true}, {172800000LL, 86400000LL, 345600000LL},
        {1, 1, 1}, {0, 0, 0, 1, 0, 2});

    // Sliced
    CheckDictEncode(
        ArrayFromJSON(
            type,
            "[172800000, 86400000, null, 345600000, 259200000, 86400000, 172800000]")
            ->Slice(1, 5),
        ArrayFromJSON(type, "[86400000, 345600000, 259200000]"),
        ArrayFromJSON(int32(), "[0, null, 1, 2, 0]"));
  } else {
    CheckDictEncode<TypeParam, T>(type, {2, 1, 2, 1, 2, 3},
                                  {true, false, true, true, true, true}, {2, 1, 3},
                                  {1, 1, 1}, {0, 0, 0, 1, 0, 2});

    // Sliced
    CheckDictEncode(ArrayFromJSON(type, "[2, 1, null, 4, 3, 1, 42]")->Slice(1, 5),
                    ArrayFromJSON(type, "[1, 4, 3]"),
                    ArrayFromJSON(int32(), "[0, null, 1, 2, 0]"));
  }
}

TYPED_TEST(TestHashKernelPrimitive, ZeroChunks) {
  auto type = TypeTraits<TypeParam>::type_singleton();

  auto zero_chunks = std::make_shared<ChunkedArray>(ArrayVector{}, type);
  ASSERT_OK_AND_ASSIGN(Datum result, DictionaryEncode(zero_chunks));

  ASSERT_EQ(result.kind(), Datum::CHUNKED_ARRAY);
  AssertChunkedEqual(*result.chunked_array(),
                     ChunkedArray({}, dictionary(int32(), type)));
}

TYPED_TEST(TestHashKernelPrimitive, PrimitiveResizeTable) {
  using T = typename TypeParam::c_type;
  auto type = TypeTraits<TypeParam>::type_singleton();

  const int64_t kTotalValues = std::min<int64_t>(INT16_MAX, 1UL << sizeof(T) / 2);
  const int64_t kRepeats = 5;
  constexpr int64_t kFullDayMillis = 1000 * 60 * 60 * 24;
  const int64_t kTotalDate64Values = kFullDayMillis * kTotalValues;

  std::vector<T> values;
  std::vector<T> uniques;
  std::vector<int32_t> indices;
  std::vector<int64_t> counts;

  if (type->id() == Type::DATE64) {
    for (int64_t i = 0; i < kTotalDate64Values * kRepeats; i += kFullDayMillis) {
      const auto val = static_cast<T>(i % kTotalDate64Values);
      values.push_back(val);

      if (i < kTotalDate64Values) {
        uniques.push_back(val);
        counts.push_back(kRepeats);
      }
      indices.push_back(static_cast<int32_t>(i % kTotalDate64Values / kFullDayMillis));
    }
  } else {
    for (int64_t i = 0; i < kTotalValues * kRepeats; i++) {
      const auto val = static_cast<T>(i % kTotalValues);
      values.push_back(val);

      if (i < kTotalValues) {
        uniques.push_back(val);
        counts.push_back(kRepeats);
      }
      indices.push_back(static_cast<int32_t>(i % kTotalValues));
    }
  }

  CheckUnique<TypeParam, T>(type, values, {}, uniques, {});
  CheckValueCounts<TypeParam, T>(type, values, {}, uniques, {}, counts);
  CheckDictEncode<TypeParam, T>(type, values, {}, uniques, {}, indices);
}

TEST_F(TestHashKernel, UniqueTimeTimestamp) {
  CheckUnique<Time32Type, int32_t>(time32(TimeUnit::SECOND), {2, 1, 2, 1},
                                   {true, false, true, true}, {2, 0, 1}, {1, 0, 1});

  CheckUnique<Time64Type, int64_t>(time64(TimeUnit::NANO), {2, 1, 2, 1},
                                   {true, false, true, true}, {2, 0, 1}, {1, 0, 1});

  CheckUnique<TimestampType, int64_t>(timestamp(TimeUnit::NANO), {2, 1, 2, 1},
                                      {true, false, true, true}, {2, 0, 1}, {1, 0, 1});
  CheckUnique<DurationType, int64_t>(duration(TimeUnit::NANO), {2, 1, 2, 1},
                                     {true, false, true, true}, {2, 0, 1}, {1, 0, 1});
}

TEST_F(TestHashKernel, ValueCountsTimeTimestamp) {
  CheckValueCounts<Time32Type, int32_t>(time32(TimeUnit::SECOND), {2, 1, 2, 1},
                                        {true, false, true, true}, {2, 0, 1}, {1, 0, 1},
                                        {2, 1, 1});

  CheckValueCounts<Time64Type, int64_t>(time64(TimeUnit::NANO), {2, 1, 2, 1},
                                        {true, false, true, true}, {2, 0, 1}, {1, 0, 1},
                                        {2, 1, 1});

  CheckValueCounts<TimestampType, int64_t>(timestamp(TimeUnit::NANO), {2, 1, 2, 1},
                                           {true, false, true, true}, {2, 0, 1},
                                           {1, 0, 1}, {2, 1, 1});
  CheckValueCounts<DurationType, int64_t>(duration(TimeUnit::NANO), {2, 1, 2, 1},
                                          {true, false, true, true}, {2, 0, 1}, {1, 0, 1},
                                          {2, 1, 1});
}

TEST_F(TestHashKernel, UniqueBoolean) {
  CheckUnique<BooleanType, bool>(boolean(), {true, true, false, true},
                                 {true, false, true, true}, {true, false, false},
                                 {1, 0, 1});

  CheckUnique<BooleanType, bool>(boolean(), {false, true, false, true},
                                 {true, false, true, true}, {false, false, true},
                                 {1, 0, 1});

  // No nulls
  CheckUnique<BooleanType, bool>(boolean(), {true, true, false, true}, {}, {true, false},
                                 {});

  CheckUnique<BooleanType, bool>(boolean(), {false, true, false, true}, {}, {false, true},
                                 {});

  // Sliced
  CheckUnique(ArrayFromJSON(boolean(), "[null, true, true, false]")->Slice(1, 2),
              ArrayFromJSON(boolean(), "[true]"));
}

TEST_F(TestHashKernel, ValueCountsBoolean) {
  CheckValueCounts<BooleanType, bool>(boolean(), {true, true, false, true},
                                      {true, false, true, true}, {true, false, false},
                                      {1, 0, 1}, {2, 1, 1});

  CheckValueCounts<BooleanType, bool>(boolean(), {false, true, false, true},
                                      {true, false, true, true}, {false, false, true},
                                      {1, 0, 1}, {2, 1, 1});

  // No nulls
  CheckValueCounts<BooleanType, bool>(boolean(), {true, true, false, true}, {},
                                      {true, false}, {}, {3, 1});

  CheckValueCounts<BooleanType, bool>(boolean(), {false, true, false, true}, {},
                                      {false, true}, {}, {2, 2});

  // Sliced
  CheckValueCounts(ArrayFromJSON(boolean(), "[true, false, false, null]")->Slice(1, 2),
                   ArrayFromJSON(boolean(), "[false]"), ArrayFromJSON(int64(), "[2]"));
}

TEST_F(TestHashKernel, ValueCountsNull) {
  CheckValueCounts(ArrayFromJSON(null(), "[null, null, null]"),
                   ArrayFromJSON(null(), "[null]"), ArrayFromJSON(int64(), "[3]"));
}

TEST_F(TestHashKernel, DictEncodeBoolean) {
  CheckDictEncode<BooleanType, bool>(boolean(), {true, true, false, true, false},
                                     {true, false, true, true, true}, {true, false}, {},
                                     {0, 0, 1, 0, 1});

  CheckDictEncode<BooleanType, bool>(boolean(), {false, true, false, true, false},
                                     {true, false, true, true, true}, {false, true}, {},
                                     {0, 0, 0, 1, 0});

  // No nulls
  CheckDictEncode<BooleanType, bool>(boolean(), {true, true, false, true, false}, {},
                                     {true, false}, {}, {0, 0, 1, 0, 1});

  CheckDictEncode<BooleanType, bool>(boolean(), {false, true, false, true, false}, {},
                                     {false, true}, {}, {0, 1, 0, 1, 0});

  // Sliced
  CheckDictEncode(
      ArrayFromJSON(boolean(), "[false, true, null, true, false]")->Slice(1, 3),
      ArrayFromJSON(boolean(), "[true]"), ArrayFromJSON(int32(), "[0, null, 0]"));
}

template <typename ArrowType>
class TestHashKernelBinaryTypes : public TestHashKernel {
 protected:
  std::shared_ptr<DataType> type() { return TypeTraits<ArrowType>::type_singleton(); }

  void CheckDictEncodeP(const std::vector<std::string>& in_values,
                        const std::vector<bool>& in_is_valid,
                        const std::vector<std::string>& out_values,
                        const std::vector<bool>& out_is_valid,
                        const std::vector<int32_t>& out_indices) {
    CheckDictEncode<ArrowType, std::string>(type(), in_values, in_is_valid, out_values,
                                            out_is_valid, out_indices);
  }

  void CheckValueCountsP(const std::vector<std::string>& in_values,
                         const std::vector<bool>& in_is_valid,
                         const std::vector<std::string>& out_values,
                         const std::vector<bool>& out_is_valid,
                         const std::vector<int64_t>& out_counts) {
    CheckValueCounts<ArrowType, std::string>(type(), in_values, in_is_valid, out_values,
                                             out_is_valid, out_counts);
  }

  void CheckUniqueP(const std::vector<std::string>& in_values,
                    const std::vector<bool>& in_is_valid,
                    const std::vector<std::string>& out_values,
                    const std::vector<bool>& out_is_valid) {
    CheckUnique<ArrowType, std::string>(type(), in_values, in_is_valid, out_values,
                                        out_is_valid);
  }
};

TYPED_TEST_SUITE(TestHashKernelBinaryTypes, BaseBinaryArrowTypes);

TYPED_TEST(TestHashKernelBinaryTypes, ZeroChunks) {
  auto type = this->type();

  auto zero_chunks = std::make_shared<ChunkedArray>(ArrayVector{}, type);
  ASSERT_OK_AND_ASSIGN(Datum result, DictionaryEncode(zero_chunks));

  ASSERT_EQ(result.kind(), Datum::CHUNKED_ARRAY);
  AssertChunkedEqual(*result.chunked_array(),
                     ChunkedArray({}, dictionary(int32(), type)));
}

TYPED_TEST(TestHashKernelBinaryTypes, TwoChunks) {
  auto type = this->type();

  auto two_chunks = std::make_shared<ChunkedArray>(
      ArrayVector{
          ArrayFromJSON(type, "[\"a\"]"),
          ArrayFromJSON(type, "[\"b\"]"),
      },
      type);
  ASSERT_OK_AND_ASSIGN(Datum result, DictionaryEncode(two_chunks));

  auto dict_type = dictionary(int32(), type);
  auto dictionary = ArrayFromJSON(type, R"(["a", "b"])");

  auto chunk_0 = std::make_shared<DictionaryArray>(
      dict_type, ArrayFromJSON(int32(), "[0]"), dictionary);
  auto chunk_1 = std::make_shared<DictionaryArray>(
      dict_type, ArrayFromJSON(int32(), "[1]"), dictionary);

  ASSERT_EQ(result.kind(), Datum::CHUNKED_ARRAY);
  AssertChunkedEqual(*result.chunked_array(),
                     ChunkedArray({chunk_0, chunk_1}, dict_type));
}

TYPED_TEST(TestHashKernelBinaryTypes, Unique) {
  this->CheckUniqueP({"test", "", "test2", "test"}, {true, false, true, true},
                     {"test", "", "test2"}, {1, 0, 1});

  // Sliced
  CheckUnique(
      ArrayFromJSON(this->type(), R"(["ab", null, "cd", "ef", "cd", "gh"])")->Slice(1, 4),
      ArrayFromJSON(this->type(), R"([null, "cd", "ef"])"));
}

TYPED_TEST(TestHashKernelBinaryTypes, ValueCounts) {
  this->CheckValueCountsP({"test", "", "test2", "test"}, {true, false, true, true},
                          {"test", "", "test2"}, {1, 0, 1}, {2, 1, 1});

  // Sliced
  CheckValueCounts(
      ArrayFromJSON(this->type(), R"(["ab", null, "cd", "ab", "cd", "ef"])")->Slice(1, 4),
      ArrayFromJSON(this->type(), R"([null, "cd", "ab"])"),
      ArrayFromJSON(int64(), "[1, 2, 1]"));
}

TYPED_TEST(TestHashKernelBinaryTypes, DictEncode) {
  this->CheckDictEncodeP({"test", "", "test2", "test", "baz"},
                         {true, false, true, true, true}, {"test", "test2", "baz"}, {},
                         {0, 0, 1, 0, 2});

  // Sliced
  CheckDictEncode(
      ArrayFromJSON(this->type(), R"(["ab", null, "cd", "ab", "cd", "ef"])")->Slice(1, 4),
      ArrayFromJSON(this->type(), R"(["cd", "ab"])"),
      ArrayFromJSON(int32(), "[null, 0, 1, 0]"));
}

TYPED_TEST(TestHashKernelBinaryTypes, BinaryResizeTable) {
  const int32_t kTotalValues = 10000;
#if !defined(ARROW_VALGRIND)
  const int32_t kRepeats = 10;
#else
  // Mitigate Valgrind's slowness
  const int32_t kRepeats = 3;
#endif

  std::vector<std::string> values;
  std::vector<std::string> uniques;
  std::vector<int32_t> indices;
  std::vector<int64_t> counts;
  char buf[20] = "test";

  for (int32_t i = 0; i < kTotalValues * kRepeats; i++) {
    int32_t index = i % kTotalValues;

    ASSERT_GE(snprintf(buf + 4, sizeof(buf) - 4, "%d", index), 0);
    values.emplace_back(buf);

    if (i < kTotalValues) {
      uniques.push_back(values.back());
      counts.push_back(kRepeats);
    }
    indices.push_back(index);
  }

  this->CheckUniqueP(values, {}, uniques, {});
  this->CheckValueCountsP(values, {}, uniques, {}, counts);
  this->CheckDictEncodeP(values, {}, uniques, {}, indices);
}

TEST_F(TestHashKernel, UniqueFixedSizeBinary) {
  auto type = fixed_size_binary(3);

  CheckUnique<FixedSizeBinaryType, std::string>(type, {"aaa", "", "bbb", "aaa"},
                                                {true, false, true, true},
                                                {"aaa", "", "bbb"}, {1, 0, 1});

  // Sliced
  CheckUnique(
      ArrayFromJSON(type, R"(["aaa", null, "bbb", "bbb", "ccc", "ddd"])")->Slice(1, 4),
      ArrayFromJSON(type, R"([null, "bbb", "ccc"])"));
}

TEST_F(TestHashKernel, ValueCountsFixedSizeBinary) {
  auto type = fixed_size_binary(3);
  auto input = ArrayFromJSON(type, R"(["aaa", null, "bbb", "bbb", "ccc", null])");

  CheckValueCounts(input, ArrayFromJSON(type, R"(["aaa", null, "bbb", "ccc"])"),
                   ArrayFromJSON(int64(), "[1, 2, 2, 1]"));

  // Sliced
  CheckValueCounts(input->Slice(1, 4), ArrayFromJSON(type, R"([null, "bbb", "ccc"])"),
                   ArrayFromJSON(int64(), "[1, 2, 1]"));
}

TEST_F(TestHashKernel, DictEncodeFixedSizeBinary) {
  auto type = fixed_size_binary(3);

  CheckDictEncode<FixedSizeBinaryType, std::string>(
      type, {"bbb", "", "bbb", "aaa", "ccc"}, {true, false, true, true, true},
      {"bbb", "aaa", "ccc"}, {}, {0, 0, 0, 1, 2});

  // Sliced
  CheckDictEncode(
      ArrayFromJSON(type, R"(["aaa", null, "bbb", "bbb", "ccc", "ddd"])")->Slice(1, 4),
      ArrayFromJSON(type, R"(["bbb", "ccc"])"),
      ArrayFromJSON(int32(), "[null, 0, 0, 1]"));
}

TEST_F(TestHashKernel, FixedSizeBinaryResizeTable) {
  const int32_t kTotalValues = 10000;
#if !defined(ARROW_VALGRIND)
  const int32_t kRepeats = 10;
#else
  // Mitigate Valgrind's slowness
  const int32_t kRepeats = 3;
#endif

  std::vector<std::string> values;
  std::vector<std::string> uniques;
  std::vector<int32_t> indices;
  char buf[7] = "test..";

  for (int32_t i = 0; i < kTotalValues * kRepeats; i++) {
    int32_t index = i % kTotalValues;

    buf[4] = static_cast<char>(index / 128);
    buf[5] = static_cast<char>(index % 128);
    values.emplace_back(buf, 6);

    if (i < kTotalValues) {
      uniques.push_back(values.back());
    }
    indices.push_back(index);
  }

  auto type = fixed_size_binary(6);
  CheckUnique<FixedSizeBinaryType, std::string>(type, values, {}, uniques, {});
  CheckDictEncode<FixedSizeBinaryType, std::string>(type, values, {}, uniques, {},
                                                    indices);
}

TEST_F(TestHashKernel, UniqueDecimal) {
  std::vector<Decimal128> values{12, 12, 11, 12};
  std::vector<Decimal128> expected{12, 0, 11};

  CheckUnique<Decimal128Type, Decimal128>(decimal(2, 0), values,
                                          {true, false, true, true}, expected, {1, 0, 1});
}

TEST_F(TestHashKernel, UniqueNull) {
  CheckUnique<NullType, std::nullptr_t>(null(), {nullptr, nullptr}, {false, true},
                                        {nullptr}, {false});
  CheckUnique<NullType, std::nullptr_t>(null(), {}, {}, {}, {});
}

TEST_F(TestHashKernel, ValueCountsDecimal) {
  std::vector<Decimal128> values{12, 12, 11, 12};
  std::vector<Decimal128> expected{12, 0, 11};

  CheckValueCounts<Decimal128Type, Decimal128>(
      decimal(2, 0), values, {true, false, true, true}, expected, {1, 0, 1}, {2, 1, 1});
}

TEST_F(TestHashKernel, DictEncodeDecimal) {
  std::vector<Decimal128> values{12, 12, 11, 12, 13};
  std::vector<Decimal128> expected{12, 11, 13};

  CheckDictEncode<Decimal128Type, Decimal128>(decimal(2, 0), values,
                                              {true, false, true, true, true}, expected,
                                              {}, {0, 0, 1, 0, 2});
}

TEST_F(TestHashKernel, UniqueIntervalMonth) {
  CheckUnique<MonthIntervalType, int32_t>(month_interval(), {2, 1, 2, 1},
                                          {true, false, true, true}, {2, 0, 1},
                                          {true, false, true});

  CheckUnique<DayTimeIntervalType, DayTimeIntervalType::DayMilliseconds>(
      day_time_interval(), {{2, 1}, {3, 2}, {2, 1}, {1, 2}}, {true, false, true, true},
      {{2, 1}, {1, 1}, {1, 2}}, {true, false, true});

  CheckUnique<MonthDayNanoIntervalType, MonthDayNanoIntervalType::MonthDayNanos>(
      month_day_nano_interval(), {{2, 1, 1}, {3, 2, 1}, {2, 1, 1}, {1, 2, 1}},
      {true, false, true, true}, {{2, 1, 1}, {1, 1, 1}, {1, 2, 1}}, {true, false, true});
}

TEST_F(TestHashKernel, ValueCountsIntervalMonth) {
  CheckValueCounts<MonthIntervalType, int32_t>(month_interval(), {2, 1, 2, 1},
                                               {true, false, true, true}, {2, 0, 1},
                                               {true, false, true}, {2, 1, 1});

  CheckValueCounts<DayTimeIntervalType, DayTimeIntervalType::DayMilliseconds>(
      day_time_interval(), {{2, 1}, {3, 2}, {2, 1}, {1, 2}}, {true, false, true, true},
      {{2, 1}, {1, 1}, {1, 2}}, {true, false, true}, {2, 1, 1});

  CheckValueCounts<MonthDayNanoIntervalType, MonthDayNanoIntervalType::MonthDayNanos>(
      month_day_nano_interval(), {{2, 1, 1}, {3, 2, 1}, {2, 1, 1}, {1, 2, 1}},
      {true, false, true, true}, {{2, 1, 1}, {1, 1, 1}, {1, 2, 1}}, {true, false, true},
      {2, 1, 1});
}

TEST_F(TestHashKernel, DictEncodeIntervalMonth) {
  CheckDictEncode<MonthIntervalType, int32_t>(month_interval(), {2, 2, 1, 2, 3},
                                              {true, false, true, true, true}, {2, 1, 3},
                                              {}, {0, 0, 1, 0, 2});

  CheckDictEncode<DayTimeIntervalType, DayTimeIntervalType::DayMilliseconds>(
      day_time_interval(), {{2, 1}, {2, 1}, {3, 2}, {2, 1}, {1, 2}},
      {true, false, true, true, true}, {{2, 1}, {3, 2}, {1, 2}}, {}, {0, 0, 1, 0, 2});

  CheckDictEncode<MonthDayNanoIntervalType, MonthDayNanoIntervalType::MonthDayNanos>(
      month_day_nano_interval(), {{2, 1, 1}, {2, 1, 1}, {3, 2, 1}, {2, 1, 1}, {1, 2, 1}},
      {true, false, true, true, true}, {{2, 1, 1}, {3, 2, 1}, {1, 2, 1}}, {},
      {0, 0, 1, 0, 2});
}

TEST_F(TestHashKernel, DictionaryUniqueAndValueCounts) {
  auto dict_json = "[10, 20, 30, 40]";
  auto dict = ArrayFromJSON(int64(), dict_json);
  for (auto index_ty : IntTypes()) {
    auto indices = ArrayFromJSON(index_ty, "[3, 0, 0, 0, 1, 1, 3, 0, 1, 3, 0, 1]");

    auto dict_ty = dictionary(index_ty, int64());

    auto ex_indices = ArrayFromJSON(index_ty, "[3, 0, 1]");

    auto input = std::make_shared<DictionaryArray>(dict_ty, indices, dict);
    auto ex_uniques = std::make_shared<DictionaryArray>(dict_ty, ex_indices, dict);
    CheckUnique(input, ex_uniques);

    auto ex_counts = ArrayFromJSON(int64(), "[3, 5, 4]");
    CheckValueCounts(input, ex_uniques, ex_counts);

    // Empty array - executor never gives the kernel any batches,
    // so result dictionary is empty
    CheckUnique(DictArrayFromJSON(dict_ty, "[]", dict_json),
                DictArrayFromJSON(dict_ty, "[]", "[]"));
    CheckValueCounts(DictArrayFromJSON(dict_ty, "[]", dict_json),
                     DictArrayFromJSON(dict_ty, "[]", "[]"),
                     ArrayFromJSON(int64(), "[]"));

    // Check chunked array
    auto chunked = *ChunkedArray::Make({input->Slice(0, 2), input->Slice(2)});
    CheckUnique(chunked, ex_uniques);
    CheckValueCounts(chunked, ex_uniques, ex_counts);

    // Different chunk dictionaries
    auto input_2 = DictArrayFromJSON(dict_ty, "[1, null, 2, 3]", "[30, 40, 50, 60]");
    auto ex_uniques_2 =
        DictArrayFromJSON(dict_ty, "[3, 0, 1, null, 4, 5]", "[10, 20, 30, 40, 50, 60]");
    auto ex_counts_2 = ArrayFromJSON(int64(), "[4, 5, 4, 1, 1, 1]");
    auto different_dictionaries = *ChunkedArray::Make({input, input_2}, dict_ty);

    CheckUnique(different_dictionaries, ex_uniques_2);
    CheckValueCounts(different_dictionaries, ex_uniques_2, ex_counts_2);

    // Dictionary with encoded nulls
    auto dict_with_null = ArrayFromJSON(int64(), "[10, null, 30, 40]");
    input = std::make_shared<DictionaryArray>(dict_ty, indices, dict_with_null);
    ex_uniques = std::make_shared<DictionaryArray>(dict_ty, ex_indices, dict_with_null);
    CheckUnique(input, ex_uniques);

    CheckValueCounts(input, ex_uniques, ex_counts);

    // Dictionary with masked nulls
    auto indices_with_null =
        ArrayFromJSON(index_ty, "[3, 0, 0, 0, null, null, 3, 0, null, 3, 0, null]");
    auto ex_indices_with_null = ArrayFromJSON(index_ty, "[3, 0, null]");
    ex_uniques = std::make_shared<DictionaryArray>(dict_ty, ex_indices_with_null, dict);
    input = std::make_shared<DictionaryArray>(dict_ty, indices_with_null, dict);
    CheckUnique(input, ex_uniques);

    CheckValueCounts(input, ex_uniques, ex_counts);

    // Dictionary with encoded AND masked nulls
    auto some_indices_with_null =
        ArrayFromJSON(index_ty, "[3, 0, 0, 0, 1, 1, 3, 0, null, 3, 0, null]");
    ex_uniques =
        std::make_shared<DictionaryArray>(dict_ty, ex_indices_with_null, dict_with_null);
    input = std::make_shared<DictionaryArray>(dict_ty, indices_with_null, dict_with_null);
    CheckUnique(input, ex_uniques);
    CheckValueCounts(input, ex_uniques, ex_counts);
  }
}

/* TODO(ARROW-4124): Determine if we want to do something that is reproducible with
 * floats.
TEST_F(TestHashKernel, ValueCountsFloat) {

    // No nulls
  CheckValueCounts<FloatType, float>(float32(), {1.0f, 0.0f, -0.0f,
std::nan("1"), std::nan("2")  },
                                      {}, {0.0f, 1.0f, std::nan("1")}, {}, {});

  CheckValueCounts<DoubleType, double>(float64(), {1.0f, 0.0f, -0.0f,
std::nan("1"), std::nan("2")  },
                                      {}, {0.0f, 1.0f, std::nan("1")}, {}, {});
}
*/

TEST_F(TestHashKernel, ChunkedArrayInvoke) {
  std::vector<std::string> values1 = {"foo", "bar", "foo"};
  std::vector<std::string> values2 = {"bar", "baz", "quuux", "foo"};

  auto type = utf8();
  auto a1 = _MakeArray<StringType, std::string>(type, values1, {});
  auto a2 = _MakeArray<StringType, std::string>(type, values2, {});

  std::vector<std::string> dict_values = {"foo", "bar", "baz", "quuux"};
  auto ex_dict = _MakeArray<StringType, std::string>(type, dict_values, {});

  auto ex_counts = _MakeArray<Int64Type, int64_t>(int64(), {3, 2, 1, 1}, {});

  ArrayVector arrays = {a1, a2};
  auto carr = std::make_shared<ChunkedArray>(arrays);

  // Unique
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result, Unique(carr));
  ASSERT_ARRAYS_EQUAL(*ex_dict, *result);

  // Dictionary encode
  auto dict_type = dictionary(int32(), type);

  auto i1 = _MakeArray<Int32Type, int32_t>(int32(), {0, 1, 0}, {});
  auto i2 = _MakeArray<Int32Type, int32_t>(int32(), {1, 2, 3, 0}, {});

  ArrayVector dict_arrays = {std::make_shared<DictionaryArray>(dict_type, i1, ex_dict),
                             std::make_shared<DictionaryArray>(dict_type, i2, ex_dict)};
  auto dict_carr = std::make_shared<ChunkedArray>(dict_arrays);

  // Unique counts
  ASSERT_OK_AND_ASSIGN(auto counts, ValueCounts(carr));
  ASSERT_ARRAYS_EQUAL(*ex_dict, *counts->field(0));
  ASSERT_ARRAYS_EQUAL(*ex_counts, *counts->field(1));

  // Dictionary encode
  ASSERT_OK_AND_ASSIGN(Datum encoded_out, DictionaryEncode(carr));
  ASSERT_EQ(Datum::CHUNKED_ARRAY, encoded_out.kind());

  AssertChunkedEqual(*dict_carr, *encoded_out.chunked_array());
}

TEST_F(TestHashKernel, ZeroLengthDictionaryEncode) {
  // ARROW-7008
  auto values = ArrayFromJSON(utf8(), "[]");
  ASSERT_OK_AND_ASSIGN(Datum datum_result, DictionaryEncode(values));
  ValidateOutput(datum_result);
}

TEST_F(TestHashKernel, NullEncodingSchemes) {
  auto values = ArrayFromJSON(uint8(), "[1, 1, null, 2, null]");

  // Masking should put null in the indices array
  auto expected_mask_indices = ArrayFromJSON(int32(), "[0, 0, null, 1, null]");
  auto expected_mask_dictionary = ArrayFromJSON(uint8(), "[1, 2]");
  auto dictionary_type = dictionary(int32(), uint8());
  std::shared_ptr<Array> expected = std::make_shared<DictionaryArray>(
      dictionary_type, expected_mask_indices, expected_mask_dictionary);

  ASSERT_OK_AND_ASSIGN(Datum datum_result, DictionaryEncode(values));
  std::shared_ptr<Array> result = datum_result.make_array();
  AssertArraysEqual(*expected, *result);

  // Encoding should put null in the dictionary
  auto expected_encoded_indices = ArrayFromJSON(int32(), "[0, 0, 1, 2, 1]");
  auto expected_encoded_dict = ArrayFromJSON(uint8(), "[1, null, 2]");
  expected = std::make_shared<DictionaryArray>(dictionary_type, expected_encoded_indices,
                                               expected_encoded_dict);

  auto options = DictionaryEncodeOptions::Defaults();
  options.null_encoding_behavior = DictionaryEncodeOptions::ENCODE;
  ASSERT_OK_AND_ASSIGN(datum_result, DictionaryEncode(values, options));
  result = datum_result.make_array();
  AssertArraysEqual(*expected, *result);
}

TEST_F(TestHashKernel, ChunkedArrayZeroChunk) {
  // ARROW-6857
  auto chunked_array = std::make_shared<ChunkedArray>(ArrayVector{}, utf8());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> result_array, Unique(chunked_array));
  auto expected = ArrayFromJSON(chunked_array->type(), "[]");
  AssertArraysEqual(*expected, *result_array);

  ASSERT_OK_AND_ASSIGN(result_array, ValueCounts(chunked_array));
  expected = ArrayFromJSON(struct_({field(kValuesFieldName, chunked_array->type()),
                                    field(kCountsFieldName, int64())}),
                           "[]");
  AssertArraysEqual(*expected, *result_array);

  ASSERT_OK_AND_ASSIGN(Datum result_datum, DictionaryEncode(chunked_array));
  auto dict_type = dictionary(int32(), chunked_array->type());
  ASSERT_EQ(result_datum.kind(), Datum::CHUNKED_ARRAY);

  AssertChunkedEqual(*std::make_shared<ChunkedArray>(ArrayVector{}, dict_type),
                     *result_datum.chunked_array());
}

}  // namespace compute
}  // namespace arrow
