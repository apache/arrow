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

#include <cstdint>
#include <cstdio>
#include <functional>
#include <locale>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/hash.h"
#include "arrow/compute/kernels/util-internal.h"
#include "arrow/compute/test-util.h"

#include "arrow/ipc/json-simple.h"

namespace arrow {
namespace compute {

// ----------------------------------------------------------------------
// Dictionary tests

template <typename Type, typename T>
void CheckUnique(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                 const std::vector<T>& in_values, const std::vector<bool>& in_is_valid,
                 const std::vector<T>& out_values,
                 const std::vector<bool>& out_is_valid) {
  std::shared_ptr<Array> input = _MakeArray<Type, T>(type, in_values, in_is_valid);
  std::shared_ptr<Array> expected = _MakeArray<Type, T>(type, out_values, out_is_valid);

  std::shared_ptr<Array> result;
  ASSERT_OK(Unique(ctx, input, &result));
  // TODO: We probably shouldn't rely on array ordering.
  ASSERT_ARRAYS_EQUAL(*expected, *result);
}

template <typename Type, typename T>
void CheckValueCountsNull(FunctionContext* ctx, const std::shared_ptr<DataType>& type) {
  std::vector<std::shared_ptr<Buffer>> data_buffers(2);
  Datum input;
  input.value =
      ArrayData::Make(type, 0 /* length */, std::move(data_buffers), 0 /* null_count */);

  std::shared_ptr<Array> ex_values = ArrayFromJSON(type, "[]");
  std::shared_ptr<Array> ex_counts = ArrayFromJSON(int64(), "[]");

  std::shared_ptr<Array> result;
  ASSERT_OK(ValueCounts(ctx, input, &result));
  auto result_struct = std::dynamic_pointer_cast<StructArray>(result);
  ASSERT_NE(result_struct->GetFieldByName(kValuesFieldName), nullptr);
  // TODO: We probably shouldn't rely on value ordering.
  ASSERT_ARRAYS_EQUAL(*ex_values, *result_struct->GetFieldByName(kValuesFieldName));
  ASSERT_ARRAYS_EQUAL(*ex_counts, *result_struct->GetFieldByName(kCountsFieldName));
}

template <typename Type, typename T>
void CheckValueCounts(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                      const std::vector<T>& in_values,
                      const std::vector<bool>& in_is_valid,
                      const std::vector<T>& out_values,
                      const std::vector<bool>& out_is_valid,
                      const std::vector<int64_t>& out_counts) {
  std::shared_ptr<Array> input = _MakeArray<Type, T>(type, in_values, in_is_valid);
  std::shared_ptr<Array> ex_values = _MakeArray<Type, T>(type, out_values, out_is_valid);
  std::shared_ptr<Array> ex_counts =
      _MakeArray<Int64Type, int64_t>(int64(), out_counts, out_is_valid);

  std::shared_ptr<Array> result;
  ASSERT_OK(ValueCounts(ctx, input, &result));
  auto result_struct = std::dynamic_pointer_cast<StructArray>(result);
  // TODO: We probably shouldn't rely on value ordering.
  ASSERT_ARRAYS_EQUAL(*ex_values, *result_struct->field(kValuesFieldIndex));
  ASSERT_ARRAYS_EQUAL(*ex_counts, *result_struct->field(kCountsFieldIndex));
}

template <typename Type, typename T>
void CheckDictEncode(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                     const std::vector<T>& in_values,
                     const std::vector<bool>& in_is_valid,
                     const std::vector<T>& out_values,
                     const std::vector<bool>& out_is_valid,
                     const std::vector<int32_t>& out_indices) {
  std::shared_ptr<Array> input = _MakeArray<Type, T>(type, in_values, in_is_valid);
  std::shared_ptr<Array> ex_dict = _MakeArray<Type, T>(type, out_values, out_is_valid);
  std::shared_ptr<Array> ex_indices =
      _MakeArray<Int32Type, int32_t>(int32(), out_indices, in_is_valid);

  DictionaryArray expected(dictionary(int32(), type), ex_indices, ex_dict);

  Datum datum_out;
  ASSERT_OK(DictionaryEncode(ctx, input, &datum_out));
  std::shared_ptr<Array> result = MakeArray(datum_out.array());

  ASSERT_ARRAYS_EQUAL(expected, *result);
}

class TestHashKernel : public ComputeFixture, public TestBase {};

template <typename Type>
class TestHashKernelPrimitive : public ComputeFixture, public TestBase {};

typedef ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type,
                         UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType,
                         Date32Type, Date64Type>
    PrimitiveDictionaries;

TYPED_TEST_CASE(TestHashKernelPrimitive, PrimitiveDictionaries);

TYPED_TEST(TestHashKernelPrimitive, Unique) {
  using T = typename TypeParam::c_type;
  auto type = TypeTraits<TypeParam>::type_singleton();
  CheckUnique<TypeParam, T>(&this->ctx_, type, {2, 1, 2, 1}, {true, false, true, true},
                            {2, 1}, {});
  CheckUnique<TypeParam, T>(&this->ctx_, type, {2, 1, 3, 1}, {false, false, true, true},
                            {3, 1}, {});
}

TYPED_TEST(TestHashKernelPrimitive, ValueCounts) {
  using T = typename TypeParam::c_type;
  auto type = TypeTraits<TypeParam>::type_singleton();
  CheckValueCounts<TypeParam, T>(&this->ctx_, type, {2, 1, 2, 1, 2, 3, 4},
                                 {true, false, true, true, true, true, false}, {2, 1, 3},
                                 {}, {3, 1, 1});
  CheckValueCounts<TypeParam, T>(&this->ctx_, type, {}, {}, {}, {}, {});
  CheckValueCountsNull<TypeParam, T>(&this->ctx_, type);
}

TYPED_TEST(TestHashKernelPrimitive, DictEncode) {
  using T = typename TypeParam::c_type;
  auto type = TypeTraits<TypeParam>::type_singleton();
  CheckDictEncode<TypeParam, T>(&this->ctx_, type, {2, 1, 2, 1, 2, 3},
                                {true, false, true, true, true, true}, {2, 1, 3}, {},
                                {0, 0, 0, 1, 0, 2});
}

TYPED_TEST(TestHashKernelPrimitive, PrimitiveResizeTable) {
  using T = typename TypeParam::c_type;

  const int64_t kTotalValues = std::min<int64_t>(INT16_MAX, 1UL << sizeof(T) / 2);
  const int64_t kRepeats = 5;

  std::vector<T> values;
  std::vector<T> uniques;
  std::vector<int32_t> indices;
  std::vector<int64_t> counts;
  for (int64_t i = 0; i < kTotalValues * kRepeats; i++) {
    const auto val = static_cast<T>(i % kTotalValues);
    values.push_back(val);

    if (i < kTotalValues) {
      uniques.push_back(val);
      counts.push_back(kRepeats);
    }
    indices.push_back(static_cast<int32_t>(i % kTotalValues));
  }

  auto type = TypeTraits<TypeParam>::type_singleton();
  CheckUnique<TypeParam, T>(&this->ctx_, type, values, {}, uniques, {});
  CheckValueCounts<TypeParam, T>(&this->ctx_, type, values, {}, uniques, {}, counts);
  CheckDictEncode<TypeParam, T>(&this->ctx_, type, values, {}, uniques, {}, indices);
}

TEST_F(TestHashKernel, UniqueTimeTimestamp) {
  CheckUnique<Time32Type, int32_t>(&this->ctx_, time32(TimeUnit::SECOND), {2, 1, 2, 1},
                                   {true, false, true, true}, {2, 1}, {});

  CheckUnique<Time64Type, int64_t>(&this->ctx_, time64(TimeUnit::NANO), {2, 1, 2, 1},
                                   {true, false, true, true}, {2, 1}, {});

  CheckUnique<TimestampType, int64_t>(&this->ctx_, timestamp(TimeUnit::NANO),
                                      {2, 1, 2, 1}, {true, false, true, true}, {2, 1},
                                      {});
}

TEST_F(TestHashKernel, ValueCountsTimeTimestamp) {
  CheckValueCounts<Time32Type, int32_t>(&this->ctx_, time32(TimeUnit::SECOND),
                                        {2, 1, 2, 1}, {true, false, true, true}, {2, 1},
                                        {}, {2, 1});

  CheckValueCounts<Time64Type, int64_t>(&this->ctx_, time64(TimeUnit::NANO), {2, 1, 2, 1},
                                        {true, false, true, true}, {2, 1}, {}, {2, 1});

  CheckValueCounts<TimestampType, int64_t>(&this->ctx_, timestamp(TimeUnit::NANO),
                                           {2, 1, 2, 1}, {true, false, true, true},
                                           {2, 1}, {}, {2, 1});
}

TEST_F(TestHashKernel, UniqueBoolean) {
  CheckUnique<BooleanType, bool>(&this->ctx_, boolean(), {true, true, false, true},
                                 {true, false, true, true}, {true, false}, {});

  CheckUnique<BooleanType, bool>(&this->ctx_, boolean(), {false, true, false, true},
                                 {true, false, true, true}, {false, true}, {});

  // No nulls
  CheckUnique<BooleanType, bool>(&this->ctx_, boolean(), {true, true, false, true}, {},
                                 {true, false}, {});

  CheckUnique<BooleanType, bool>(&this->ctx_, boolean(), {false, true, false, true}, {},
                                 {false, true}, {});
}

TEST_F(TestHashKernel, ValueCountsBoolean) {
  CheckValueCounts<BooleanType, bool>(&this->ctx_, boolean(), {true, true, false, true},
                                      {true, false, true, true}, {true, false}, {},
                                      {2, 1});

  CheckValueCounts<BooleanType, bool>(&this->ctx_, boolean(), {false, true, false, true},
                                      {true, false, true, true}, {false, true}, {},
                                      {2, 1});

  // No nulls
  CheckValueCounts<BooleanType, bool>(&this->ctx_, boolean(), {true, true, false, true},
                                      {}, {true, false}, {}, {3, 1});

  CheckValueCounts<BooleanType, bool>(&this->ctx_, boolean(), {false, true, false, true},
                                      {}, {false, true}, {}, {2, 2});
}

TEST_F(TestHashKernel, DictEncodeBoolean) {
  CheckDictEncode<BooleanType, bool>(
      &this->ctx_, boolean(), {true, true, false, true, false},
      {true, false, true, true, true}, {true, false}, {}, {0, 0, 1, 0, 1});

  CheckDictEncode<BooleanType, bool>(
      &this->ctx_, boolean(), {false, true, false, true, false},
      {true, false, true, true, true}, {false, true}, {}, {0, 0, 0, 1, 0});

  // No nulls
  CheckDictEncode<BooleanType, bool>(&this->ctx_, boolean(),
                                     {true, true, false, true, false}, {}, {true, false},
                                     {}, {0, 0, 1, 0, 1});

  CheckDictEncode<BooleanType, bool>(&this->ctx_, boolean(),
                                     {false, true, false, true, false}, {}, {false, true},
                                     {}, {0, 1, 0, 1, 0});
}

TEST_F(TestHashKernel, UniqueBinary) {
  CheckUnique<BinaryType, std::string>(&this->ctx_, binary(),
                                       {"test", "", "test2", "test"},
                                       {true, false, true, true}, {"test", "test2"}, {});

  CheckUnique<StringType, std::string>(&this->ctx_, utf8(), {"test", "", "test2", "test"},
                                       {true, false, true, true}, {"test", "test2"}, {});
}

TEST_F(TestHashKernel, ValueCountsBinary) {
  CheckValueCounts<BinaryType, std::string>(
      &this->ctx_, binary(), {"test", "", "test2", "test"}, {true, false, true, true},
      {"test", "test2"}, {}, {2, 1});

  CheckValueCounts<StringType, std::string>(
      &this->ctx_, utf8(), {"test", "", "test2", "test"}, {true, false, true, true},
      {"test", "test2"}, {}, {2, 1});
}

TEST_F(TestHashKernel, DictEncodeBinary) {
  CheckDictEncode<BinaryType, std::string>(
      &this->ctx_, binary(), {"test", "", "test2", "test", "baz"},
      {true, false, true, true, true}, {"test", "test2", "baz"}, {}, {0, 0, 1, 0, 2});

  CheckDictEncode<StringType, std::string>(
      &this->ctx_, utf8(), {"test", "", "test2", "test", "baz"},
      {true, false, true, true, true}, {"test", "test2", "baz"}, {}, {0, 0, 1, 0, 2});
}

TEST_F(TestHashKernel, BinaryResizeTable) {
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

  CheckUnique<BinaryType, std::string>(&this->ctx_, binary(), values, {}, uniques, {});
  CheckValueCounts<BinaryType, std::string>(&this->ctx_, binary(), values, {}, uniques,
                                            {}, counts);

  CheckDictEncode<BinaryType, std::string>(&this->ctx_, binary(), values, {}, uniques, {},
                                           indices);

  CheckUnique<StringType, std::string>(&this->ctx_, utf8(), values, {}, uniques, {});
  CheckValueCounts<StringType, std::string>(&this->ctx_, utf8(), values, {}, uniques, {},
                                            counts);
  CheckDictEncode<StringType, std::string>(&this->ctx_, utf8(), values, {}, uniques, {},
                                           indices);
}

TEST_F(TestHashKernel, UniqueFixedSizeBinary) {
  CheckUnique<FixedSizeBinaryType, std::string>(
      &this->ctx_, fixed_size_binary(5), {"aaaaa", "", "bbbbb", "aaaaa"},
      {true, false, true, true}, {"aaaaa", "bbbbb"}, {});
}

TEST_F(TestHashKernel, DictEncodeFixedSizeBinary) {
  CheckDictEncode<FixedSizeBinaryType, std::string>(
      &this->ctx_, fixed_size_binary(5), {"bbbbb", "", "bbbbb", "aaaaa", "ccccc"},
      {true, false, true, true, true}, {"bbbbb", "aaaaa", "ccccc"}, {}, {0, 0, 0, 1, 2});
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
  CheckUnique<FixedSizeBinaryType, std::string>(&this->ctx_, type, values, {}, uniques,
                                                {});
  CheckDictEncode<FixedSizeBinaryType, std::string>(&this->ctx_, type, values, {},
                                                    uniques, {}, indices);
}

TEST_F(TestHashKernel, UniqueDecimal) {
  std::vector<Decimal128> values{12, 12, 11, 12};
  std::vector<Decimal128> expected{12, 11};

  CheckUnique<Decimal128Type, Decimal128>(&this->ctx_, decimal(2, 0), values,
                                          {true, false, true, true}, expected, {});
}

TEST_F(TestHashKernel, ValueCountsDecimal) {
  std::vector<Decimal128> values{12, 12, 11, 12};
  std::vector<Decimal128> expected{12, 11};

  CheckValueCounts<Decimal128Type, Decimal128>(&this->ctx_, decimal(2, 0), values,
                                               {true, false, true, true}, expected, {},
                                               {2, 1});
}

TEST_F(TestHashKernel, DictEncodeDecimal) {
  std::vector<Decimal128> values{12, 12, 11, 12, 13};
  std::vector<Decimal128> expected{12, 11, 13};

  CheckDictEncode<Decimal128Type, Decimal128>(&this->ctx_, decimal(2, 0), values,
                                              {true, false, true, true, true}, expected,
                                              {}, {0, 0, 1, 0, 2});
}

/* TODO(ARROW-4124): Determine if we wan to do something that is reproducable with floats.
TEST_F(TestHashKernel, ValueCountsFloat) {

    // No nulls
  CheckValueCounts<FloatType, float>(&this->ctx_, float32(), {1.0f, 0.0f, -0.0f,
std::nan("1"), std::nan("2")  },
                                      {}, {0.0f, 1.0f, std::nan("1")}, {}, {});

  CheckValueCounts<DoubleType, double>(&this->ctx_, float64(), {1.0f, 0.0f, -0.0f,
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

  std::vector<int64_t> counts = {3, 2, 1, 1};
  auto ex_counts = _MakeArray<Int64Type, int64_t>(int64(), counts, {});

  ArrayVector arrays = {a1, a2};
  auto carr = std::make_shared<ChunkedArray>(arrays);

  // Unique
  std::shared_ptr<Array> result;
  ASSERT_OK(Unique(&this->ctx_, carr, &result));
  ASSERT_ARRAYS_EQUAL(*ex_dict, *result);

  // Dictionary encode
  auto dict_type = dictionary(int32(), type);

  auto i1 = _MakeArray<Int32Type, int32_t>(int32(), {0, 1, 0}, {});
  auto i2 = _MakeArray<Int32Type, int32_t>(int32(), {1, 2, 3, 0}, {});

  ArrayVector dict_arrays = {std::make_shared<DictionaryArray>(dict_type, i1, ex_dict),
                             std::make_shared<DictionaryArray>(dict_type, i2, ex_dict)};
  auto dict_carr = std::make_shared<ChunkedArray>(dict_arrays);

  // Unique counts
  std::shared_ptr<Array> counts_array;
  ASSERT_OK(ValueCounts(&this->ctx_, carr, &counts_array));
  auto counts_struct = std::dynamic_pointer_cast<StructArray>(counts_array);
  ASSERT_ARRAYS_EQUAL(*ex_dict, *counts_struct->field(0));
  ASSERT_ARRAYS_EQUAL(*ex_counts, *counts_struct->field(1));

  // Dictionary encode
  Datum encoded_out;
  ASSERT_OK(DictionaryEncode(&this->ctx_, carr, &encoded_out));
  ASSERT_EQ(Datum::CHUNKED_ARRAY, encoded_out.kind());

  AssertChunkedEqual(*dict_carr, *encoded_out.chunked_array());
}

}  // namespace compute
}  // namespace arrow
