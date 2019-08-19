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
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/isin.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/compute/test_util.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"

namespace arrow {
namespace compute {

// ----------------------------------------------------------------------
// IsIn tests

template <typename Type, typename T = typename TypeTraits<Type>::c_type>
void CheckIsIn(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
               const std::vector<T>& in_values, const std::vector<bool>& in_is_valid,
               const std::vector<T>& member_set_values,
               const std::vector<bool>& member_set_is_valid,
               const std::vector<bool>& out_values,
               const std::vector<bool>& out_is_valid) {
  std::shared_ptr<Array> input = _MakeArray<Type, T>(type, in_values, in_is_valid);
  std::shared_ptr<Array> member_set =
      _MakeArray<Type, T>(type, member_set_values, member_set_is_valid);
  std::shared_ptr<Array> expected =
      _MakeArray<BooleanType, bool>(boolean(), out_values, out_is_valid);

  Datum datum_out;
  ASSERT_OK(IsIn(ctx, input, member_set, &datum_out));
  std::shared_ptr<Array> result = datum_out.make_array();
  ASSERT_ARRAYS_EQUAL(*expected, *result);
}

class TestIsInKernel : public ComputeFixture, public TestBase {};

template <typename Type>
class TestIsInKernelPrimitive : public ComputeFixture, public TestBase {};

typedef ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type,
                         UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType,
                         Date32Type, Date64Type>
    PrimitiveDictionaries;

TYPED_TEST_CASE(TestIsInKernelPrimitive, PrimitiveDictionaries);

TYPED_TEST(TestIsInKernelPrimitive, IsIn) {
  using T = typename TypeParam::c_type;
  auto type = TypeTraits<TypeParam>::type_singleton();

  // No Nulls
  CheckIsIn<TypeParam, T>(&this->ctx_, type, {2, 1, 2, 1, 2, 3},
                          {true, true, true, true, true, true}, {2, 1, 2, 3},
                          {true, true, true, true, true},
                          {true, true, true, true, true, true}, {});
  // Nulls in left array
  CheckIsIn<TypeParam, T>(&this->ctx_, type, {2, 1, 2, 1, 2, 3},
                          {false, false, false, false, false, false}, {2, 1, 2, 1, 3}, {},
                          {false, false, false, false, false, false},
                          {false, false, false, false, false, false});
  // Nulls in right array
  CheckIsIn<TypeParam, T>(&this->ctx_, type, {2, 1, 2, 1, 2, 3}, {}, {2, 1, 2, 3},
                          {false, false, false, false},
                          {false, false, false, false, false, false}, {});
  // Nulls in both the arrays
  CheckIsIn<TypeParam, T>(&this->ctx_, type, {2, 1, 2, 3}, {false, false, false, false},
                          {2, 1, 2, 1, 2, 3, 3},
                          {false, false, false, false, false, false, false},
                          {true, true, true, true}, {});
  // No Match
  CheckIsIn<TypeParam, T>(&this->ctx_, type, {2, 1, 7, 3, 8},
                          {true, false, true, true, true}, {2, 1, 2, 1, 6, 3, 3},
                          {true, false, true, false, true, true, true},
                          {true, true, false, true, false}, {});

  // Empty Arrays
  CheckIsIn<TypeParam, T>(&this->ctx_, type, {}, {}, {}, {}, {}, {});
}

TYPED_TEST(TestIsInKernelPrimitive, PrimitiveResizeTable) {
  using T = typename TypeParam::c_type;

  const int64_t kTotalValues = std::min<int64_t>(INT16_MAX, 1UL << sizeof(T) / 2);
  const int64_t kRepeats = 5;

  std::vector<T> values;
  std::vector<T> member_set;
  std::vector<bool> expected;
  for (int64_t i = 0; i < kTotalValues * kRepeats; i++) {
    const auto val = static_cast<T>(i % kTotalValues);
    values.push_back(val);
    member_set.push_back(val);
    expected.push_back(static_cast<bool>(true));
  }

  auto type = TypeTraits<TypeParam>::type_singleton();
  CheckIsIn<TypeParam, T>(&this->ctx_, type, values, {}, member_set, {}, expected, {});
}

TEST_F(TestIsInKernel, IsInNull) {
  CheckIsIn<NullType, std::nullptr_t>(&this->ctx_, null(), {0, 0, 0},
                                      {false, false, false}, {0, 0, 0},
                                      {false, false, false}, {true, true, true}, {});

  CheckIsIn<NullType, std::nullptr_t>(&this->ctx_, null(), {NULL, NULL, NULL}, {},
                                      {NULL, NULL, NULL, NULL}, {}, {true, true, true},
                                      {});

  CheckIsIn<NullType, std::nullptr_t>(&this->ctx_, null(), {nullptr, nullptr, nullptr},
                                      {}, {nullptr}, {}, {true, true, true}, {});

  // Empty left array
  CheckIsIn<NullType, std::nullptr_t>(&this->ctx_, null(), {}, {},
                                      {nullptr, nullptr, nullptr}, {}, {}, {});

  // Empty right array
  CheckIsIn<NullType, std::nullptr_t>(&this->ctx_, null(), {nullptr, nullptr, nullptr},
                                      {}, {}, {}, {false, false, false},
                                      {false, false, false});

  // Empty arrays
  CheckIsIn<NullType, std::nullptr_t>(&this->ctx_, null(), {}, {}, {}, {}, {}, {});
}

TEST_F(TestIsInKernel, IsInTimeTimestamp) {
  CheckIsIn<Time32Type, int32_t>(
      &this->ctx_, time32(TimeUnit::SECOND), {2, 1, 5, 1}, {true, false, true, true},
      {2, 1, 2, 1}, {true, false, true, true}, {true, true, false, true}, {});

  // Right array has no Nulls
  CheckIsIn<Time32Type, int32_t>(&this->ctx_, time32(TimeUnit::SECOND), {2, 1, 5, 1},
                                 {true, false, true, true}, {2, 1, 1}, {true, true, true},
                                 {true, false, false, true}, {true, false, true, true});

  // No match
  CheckIsIn<Time32Type, int32_t>(&this->ctx_, time32(TimeUnit::SECOND), {3, 5, 5, 3},
                                 {true, false, true, true}, {2, 1, 2, 1, 2},
                                 {true, true, true, true, true},
                                 {false, false, false, false}, {true, false, true, true});

  // Empty arrays
  CheckIsIn<Time32Type, int32_t>(&this->ctx_, time32(TimeUnit::SECOND), {}, {}, {}, {},
                                 {}, {});

  CheckIsIn<Time64Type, int64_t>(&this->ctx_, time64(TimeUnit::NANO), {2, 1, 2, 1},
                                 {true, false, true, true}, {2, 1, 1},
                                 {true, false, true}, {true, true, true, true}, {});

  CheckIsIn<TimestampType, int64_t>(
      &this->ctx_, timestamp(TimeUnit::NANO), {2, 1, 2, 1}, {true, false, true, true},
      {2, 1, 2, 1}, {true, false, true, true}, {true, true, true, true}, {});

  // Empty left array
  CheckIsIn<TimestampType, int64_t>(&this->ctx_, timestamp(TimeUnit::NANO), {}, {},
                                    {2, 1, 2, 1}, {true, false, true, true}, {}, {});

  // Empty right array
  CheckIsIn<TimestampType, int64_t>(
      &this->ctx_, timestamp(TimeUnit::NANO), {2, 1, 2, 1}, {true, false, true, true}, {},
      {}, {false, false, false, false}, {true, false, true, true});

  // Both array have Nulls
  CheckIsIn<Time32Type, int32_t>(&this->ctx_, time32(TimeUnit::SECOND), {2, 1, 2, 1},
                                 {false, false, false, false}, {2, 1}, {false, false},
                                 {true, true, true, true}, {});
}

TEST_F(TestIsInKernel, IsInBoolean) {
  CheckIsIn<BooleanType, bool>(&this->ctx_, boolean(), {false, true, false, true},
                               {true, false, true, true}, {true, false, true},
                               {false, true, true}, {true, true, true, true}, {});

  CheckIsIn<BooleanType, bool>(
      &this->ctx_, boolean(), {false, true, false, true}, {true, false, true, true},
      {false, true, false, true, false}, {true, true, false, true, false},
      {true, true, true, true}, {});

  // No Nulls
  CheckIsIn<BooleanType, bool>(&this->ctx_, boolean(), {true, true, false, true}, {},
                               {false, true}, {}, {true, true, true, true}, {});

  CheckIsIn<BooleanType, bool>(&this->ctx_, boolean(), {false, true, false, true}, {},
                               {true, true, true, true}, {}, {false, true, false, true},
                               {});

  // No match
  CheckIsIn<BooleanType, bool>(&this->ctx_, boolean(), {true, true, true, true}, {},
                               {false, false, false, false, false}, {},
                               {false, false, false, false}, {});

  // Nulls in left array
  CheckIsIn<BooleanType, bool>(
      &this->ctx_, boolean(), {false, true, false, true}, {false, false, false, false},
      {true, true}, {}, {false, false, false, false}, {false, false, false, false});

  // Nulls in right array
  CheckIsIn<BooleanType, bool>(&this->ctx_, boolean(), {true, true, false, true}, {},
                               {true, true, false, true, true},
                               {false, false, false, false, false},
                               {false, false, false, false}, {});

  // Both array have Nulls
  CheckIsIn<BooleanType, bool>(&this->ctx_, boolean(), {false, true, false, true},
                               {false, false, false, false}, {true, true, true, true},
                               {false, false, false, false}, {true, true, true, true},
                               {});
}

template <typename Type>
class TestIsInKernelBinary : public ComputeFixture, public TestBase {};

using BinaryTypes = ::testing::Types<BinaryType, StringType>;
TYPED_TEST_CASE(TestIsInKernelBinary, BinaryTypes);

TYPED_TEST(TestIsInKernelBinary, IsInBinary) {
  auto type = TypeTraits<TypeParam>::type_singleton();
  CheckIsIn<TypeParam, std::string>(&this->ctx_, type, {"test", "", "test2", "test"},
                                    {true, false, true, true}, {"test", "", "test2"},
                                    {true, false, true}, {true, true, true, true}, {});

  // No match
  CheckIsIn<TypeParam, std::string>(
      &this->ctx_, type, {"test", "", "test2", "test"}, {true, false, true, true},
      {"test3", "test4", "test3", "test4"}, {true, true, true, true},
      {false, false, false, false}, {true, false, true, true});

  // Nulls in left array
  CheckIsIn<TypeParam, std::string>(
      &this->ctx_, type, {"test", "", "test2", "test"}, {false, false, false, false},
      {"test", "test2", "test"}, {true, true, true}, {false, false, false, false},
      {false, false, false, false});

  // Nulls in right array
  CheckIsIn<TypeParam, std::string>(&this->ctx_, type, {"test", "test2", "test"},
                                    {true, true, true}, {"test", "", "test2", "test"},
                                    {false, false, false, false}, {false, false, false},
                                    {});

  // Both array have Nulls
  CheckIsIn<TypeParam, std::string>(
      &this->ctx_, type, {"test", "", "test2", "test"}, {false, false, false, false},
      {"test", "", "test2", "test"}, {false, false, false, false},
      {true, true, true, true}, {});

  // Empty arrays
  CheckIsIn<TypeParam, std::string>(&this->ctx_, type, {}, {}, {}, {}, {}, {});

  // Empty left array
  CheckIsIn<TypeParam, std::string>(&this->ctx_, type, {}, {},
                                    {"test", "", "test2", "test"},
                                    {true, false, true, false}, {}, {});

  // Empty right array
  CheckIsIn<TypeParam, std::string>(
      &this->ctx_, type, {"test", "", "test2", "test"}, {true, false, true, true}, {}, {},
      {false, false, false, false}, {true, false, true, true});
}

TEST_F(TestIsInKernel, BinaryResizeTable) {
  const int32_t kTotalValues = 10000;
#if !defined(ARROW_VALGRIND)
  const int32_t kRepeats = 10;
#else
  // Mitigate Valgrind's slowness
  const int32_t kRepeats = 3;
#endif

  std::vector<std::string> values;
  std::vector<std::string> member_set;
  std::vector<bool> expected;
  char buf[20] = "test";

  for (int32_t i = 0; i < kTotalValues * kRepeats; i++) {
    int32_t index = i % kTotalValues;

    ASSERT_GE(snprintf(buf + 4, sizeof(buf) - 4, "%d", index), 0);
    values.emplace_back(buf);
    member_set.emplace_back(buf);
    expected.push_back(true);
  }

  CheckIsIn<BinaryType, std::string>(&this->ctx_, binary(), values, {}, member_set, {},
                                     expected, {});

  CheckIsIn<StringType, std::string>(&this->ctx_, utf8(), values, {}, member_set, {},
                                     expected, {});
}

TEST_F(TestIsInKernel, IsInFixedSizeBinary) {
  CheckIsIn<FixedSizeBinaryType, std::string>(
      &this->ctx_, fixed_size_binary(5), {"bbbbb", "", "aaaaa", "ccccc"},
      {true, false, true, true}, {"bbbbb", "", "bbbbb", "aaaaa", "ccccc"},
      {true, false, true, true, true}, {true, true, true, true}, {});

  // Nulls in left
  CheckIsIn<FixedSizeBinaryType, std::string>(
      &this->ctx_, fixed_size_binary(5), {"bbbbb", "", "bbbbb", "aaaaa", "ccccc"},
      {false, false, false, false, false}, {"bbbbb", "aabbb", "bbbbb", "aaaaa", "ccccc"},
      {true, true, true, true, true}, {false, false, false, false, false},
      {false, false, false, false, false});

  // Nulls in right
  CheckIsIn<FixedSizeBinaryType, std::string>(
      &this->ctx_, fixed_size_binary(5), {"bbbbb", "", "bbbbb", "aaaaa", "ccccc"},
      {true, false, true, true, true}, {"bbbbb", "", "bbbbb"}, {false, false, false},
      {false, true, false, false, false}, {});

  // Both array have Nulls
  CheckIsIn<FixedSizeBinaryType, std::string>(
      &this->ctx_, fixed_size_binary(5), {"bbbbb", "", "bbbbb", "aaaaa", "ccccc"},
      {false, false, false, false, false}, {"", "", "bbbbb", "aaaaa"},
      {false, false, false, false}, {true, true, true, true, true}, {});

  // No match
  CheckIsIn<FixedSizeBinaryType, std::string>(
      &this->ctx_, fixed_size_binary(5), {"bbbbc", "bbbbc", "aaaad", "cccca"},
      {true, true, true, true}, {"bbbbb", "", "bbbbb", "aaaaa", "ddddd"},
      {true, false, true, true, true}, {false, false, false, false}, {});

  // Empty left array
  CheckIsIn<FixedSizeBinaryType, std::string>(&this->ctx_, fixed_size_binary(5), {}, {},
                                              {"bbbbb", "", "bbbbb", "aaaaa", "ccccc"},
                                              {true, false, true, true, true}, {}, {});

  // Empty right array
  CheckIsIn<FixedSizeBinaryType, std::string>(
      &this->ctx_, fixed_size_binary(5), {"bbbbb", "", "bbbbb", "aaaaa", "ccccc"},
      {true, false, true, true, true}, {}, {}, {false, false, false, false, false},
      {true, false, true, true, true});

  // Empty arrays
  CheckIsIn<FixedSizeBinaryType, std::string>(&this->ctx_, fixed_size_binary(0), {}, {},
                                              {}, {}, {}, {});
}

TEST_F(TestIsInKernel, IsInDecimal) {
  std::vector<Decimal128> input{12, 12, 11, 12};
  std::vector<Decimal128> member_set{12, 12, 11, 12};
  std::vector<bool> expected{true, true, true, true};

  CheckIsIn<Decimal128Type, Decimal128>(&this->ctx_, decimal(2, 0), input,
                                        {true, false, true, true}, member_set,
                                        {true, false, true, true}, expected, {});
}

TEST_F(TestIsInKernel, IsInChunkedArrayInvoke) {
  std::vector<std::string> values1 = {"foo", "bar", "foo"};
  std::vector<std::string> values2 = {"bar", "baz", "quuux", "foo"};
  std::vector<std::string> values3 = {"foo", "bar", "foo"};
  std::vector<std::string> values4 = {"bar", "baz", "barr", "foo"};

  auto type = utf8();
  auto a1 = _MakeArray<StringType, std::string>(type, values1, {});
  auto a2 = _MakeArray<StringType, std::string>(type, values2, {true, true, true, false});
  auto a3 = _MakeArray<StringType, std::string>(type, values3, {});
  auto a4 = _MakeArray<StringType, std::string>(type, values4, {});

  ArrayVector array1 = {a1, a2};
  auto carr = std::make_shared<ChunkedArray>(array1);
  ArrayVector array2 = {a3, a4};
  auto member_set = std::make_shared<ChunkedArray>(array2);

  auto i1 = _MakeArray<BooleanType, bool>(boolean(), {true, true, true}, {});
  auto i2 = _MakeArray<BooleanType, bool>(boolean(), {true, true, false, false},
                                          {true, true, true, false});

  ArrayVector expected = {i1, i2};
  auto expected_carr = std::make_shared<ChunkedArray>(expected);

  Datum encoded_out;
  ASSERT_OK(IsIn(&this->ctx_, carr, member_set, &encoded_out));
  ASSERT_EQ(Datum::CHUNKED_ARRAY, encoded_out.kind());

  AssertChunkedEqual(*expected_carr, *encoded_out.chunked_array());
}

}  // namespace compute
}  // namespace arrow
