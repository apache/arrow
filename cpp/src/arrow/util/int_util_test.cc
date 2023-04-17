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
#include <limits>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/datum.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/int_util.h"
#include "arrow/util/int_util_overflow.h"

namespace arrow {
namespace internal {

static std::vector<uint8_t> all_widths = {1, 2, 4, 8};

template <typename T>
void CheckUIntWidth(const std::vector<T>& values, uint8_t expected_width) {
  for (const uint8_t min_width : all_widths) {
    uint8_t width =
        DetectUIntWidth(values.data(), static_cast<int64_t>(values.size()), min_width);
    ASSERT_EQ(width, std::max(min_width, expected_width));
    width = DetectUIntWidth(values.data(), nullptr, static_cast<int64_t>(values.size()),
                            min_width);
    ASSERT_EQ(width, std::max(min_width, expected_width));
  }
}

template <typename T>
void CheckUIntWidth(const std::vector<T>& values, const std::vector<uint8_t>& valid_bytes,
                    uint8_t expected_width) {
  for (const uint8_t min_width : all_widths) {
    uint8_t width = DetectUIntWidth(values.data(), valid_bytes.data(),
                                    static_cast<int64_t>(values.size()), min_width);
    ASSERT_EQ(width, std::max(min_width, expected_width));
  }
}

template <typename T>
void CheckIntWidth(const std::vector<T>& values, uint8_t expected_width) {
  for (const uint8_t min_width : all_widths) {
    uint8_t width =
        DetectIntWidth(values.data(), static_cast<int64_t>(values.size()), min_width);
    ASSERT_EQ(width, std::max(min_width, expected_width));
    width = DetectIntWidth(values.data(), nullptr, static_cast<int64_t>(values.size()),
                           min_width);
    ASSERT_EQ(width, std::max(min_width, expected_width));
  }
}

template <typename T>
void CheckIntWidth(const std::vector<T>& values, const std::vector<uint8_t>& valid_bytes,
                   uint8_t expected_width) {
  for (const uint8_t min_width : all_widths) {
    uint8_t width = DetectIntWidth(values.data(), valid_bytes.data(),
                                   static_cast<int64_t>(values.size()), min_width);
    ASSERT_EQ(width, std::max(min_width, expected_width));
  }
}

template <typename T>
std::vector<T> MakeRandomVector(const std::vector<T>& base_values, int n_values) {
  std::default_random_engine gen(42);
  std::uniform_int_distribution<int> index_dist(0,
                                                static_cast<int>(base_values.size() - 1));

  std::vector<T> values(n_values);
  for (int i = 0; i < n_values; ++i) {
    values[i] = base_values[index_dist(gen)];
  }
  return values;
}

template <typename T>
std::vector<std::pair<std::vector<T>, std::vector<uint8_t>>> AlmostAllNullValues(
    int n_values, T null_value, T non_null_value) {
  std::vector<std::pair<std::vector<T>, std::vector<uint8_t>>> vectors;
  vectors.reserve(n_values);
  for (int i = 0; i < n_values; ++i) {
    std::vector<T> values(n_values, null_value);
    std::vector<uint8_t> valid_bytes(n_values, 0);
    values[i] = non_null_value;
    valid_bytes[i] = 1;
    vectors.push_back({std::move(values), std::move(valid_bytes)});
  }
  return vectors;
}

template <typename T>
std::vector<std::vector<T>> AlmostAllZeros(int n_values, T nonzero_value) {
  std::vector<std::vector<T>> vectors;
  vectors.reserve(n_values);
  for (int i = 0; i < n_values; ++i) {
    std::vector<T> values(n_values, 0);
    values[i] = nonzero_value;
    vectors.push_back(std::move(values));
  }
  return vectors;
}

std::vector<uint64_t> valid_uint8 = {0, 0x7f, 0xff};
std::vector<uint64_t> valid_uint16 = {0, 0x7f, 0xff, 0x1000, 0xffff};
std::vector<uint64_t> valid_uint32 = {0, 0x7f, 0xff, 0x10000, 0xffffffffULL};
std::vector<uint64_t> valid_uint64 = {0, 0x100000000ULL, 0xffffffffffffffffULL};

TEST(UIntWidth, NoNulls) {
  std::vector<uint64_t> values{0, 0x7f, 0xff};
  CheckUIntWidth(values, 1);

  values = {0, 0x100};
  CheckUIntWidth(values, 2);

  values = {0, 0xffff};
  CheckUIntWidth(values, 2);

  values = {0, 0x10000};
  CheckUIntWidth(values, 4);

  values = {0, 0xffffffffULL};
  CheckUIntWidth(values, 4);

  values = {0, 0x100000000ULL};
  CheckUIntWidth(values, 8);

  values = {0, 0xffffffffffffffffULL};
  CheckUIntWidth(values, 8);
}

TEST(UIntWidth, Nulls) {
  std::vector<uint8_t> valid10{true, false};
  std::vector<uint8_t> valid01{false, true};

  std::vector<uint64_t> values{0, 0xff};
  CheckUIntWidth(values, valid01, 1);
  CheckUIntWidth(values, valid10, 1);

  values = {0, 0x100};
  CheckUIntWidth(values, valid01, 2);
  CheckUIntWidth(values, valid10, 1);

  values = {0, 0xffff};
  CheckUIntWidth(values, valid01, 2);
  CheckUIntWidth(values, valid10, 1);

  values = {0, 0x10000};
  CheckUIntWidth(values, valid01, 4);
  CheckUIntWidth(values, valid10, 1);

  values = {0, 0xffffffffULL};
  CheckUIntWidth(values, valid01, 4);
  CheckUIntWidth(values, valid10, 1);

  values = {0, 0x100000000ULL};
  CheckUIntWidth(values, valid01, 8);
  CheckUIntWidth(values, valid10, 1);

  values = {0, 0xffffffffffffffffULL};
  CheckUIntWidth(values, valid01, 8);
  CheckUIntWidth(values, valid10, 1);
}

TEST(UIntWidth, NoNullsMany) {
  constexpr int N = 40;
  for (const auto& values : AlmostAllZeros<uint64_t>(N, 0xff)) {
    CheckUIntWidth(values, 1);
  }
  for (const auto& values : AlmostAllZeros<uint64_t>(N, 0xffff)) {
    CheckUIntWidth(values, 2);
  }
  for (const auto& values : AlmostAllZeros<uint64_t>(N, 0xffffffffULL)) {
    CheckUIntWidth(values, 4);
  }
  for (const auto& values : AlmostAllZeros<uint64_t>(N, 0xffffffffffffffffULL)) {
    CheckUIntWidth(values, 8);
  }
  auto values = MakeRandomVector(valid_uint8, N);
  CheckUIntWidth(values, 1);

  values = MakeRandomVector(valid_uint16, N);
  CheckUIntWidth(values, 2);

  values = MakeRandomVector(valid_uint32, N);
  CheckUIntWidth(values, 4);

  values = MakeRandomVector(valid_uint64, N);
  CheckUIntWidth(values, 8);
}

TEST(UIntWidth, NullsMany) {
  constexpr uint64_t huge = 0x123456789abcdefULL;
  constexpr int N = 40;
  for (const auto& p : AlmostAllNullValues<uint64_t>(N, 0, 0xff)) {
    CheckUIntWidth(p.first, p.second, 1);
  }
  for (const auto& p : AlmostAllNullValues<uint64_t>(N, huge, 0xff)) {
    CheckUIntWidth(p.first, p.second, 1);
  }
  for (const auto& p : AlmostAllNullValues<uint64_t>(N, 0, 0xffff)) {
    CheckUIntWidth(p.first, p.second, 2);
  }
  for (const auto& p : AlmostAllNullValues<uint64_t>(N, huge, 0xffff)) {
    CheckUIntWidth(p.first, p.second, 2);
  }
  for (const auto& p : AlmostAllNullValues<uint64_t>(N, 0, 0xffffffffULL)) {
    CheckUIntWidth(p.first, p.second, 4);
  }
  for (const auto& p : AlmostAllNullValues<uint64_t>(N, huge, 0xffffffffULL)) {
    CheckUIntWidth(p.first, p.second, 4);
  }
  for (const auto& p : AlmostAllNullValues<uint64_t>(N, 0, 0xffffffffffffffffULL)) {
    CheckUIntWidth(p.first, p.second, 8);
  }
  for (const auto& p : AlmostAllNullValues<uint64_t>(N, huge, 0xffffffffffffffffULL)) {
    CheckUIntWidth(p.first, p.second, 8);
  }
}

TEST(IntWidth, NoNulls) {
  std::vector<int64_t> values{0, 0x7f, -0x80};
  CheckIntWidth(values, 1);

  values = {0, 0x80};
  CheckIntWidth(values, 2);

  values = {0, -0x81};
  CheckIntWidth(values, 2);

  values = {0, 0x7fff, -0x8000};
  CheckIntWidth(values, 2);

  values = {0, 0x8000};
  CheckIntWidth(values, 4);

  values = {0, -0x8001};
  CheckIntWidth(values, 4);

  values = {0, 0x7fffffffLL, -0x80000000LL};
  CheckIntWidth(values, 4);

  values = {0, 0x80000000LL};
  CheckIntWidth(values, 8);

  values = {0, -0x80000001LL};
  CheckIntWidth(values, 8);

  values = {0, 0x7fffffffffffffffLL, -0x7fffffffffffffffLL - 1};
  CheckIntWidth(values, 8);
}

TEST(IntWidth, Nulls) {
  std::vector<uint8_t> valid100{true, false, false};
  std::vector<uint8_t> valid010{false, true, false};
  std::vector<uint8_t> valid001{false, false, true};

  std::vector<int64_t> values{0, 0x7f, -0x80};
  CheckIntWidth(values, valid100, 1);
  CheckIntWidth(values, valid010, 1);
  CheckIntWidth(values, valid001, 1);

  values = {0, 0x80, -0x81};
  CheckIntWidth(values, valid100, 1);
  CheckIntWidth(values, valid010, 2);
  CheckIntWidth(values, valid001, 2);

  values = {0, 0x7fff, -0x8000};
  CheckIntWidth(values, valid100, 1);
  CheckIntWidth(values, valid010, 2);
  CheckIntWidth(values, valid001, 2);

  values = {0, 0x8000, -0x8001};
  CheckIntWidth(values, valid100, 1);
  CheckIntWidth(values, valid010, 4);
  CheckIntWidth(values, valid001, 4);

  values = {0, 0x7fffffffLL, -0x80000000LL};
  CheckIntWidth(values, valid100, 1);
  CheckIntWidth(values, valid010, 4);
  CheckIntWidth(values, valid001, 4);

  values = {0, 0x80000000LL, -0x80000001LL};
  CheckIntWidth(values, valid100, 1);
  CheckIntWidth(values, valid010, 8);
  CheckIntWidth(values, valid001, 8);

  values = {0, 0x7fffffffffffffffLL, -0x7fffffffffffffffLL - 1};
  CheckIntWidth(values, valid100, 1);
  CheckIntWidth(values, valid010, 8);
  CheckIntWidth(values, valid001, 8);
}

TEST(IntWidth, NoNullsMany) {
  constexpr int N = 40;
  // 1 byte wide
  for (const int64_t value : {0x7f, -0x80}) {
    for (const auto& values : AlmostAllZeros<int64_t>(N, value)) {
      CheckIntWidth(values, 1);
    }
  }
  // 2 bytes wide
  for (const int64_t value : {0x80, -0x81, 0x7fff, -0x8000}) {
    for (const auto& values : AlmostAllZeros<int64_t>(N, value)) {
      CheckIntWidth(values, 2);
    }
  }
  // 4 bytes wide
  for (const int64_t value : {0x8000LL, -0x8001LL, 0x7fffffffLL, -0x80000000LL}) {
    for (const auto& values : AlmostAllZeros<int64_t>(N, value)) {
      CheckIntWidth(values, 4);
    }
  }
  // 8 bytes wide
  for (const int64_t value : {0x80000000LL, -0x80000001LL, 0x7fffffffffffffffLL}) {
    for (const auto& values : AlmostAllZeros<int64_t>(N, value)) {
      CheckIntWidth(values, 8);
    }
  }
}

TEST(IntWidth, NullsMany) {
  constexpr int64_t huge = 0x123456789abcdefLL;
  constexpr int N = 40;
  // 1 byte wide
  for (const int64_t value : {0x7f, -0x80}) {
    for (const auto& p : AlmostAllNullValues<int64_t>(N, 0, value)) {
      CheckIntWidth(p.first, p.second, 1);
    }
    for (const auto& p : AlmostAllNullValues<int64_t>(N, huge, value)) {
      CheckIntWidth(p.first, p.second, 1);
    }
  }
  // 2 bytes wide
  for (const int64_t value : {0x80, -0x81, 0x7fff, -0x8000}) {
    for (const auto& p : AlmostAllNullValues<int64_t>(N, 0, value)) {
      CheckIntWidth(p.first, p.second, 2);
    }
    for (const auto& p : AlmostAllNullValues<int64_t>(N, huge, value)) {
      CheckIntWidth(p.first, p.second, 2);
    }
  }
  // 4 bytes wide
  for (const int64_t value : {0x8000LL, -0x8001LL, 0x7fffffffLL, -0x80000000LL}) {
    for (const auto& p : AlmostAllNullValues<int64_t>(N, 0, value)) {
      CheckIntWidth(p.first, p.second, 4);
    }
    for (const auto& p : AlmostAllNullValues<int64_t>(N, huge, value)) {
      CheckIntWidth(p.first, p.second, 4);
    }
  }
  // 8 bytes wide
  for (const int64_t value : {0x80000000LL, -0x80000001LL, 0x7fffffffffffffffLL}) {
    for (const auto& p : AlmostAllNullValues<int64_t>(N, 0, value)) {
      CheckIntWidth(p.first, p.second, 8);
    }
    for (const auto& p : AlmostAllNullValues<int64_t>(N, huge, value)) {
      CheckIntWidth(p.first, p.second, 8);
    }
  }
}

TEST(TransposeInts, Int8ToInt64) {
  std::vector<int8_t> src = {1, 3, 5, 0, 3, 2};
  std::vector<int32_t> transpose_map = {1111, 2222, 3333, 4444, 5555, 6666, 7777};
  std::vector<int64_t> dest(src.size());

  TransposeInts(src.data(), dest.data(), 6, transpose_map.data());
  ASSERT_EQ(dest, std::vector<int64_t>({2222, 4444, 6666, 1111, 4444, 3333}));
}

void BoundsCheckPasses(const std::shared_ptr<DataType>& type,
                       const std::string& indices_json, uint64_t upper_limit) {
  auto indices = ArrayFromJSON(type, indices_json);
  ASSERT_OK(CheckIndexBounds(*indices->data(), upper_limit));
}

void BoundsCheckFails(const std::shared_ptr<DataType>& type,
                      const std::string& indices_json, uint64_t upper_limit) {
  auto indices = ArrayFromJSON(type, indices_json);
  ASSERT_RAISES(IndexError, CheckIndexBounds(*indices->data(), upper_limit));
}

TEST(CheckIndexBounds, Batching) {
  auto rand = random::RandomArrayGenerator(/*seed=*/0);

  const int64_t length = 200;

  auto indices = rand.Int16(length, 0, 0, /*null_probability=*/0);
  ArrayData* index_data = indices->data().get();
  index_data->buffers[0] = *AllocateBitmap(length);

  int16_t* values = index_data->GetMutableValues<int16_t>(1);
  uint8_t* bitmap = index_data->buffers[0]->mutable_data();
  bit_util::SetBitsTo(bitmap, 0, length, true);

  ArraySpan index_span(*index_data);
  ASSERT_OK(CheckIndexBounds(index_span, 1));

  // We'll place out of bounds indices at various locations
  values[99] = 1;
  ASSERT_RAISES(IndexError, CheckIndexBounds(index_span, 1));

  // Make that value null
  bit_util::ClearBit(bitmap, 99);
  ASSERT_OK(CheckIndexBounds(index_span, 1));

  values[199] = 1;
  ASSERT_RAISES(IndexError, CheckIndexBounds(index_span, 1));

  // Make that value null
  bit_util::ClearBit(bitmap, 199);
  ASSERT_OK(CheckIndexBounds(index_span, 1));
}

TEST(CheckIndexBounds, SignedInts) {
  auto CheckCommon = [&](const std::shared_ptr<DataType>& ty) {
    BoundsCheckPasses(ty, "[0, 0, 0]", 1);
    BoundsCheckFails(ty, "[0, 0, 0]", 0);
    BoundsCheckFails(ty, "[-1]", 1);
    BoundsCheckFails(ty, "[-128]", 1);
    BoundsCheckFails(ty, "[0, 100, 127]", 127);
    BoundsCheckPasses(ty, "[0, 100, 127]", 128);
  };

  CheckCommon(int8());

  CheckCommon(int16());
  BoundsCheckPasses(int16(), "[0, 999, 999]", 1000);
  BoundsCheckFails(int16(), "[0, 1000, 1000]", 1000);
  BoundsCheckPasses(int16(), "[0, 32767]", 1 << 15);

  CheckCommon(int32());
  BoundsCheckPasses(int32(), "[0, 999999, 999999]", 1000000);
  BoundsCheckFails(int32(), "[0, 1000000, 1000000]", 1000000);
  BoundsCheckPasses(int32(), "[0, 2147483647]", 1LL << 31);

  CheckCommon(int64());
  BoundsCheckPasses(int64(), "[0, 9999999999, 9999999999]", 10000000000LL);
  BoundsCheckFails(int64(), "[0, 10000000000, 10000000000]", 10000000000LL);
}

TEST(CheckIndexBounds, UnsignedInts) {
  auto CheckCommon = [&](const std::shared_ptr<DataType>& ty) {
    BoundsCheckPasses(ty, "[0, 0, 0]", 1);
    BoundsCheckFails(ty, "[0, 0, 0]", 0);
    BoundsCheckFails(ty, "[0, 100, 200]", 200);
    BoundsCheckPasses(ty, "[0, 100, 200]", 201);
  };

  CheckCommon(uint8());
  BoundsCheckPasses(uint8(), "[255, 255, 255]", 1000);
  BoundsCheckFails(uint8(), "[255, 255, 255]", 255);

  CheckCommon(uint16());
  BoundsCheckPasses(uint16(), "[0, 999, 999]", 1000);
  BoundsCheckFails(uint16(), "[0, 1000, 1000]", 1000);
  BoundsCheckPasses(uint16(), "[0, 65535]", 1 << 16);

  CheckCommon(uint32());
  BoundsCheckPasses(uint32(), "[0, 999999, 999999]", 1000000);
  BoundsCheckFails(uint32(), "[0, 1000000, 1000000]", 1000000);
  BoundsCheckPasses(uint32(), "[0, 4294967295]", 1LL << 32);

  CheckCommon(uint64());
  BoundsCheckPasses(uint64(), "[0, 9999999999, 9999999999]", 10000000000LL);
  BoundsCheckFails(uint64(), "[0, 10000000000, 10000000000]", 10000000000LL);
}

void CheckInRangePasses(const std::shared_ptr<DataType>& type,
                        const std::string& values_json, const std::string& limits_json) {
  auto values = ArrayFromJSON(type, values_json);
  auto limits = ArrayFromJSON(type, limits_json);
  ASSERT_OK(CheckIntegersInRange(*values->data(), **limits->GetScalar(0),
                                 **limits->GetScalar(1)));
}

void CheckInRangeFails(const std::shared_ptr<DataType>& type,
                       const std::string& values_json, const std::string& limits_json) {
  auto values = ArrayFromJSON(type, values_json);
  auto limits = ArrayFromJSON(type, limits_json);
  ASSERT_RAISES(Invalid, CheckIntegersInRange(*values->data(), **limits->GetScalar(0),
                                              **limits->GetScalar(1)));
}

TEST(CheckIntegersInRange, Batching) {
  auto rand = random::RandomArrayGenerator(/*seed=*/0);

  const int64_t length = 200;

  auto indices = rand.Int16(length, 0, 0, /*null_probability=*/0);
  ArrayData* index_data = indices->data().get();
  index_data->buffers[0] = *AllocateBitmap(length);

  int16_t* values = index_data->GetMutableValues<int16_t>(1);
  uint8_t* bitmap = index_data->buffers[0]->mutable_data();
  bit_util::SetBitsTo(bitmap, 0, length, true);

  auto zero = std::make_shared<Int16Scalar>(0);
  auto one = std::make_shared<Int16Scalar>(1);

  ArraySpan index_span(*index_data);
  ASSERT_OK(CheckIntegersInRange(index_span, *zero, *one));

  // 1 is included
  values[99] = 1;
  ASSERT_OK(CheckIntegersInRange(index_span, *zero, *one));

  // We'll place out of bounds indices at various locations
  values[99] = 2;
  ASSERT_RAISES(Invalid, CheckIntegersInRange(index_span, *zero, *one));

  // Make that value null
  bit_util::ClearBit(bitmap, 99);
  ASSERT_OK(CheckIntegersInRange(index_span, *zero, *one));

  values[199] = 2;
  ASSERT_RAISES(Invalid, CheckIntegersInRange(index_span, *zero, *one));

  // Make that value null
  bit_util::ClearBit(bitmap, 199);
  ASSERT_OK(CheckIntegersInRange(index_span, *zero, *one));
}

TEST(CheckIntegersInRange, SignedInts) {
  auto CheckCommon = [&](const std::shared_ptr<DataType>& ty) {
    CheckInRangePasses(ty, "[0, 0, 0]", "[0, 0]");
    CheckInRangeFails(ty, "[0, 1, 0]", "[0, 0]");
    CheckInRangeFails(ty, "[1, 1, 1]", "[2, 4]");
    CheckInRangeFails(ty, "[-1]", "[0, 0]");
    CheckInRangeFails(ty, "[-128]", "[-127, 0]");
    CheckInRangeFails(ty, "[0, 100, 127]", "[0, 126]");
    CheckInRangePasses(ty, "[0, 100, 127]", "[0, 127]");
  };

  CheckCommon(int8());

  CheckCommon(int16());
  CheckInRangePasses(int16(), "[0, 999, 999]", "[0, 999]");
  CheckInRangeFails(int16(), "[0, 1000, 1000]", "[0, 999]");

  CheckCommon(int32());
  CheckInRangePasses(int32(), "[0, 999999, 999999]", "[0, 999999]");
  CheckInRangeFails(int32(), "[0, 1000000, 1000000]", "[0, 999999]");

  CheckCommon(int64());
  CheckInRangePasses(int64(), "[0, 9999999999, 9999999999]", "[0, 9999999999]");
  CheckInRangeFails(int64(), "[0, 10000000000, 10000000000]", "[0, 9999999999]");
}

TEST(CheckIntegersInRange, UnsignedInts) {
  auto CheckCommon = [&](const std::shared_ptr<DataType>& ty) {
    CheckInRangePasses(ty, "[0, 0, 0]", "[0, 0]");
    CheckInRangeFails(ty, "[0, 1, 0]", "[0, 0]");
    CheckInRangeFails(ty, "[1, 1, 1]", "[2, 4]");
    CheckInRangeFails(ty, "[0, 100, 200]", "[0, 199]");
    CheckInRangePasses(ty, "[0, 100, 200]", "[0, 200]");
  };

  CheckCommon(uint8());
  CheckInRangePasses(uint8(), "[255, 255, 255]", "[0, 255]");

  CheckCommon(uint16());
  CheckInRangePasses(uint16(), "[0, 999, 999]", "[0, 999]");
  CheckInRangeFails(uint16(), "[0, 1000, 1000]", "[0, 999]");
  CheckInRangePasses(uint16(), "[0, 65535]", "[0, 65535]");

  CheckCommon(uint32());
  CheckInRangePasses(uint32(), "[0, 999999, 999999]", "[0, 999999]");
  CheckInRangeFails(uint32(), "[0, 1000000, 1000000]", "[0, 999999]");
  CheckInRangePasses(uint32(), "[0, 4294967295]", "[0, 4294967295]");

  CheckCommon(uint64());
  CheckInRangePasses(uint64(), "[0, 9999999999, 9999999999]", "[0, 9999999999]");
  CheckInRangeFails(uint64(), "[0, 10000000000, 10000000000]", "[0, 9999999999]");
}

template <typename T>
class TestAddWithOverflow : public ::testing::Test {
 public:
  void CheckOk(T a, T b, T expected_result = {}) {
    ARROW_SCOPED_TRACE("a = ", a, ", b = ", b);
    T result;
    ASSERT_FALSE(AddWithOverflow(a, b, &result));
    ASSERT_EQ(result, expected_result);
  }

  void CheckOverflow(T a, T b) {
    ARROW_SCOPED_TRACE("a = ", a, ", b = ", b);
    T result;
    ASSERT_TRUE(AddWithOverflow(a, b, &result));
  }
};

using SignedIntegerTypes = ::testing::Types<int8_t, int16_t, int32_t, int64_t>;

TYPED_TEST_SUITE(TestAddWithOverflow, SignedIntegerTypes);

TYPED_TEST(TestAddWithOverflow, Basics) {
  using T = TypeParam;

  const T almost_max = std::numeric_limits<T>::max() - T{2};
  const T almost_min = std::numeric_limits<T>::min() + T{2};

  this->CheckOk(T{1}, T{2}, T{3});
  this->CheckOk(T{-1}, T{2}, T{1});
  this->CheckOk(T{-1}, T{-2}, T{-3});

  this->CheckOk(almost_min, T{0}, almost_min);
  this->CheckOk(almost_min, T{-2}, almost_min - T{2});
  this->CheckOk(almost_min, T{1}, almost_min + T{1});
  this->CheckOverflow(almost_min, T{-3});
  this->CheckOverflow(almost_min, almost_min);

  this->CheckOk(almost_max, T{0}, almost_max);
  this->CheckOk(almost_max, T{2}, almost_max + T{2});
  this->CheckOk(almost_max, T{-1}, almost_max - T{1});
  this->CheckOverflow(almost_max, T{3});
  this->CheckOverflow(almost_max, almost_max);

  // In 2's complement, almost_min == - almost_max - 1
  this->CheckOk(almost_min, almost_max, T{-1});
  this->CheckOk(almost_max, almost_min, T{-1});
  this->CheckOk(almost_min - T{1}, almost_max, T{-2});
  this->CheckOk(almost_min + T{1}, almost_max, T{0});
  this->CheckOk(almost_min + T{2}, almost_max, T{1});
  this->CheckOk(almost_min, almost_max - T{1}, T{-2});
  this->CheckOk(almost_min, almost_max + T{1}, T{0});
  this->CheckOk(almost_min, almost_max + T{2}, T{1});
}

}  // namespace internal
}  // namespace arrow
