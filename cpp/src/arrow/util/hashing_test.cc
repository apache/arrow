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

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace internal {

template <typename Integer>
static std::unordered_set<Integer> MakeDistinctIntegers(int32_t n_values) {
  std::default_random_engine gen(42);
  std::uniform_int_distribution<Integer> values_dist(0,
                                                     std::numeric_limits<Integer>::max());

  std::unordered_set<Integer> values;
  values.reserve(n_values);

  while (values.size() < static_cast<uint32_t>(n_values)) {
    values.insert(static_cast<Integer>(values_dist(gen)));
  }
  return values;
}

template <typename Integer>
static std::unordered_set<Integer> MakeSequentialIntegers(int32_t n_values) {
  std::unordered_set<Integer> values;
  values.reserve(n_values);

  for (int32_t i = 0; i < n_values; ++i) {
    values.insert(static_cast<Integer>(i));
  }
  DCHECK_EQ(values.size(), static_cast<uint32_t>(n_values));
  return values;
}

static std::unordered_set<std::string> MakeDistinctStrings(int32_t n_values) {
  std::unordered_set<std::string> values;
  values.reserve(n_values);

  // Generate strings between 0 and 24 bytes, with ASCII characters
  std::default_random_engine gen(42);
  std::uniform_int_distribution<int32_t> length_dist(0, 24);
  std::uniform_int_distribution<uint32_t> char_dist('0', 'z');

  while (values.size() < static_cast<uint32_t>(n_values)) {
    auto length = length_dist(gen);
    std::string s(length, 'X');
    for (int32_t i = 0; i < length; ++i) {
      s[i] = static_cast<uint8_t>(char_dist(gen));
    }
    values.insert(std::move(s));
  }
  return values;
}

template <typename T>
static void CheckScalarHashQuality(const std::unordered_set<T>& distinct_values) {
  std::unordered_set<hash_t> hashes;
  for (const auto v : distinct_values) {
    hashes.insert(ScalarHelper<T, 0>::ComputeHash(v));
    hashes.insert(ScalarHelper<T, 1>::ComputeHash(v));
  }
  ASSERT_GE(static_cast<double>(hashes.size()),
            0.96 * static_cast<double>(2 * distinct_values.size()));
}

TEST(HashingQuality, Int64) {
#ifdef ARROW_VALGRIND
  const int32_t n_values = 500;
#else
  const int32_t n_values = 10000;
#endif
  {
    const auto values = MakeDistinctIntegers<int64_t>(n_values);
    CheckScalarHashQuality<int64_t>(values);
  }
  {
    const auto values = MakeSequentialIntegers<int64_t>(n_values);
    CheckScalarHashQuality<int64_t>(values);
  }
}

TEST(HashingQuality, Strings) {
#ifdef ARROW_VALGRIND
  const int32_t n_values = 500;
#else
  const int32_t n_values = 10000;
#endif
  const auto values = MakeDistinctStrings(n_values);

  std::unordered_set<hash_t> hashes;
  for (const auto& v : values) {
    hashes.insert(ComputeStringHash<0>(v.data(), static_cast<int64_t>(v.size())));
    hashes.insert(ComputeStringHash<1>(v.data(), static_cast<int64_t>(v.size())));
  }
  ASSERT_GE(static_cast<double>(hashes.size()),
            0.96 * static_cast<double>(2 * values.size()));
}

TEST(HashingBounds, Strings) {
  std::vector<size_t> sizes({1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 18, 19, 20, 21});
  for (const auto s : sizes) {
    std::string str;
    for (size_t i = 0; i < s; i++) {
      str.push_back(static_cast<char>(i));
    }
    hash_t h = ComputeStringHash<1>(str.c_str(), str.size());
    int different = 0;
    for (char i = 0; i < 120; i++) {
      str[str.size() - 1] = i;
      if (ComputeStringHash<1>(str.c_str(), str.size()) != h) {
        different++;
      }
    }
    ASSERT_GE(different, 118);
  }
}

template <typename MemoTable, typename Value>
void AssertGet(MemoTable& table, const Value& v, int32_t expected) {
  ASSERT_EQ(table.Get(v), expected);
}

template <typename MemoTable, typename Value>
void AssertGetOrInsert(MemoTable& table, const Value& v, int32_t expected) {
  int32_t memo_index;
  ASSERT_OK(table.GetOrInsert(v, &memo_index));
  ASSERT_EQ(memo_index, expected);
}

template <typename MemoTable>
void AssertGetNull(MemoTable& table, int32_t expected) {
  ASSERT_EQ(table.GetNull(), expected);
}

template <typename MemoTable>
void AssertGetOrInsertNull(MemoTable& table, int32_t expected) {
  ASSERT_EQ(table.GetOrInsertNull(), expected);
}

TEST(ScalarMemoTable, Int64) {
  const int64_t A = 1234, B = 0, C = -98765321, D = 12345678901234LL, E = -1, F = 1,
                G = 9223372036854775807LL, H = -9223372036854775807LL - 1;

  ScalarMemoTable<int64_t> table(default_memory_pool(), 0);
  ASSERT_EQ(table.size(), 0);
  AssertGet(table, A, kKeyNotFound);
  AssertGetNull(table, kKeyNotFound);
  AssertGetOrInsert(table, A, 0);
  AssertGet(table, B, kKeyNotFound);
  AssertGetOrInsert(table, B, 1);
  AssertGetOrInsert(table, C, 2);
  AssertGetOrInsert(table, D, 3);
  AssertGetOrInsert(table, E, 4);
  AssertGetOrInsertNull(table, 5);

  AssertGet(table, A, 0);
  AssertGetOrInsert(table, A, 0);
  AssertGet(table, E, 4);
  AssertGetOrInsert(table, E, 4);

  AssertGetOrInsert(table, F, 6);
  AssertGetOrInsert(table, G, 7);
  AssertGetOrInsert(table, H, 8);

  AssertGetOrInsert(table, G, 7);
  AssertGetOrInsert(table, F, 6);
  AssertGetOrInsertNull(table, 5);
  AssertGetOrInsert(table, E, 4);
  AssertGetOrInsert(table, D, 3);
  AssertGetOrInsert(table, C, 2);
  AssertGetOrInsert(table, B, 1);
  AssertGetOrInsert(table, A, 0);

  const int64_t size = 9;
  ASSERT_EQ(table.size(), size);
  {
    std::vector<int64_t> values(size);
    table.CopyValues(values.data());
    EXPECT_THAT(values, testing::ElementsAre(A, B, C, D, E, 0, F, G, H));
  }
  {
    const int32_t start_offset = 3;
    std::vector<int64_t> values(size - start_offset);
    table.CopyValues(start_offset, values.data());
    EXPECT_THAT(values, testing::ElementsAre(D, E, 0, F, G, H));
  }
}

TEST(ScalarMemoTable, UInt16) {
  const uint16_t A = 1236, B = 0, C = 65535, D = 32767, E = 1;

  ScalarMemoTable<uint16_t> table(default_memory_pool(), 0);
  ASSERT_EQ(table.size(), 0);
  AssertGet(table, A, kKeyNotFound);
  AssertGetNull(table, kKeyNotFound);
  AssertGetOrInsert(table, A, 0);
  AssertGet(table, B, kKeyNotFound);
  AssertGetOrInsert(table, B, 1);
  AssertGetOrInsert(table, C, 2);
  AssertGetOrInsert(table, D, 3);

  {
    EXPECT_EQ(table.size(), 4);
    std::vector<uint16_t> values(table.size());
    table.CopyValues(values.data());
    EXPECT_THAT(values, testing::ElementsAre(A, B, C, D));
  }

  AssertGetOrInsertNull(table, 4);
  AssertGetOrInsert(table, E, 5);

  AssertGet(table, A, 0);
  AssertGetOrInsert(table, A, 0);
  AssertGetOrInsert(table, B, 1);
  AssertGetOrInsert(table, C, 2);
  AssertGetOrInsert(table, D, 3);
  AssertGetNull(table, 4);
  AssertGet(table, E, 5);
  AssertGetOrInsert(table, E, 5);

  ASSERT_EQ(table.size(), 6);
  std::vector<uint16_t> values(table.size());
  table.CopyValues(values.data());
  EXPECT_THAT(values, testing::ElementsAre(A, B, C, D, 0, E));
}

TEST(SmallScalarMemoTable, Int8) {
  const int8_t A = 1, B = 0, C = -1, D = -128, E = 127;

  SmallScalarMemoTable<int8_t> table(default_memory_pool(), 0);
  AssertGet(table, A, kKeyNotFound);
  AssertGetNull(table, kKeyNotFound);
  AssertGetOrInsert(table, A, 0);
  AssertGet(table, B, kKeyNotFound);
  AssertGetOrInsert(table, B, 1);
  AssertGetOrInsert(table, C, 2);
  AssertGetOrInsert(table, D, 3);
  AssertGetOrInsert(table, E, 4);
  AssertGetOrInsertNull(table, 5);

  AssertGet(table, A, 0);
  AssertGetOrInsert(table, A, 0);
  AssertGetOrInsert(table, B, 1);
  AssertGetOrInsert(table, C, 2);
  AssertGetOrInsert(table, D, 3);
  AssertGet(table, E, 4);
  AssertGetOrInsert(table, E, 4);
  AssertGetNull(table, 5);
  AssertGetOrInsertNull(table, 5);

  ASSERT_EQ(table.size(), 6);
  std::vector<int8_t> values(table.size());
  table.CopyValues(values.data());
  EXPECT_THAT(values, testing::ElementsAre(A, B, C, D, E, 0));
}

TEST(SmallScalarMemoTable, Bool) {
  SmallScalarMemoTable<bool> table(default_memory_pool(), 0);
  ASSERT_EQ(table.size(), 0);
  AssertGet(table, true, kKeyNotFound);
  AssertGetOrInsert(table, true, 0);
  AssertGetOrInsertNull(table, 1);
  AssertGetOrInsert(table, false, 2);

  AssertGet(table, true, 0);
  AssertGetOrInsert(table, true, 0);
  AssertGetNull(table, 1);
  AssertGetOrInsertNull(table, 1);
  AssertGet(table, false, 2);
  AssertGetOrInsert(table, false, 2);

  ASSERT_EQ(table.size(), 3);
  EXPECT_THAT(table.values(), testing::ElementsAre(true, 0, false));
  // NOTE std::vector<bool> doesn't have a data() method
}

TEST(ScalarMemoTable, Float64) {
  const double A = 0.0, B = 1.5, C = -0.0, D = std::numeric_limits<double>::infinity(),
               E = -D, F = std::nan("");

  ScalarMemoTable<double> table(default_memory_pool(), 0);
  ASSERT_EQ(table.size(), 0);
  AssertGet(table, A, kKeyNotFound);
  AssertGetNull(table, kKeyNotFound);
  AssertGetOrInsert(table, A, 0);
  AssertGet(table, B, kKeyNotFound);
  AssertGetOrInsert(table, B, 1);
  AssertGetOrInsert(table, C, 2);
  AssertGetOrInsert(table, D, 3);
  AssertGetOrInsert(table, E, 4);
  AssertGetOrInsert(table, F, 5);

  AssertGet(table, A, 0);
  AssertGetOrInsert(table, A, 0);
  AssertGetOrInsert(table, B, 1);
  AssertGetOrInsert(table, C, 2);
  AssertGetOrInsert(table, D, 3);
  AssertGet(table, E, 4);
  AssertGetOrInsert(table, E, 4);
  AssertGet(table, F, 5);
  AssertGetOrInsert(table, F, 5);

  ASSERT_EQ(table.size(), 6);
  std::vector<double> expected({A, B, C, D, E, F});
  std::vector<double> values(table.size());
  table.CopyValues(values.data());
  for (uint32_t i = 0; i < expected.size(); ++i) {
    auto u = expected[i];
    auto v = values[i];
    if (std::isnan(u)) {
      ASSERT_TRUE(std::isnan(v));
    } else {
      ASSERT_EQ(u, v);
    }
  }
}

TEST(ScalarMemoTable, StressInt64) {
  std::default_random_engine gen(42);
  std::uniform_int_distribution<int64_t> value_dist(-50, 50);
#ifdef ARROW_VALGRIND
  const int32_t n_repeats = 500;
#else
  const int32_t n_repeats = 10000;
#endif

  ScalarMemoTable<int64_t> table(default_memory_pool(), 0);
  std::unordered_map<int64_t, int32_t> map;

  for (int32_t i = 0; i < n_repeats; ++i) {
    int64_t value = value_dist(gen);
    int32_t expected, actual;
    auto it = map.find(value);
    if (it == map.end()) {
      expected = static_cast<int32_t>(map.size());
      map[value] = expected;
    } else {
      expected = it->second;
    }
    ASSERT_OK(table.GetOrInsert(value, &actual));
    ASSERT_EQ(actual, expected);
  }
  ASSERT_EQ(table.size(), map.size());
}

TEST(BinaryMemoTable, Basics) {
  std::string A = "", B = "a", C = "foo", D = "bar", E, F;
  E += '\0';
  F += '\0';
  F += "trailing";

  BinaryMemoTable<BinaryBuilder> table(default_memory_pool(), 0);
  ASSERT_EQ(table.size(), 0);
  AssertGet(table, A, kKeyNotFound);
  AssertGetNull(table, kKeyNotFound);
  AssertGetOrInsert(table, A, 0);
  AssertGet(table, B, kKeyNotFound);
  AssertGetOrInsert(table, B, 1);
  AssertGetOrInsert(table, C, 2);
  AssertGetOrInsert(table, D, 3);
  AssertGetOrInsert(table, E, 4);
  AssertGetOrInsert(table, F, 5);
  AssertGetOrInsertNull(table, 6);

  AssertGet(table, A, 0);
  AssertGetOrInsert(table, A, 0);
  AssertGet(table, B, 1);
  AssertGetOrInsert(table, B, 1);
  AssertGetOrInsert(table, C, 2);
  AssertGetOrInsert(table, D, 3);
  AssertGetOrInsert(table, E, 4);
  AssertGet(table, F, 5);
  AssertGetOrInsert(table, F, 5);
  AssertGetNull(table, 6);
  AssertGetOrInsertNull(table, 6);

  ASSERT_EQ(table.size(), 7);
  ASSERT_EQ(table.values_size(), 17);

  const int32_t size = table.size();
  {
    std::vector<int8_t> offsets(size + 1);
    table.CopyOffsets(offsets.data());
    EXPECT_THAT(offsets, testing::ElementsAre(0, 0, 1, 4, 7, 8, 17, 17));

    std::string expected_values;
    expected_values += "afoobar";
    expected_values += '\0';
    expected_values += '\0';
    expected_values += "trailing";
    std::string values(17, 'X');
    table.CopyValues(reinterpret_cast<uint8_t*>(&values[0]));
    ASSERT_EQ(values, expected_values);
  }
  {
    const int32_t start_offset = 4;
    std::vector<int8_t> offsets(size + 1 - start_offset);
    table.CopyOffsets(start_offset, offsets.data());
    EXPECT_THAT(offsets, testing::ElementsAre(0, 1, 10, 10));

    std::string expected_values;
    expected_values += '\0';
    expected_values += '\0';
    expected_values += "trailing";
    std::string values(10, 'X');
    table.CopyValues(4 /* start offset */, reinterpret_cast<uint8_t*>(&values[0]));
    ASSERT_EQ(values, expected_values);
  }
  {
    const int32_t start_offset = 1;
    std::vector<std::string> actual;
    table.VisitValues(start_offset, [&](std::string_view v) {
      actual.emplace_back(v.data(), v.length());
    });
    EXPECT_THAT(actual, testing::ElementsAre(B, C, D, E, F, ""));
  }
}

TEST(BinaryMemoTable, Stress) {
#ifdef ARROW_VALGRIND
  const int32_t n_values = 20;
  const int32_t n_repeats = 20;
#else
  const int32_t n_values = 100;
  const int32_t n_repeats = 100;
#endif

  const auto values = MakeDistinctStrings(n_values);

  BinaryMemoTable<BinaryBuilder> table(default_memory_pool(), 0);
  std::unordered_map<std::string, int32_t> map;

  for (int32_t i = 0; i < n_repeats; ++i) {
    for (const auto& value : values) {
      int32_t expected, actual;
      auto it = map.find(value);
      if (it == map.end()) {
        expected = static_cast<int32_t>(map.size());
        map[value] = expected;
      } else {
        expected = it->second;
      }
      ASSERT_OK(table.GetOrInsert(value, &actual));
      ASSERT_EQ(actual, expected);
    }
  }
  ASSERT_EQ(table.size(), map.size());
}

TEST(BinaryMemoTable, Empty) {
  BinaryMemoTable<BinaryBuilder> table(default_memory_pool());
  ASSERT_EQ(table.size(), 0);
  BinaryMemoTable<BinaryBuilder>::builder_offset_type offsets[1];
  table.CopyOffsets(0, offsets);
  EXPECT_EQ(offsets[0], 0);
}

hash_t HashDataBitmap(const ArraySpan& array) {
  EXPECT_EQ(array.type->id(), Type::BOOL);
  const auto& bitmap = array.buffers[1];
  return ComputeBitmapHash(bitmap.data,
                           /*seed=*/0,
                           /*bit_offset=*/array.offset,
                           /*num_bits=*/array.length);
}

std::shared_ptr<BooleanArray> BuildBooleanArray(int len, bool start) {
  // This could be memoized in the future to speed up tests.
  BooleanBuilder builder;
  for (int i = 0; i < len; ++i) {
    EXPECT_TRUE(builder.Append(((i % 2 != 0) ^ start) == 1).ok());
  }
  std::shared_ptr<BooleanArray> array;
  EXPECT_TRUE(builder.Finish(&array).ok());
  return array;
}

hash_t HashConcatenation(const ArrayVector& arrays, int64_t bits_offset = -1,
                         int64_t num_bits = -1) {
  EXPECT_OK_AND_ASSIGN(auto concat, Concatenate(arrays));
  EXPECT_EQ(concat->type()->id(), Type::BOOL);
  if (bits_offset == -1 || num_bits == -1) {
    return HashDataBitmap(*concat->data());
  }
  auto slice = concat->Slice(bits_offset, num_bits);
  return HashDataBitmap(*slice->data());
}

TEST(BitmapHashTest, SmallInputs) {
  for (bool start : {false, true}) {
    auto block = BuildBooleanArray(64, start);
    for (int len = 0; len < 64; len++) {
      auto prefix = BuildBooleanArray(len, start);
      auto expected_hash = HashDataBitmap(*prefix->data());

      auto slice = block->Slice(0, len);
      auto slice_hash = HashDataBitmap(*slice->data());
      ASSERT_EQ(expected_hash, slice_hash);

      for (int j = 1; j < len; j++) {
        auto fragment = BuildBooleanArray(len - j, start ^ (j % 2 != 0));
        expected_hash = HashDataBitmap(*fragment->data());

        slice = block->Slice(j, len - j);
        slice_hash = HashDataBitmap(*slice->data());
        ASSERT_EQ(expected_hash, slice_hash);
      }
    }
  }
}

TEST(BitmapHashTest, LongerInputs) {
  BooleanBuilder builder;
  std::shared_ptr<BooleanArray> block_of_bools;
  {
    ASSERT_OK(builder.AppendValues(2, true));
    ASSERT_OK(builder.AppendValues(3, false));
    ASSERT_OK(builder.AppendValues(5, true));
    ASSERT_OK(builder.AppendValues(7, false));
    ASSERT_OK(builder.AppendValues(11, true));
    ASSERT_OK(builder.AppendValues(13, false));
    ASSERT_OK(builder.AppendValues(17, true));
    ASSERT_OK(builder.AppendValues(5, false));
    ASSERT_OK(builder.AppendValues(1, true));
    ASSERT_OK(builder.Finish(&block_of_bools));
    ASSERT_EQ(block_of_bools->length(), 64);
  }
  const auto hash_of_block = HashDataBitmap(*block_of_bools->data());

  const auto kStep = 13;
  constexpr auto kMaxPadding = 64 + 32 + kStep;

  for (int prefix_pad_len = 0; prefix_pad_len < kMaxPadding; prefix_pad_len += kStep) {
    auto prefix_pad = BuildBooleanArray(prefix_pad_len, true);
    for (int suffix_pad_len = 0; suffix_pad_len < kMaxPadding; suffix_pad_len += kStep) {
      auto suffix_pad = BuildBooleanArray(suffix_pad_len, true);

      // A block of 64 bools in the middle
      auto hash =
          HashConcatenation({prefix_pad, block_of_bools, suffix_pad}, prefix_pad_len, 64);
      ASSERT_EQ(hash, hash_of_block);

      // Trailing bits and leading bits around a block
      for (int trailing_len = 1; trailing_len < kMaxPadding; trailing_len += kStep) {
        auto trailing = BuildBooleanArray(trailing_len, true);
        auto expected_hash = HashConcatenation({block_of_bools, trailing});
        auto hash = HashConcatenation({prefix_pad, block_of_bools, trailing, suffix_pad},
                                      prefix_pad_len, 64 + trailing_len);
        ASSERT_EQ(hash, expected_hash);

        // Use the trailing bits as leading bits now
        auto leading = trailing;
        auto leading_len = trailing_len;
        expected_hash = HashConcatenation({leading, block_of_bools});
        hash = HashConcatenation({prefix_pad, leading, block_of_bools, suffix_pad},
                                 prefix_pad_len, leading_len + 64);
        ASSERT_EQ(hash, expected_hash);

        // Leading and trailing at the same time
        expected_hash = HashConcatenation({leading, block_of_bools, trailing});
        hash =
            HashConcatenation({prefix_pad, leading, block_of_bools, trailing, suffix_pad},
                              prefix_pad_len, leading_len + 64 + trailing_len);
        ASSERT_EQ(hash, expected_hash);
      }
    }
  }
}

}  // namespace internal
}  // namespace arrow
