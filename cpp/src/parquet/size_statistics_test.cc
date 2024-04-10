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

#include "parquet/size_statistics.h"

#include <algorithm>
#include <random>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/bit_util.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"
#include "parquet/types.h"

namespace parquet {

using namespace parquet::schema;

TEST(SizeStatistics, WriteBatchLevels) {
  std::vector<int64_t> expected_def_level_histogram = {256, 128, 64, 32, 16, 8, 4, 2, 2};
  std::vector<int64_t> expected_rep_level_histogram = {256, 128, 64, 32, 32};
  constexpr int16_t kMaxDefLevel = 8;
  constexpr int16_t kMaxRefLevel = 4;
  auto descr = std::make_unique<ColumnDescriptor>(Int32("a"), kMaxDefLevel, kMaxRefLevel);
  auto builder = SizeStatisticsBuilder::Make(descr.get());

  auto write_batch_levels =
      [&](const std::vector<int64_t>& histogram,
          const std::function<void(SizeStatisticsBuilder*, int64_t, const int16_t*)>&
              write_levels_func) {
        std::vector<int16_t> levels;
        for (int16_t level = 0; level < static_cast<int16_t>(histogram.size()); level++) {
          levels.insert(levels.end(), histogram[level], level);
        }

        auto rng = std::default_random_engine{};
        std::shuffle(std::begin(levels), std::end(levels), rng);

        constexpr size_t kBatchSize = 64;
        for (size_t i = 0; i < levels.size(); i += kBatchSize) {
          auto batch_size = static_cast<int64_t>(std::min(kBatchSize, levels.size() - i));
          write_levels_func(builder.get(), batch_size, levels.data() + i);
        }
      };

  write_batch_levels(expected_def_level_histogram,
                     &SizeStatisticsBuilder::WriteDefinitionLevels);
  write_batch_levels(expected_rep_level_histogram,
                     &SizeStatisticsBuilder::WriteRepetitionLevels);
  auto size_statistics = builder->Build();
  EXPECT_EQ(size_statistics->definition_level_histogram(), expected_def_level_histogram);
  EXPECT_EQ(size_statistics->repetition_level_histogram(), expected_rep_level_histogram);
}

TEST(SizeStatistics, WriteRepeatedLevels) {
  constexpr int16_t kMaxDefLevel = 2;
  constexpr int16_t kMaxRepLevel = 3;
  auto descr = std::make_unique<ColumnDescriptor>(Int32("a"), kMaxDefLevel, kMaxRepLevel);
  auto builder = SizeStatisticsBuilder::Make(descr.get());

  constexpr int64_t kNumRounds = 10;
  for (int64_t round = 1; round <= kNumRounds; round++) {
    for (int16_t def_level = 0; def_level <= kMaxDefLevel; def_level++) {
      builder->WriteDefinitionLevel(/*num_levels=*/round + def_level, def_level);
    }
    for (int16_t rep_level = 0; rep_level <= kMaxRepLevel; rep_level++) {
      builder->WriteRepetitionLevel(/*num_levels=*/round + rep_level * rep_level,
                                    rep_level);
    }
  }

  auto size_statistics = builder->Build();
  EXPECT_EQ(size_statistics->definition_level_histogram(),
            std::vector<int64_t>({55, 65, 75}));
  EXPECT_EQ(size_statistics->repetition_level_histogram(),
            std::vector<int64_t>({55, 65, 95, 145}));
}

TEST(SizeStatistics, WriteDenseByteArrayValues) {
  constexpr std::string_view kValue = "foo";
  constexpr int kNumValues = 1000;
  constexpr int kBatchSize = 64;
  const std::vector<parquet::ByteArray> values(kNumValues, kValue);

  auto descr = std::make_unique<ColumnDescriptor>(
      schema::ByteArray("a"), /*max_def_level=*/0, /*max_rep_level=*/0);
  auto builder = SizeStatisticsBuilder::Make(descr.get());
  for (int i = 0; i < kNumValues; i += kBatchSize) {
    auto batch_size = std::min(kBatchSize, kNumValues - i);
    builder->WriteValues(values.data() + i, batch_size);
  }

  auto size_statistics = builder->Build();
  EXPECT_EQ(size_statistics->unencoded_byte_array_data_bytes().value_or(-1),
            kNumValues * kValue.size());
}

TEST(SizeStatistics, WriteSpacedByteArrayValues) {
  constexpr std::string_view kValue = "foo";
  constexpr int kNumValues = 1000;
  constexpr int kBatchSize = 63;
  const std::vector<parquet::ByteArray> values(kNumValues, kValue);
  ASSERT_OK_AND_ASSIGN(auto not_null_bitmap, ::arrow::AllocateBitmap(kNumValues));
  int not_null_count = 0;
  for (int i = 0; i < kNumValues; i++) {
    if (i % 3 == 0) {
      ::arrow::bit_util::ClearBit(not_null_bitmap->mutable_data(), i);
    } else {
      ::arrow::bit_util::SetBit(not_null_bitmap->mutable_data(), i);
      not_null_count++;
    }
  }

  auto descr = std::make_unique<ColumnDescriptor>(
      schema::ByteArray("a"), /*max_def_level=*/1, /*max_rep_level=*/0);
  auto builder = SizeStatisticsBuilder::Make(descr.get());
  for (int i = 0; i < kNumValues; i += kBatchSize) {
    auto batch_size = std::min(kBatchSize, kNumValues - i);
    builder->WriteValuesSpaced(values.data() + i, not_null_bitmap->data(), i, batch_size);
  }

  auto size_statistics = builder->Build();
  EXPECT_EQ(size_statistics->unencoded_byte_array_data_bytes().value_or(-1),
            not_null_count * kValue.size());
}

TEST(SizeStatistics, WriteBinaryArray) {
  std::vector<std::shared_ptr<::arrow::Array>> arrays = {
      ::arrow::ArrayFromJSON(::arrow::binary(), R"(["foo", null, "bar", "baz"])"),
      ::arrow::ArrayFromJSON(::arrow::large_binary(), R"(["foo", null, "bar", "baz"])"),
  };
  for (const auto& array : arrays) {
    auto descr = std::make_unique<ColumnDescriptor>(
        schema::ByteArray("a"), /*max_def_level=*/1, /*max_rep_level=*/0);
    auto builder = SizeStatisticsBuilder::Make(descr.get());
    builder->WriteValues(*array);
    auto size_statistics = builder->Build();
    EXPECT_EQ(size_statistics->unencoded_byte_array_data_bytes().value_or(-1), 9);
  }
}

TEST(SizeStatistics, MergeStatistics) {
  constexpr int kNumValues = 16;
  const std::array<int16_t, kNumValues> def_levels = {0, 0, 0, 0, 1, 1, 1, 1,
                                                      2, 2, 2, 2, 3, 3, 3, 3};
  const std::array<int16_t, kNumValues> rep_levels = {0, 1, 2, 3, 0, 1, 2, 3,
                                                      0, 1, 2, 3, 0, 1, 2, 3};
  const std::vector<int64_t> expected_histogram = {8, 8, 8, 8};
  constexpr std::string_view kByteArrayValue = "foo";
  const std::vector<parquet::ByteArray> values(kNumValues,
                                               parquet::ByteArray{kByteArrayValue});

  for (const auto& descr :
       {std::make_unique<ColumnDescriptor>(schema::Int32("a"), /*max_def_level=*/3,
                                           /*max_rep_level=*/3),
        std::make_unique<ColumnDescriptor>(schema::ByteArray("a"), /*max_def_level=*/3,
                                           /*max_rep_level=*/3)}) {
    auto builder = SizeStatisticsBuilder::Make(descr.get());
    builder->WriteRepetitionLevels(kNumValues, def_levels.data());
    builder->WriteDefinitionLevels(kNumValues, rep_levels.data());
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      builder->WriteValues(values.data(), kNumValues);
    }
    auto size_statistics_1 = builder->Build();

    builder->Reset();
    builder->WriteRepetitionLevels(kNumValues, def_levels.data());
    builder->WriteDefinitionLevels(kNumValues, rep_levels.data());
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      builder->WriteValues(values.data(), kNumValues);
    }
    auto size_statistics_2 = builder->Build();

    size_statistics_1->Merge(*size_statistics_2);
    EXPECT_EQ(size_statistics_1->definition_level_histogram(), expected_histogram);
    EXPECT_EQ(size_statistics_1->repetition_level_histogram(), expected_histogram);
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      EXPECT_TRUE(size_statistics_1->unencoded_byte_array_data_bytes().has_value());
      EXPECT_EQ(size_statistics_1->unencoded_byte_array_data_bytes().value(),
                kByteArrayValue.size() * kNumValues * 2);
    } else {
      EXPECT_FALSE(size_statistics_1->unencoded_byte_array_data_bytes().has_value());
    }
  }
}

TEST(SizeStatistics, ThriftSerDe) {
  constexpr int kNumValues = 16;
  const std::array<int16_t, kNumValues> def_levels = {0, 0, 0, 0, 1, 1, 1, 1,
                                                      2, 2, 2, 2, 3, 3, 3, 3};
  const std::array<int16_t, kNumValues> rep_levels = {0, 1, 2, 3, 0, 1, 2, 3,
                                                      0, 1, 2, 3, 0, 1, 2, 3};
  const std::vector<int64_t> expected_histogram = {4, 4, 4, 4};
  constexpr std::string_view kByteArrayValue = "foo";
  const std::vector<parquet::ByteArray> values(kNumValues,
                                               parquet::ByteArray{kByteArrayValue});

  for (const auto& descr :
       {std::make_unique<ColumnDescriptor>(schema::Int32("a"), /*max_def_level=*/3,
                                           /*max_rep_level=*/3),
        std::make_unique<ColumnDescriptor>(schema::ByteArray("a"), /*max_def_level=*/3,
                                           /*max_rep_level=*/3)}) {
    auto builder = SizeStatisticsBuilder::Make(descr.get());
    builder->WriteRepetitionLevels(kNumValues, def_levels.data());
    builder->WriteDefinitionLevels(kNumValues, rep_levels.data());
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      builder->WriteValues(values.data(), kNumValues);
    }
    auto size_statistics = builder->Build();
    auto thrift_statistics = ToThrift(*size_statistics);
    auto restored_statistics = SizeStatistics::Make(&thrift_statistics, descr.get());
    EXPECT_EQ(restored_statistics->definition_level_histogram(), expected_histogram);
    EXPECT_EQ(restored_statistics->repetition_level_histogram(), expected_histogram);
    if (descr->physical_type() == Type::BYTE_ARRAY) {
      EXPECT_TRUE(restored_statistics->unencoded_byte_array_data_bytes().has_value());
      EXPECT_EQ(restored_statistics->unencoded_byte_array_data_bytes().value(),
                kByteArrayValue.size() * kNumValues);
    } else {
      EXPECT_FALSE(restored_statistics->unencoded_byte_array_data_bytes().has_value());
    }
  }
}

}  // namespace parquet
