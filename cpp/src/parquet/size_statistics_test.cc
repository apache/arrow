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

#include "arrow/io/file.h"
#include "arrow/util/float16.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"

namespace parquet {

using namespace parquet::schema;

TEST(SizeStatistics, WriteBatchLevels) {
  std::vector<int64_t> expected_def_level_histogram = {256, 128, 64, 32, 16, 8, 4, 2, 2};
  std::vector<int64_t> expected_rep_level_histogram = {256, 128, 64, 32, 32};

  const int16_t max_def_level =
      static_cast<int16_t>(expected_def_level_histogram.size()) - 1;
  const int16_t max_rep_level =
      static_cast<int16_t>(expected_rep_level_histogram.size()) - 1;
  auto descr =
      std::make_unique<ColumnDescriptor>(Int32("a"), max_def_level, max_rep_level);
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

// TODO: Add tests for write binary variants.
// TODO: Add tests for merge two size statistics.
// TODO: Add tests for thrift serialization.

}  // namespace parquet
