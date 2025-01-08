// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliancec
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

#include "arrow/util/logging.h"
#include "parquet/exception.h"
#include "parquet/schema.h"

namespace parquet {

namespace {

void MergeLevelHistogram(::arrow::util::span<int64_t> histogram,
                         ::arrow::util::span<const int64_t> other) {
  ARROW_DCHECK_EQ(histogram.size(), other.size());
  std::transform(histogram.begin(), histogram.end(), other.begin(), histogram.begin(),
                 std::plus<>());
}

}  // namespace

void SizeStatistics::Merge(const SizeStatistics& other) {
  if (repetition_level_histogram.size() != other.repetition_level_histogram.size()) {
    throw ParquetException("Repetition level histogram size mismatch");
  }
  if (definition_level_histogram.size() != other.definition_level_histogram.size()) {
    throw ParquetException("Definition level histogram size mismatch");
  }
  if (unencoded_byte_array_data_bytes.has_value() !=
      other.unencoded_byte_array_data_bytes.has_value()) {
    throw ParquetException("Unencoded byte array data bytes are not consistent");
  }
  MergeLevelHistogram(repetition_level_histogram, other.repetition_level_histogram);
  MergeLevelHistogram(definition_level_histogram, other.definition_level_histogram);
  if (unencoded_byte_array_data_bytes.has_value()) {
    unencoded_byte_array_data_bytes = unencoded_byte_array_data_bytes.value() +
                                      other.unencoded_byte_array_data_bytes.value();
  }
}

void SizeStatistics::IncrementUnencodedByteArrayDataBytes(int64_t value) {
  ARROW_CHECK(unencoded_byte_array_data_bytes.has_value());
  unencoded_byte_array_data_bytes = unencoded_byte_array_data_bytes.value() + value;
}

void SizeStatistics::Validate(const ColumnDescriptor* descr) const {
  if (repetition_level_histogram.size() !=
      static_cast<size_t>(descr->max_repetition_level() + 1)) {
    throw ParquetException("Repetition level histogram size mismatch");
  }
  if (definition_level_histogram.size() !=
      static_cast<size_t>(descr->max_definition_level() + 1)) {
    throw ParquetException("Definition level histogram size mismatch");
  }
  if (unencoded_byte_array_data_bytes.has_value() &&
      descr->physical_type() != Type::BYTE_ARRAY) {
    throw ParquetException("Unencoded byte array data bytes does not support " +
                           TypeToString(descr->physical_type()));
  }
  if (!unencoded_byte_array_data_bytes.has_value() &&
      descr->physical_type() == Type::BYTE_ARRAY) {
    throw ParquetException("Missing unencoded byte array data bytes");
  }
}

void SizeStatistics::Reset() {
  repetition_level_histogram.assign(repetition_level_histogram.size(), 0);
  definition_level_histogram.assign(definition_level_histogram.size(), 0);
  if (unencoded_byte_array_data_bytes.has_value()) {
    unencoded_byte_array_data_bytes = 0;
  }
}

std::unique_ptr<SizeStatistics> SizeStatistics::Make(const ColumnDescriptor* descr) {
  auto size_stats = std::make_unique<SizeStatistics>();
  size_stats->repetition_level_histogram.resize(descr->max_repetition_level() + 1, 0);
  size_stats->definition_level_histogram.resize(descr->max_definition_level() + 1, 0);
  if (descr->physical_type() == Type::BYTE_ARRAY) {
    size_stats->unencoded_byte_array_data_bytes = 0;
  }
  return size_stats;
}

void UpdateLevelHistogram(::arrow::util::span<const int16_t> levels,
                          ::arrow::util::span<int64_t> histogram) {
  const int64_t num_levels = static_cast<int64_t>(levels.size());
  const int16_t max_level = static_cast<int16_t>(histogram.size() - 1);
  if (max_level == 0) {
    histogram[0] += num_levels;
    return;
  }
  // The goal of the two specialized paths below is to accelerate common cases
  // by keeping histogram values in registers.
  // The fallback implementation (`++histogram[level]`) issues a series of
  // load-stores with frequent conflicts.
  if (max_level == 1) {
    // Specialize the common case for non-repeated non-nested columns
    // by keeping histogram values in a register, which avoids being limited
    // by CPU cache latency.
    int64_t hist0 = 0;
    for (int16_t level : levels) {
      ARROW_DCHECK_LE(level, max_level);
      hist0 += (level == 0);
    }
    // max_level is usually the most frequent case, update it in one single step
    histogram[1] += num_levels - hist0;
    histogram[0] += hist0;
    return;
  }

  // The general case cannot avoid repeated loads/stores in the CPU cache,
  // but it limits store-to-load dependencies by interleaving partial histogram
  // updates.
  constexpr int kUnroll = 4;
  std::array<std::vector<int64_t>, kUnroll> partial_hist;
  for (auto& hist : partial_hist) {
    hist.assign(histogram.size(), 0);
  }
  int64_t i = 0;
  for (; i <= num_levels - kUnroll; i += kUnroll) {
    for (int j = 0; j < kUnroll; ++j) {
      ARROW_DCHECK_LE(levels[i + j], max_level);
      ++partial_hist[j][levels[i + j]];
    }
  }
  for (; i < num_levels; ++i) {
    ARROW_DCHECK_LE(levels[i], max_level);
    ++partial_hist[0][levels[i]];
  }
  for (const auto& hist : partial_hist) {
    MergeLevelHistogram(histogram, hist);
  }
}

}  // namespace parquet
