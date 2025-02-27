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
#include <array>
#include <numeric>
#include <ostream>
#include <string_view>
#include <vector>

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
  auto validate_histogram = [](const std::vector<int64_t>& histogram, int16_t max_level,
                               const std::string& name) {
    if (histogram.empty()) {
      // A levels histogram is always allowed to be missing.
      return;
    }
    if (histogram.size() != static_cast<size_t>(max_level + 1)) {
      std::stringstream ss;
      ss << name << " level histogram size mismatch, size: " << histogram.size()
         << ", expected: " << (max_level + 1);
      throw ParquetException(ss.str());
    }
  };
  validate_histogram(repetition_level_histogram, descr->max_repetition_level(),
                     "Repetition");
  validate_histogram(definition_level_histogram, descr->max_definition_level(),
                     "Definition");
  if (unencoded_byte_array_data_bytes.has_value() &&
      descr->physical_type() != Type::BYTE_ARRAY) {
    throw ParquetException("Unencoded byte array data bytes does not support " +
                           TypeToString(descr->physical_type()));
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
  // If the max level is 0, the level histogram can be omitted because it contains
  // only single level (a.k.a. 0) and its count is equivalent to `num_values` of the
  // column chunk or data page.
  if (descr->max_repetition_level() != 0) {
    size_stats->repetition_level_histogram.resize(descr->max_repetition_level() + 1, 0);
  }
  if (descr->max_definition_level() != 0) {
    size_stats->definition_level_histogram.resize(descr->max_definition_level() + 1, 0);
  }
  if (descr->physical_type() == Type::BYTE_ARRAY) {
    size_stats->unencoded_byte_array_data_bytes = 0;
  }
  return size_stats;
}

std::ostream& operator<<(std::ostream& os, const SizeStatistics& size_stats) {
  constexpr std::string_view kComma = ", ";
  os << "SizeStatistics{";
  std::string_view sep = "";
  if (size_stats.unencoded_byte_array_data_bytes.has_value()) {
    os << "unencoded_byte_array_data_bytes="
       << *size_stats.unencoded_byte_array_data_bytes;
    sep = kComma;
  }
  auto print_histogram = [&](std::string_view name,
                             const std::vector<int64_t>& histogram) {
    if (!histogram.empty()) {
      os << sep << name << "={";
      sep = kComma;
      std::string_view value_sep = "";
      for (int64_t v : histogram) {
        os << value_sep << v;
        value_sep = kComma;
      }
      os << "}";
    }
  };
  print_histogram("repetition_level_histogram", size_stats.repetition_level_histogram);
  print_histogram("definition_level_histogram", size_stats.definition_level_histogram);
  os << "}";
  return os;
}

void UpdateLevelHistogram(::arrow::util::span<const int16_t> levels,
                          ::arrow::util::span<int64_t> histogram) {
  const int64_t num_levels = static_cast<int64_t>(levels.size());
  DCHECK_GE(histogram.size(), 1);
  const int16_t max_level = static_cast<int16_t>(histogram.size() - 1);
  if (max_level == 0) {
    histogram[0] += num_levels;
    return;
  }

#ifndef NDEBUG
  for (auto level : levels) {
    ARROW_DCHECK_LE(level, max_level);
  }
#endif

  if (max_level == 1) {
    // Specialize the common case for non-repeated non-nested columns.
    // Summing the levels gives us the number of 1s, and the number of 0s follows.
    // We do repeated sums in the int16_t space, which the compiler is likely
    // to vectorize efficiently.
    constexpr int64_t kChunkSize = 1 << 14;  // to avoid int16_t overflows
    int64_t hist1 = 0;
    auto it = levels.begin();
    while (it != levels.end()) {
      const auto chunk_size = std::min<int64_t>(levels.end() - it, kChunkSize);
      hist1 += std::accumulate(levels.begin(), levels.begin() + chunk_size, int16_t{0});
      it += chunk_size;
    }
    histogram[0] += num_levels - hist1;
    histogram[1] += hist1;
    return;
  }

  // The generic implementation issues a series of histogram load-stores.
  // However, it limits store-to-load dependencies by interleaving partial histogram
  // updates.
  constexpr int kUnroll = 4;
  std::array<std::vector<int64_t>, kUnroll> partial_hist;
  for (auto& hist : partial_hist) {
    hist.assign(histogram.size(), 0);
  }
  int64_t i = 0;
  for (; i <= num_levels - kUnroll; i += kUnroll) {
    for (int j = 0; j < kUnroll; ++j) {
      ++partial_hist[j][levels[i + j]];
    }
  }
  for (; i < num_levels; ++i) {
    ++partial_hist[0][levels[i]];
  }
  for (const auto& hist : partial_hist) {
    MergeLevelHistogram(histogram, hist);
  }
}

}  // namespace parquet
