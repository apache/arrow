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

#include "parquet/chunker_internal.h"

#include <cmath>
#include <string>
#include <vector>
#include "arrow/array.h"
#include "arrow/util/logging.h"
#include "parquet/chunker_internal_generated.h"
#include "parquet/exception.h"
#include "parquet/level_conversion.h"

namespace parquet::internal {

static uint64_t GetMask(uint64_t min_size, uint64_t max_size, uint8_t norm_factor) {
  // we aim for gaussian-like distribution of chunk sizes between min_size and max_size
  uint64_t avg_size = (min_size + max_size) / 2;
  // we skip calculating gearhash for the first `min_size` bytes, so we are looking for
  // a smaller chunk as the average size
  uint64_t target_size = avg_size - min_size;
  size_t mask_bits = static_cast<size_t>(std::floor(std::log2(target_size)));
  // -3 because we are using 8 hash tables to have more gaussian-like distribution
  // `norm_factor` narrows the chunk size distribution aroun avg_size
  size_t effective_bits = mask_bits - 3 - norm_factor;
  return std::numeric_limits<uint64_t>::max() << (64 - effective_bits);
}

ContentDefinedChunker::ContentDefinedChunker(const LevelInfo& level_info,
                                             uint64_t min_size, uint64_t max_size,
                                             uint8_t norm_factor)
    : level_info_(level_info),
      min_size_(min_size),
      max_size_(max_size),
      hash_mask_(GetMask(min_size, max_size, norm_factor)) {}

void ContentDefinedChunker::Roll(const bool value) {
  if (chunk_size_++ < min_size_) {
    // short-circuit if we haven't reached the minimum chunk size, this speeds up the
    // chunking process since the gearhash doesn't need to be updated
    return;
  }
  rolling_hash_ = (rolling_hash_ << 1) + kGearhashTable[nth_run_][value];
  has_matched_ = has_matched_ || ((rolling_hash_ & hash_mask_) == 0);
}

template <typename T>
void ContentDefinedChunker::Roll(const T* value) {
  constexpr size_t BYTE_WIDTH = sizeof(T);
  chunk_size_ += BYTE_WIDTH;
  if (chunk_size_ < min_size_) {
    // short-circuit if we haven't reached the minimum chunk size, this speeds up the
    // chunking process since the gearhash doesn't need to be updated
    return;
  }
  auto bytes = reinterpret_cast<const uint8_t*>(value);
  for (size_t i = 0; i < BYTE_WIDTH; ++i) {
    rolling_hash_ = (rolling_hash_ << 1) + kGearhashTable[nth_run_][bytes[i]];
    has_matched_ = has_matched_ || ((rolling_hash_ & hash_mask_) == 0);
  }
}

void ContentDefinedChunker::Roll(const uint8_t* value, int64_t num_bytes) {
  chunk_size_ += num_bytes;
  if (chunk_size_ < min_size_) {
    // short-circuit if we haven't reached the minimum chunk size, this speeds up the
    // chunking process since the gearhash doesn't need to be updated
    return;
  }
  for (int64_t i = 0; i < num_bytes; ++i) {
    rolling_hash_ = (rolling_hash_ << 1) + kGearhashTable[nth_run_][value[i]];
    has_matched_ = has_matched_ || ((rolling_hash_ & hash_mask_) == 0);
  }
}

bool ContentDefinedChunker::NeedNewChunk() {
  // decide whether to create a new chunk based on the rolling hash; has_matched_ is
  // set to true if we encountered a match since the last NeedNewChunk() call
  if (ARROW_PREDICT_FALSE(has_matched_)) {
    has_matched_ = false;
    // in order to have a normal distribution of chunk sizes, we only create a new chunk
    // if the adjused mask matches the rolling hash 8 times in a row, each run uses a
    // different gearhash table (gearhash's chunk size has exponential distribution, and
    // we use central limit theorem to approximate normal distribution)
    if (ARROW_PREDICT_FALSE(++nth_run_ >= 7)) {
      nth_run_ = 0;
      chunk_size_ = 0;
      return true;
    }
  }
  if (ARROW_PREDICT_FALSE(chunk_size_ >= max_size_)) {
    // we have a hard limit on the maximum chunk size, note that we don't reset the
    // rolling hash state here, so the next NeedNewChunk() call will continue from the
    // current state
    chunk_size_ = 0;
    return true;
  }
  return false;
}

template <typename RollFunc>
const std::vector<Chunk> ContentDefinedChunker::Calculate(const int16_t* def_levels,
                                                          const int16_t* rep_levels,
                                                          int64_t num_levels,
                                                          const RollFunc& RollValue) {
  std::vector<Chunk> chunks;
  int64_t offset;
  int64_t prev_offset = 0;
  int64_t prev_value_offset = 0;
  bool has_def_levels = level_info_.def_level > 0;
  bool has_rep_levels = level_info_.rep_level > 0;

  if (!has_rep_levels && !has_def_levels) {
    // fastest path for non-nested non-null data
    for (offset = 0; offset < num_levels; ++offset) {
      RollValue(offset);
      if (NeedNewChunk()) {
        chunks.emplace_back(prev_offset, prev_offset, offset - prev_offset);
        prev_offset = offset;
      }
    }
    // set the previous value offset to add the last chunk
    prev_value_offset = prev_offset;
  } else if (!has_rep_levels) {
    // non-nested data with nulls
    int16_t def_level;
    for (int64_t offset = 0; offset < num_levels; ++offset) {
      def_level = def_levels[offset];

      Roll(&def_level);
      if (def_level == level_info_.def_level) {
        RollValue(offset);
      }
      if (NeedNewChunk()) {
        chunks.emplace_back(prev_offset, prev_offset, offset - prev_offset);
        prev_offset = offset;
      }
    }
    // set the previous value offset to add the last chunk
    prev_value_offset = prev_offset;
  } else {
    // nested data with nulls
    int16_t def_level;
    int16_t rep_level;
    int64_t value_offset = 0;

    for (offset = 0; offset < num_levels; ++offset) {
      def_level = def_levels[offset];
      rep_level = rep_levels[offset];

      Roll(&def_level);
      Roll(&rep_level);
      if (def_level == level_info_.def_level) {
        RollValue(value_offset);
      }

      if ((rep_level == 0) && NeedNewChunk()) {
        // if we are at a record boundary and need a new chunk, we create a new chunk
        auto levels_to_write = offset - prev_offset;
        if (levels_to_write > 0) {
          chunks.emplace_back(prev_offset, prev_value_offset, levels_to_write);
          prev_offset = offset;
          prev_value_offset = value_offset;
        }
      }
      if (def_level >= level_info_.repeated_ancestor_def_level) {
        // we only increment the value offset if we have a leaf value
        ++value_offset;
      }
    }
  }

  // add the last chunk if we have any levels left
  if (prev_offset < num_levels) {
    chunks.emplace_back(prev_offset, prev_value_offset, num_levels - prev_offset);
  }
  return chunks;
}

#define FIXED_WIDTH_CASE(CType)                                        \
  {                                                                    \
    const auto raw_values = values.data()->GetValues<CType>(1);        \
    return Calculate(def_levels, rep_levels, num_levels,               \
                     [&](int64_t i) { return Roll(raw_values + i); }); \
  }

#define BINARY_LIKE_CASE(OffsetCType)                                     \
  {                                                                       \
    const auto raw_offsets = values.data()->GetValues<OffsetCType>(1);    \
    const auto raw_values = values.data()->GetValues<uint8_t>(2);         \
    return Calculate(def_levels, rep_levels, num_levels, [&](int64_t i) { \
      const OffsetCType pos = raw_offsets[i];                             \
      const OffsetCType length = raw_offsets[i + 1] - pos;                \
      Roll(raw_values + pos, length);                                     \
    });                                                                   \
  }

const std::vector<Chunk> ContentDefinedChunker::GetBoundaries(
    const int16_t* def_levels, const int16_t* rep_levels, int64_t num_levels,
    const ::arrow::Array& values) {
  auto type_id = values.type()->id();
  switch (type_id) {
    case ::arrow::Type::NA: {
      return Calculate(def_levels, rep_levels, num_levels, [](int64_t) {});
    }
    case ::arrow::Type::BOOL: {
      const auto& bool_array = static_cast<const ::arrow::BooleanArray&>(values);
      return Calculate(def_levels, rep_levels, num_levels,
                       [&](int64_t i) { return Roll(bool_array.Value(i)); });
    }
    case ::arrow::Type::INT8:
    case ::arrow::Type::UINT8:
      FIXED_WIDTH_CASE(uint8_t)
    case ::arrow::Type::INT16:
    case ::arrow::Type::UINT16:
    case ::arrow::Type::HALF_FLOAT:
      FIXED_WIDTH_CASE(uint16_t)
    case ::arrow::Type::INT32:
    case ::arrow::Type::UINT32:
    case ::arrow::Type::FLOAT:
    case ::arrow::Type::DATE32:
    case ::arrow::Type::TIME32:
      FIXED_WIDTH_CASE(uint32_t)
    case ::arrow::Type::INT64:
    case ::arrow::Type::UINT64:
    case ::arrow::Type::DOUBLE:
    case ::arrow::Type::DATE64:
    case ::arrow::Type::TIME64:
    case ::arrow::Type::TIMESTAMP:
    case ::arrow::Type::DURATION:
      FIXED_WIDTH_CASE(uint64_t)
    case ::arrow::Type::BINARY:
    case ::arrow::Type::STRING:
      BINARY_LIKE_CASE(int32_t)
    case ::arrow::Type::LARGE_BINARY:
    case ::arrow::Type::LARGE_STRING:
      BINARY_LIKE_CASE(int64_t)
    case ::arrow::Type::DECIMAL128:
    case ::arrow::Type::DECIMAL256:
    case ::arrow::Type::FIXED_SIZE_BINARY: {
      const auto raw_values = values.data()->GetValues<uint8_t>(1);
      const auto byte_width =
          static_cast<const ::arrow::FixedSizeBinaryArray&>(values).byte_width();
      return Calculate(def_levels, rep_levels, num_levels, [&](int64_t i) {
        return Roll(raw_values + i * byte_width, byte_width);
      });
    }
    case ::arrow::Type::DICTIONARY:
      return GetBoundaries(
          def_levels, rep_levels, num_levels,
          *static_cast<const ::arrow::DictionaryArray&>(values).indices());
    default:
      throw ParquetException("Unsupported Arrow array type " + values.type()->ToString());
  }
}

}  // namespace parquet::internal
