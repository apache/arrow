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
#include "parquet/chunker_internal_hashtable.h"
#include "parquet/exception.h"
#include "parquet/level_conversion.h"

namespace parquet::internal {

// create a fake null array class with a GetView method returning 0 always
class FakeNullArray {
 public:
  uint8_t GetView(int64_t i) const { return 0; }

  std::shared_ptr<::arrow::DataType> type() const { return ::arrow::null(); }

  int64_t null_count() const { return 0; }
};

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
                                             std::pair<uint64_t, uint64_t> size_range,
                                             uint8_t norm_factor)
    : level_info_(level_info),
      min_size_(size_range.first),
      max_size_(size_range.second),
      hash_mask_(GetMask(size_range.first, size_range.second, norm_factor)) {}

template <typename T>
void ContentDefinedChunker::Roll(const T value) {
  constexpr size_t BYTE_WIDTH = sizeof(T);
  chunk_size_ += BYTE_WIDTH;
  if (chunk_size_ < min_size_) {
    // short-circuit if we haven't reached the minimum chunk size, this speeds up the
    // chunking process since the gearhash doesn't need to be updated
    return;
  }
  auto bytes = reinterpret_cast<const uint8_t*>(&value);
  for (size_t i = 0; i < BYTE_WIDTH; ++i) {
    rolling_hash_ = (rolling_hash_ << 1) + GEARHASH_TABLE[nth_run_][bytes[i]];
    has_matched_ = has_matched_ || ((rolling_hash_ & hash_mask_) == 0);
  }
}

void ContentDefinedChunker::Roll(std::string_view value) {
  chunk_size_ += value.size();
  if (chunk_size_ < min_size_) {
    // short-circuit if we haven't reached the minimum chunk size, this speeds up the
    // chunking process since the gearhash doesn't need to be updated
    return;
  }
  for (char c : value) {
    rolling_hash_ =
        (rolling_hash_ << 1) + GEARHASH_TABLE[nth_run_][static_cast<uint8_t>(c)];
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
    // we have a hard limit on the maximum chunk size, not that we don't reset the rolling
    // hash state here, so the next NeedNewChunk() call will continue from the current
    // state
    chunk_size_ = 0;
    return true;
  }
  return false;
}

template <typename T>
const std::vector<Chunk> ContentDefinedChunker::Calculate(const int16_t* def_levels,
                                                          const int16_t* rep_levels,
                                                          int64_t num_levels,
                                                          const T& leaf_array) {
  std::vector<Chunk> chunks;
  bool has_def_levels = level_info_.def_level > 0;
  bool has_rep_levels = level_info_.rep_level > 0;

  if (!has_rep_levels && !has_def_levels) {
    // fastest path for non-nested non-null data
    int64_t offset = 0;
    int64_t prev_offset = 0;
    while (offset < num_levels) {
      Roll(leaf_array.GetView(offset));
      ++offset;
      if (NeedNewChunk()) {
        chunks.emplace_back(prev_offset, prev_offset, offset - prev_offset);
        prev_offset = offset;
      }
    }
    if (prev_offset < num_levels) {
      chunks.emplace_back(prev_offset, prev_offset, num_levels - prev_offset);
    }
  } else if (!has_rep_levels) {
    // non-nested data with nulls
    int64_t offset = 0;
    int64_t prev_offset = 0;
    while (offset < num_levels) {
      Roll(def_levels[offset]);
      Roll(leaf_array.GetView(offset));
      ++offset;
      if (NeedNewChunk()) {
        chunks.emplace_back(prev_offset, prev_offset, offset - prev_offset);
        prev_offset = offset;
      }
    }
    if (prev_offset < num_levels) {
      chunks.emplace_back(prev_offset, prev_offset, num_levels - prev_offset);
    }
  } else {
    // nested data with nulls
    bool has_leaf_value;
    bool is_record_boundary;
    int16_t def_level;
    int16_t rep_level;
    int64_t value_offset = 0;
    int64_t record_level_offset = 0;
    int64_t record_value_offset = 0;

    for (int64_t level_offset = 0; level_offset < num_levels; ++level_offset) {
      def_level = def_levels[level_offset];
      rep_level = rep_levels[level_offset];

      has_leaf_value = def_level >= level_info_.repeated_ancestor_def_level;
      is_record_boundary = rep_level == 0;

      Roll(def_level);
      Roll(rep_level);
      if (has_leaf_value) {
        Roll(leaf_array.GetView(value_offset));
      }

      if (is_record_boundary && NeedNewChunk()) {
        auto levels_to_write = level_offset - record_level_offset;
        if (levels_to_write > 0) {
          chunks.emplace_back(record_level_offset, record_value_offset, levels_to_write);
          record_level_offset = level_offset;
          record_value_offset = value_offset;
        }
      }

      if (has_leaf_value) {
        ++value_offset;
      }
    }

    auto levels_to_write = num_levels - record_level_offset;
    if (levels_to_write > 0) {
      chunks.emplace_back(record_level_offset, record_value_offset, levels_to_write);
    }
  }

  return chunks;
}

#define PRIMITIVE_CASE(TYPE_ID, ArrowType)               \
  case ::arrow::Type::TYPE_ID:                           \
    return Calculate(def_levels, rep_levels, num_levels, \
                     static_cast<const ::arrow::ArrowType##Array&>(values));

const std::vector<Chunk> ContentDefinedChunker::GetBoundaries(
    const int16_t* def_levels, const int16_t* rep_levels, int64_t num_levels,
    const ::arrow::Array& values) {
  auto type_id = values.type()->id();
  switch (type_id) {
    PRIMITIVE_CASE(BOOL, Boolean)
    PRIMITIVE_CASE(INT8, Int8)
    PRIMITIVE_CASE(INT16, Int16)
    PRIMITIVE_CASE(INT32, Int32)
    PRIMITIVE_CASE(INT64, Int64)
    PRIMITIVE_CASE(UINT8, UInt8)
    PRIMITIVE_CASE(UINT16, UInt16)
    PRIMITIVE_CASE(UINT32, UInt32)
    PRIMITIVE_CASE(UINT64, UInt64)
    PRIMITIVE_CASE(HALF_FLOAT, HalfFloat)
    PRIMITIVE_CASE(FLOAT, Float)
    PRIMITIVE_CASE(DOUBLE, Double)
    PRIMITIVE_CASE(STRING, String)
    PRIMITIVE_CASE(LARGE_STRING, LargeString)
    PRIMITIVE_CASE(BINARY, Binary)
    PRIMITIVE_CASE(LARGE_BINARY, LargeBinary)
    PRIMITIVE_CASE(FIXED_SIZE_BINARY, FixedSizeBinary)
    PRIMITIVE_CASE(DATE32, Date32)
    PRIMITIVE_CASE(DATE64, Date64)
    PRIMITIVE_CASE(TIME32, Time32)
    PRIMITIVE_CASE(TIME64, Time64)
    PRIMITIVE_CASE(TIMESTAMP, Timestamp)
    PRIMITIVE_CASE(DURATION, Duration)
    PRIMITIVE_CASE(DECIMAL128, Decimal128)
    PRIMITIVE_CASE(DECIMAL256, Decimal256)
    case ::arrow::Type::DICTIONARY:
      return GetBoundaries(
          def_levels, rep_levels, num_levels,
          *static_cast<const ::arrow::DictionaryArray&>(values).indices());
    case ::arrow::Type::NA:
      FakeNullArray fake_null_array;
      return Calculate(def_levels, rep_levels, num_levels, fake_null_array);
    default:
      throw ParquetException("Unsupported Arrow array type " + values.type()->ToString());
  }
}

}  // namespace parquet::internal
