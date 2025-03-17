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
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/unreachable.h"
#include "arrow/visit_type_inline.h"
#include "parquet/chunker_internal_generated.h"
#include "parquet/exception.h"
#include "parquet/level_conversion.h"

namespace parquet::internal {

using ::arrow::internal::checked_cast;

/// Calculate the mask to use for the rolling hash, the mask is used to determine if a
/// new chunk should be created based on the rolling hash value. The mask is calculated
/// based on the min_size, max_size and norm_factor parameters.
///
/// Assuming that the gear hash hash random values with a uniform distribution, then each
/// bit in the actual value of rolling_hash_ has even probability of being set so a mask
/// with the top N bits set has a probability of 1/2^N of matching the rolling hash. This
/// is the judgment criteria for the original gear hash based content-defined chunking.
/// The main drawback of this approach is the non-uniform distribution of the chunk sizes.
///
/// Later on the FastCDC has improved the process by introducing:
/// - sub-minimum chunk cut-point skipping (not hashing the first `min_size` bytes)
/// - chunk size normalization (using two masks)
///
/// This implementation uses cut-point skipping because it improves the overall
/// performance and a more accurate alternative to have less skewed chunk size
/// distribution. Instead of using two different masks (one with a lower and one with a
/// higher probability of matching and switching them based on the actual chunk size), we
/// rather use 8 different gear hash tables and require having 8 consecutive matches while
/// switching between the used hashtables. This approach is based on central limit theorem
/// and approximates normal distribution of the chunk sizes.
//
// @param min_size The minimum chunk size (default 256KiB)
// @param max_size The maximum chunk size (default 1MiB)
// @param norm_factor Normalization factor (default 0)
// @return The mask used to compare against the rolling hash
static uint64_t GetMask(int64_t min_size, int64_t max_size, uint8_t norm_factor) {
  // calculate the average size of the chunks
  int64_t avg_size = (min_size + max_size) / 2;
  // since we are skipping the first `min_size` bytes for each chunk, we need to
  // target a smaller chunk size to reach the average size after skipping the first
  // `min_size` bytes
  int64_t target_size = avg_size - min_size;
  // assuming that the gear hash has a uniform distribution, we can calculate the mask
  // by taking the floor(log2(target_size))
  size_t mask_bits = ::arrow::bit_util::NumRequiredBits(target_size) - 1;
  // -3 because we are using 8 hash tables to have more gaussian-like distribution,
  // a user defined `norm_factor` can be used to adjust the mask size, hence the matching
  // probability, by increasing the norm_factor we increase the probability of matching
  // the mask, forcing the distribution closer to the average size
  size_t effective_bits = mask_bits - 3 - norm_factor;
  return std::numeric_limits<uint64_t>::max() << (64 - effective_bits);
}

class ContentDefinedChunker::Impl {
 public:
  Impl(const LevelInfo& level_info, int64_t min_size, int64_t max_size,
       int8_t norm_factor)
      : level_info_(level_info),
        min_size_(min_size),
        max_size_(max_size),
        hash_mask_(GetMask(min_size, max_size, norm_factor)) {
    if (min_size_ < 0) {
      throw ParquetException("min_size must be non-negative");
    }
    if (max_size_ < 0) {
      throw ParquetException("max_size must be non-negative");
    }
    if (min_size_ > max_size_) {
      throw ParquetException("min_size must be less than or equal to max_size");
    }
  }

  void Roll(const bool value) {
    if (chunk_size_++ < min_size_) {
      // short-circuit if we haven't reached the minimum chunk size, this speeds up the
      // chunking process since the gearhash doesn't need to be updated
      return;
    }
    rolling_hash_ = (rolling_hash_ << 1) + kGearhashTable[nth_run_][value];
    has_matched_ = has_matched_ || ((rolling_hash_ & hash_mask_) == 0);
  }

  template <int ByteWidth>
  void Roll(const uint8_t* value) {
    // Update the rolling hash with a compile-time known sized value, set has_matched_ to
    // true if the hash matches the mask.

    chunk_size_ += ByteWidth;
    if (chunk_size_ < min_size_) {
      // short-circuit if we haven't reached the minimum chunk size, this speeds up the
      // chunking process since the gearhash doesn't need to be updated
      return;
    }
    for (size_t i = 0; i < ByteWidth; ++i) {
      rolling_hash_ = (rolling_hash_ << 1) + kGearhashTable[nth_run_][value[i]];
      has_matched_ = has_matched_ || ((rolling_hash_ & hash_mask_) == 0);
    }
  }

  template <typename T>
  void Roll(const T* value) {
    return Roll<sizeof(T)>(reinterpret_cast<const uint8_t*>(value));
  }

  void Roll(const uint8_t* value, int64_t length) {
    // Update the rolling hash with a binary-like value, set has_matched_ to true if the
    // hash matches the mask.

    chunk_size_ += length;
    if (chunk_size_ < min_size_) {
      // short-circuit if we haven't reached the minimum chunk size, this speeds up the
      // chunking process since the gearhash doesn't need to be updated
      return;
    }
    for (auto i = 0; i < length; ++i) {
      rolling_hash_ = (rolling_hash_ << 1) + kGearhashTable[nth_run_][value[i]];
      has_matched_ = has_matched_ || ((rolling_hash_ & hash_mask_) == 0);
    }
  }

  bool NeedNewChunk() {
    // decide whether to create a new chunk based on the rolling hash; has_matched_ is
    // set to true if we encountered a match since the last NeedNewChunk() call
    if (ARROW_PREDICT_FALSE(has_matched_)) {
      has_matched_ = false;
      // in order to have a normal distribution of chunk sizes, we only create a new chunk
      // if the adjused mask matches the rolling hash 8 times in a row, each run uses a
      // different gearhash table (gearhash's chunk size has geometric distribution, and
      // we use central limit theorem to approximate normal distribution, see
      // section 6.2.1 in paper https://www.cidrdb.org/cidr2023/papers/p43-low.pdf)
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
  std::vector<Chunk> Calculate(const int16_t* def_levels, const int16_t* rep_levels,
                               int64_t num_levels, const RollFunc& RollValue) {
    // Calculate the chunk boundaries for typed Arrow arrays.
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
          chunks.push_back({prev_offset, prev_offset, offset - prev_offset});
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
          chunks.push_back({prev_offset, prev_offset, offset - prev_offset});
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
            chunks.push_back({prev_offset, prev_value_offset, levels_to_write});
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
      chunks.push_back({prev_offset, prev_value_offset, num_levels - prev_offset});
    }
    return chunks;
  }

  template <int kByteWidth>
  std::vector<Chunk> CalculateFixedWidth(const int16_t* def_levels,
                                         const int16_t* rep_levels, int64_t num_levels,
                                         const ::arrow::Array& values) {
    const uint8_t* raw_values =
        values.data()->GetValues<uint8_t>(1, 0) + values.offset() * kByteWidth;
    return Calculate(def_levels, rep_levels, num_levels, [&](int64_t i) {
      return Roll<kByteWidth>(&raw_values[i * kByteWidth]);
    });
  }

  template <typename ArrayType>
  std::vector<Chunk> CalculateBinaryLike(const int16_t* def_levels,
                                         const int16_t* rep_levels, int64_t num_levels,
                                         const ::arrow::Array& values) {
    const auto& array = checked_cast<const ArrayType&>(values);
    const uint8_t* value;
    typename ArrayType::offset_type length;
    return Calculate(def_levels, rep_levels, num_levels, [&](int64_t i) {
      value = array.GetValue(i, &length);
      Roll(value, length);
    });
  }

  std::vector<Chunk> GetChunks(const int16_t* def_levels, const int16_t* rep_levels,
                               int64_t num_levels, const ::arrow::Array& values) {
    auto handle_type = [&](auto&& type) -> std::vector<Chunk> {
      using ArrowType = std::decay_t<decltype(type)>;
      if constexpr (std::is_same<::arrow::DataType, ArrowType>::value) {
        // TODO(kszucs): this branch should be removed once #45816 is resolved
        ::arrow::Unreachable("DataType is not a concrete type");
      } else if constexpr (ArrowType::type_id == ::arrow::Type::NA) {
        return Calculate(def_levels, rep_levels, num_levels, [](int64_t) {});
      } else if constexpr (ArrowType::type_id == ::arrow::Type::BOOL) {
        const auto& array = static_cast<const ::arrow::BooleanArray&>(values);
        return Calculate(def_levels, rep_levels, num_levels,
                         [&](int64_t i) { return Roll(array.Value(i)); });
      } else if constexpr (ArrowType::type_id == ::arrow::Type::FIXED_SIZE_BINARY) {
        const auto& array = static_cast<const ::arrow::FixedSizeBinaryArray&>(values);
        const auto byte_width = array.byte_width();
        return Calculate(def_levels, rep_levels, num_levels,
                         [&](int64_t i) { Roll(array.GetValue(i), byte_width); });
      } else if constexpr (::arrow::is_primitive(ArrowType::type_id)) {
        using c_type = typename ArrowType::c_type;
        return CalculateFixedWidth<sizeof(c_type)>(def_levels, rep_levels, num_levels,
                                                   values);
      } else if constexpr (::arrow::is_decimal(ArrowType::type_id)) {
        return CalculateFixedWidth<ArrowType::kByteWidth>(def_levels, rep_levels,
                                                          num_levels, values);
      } else if constexpr (::arrow::is_binary_like(ArrowType::type_id)) {
        return CalculateBinaryLike<::arrow::BinaryArray>(def_levels, rep_levels,
                                                         num_levels, values);
      } else if constexpr (::arrow::is_large_binary_like(ArrowType::type_id)) {
        return CalculateBinaryLike<::arrow::LargeBinaryArray>(def_levels, rep_levels,
                                                              num_levels, values);
      } else if constexpr (::arrow::is_dictionary(ArrowType::type_id)) {
        return GetChunks(def_levels, rep_levels, num_levels,
                         *static_cast<const ::arrow::DictionaryArray&>(values).indices());
      } else {
        throw ParquetException("Unsupported Arrow array type " +
                               values.type()->ToString());
      }
    };
    return ::arrow::VisitType(*values.type(), handle_type);
  }

 private:
  // Reference to the column's level information
  const internal::LevelInfo& level_info_;
  // Minimum chunk size in bytes, the rolling hash will not be updated until this size is
  // reached for each chunk. Note that all data sent through the hash function is counted
  // towards the chunk size, including definition and repetition levels.
  const int64_t min_size_;
  const int64_t max_size_;
  // The mask to match the rolling hash against to determine if a new chunk should be
  // created. The mask is calculated based on min/max chunk size and the normalization
  // factor.
  const uint64_t hash_mask_;

  // Whether the rolling hash has matched the mask since the last chunk creation. This
  // flag is set true by the Roll() function when the mask is matched and reset to false
  // by NeedNewChunk() method.
  bool has_matched_ = false;
  // The current run of the rolling hash, used to normalize the chunk size distribution
  // by requiring multiple consecutive matches to create a new chunk.
  int8_t nth_run_ = 0;
  // Current chunk size in bytes, reset to 0 when a new chunk is created.
  int64_t chunk_size_ = 0;
  // Rolling hash state, never reset only initialized once for the entire column.
  uint64_t rolling_hash_ = 0;
};

ContentDefinedChunker::ContentDefinedChunker(const LevelInfo& level_info,
                                             int64_t min_size, int64_t max_size,
                                             int8_t norm_factor)
    : impl_(new Impl(level_info, min_size, max_size, norm_factor)) {}

ContentDefinedChunker::~ContentDefinedChunker() = default;

std::vector<Chunk> ContentDefinedChunker::GetChunks(const int16_t* def_levels,
                                                    const int16_t* rep_levels,
                                                    int64_t num_levels,
                                                    const ::arrow::Array& values) {
  return impl_->GetChunks(def_levels, rep_levels, num_levels, values);
}

}  // namespace parquet::internal
