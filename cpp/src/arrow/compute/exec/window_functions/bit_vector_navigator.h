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

#pragma once

#include <cstdint>
#include "arrow/compute/exec/util.h"
#include "arrow/util/bit_util.h"

namespace arrow {
namespace compute {

// Storage for a bit vector to be used with BitVectorNavigator and its variants.
//
// Supports weaved bit vectors.
//
class BitVectorWithCountsBase {
  template <bool T>
  friend class BitVectorNavigatorImp;

 public:
  BitVectorWithCountsBase() : num_children_(0), num_bits_per_child_(0) {}

  void Resize(int64_t num_bits_per_child, int64_t num_children = 1) {
    ARROW_DCHECK(num_children > 0 && num_bits_per_child > 0);
    num_children_ = num_children;
    num_bits_per_child_ = num_bits_per_child;
    int64_t num_words =
        bit_util::CeilDiv(num_bits_per_child, kBitsPerWord) * num_children;
    bits_.resize(num_words);
    mid_counts_.resize(num_words);
    int64_t num_blocks =
        bit_util::CeilDiv(num_bits_per_child, kBitsPerBlock) * num_children;
    top_counts_.resize(num_blocks);
  }

  void ClearBits() { memset(bits_.data(), 0, bits_.size() * sizeof(bits_[0])); }

  // Word is 64 adjacent bits
  //
  static constexpr int64_t kBitsPerWord = 64;
  // Block is 65536 adjacent bits
  // (that means that 16-bit counters can be used within the block)
  //
#ifndef NDEBUG
  static constexpr int kLogBitsPerBlock = 7;
#else
  static constexpr int kLogBitsPerBlock = 16;
#endif
  static constexpr int64_t kBitsPerBlock = 1LL << kLogBitsPerBlock;

 protected:
  int64_t num_children_;
  int64_t num_bits_per_child_;
  // TODO: Replace vectors with ResizableBuffers. Return error status from
  // Resize on out-of-memory.
  //
  std::vector<uint64_t> bits_;
  std::vector<int64_t> top_counts_;
  std::vector<uint16_t> mid_counts_;
};

template <bool SINGLE_CHILD_BIT_VECTOR>
class BitVectorNavigatorImp {
 public:
  BitVectorNavigatorImp() : container_(NULLPTR) {}

  BitVectorNavigatorImp(BitVectorWithCountsBase* container, int64_t child_index)
      : container_(container), child_index_(child_index) {}

  int64_t block_count() const {
    return bit_util::CeilDiv(container_->num_bits_per_child_,
                             BitVectorWithCountsBase::kBitsPerBlock);
  }

  int64_t word_count() const {
    return bit_util::CeilDiv(container_->num_bits_per_child_,
                             BitVectorWithCountsBase::kBitsPerWord);
  }

  int64_t bit_count() const { return container_->num_bits_per_child_; }

  int64_t pop_count() const {
    int64_t last_block = block_count() - 1;
    int64_t last_word = word_count() - 1;
    int num_bits_last_word =
        static_cast<int>((bit_count() - 1) % BitVectorWithCountsBase::kBitsPerWord + 1);
    uint64_t last_word_mask = ~0ULL >> (64 - num_bits_last_word);
    return container_->top_counts_[apply_stride_and_offset(last_block)] +
           container_->mid_counts_[apply_stride_and_offset(last_word)] +
           ARROW_POPCOUNT64(container_->bits_[apply_stride_and_offset(last_word)] &
                            last_word_mask);
  }

  const uint8_t* GetBytes() const {
    return reinterpret_cast<const uint8_t*>(container_->bits_.data());
  }

  void BuildMidCounts(int64_t block_index) {
    ARROW_DCHECK(block_index >= 0 &&
                 block_index < static_cast<int64_t>(container_->mid_counts_.size()));
    constexpr int64_t words_per_block =
        BitVectorWithCountsBase::kBitsPerBlock / BitVectorWithCountsBase::kBitsPerWord;
    int64_t word_begin = block_index * words_per_block;
    int64_t word_end = std::min(word_count(), word_begin + words_per_block);

    const uint64_t* words = container_->bits_.data();
    uint16_t* counters = container_->mid_counts_.data();

    uint16_t count = 0;
    for (int64_t word_index = word_begin; word_index < word_end; ++word_index) {
      counters[apply_stride_and_offset(word_index)] = count;
      count += static_cast<uint16_t>(
          ARROW_POPCOUNT64(words[apply_stride_and_offset(word_index)]));
    }
  }

  void BuildTopCounts(int64_t block_index_begin, int64_t block_index_end,
                      int64_t initial_count = 0) {
    const uint64_t* words = container_->bits_.data();
    int64_t* counters = container_->top_counts_.data();
    const uint16_t* mid_counters = container_->mid_counts_.data();

    int64_t count = initial_count;

    for (int64_t block_index = block_index_begin; block_index < block_index_end - 1;
         ++block_index) {
      counters[apply_stride_and_offset(block_index)] = count;

      constexpr int64_t words_per_block =
          BitVectorWithCountsBase::kBitsPerBlock / BitVectorWithCountsBase::kBitsPerWord;

      int64_t word_begin = block_index * words_per_block;
      int64_t word_end = std::min(word_count(), word_begin + words_per_block);

      count += mid_counters[apply_stride_and_offset(word_end - 1)];
      count += ARROW_POPCOUNT64(words[apply_stride_and_offset(word_end - 1)]);
    }
    counters[apply_stride_and_offset(block_index_end - 1)] = count;
  }

  // Position of the nth bit set (input argument zero corresponds to the first
  // bit set).
  //
  int64_t Select(int64_t rank) const {
    if (rank < 0) {
      return BeforeFirstBit();
    }
    if (rank >= pop_count()) {
      return AfterLastBit();
    }

    constexpr int64_t bits_per_block = BitVectorWithCountsBase::kBitsPerBlock;
    constexpr int64_t bits_per_word = BitVectorWithCountsBase::kBitsPerWord;
    constexpr int64_t words_per_block = bits_per_block / bits_per_word;
    const int64_t* top_counters = container_->top_counts_.data();
    const uint16_t* mid_counters = container_->mid_counts_.data();
    const uint64_t* words = container_->bits_.data();

    // Binary search in top level counters.
    //
    // Equivalent of std::upper_bound() - 1, but not using iterators.
    //
    int64_t begin = 0;
    int64_t end = block_count();
    while (end - begin > 1) {
      int64_t middle = (begin + end) / 2;
      int reject_left_half =
          (rank >= top_counters[apply_stride_and_offset(middle)]) ? 1 : 0;
      begin = begin + (middle - begin) * reject_left_half;
      end = middle + (end - middle) * reject_left_half;
    }

    int64_t block_index = begin;
    rank -= top_counters[apply_stride_and_offset(begin)];

    // Continue with binary search in intermediate level counters of the
    // selected block.
    //
    begin = block_index * words_per_block;
    end = std::min(word_count(), begin + words_per_block);
    while (end - begin > 1) {
      int64_t middle = (begin + end) / 2;
      int reject_left_half =
          (rank >= mid_counters[apply_stride_and_offset(middle)]) ? 1 : 0;
      begin = begin + (middle - begin) * reject_left_half;
      end = middle + (end - middle) * reject_left_half;
    }

    int64_t word_index = begin;
    rank -= mid_counters[apply_stride_and_offset(begin)];

    // Continue with binary search in the selected word.
    //
    uint64_t word = words[apply_stride_and_offset(word_index)];
    int pop_count_prefix = 0;
    int bit_count_prefix = 0;
    const uint64_t masks[6] = {0xFFFFFFFFULL, 0xFFFFULL, 0xFFULL, 0xFULL, 0x3ULL, 0x1ULL};
    int bit_count_left_half = 32;
    for (int i = 0; i < 6; ++i) {
      int pop_count_left_half =
          static_cast<int>(ARROW_POPCOUNT64((word >> bit_count_prefix) & masks[i]));
      int reject_left_half = (rank >= pop_count_prefix + pop_count_left_half) ? 1 : 0;
      pop_count_prefix += reject_left_half * pop_count_left_half;
      bit_count_prefix += reject_left_half * bit_count_left_half;
      bit_count_left_half /= 2;
    }

    return word_index * bits_per_word + bit_count_prefix;
  }

  void Select(int64_t rank_begin, int64_t rank_end, int64_t* selects,
              const ThreadContext& thread_ctx) const {
    ARROW_DCHECK(rank_begin <= rank_end);

    // For ranks out of the range represented in the bit vector return
    // BeforeFirstBit() or AfterLastBit().
    //
    if (rank_begin < 0) {
      int64_t num_ranks_to_skip =
          std::min(rank_end, static_cast<int64_t>(0)) - rank_begin;
      for (int64_t i = 0LL; i < num_ranks_to_skip; ++i) {
        selects[i] = BeforeFirstBit();
      }
      selects += num_ranks_to_skip;
      rank_begin += num_ranks_to_skip;
    }

    int64_t rank_max = pop_count() - 1;
    if (rank_end > rank_max + 1) {
      int64_t num_ranks_to_skip = rank_end - std::max(rank_begin, rank_max + 1);
      for (int64_t i = 0LL; i < num_ranks_to_skip; ++i) {
        selects[rank_end - num_ranks_to_skip + i] = AfterLastBit();
      }
      rank_end -= num_ranks_to_skip;
    }

    // If there are no more ranks left then we are done.
    //
    if (rank_begin == rank_end) {
      return;
    }

    auto temp_vector_stack = thread_ctx.temp_vector_stack;  // For TEMP_VECTOR
    TEMP_VECTOR(uint16_t, ids);
    int num_ids;
    TEMP_VECTOR(uint64_t, temp_words);

    int64_t select_begin = Select(rank_begin);
    int64_t select_end = Select(rank_end - 1) + 1;

    constexpr int64_t bits_per_word = BitVectorWithCountsBase::kBitsPerWord;
    const uint64_t* words = container_->bits_.data();

    // Split processing into mini batches, in order to use small buffers on
    // the stack (and in CPU cache) for intermediate vectors.
    //
    BEGIN_MINI_BATCH_FOR(batch_begin, batch_length, select_end - select_begin)

    int64_t bit_begin = select_begin + batch_begin;
    int64_t word_begin = bit_begin / bits_per_word;
    int64_t word_end =
        (select_begin + batch_begin + batch_length - 1) / bits_per_word + 1;

    // Copy words from interleaved bit vector to the temporary buffer that will
    // have them in a contiguous block of memory.
    //
    for (int64_t word_index = word_begin; word_index < word_end; ++word_index) {
      temp_words[word_index - word_begin] = words[apply_stride_and_offset(word_index)];
    }

    // Find positions of all bits set in current mini-batch of bits
    //
    util::bit_util::bits_to_indexes(
        /*bit_to_search=*/1, thread_ctx.hardware_flags, static_cast<int>(batch_length),
        reinterpret_cast<const uint8_t*>(temp_words), &num_ids, ids,
        static_cast<int>(bit_begin % bits_per_word));

    // Output positions of bits set.
    //
    for (int i = 0; i < num_ids; ++i) {
      selects[i] = bit_begin + ids[i];
    }
    selects += num_ids;

    END_MINI_BATCH_FOR
  }

  template <bool INCLUSIVE_RANK>
  int64_t RankImp(int64_t bit_index) const {
    const int64_t* top_counters = container_->top_counts_.data();
    const uint16_t* mid_counters = container_->mid_counts_.data();
    const uint64_t* words = container_->bits_.data();
    constexpr int64_t bits_per_block = BitVectorWithCountsBase::kBitsPerBlock;
    constexpr int64_t bits_per_word = BitVectorWithCountsBase::kBitsPerWord;
    uint64_t bit_mask = INCLUSIVE_RANK
                            ? (~0ULL >> (bits_per_word - 1 - (bit_index % bits_per_word)))
                            : ((1ULL << (bit_index % bits_per_word)) - 1ULL);
    return top_counters[apply_stride_and_offset(bit_index / bits_per_block)] +
           mid_counters[apply_stride_and_offset(bit_index / bits_per_word)] +
           ARROW_POPCOUNT64(words[apply_stride_and_offset(bit_index / bits_per_word)] &
                            bit_mask);
  }

  // Number of bits in the range [0, bit_index - 1] that are set.
  //
  int64_t Rank(int64_t bit_index) const {
    return RankImp</*INCLUSIVE_RANK=*/false>(bit_index);
  }

  void Rank(int64_t bit_index_begin, int64_t bit_index_end, int64_t* ranks) const {
    const uint64_t* words = container_->bits_.data();
    constexpr int64_t bits_per_word = BitVectorWithCountsBase::kBitsPerWord;

    int64_t rank = Rank(bit_index_begin);
    uint64_t word = words[apply_stride_and_offset(bit_index_begin / bits_per_word)];
    for (int64_t bit_index = bit_index_begin; bit_index < bit_index_end; ++bit_index) {
      if (bit_index % bits_per_word == 0) {
        word = words[apply_stride_and_offset(bit_index / bits_per_word)];
      }
      ranks[bit_index - bit_index_begin] = rank;
      rank += (word >> (bit_index % bits_per_word)) & 1;
    }
  }

  // Number of bits in the range [0, bit_index] that are set.
  //
  int64_t RankNext(int64_t bit_index) const {
    return RankImp</*INCLUSIVE_RANK=*/true>(bit_index);
  }

  uint64_t GetBit(int64_t bit_index) const {
    constexpr int64_t bits_per_word = BitVectorWithCountsBase::kBitsPerWord;
    return (GetWord(bit_index / bits_per_word) >> (bit_index % bits_per_word)) & 1ULL;
  }

  uint64_t GetWord(int64_t word_index) const {
    const uint64_t* words = container_->bits_.data();
    return words[apply_stride_and_offset(word_index)];
  }

  void SetBit(int64_t bit_index) {
    constexpr int64_t bits_per_word = BitVectorWithCountsBase::kBitsPerWord;
    int64_t word_index = bit_index / bits_per_word;
    SetWord(word_index, GetWord(word_index) | (1ULL << (bit_index % bits_per_word)));
  }

  void SetWord(int64_t word_index, uint64_t word_value) {
    uint64_t* words = container_->bits_.data();
    words[apply_stride_and_offset(word_index)] = word_value;
  }

  // Constants returned from select query when the rank is outside of the
  // range of ranks represented in the bit vector.
  //
  int64_t BeforeFirstBit() const { return -1LL; }

  int64_t AfterLastBit() const { return bit_count(); }

  // Populate bit vector and counters marking the first position in each group
  // of ties for the sequence of values.
  //
  template <typename T>
  void MarkTieBegins(int64_t length, const T* sorted) {
    container_->Resize(length);

    // We start from position 1, in order to not check (i==0) condition inside
    // the loop. First position always starts a new group.
    //
    uint64_t word = 1ULL;
    for (int64_t i = 1; i < length; ++i) {
      uint64_t bit_value = (sorted[i - 1] != sorted[i]) ? 1ULL : 0ULL;
      word |= bit_value << (i & 63);
      if ((i & 63) == 63) {
        SetWord(i / 64, word);
        word = 0ULL;
      }
    }
    if (length % 64 > 0) {
      SetWord(length / 64, word);
    }

    // Generate population counters for the bit vector.
    //
    for (int64_t block_index = 0; block_index < block_count(); ++block_index) {
      BuildMidCounts(block_index);
    }
    BuildTopCounts(0, block_count());
  }

  void DebugPrintCountersToFile(FILE* fout) const {
    int64_t num_words = bit_util::CeilDiv(container_->num_bits_per_child_,
                                          BitVectorWithCountsBase::kBitsPerWord);
    int64_t num_blocks = bit_util::CeilDiv(container_->num_bits_per_child_,
                                           BitVectorWithCountsBase::kBitsPerBlock);
    fprintf(fout, "\nmid_counts: ");
    for (int64_t word_index = 0; word_index < num_words; ++word_index) {
      fprintf(
          fout, "%d ",
          static_cast<int>(container_->mid_counts_[apply_stride_and_offset(word_index)]));
    }
    fprintf(fout, "\ntop_counts: ");
    for (int64_t block_index = 0; block_index < num_blocks; ++block_index) {
      fprintf(fout, "%d ",
              static_cast<int>(
                  container_->top_counts_[apply_stride_and_offset(block_index)]));
    }
  }

 private:
  int64_t apply_stride_and_offset(int64_t index) const {
    if (SINGLE_CHILD_BIT_VECTOR) {
      return index;
    }
    int64_t stride = container_->num_children_;
    int64_t offset = child_index_;
    return index * stride + offset;
  }

  BitVectorWithCountsBase* container_;
  int64_t child_index_;
};

using BitVectorNavigator = BitVectorNavigatorImp</*SINGLE_CHILD_BIT_VECTOR=*/true>;
using BitWeaverNavigator = BitVectorNavigatorImp</*SINGLE_CHILD_BIT_VECTOR=*/false>;

class BitVectorWithCounts : public BitVectorWithCountsBase {
 public:
  BitVectorNavigator GetNavigator() {
    ARROW_DCHECK(num_children_ == 1);
    return BitVectorNavigator(this, 0);
  }
  BitWeaverNavigator GetChildNavigator(int64_t child_index) {
    ARROW_DCHECK(child_index >= 0 && child_index < num_children_);
    return BitWeaverNavigator(this, child_index);
  }
};

class BitMatrixWithCounts {
 public:
  ~BitMatrixWithCounts() {
    for (size_t i = 0; i < bands_.size(); ++i) {
      if (bands_[i]) {
        delete bands_[i];
      }
    }
  }

  BitMatrixWithCounts() : band_size_(0), bit_count_(0), num_rows_allocated_(0) {}

  void Init(int band_size, int64_t bit_count) {
    ARROW_DCHECK(band_size > 0 && bit_count > 0);
    ARROW_DCHECK(band_size_ == 0);
    band_size_ = band_size;
    bit_count_ = bit_count;
    num_rows_allocated_ = 0;
  }

  void AddRow(int row_index) {
    // Make a room in a lookup table for row with this index if needed.
    //
    int row_index_end = static_cast<int>(row_navigators_.size());
    if (row_index >= row_index_end) {
      row_navigators_.resize(row_index + 1);
    }

    // Check if we need to allocate a new band.
    //
    int num_bands = static_cast<int>(bands_.size());
    if (num_rows_allocated_ == num_bands * band_size_) {
      bands_.push_back(new BitVectorWithCountsBase());
      bands_.back()->Resize(bit_count_, band_size_);
    }

    // Initialize BitWeaverNavigator for that row.
    //
    row_navigators_[row_index] =
        BitWeaverNavigator(bands_[num_rows_allocated_ / band_size_],
                           static_cast<int64_t>(num_rows_allocated_ % band_size_));

    ++num_rows_allocated_;
  }

  BitWeaverNavigator& GetMutableRow(int row_index) { return row_navigators_[row_index]; }

  const BitWeaverNavigator& GetRow(int row_index) const {
    return row_navigators_[row_index];
  }

 private:
  int band_size_;
  int64_t bit_count_;
  int num_rows_allocated_;
  std::vector<BitVectorWithCountsBase*> bands_;
  std::vector<BitWeaverNavigator> row_navigators_;
};

}  // namespace compute
}  // namespace arrow
