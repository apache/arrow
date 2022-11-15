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

#include "arrow/compute/exec/window_functions/merge_tree.h"

namespace arrow {
namespace compute {

bool MergeTree::IsPermutation(int64_t length, const int64_t* values) {
  std::vector<bool> present(length, false);
  for (int64_t i = 0; i < length; ++i) {
    auto value = values[i];
    if (value < 0LL || value >= length || present[value]) {
      return false;
    }
    present[value] = true;
  }
  return true;
}

int64_t MergeTree::NodeBegin(int level, int64_t pos) const {
  return pos & ~((1LL << level) - 1);
}

int64_t MergeTree::NodeEnd(int level, int64_t pos) const {
  return std::min(NodeBegin(level, pos) + (static_cast<int64_t>(1) << level), length_);
}

void MergeTree::CascadeBegin(int from_level, int64_t begin, int64_t* lbegin,
                             int64_t* rbegin) const {
  ARROW_DCHECK(begin >= 0 && begin < length_);
  ARROW_DCHECK(from_level >= 1);
  auto& split_bits = bit_matrix_.GetRow(from_level);
  auto node_begin = NodeBegin(from_level, begin);
  auto node_begin_plus_whole = node_begin + (1LL << from_level);
  auto node_begin_plus_half = node_begin + (1LL << (from_level - 1));
  int64_t node_popcnt = split_bits.Rank(begin) - node_begin / 2;
  *rbegin = node_begin_plus_half + node_popcnt;
  *lbegin = begin - node_popcnt;
  *lbegin =
      (*lbegin == node_begin_plus_half || *lbegin == length_) ? kEmptyRange : *lbegin;
  *rbegin =
      (*rbegin == node_begin_plus_whole || *rbegin == length_) ? kEmptyRange : *rbegin;
}

void MergeTree::CascadeEnd(int from_level, int64_t end, int64_t* lend,
                           int64_t* rend) const {
  ARROW_DCHECK(end > 0 && end <= length_);
  ARROW_DCHECK(from_level >= 1);
  auto& split_bits = bit_matrix_.GetRow(from_level);
  auto node_begin = NodeBegin(from_level, end - 1);
  auto node_begin_plus_half = node_begin + (1LL << (from_level - 1));
  int64_t node_popcnt = split_bits.RankNext(end - 1) - node_begin / 2;
  *rend = node_begin_plus_half + node_popcnt;
  *lend = end - node_popcnt;
  *rend = (*rend == node_begin_plus_half) ? kEmptyRange : *rend;
  *lend = (*lend == node_begin) ? kEmptyRange : *lend;
}

int64_t MergeTree::CascadePos(int from_level, int64_t pos) const {
  ARROW_DCHECK(pos >= 0 && pos < length_);
  ARROW_DCHECK(from_level >= 1);
  auto& split_bits = bit_matrix_.GetRow(from_level);
  auto node_begin = NodeBegin(from_level, pos);
  auto node_begin_plus_half = node_begin + (1LL << (from_level - 1));
  int64_t node_popcnt = split_bits.Rank(pos) - node_begin / 2;
  return split_bits.GetBit(pos) ? node_begin_plus_half + node_popcnt : pos - node_popcnt;
}

MergeTree::NodeSubsetType MergeTree::NodeIntersect(int level, int64_t pos, int64_t begin,
                                                   int64_t end) {
  auto node_begin = NodeBegin(level, pos);
  auto node_end = NodeEnd(level, pos);
  return (node_begin >= begin && node_end <= end) ? NodeSubsetType::FULL
         : (node_begin < end && node_end > begin) ? NodeSubsetType::PARTIAL
                                                  : NodeSubsetType::EMPTY;
}

template <typename T, bool MULTIPLE_SOURCE_NODES>
void MergeTree::SplitSubsetImp(const BitWeaverNavigator& split_bits, int source_level,
                               const T* source_level_vector, T* target_level_vector,
                               int64_t read_begin, int64_t read_end,
                               int64_t write_begin_bit0, int64_t write_begin_bit1,
                               ThreadContext& thread_ctx) {
  ARROW_DCHECK(source_level >= 1);

  if (read_end == read_begin) {
    return;
  }

  int64_t write_begin[2];
  write_begin[0] = write_begin_bit0;
  write_begin[1] = write_begin_bit1;
  int64_t write_offset[2];
  write_offset[0] = write_offset[1] = 0;
  int target_level = source_level - 1;
  int64_t target_node_mask = (1LL << target_level) - 1LL;
  if (MULTIPLE_SOURCE_NODES) {
    // In case of processing multiple input nodes,
    // we must align write_begin to the target level node boundary,
    // so that the target node index calculation inside the main loop behaves
    // correctly.
    //
    write_offset[0] = write_begin[0] & target_node_mask;
    write_offset[1] = write_begin[1] & target_node_mask;
    write_begin[0] &= ~target_node_mask;
    write_begin[1] &= ~target_node_mask;
  }

  uint64_t split_bits_batch[util::MiniBatch::kMiniBatchLength / 64 + 1];
  int num_ids_batch;
  auto temp_vector_stack = thread_ctx.temp_vector_stack;
  TEMP_VECTOR(uint16_t, ids_batch);

  // Split processing into mini batches, in order to use small buffers on
  // the stack (and in CPU cache) for intermediate vectors.
  //
  BEGIN_MINI_BATCH_FOR(batch_begin, batch_length, read_end - read_begin)

  // Copy bit vector words related to the current batch on the stack.
  //
  // Bit vector words from multiple levels are interleaved in memory, that
  // is why we make a copy here to form a contiguous block.
  //
  int64_t word_index_base = (read_begin + batch_begin) / 64;
  for (int64_t word_index = word_index_base;
       word_index <= (read_begin + (batch_begin + batch_length) - 1) / 64; ++word_index) {
    split_bits_batch[word_index - word_index_base] = split_bits.GetWord(word_index);
  }

  for (int bit = 0; bit <= 1; ++bit) {
    // Convert bits to lists of bit indices for each bit value.
    //
    util::bit_util::bits_to_indexes(
        bit, thread_ctx.hardware_flags, static_cast<int>(batch_length),
        reinterpret_cast<const uint8_t*>(split_bits_batch), &num_ids_batch, ids_batch,
        /*bit_offset=*/(read_begin + batch_begin) % 64);

    // For each bit index on the list, calculate position in the input array
    // and position in the output array, then make a copy of the value.
    //
    for (int64_t i = 0; i < num_ids_batch; ++i) {
      int64_t read_pos = read_begin + batch_begin + ids_batch[i];
      int64_t write_pos = write_offset[bit] + i;
      if (MULTIPLE_SOURCE_NODES) {
        // We may need to jump from one target node to the next in case of
        // processing multiple source nodes.
        // Update write position accordingly
        //
        write_pos = write_pos + (write_pos & ~target_node_mask);
      }
      write_pos += write_begin[bit];
      target_level_vector[write_pos] = source_level_vector[read_pos];
    }

    // Advance the write cursor for current bit value (bit 0 or 1).
    //
    write_offset[bit] += num_ids_batch;
  }

  END_MINI_BATCH_FOR
}

template <typename T>
void MergeTree::SplitSubset(int source_level, const T* source_level_vector,
                            T* target_level_vector, int64_t read_begin, int64_t read_end,
                            ThreadContext& thread_ctx) {
  auto& split_bits = bit_matrix_.GetRow(source_level);
  int64_t source_node_length = (1LL << source_level);
  bool single_node = (read_end - read_begin) <= source_node_length;

  // Calculate initial output positions for bits 0 and bits 1 respectively
  // and call a helper function to do the remaining processing.
  //
  int64_t source_node_begin = NodeBegin(source_level, read_begin);
  int64_t target_node_length = (1LL << (source_level - 1));
  int64_t write_begin[2];
  write_begin[1] = split_bits.Rank(read_begin);
  write_begin[0] = read_begin - write_begin[1];
  write_begin[0] += source_node_begin / 2;
  write_begin[1] += source_node_begin / 2 + target_node_length;

  if (single_node) {
    // The case when the entire input subset is contained within a single
    // node in the source level.
    //
    SplitSubsetImp<T, false>(split_bits, source_level, source_level_vector,
                             target_level_vector, read_begin, read_end, write_begin[0],
                             write_begin[1], thread_ctx);
  } else {
    SplitSubsetImp<T, true>(split_bits, source_level, source_level_vector,
                            target_level_vector, read_begin, read_end, write_begin[0],
                            write_begin[1], thread_ctx);
  }
}

void MergeTree::SetMorselLoglen(int morsel_loglen) { morsel_loglen_ = morsel_loglen; }

uint64_t MergeTree::GetWordUnaligned(const BitWeaverNavigator& source, int64_t bit_index,
                                     int num_bits) {
  ARROW_DCHECK(num_bits > 0 && num_bits <= 64);
  int64_t word_index = bit_index / 64;
  int64_t word_offset = bit_index % 64;
  uint64_t word = source.GetWord(word_index) >> word_offset;
  if (word_offset + num_bits > 64) {
    word |= source.GetWord(word_index + 1) << (64 - word_offset);
  }
  word &= (~0ULL >> (64 - num_bits));
  return word;
}

void MergeTree::UpdateWord(BitWeaverNavigator& target, int64_t bit_index, int num_bits,
                           uint64_t bits) {
  ARROW_DCHECK(num_bits > 0 && num_bits <= 64);
  ARROW_DCHECK(bit_index % 64 + num_bits <= 64);
  int64_t word_index = bit_index / 64;
  int64_t word_offset = bit_index % 64;
  uint64_t mask = (~0ULL >> (64 - num_bits)) << word_offset;
  bits = ((bits << word_offset) & mask);
  target.SetWord(word_index, (target.GetWord(word_index) & ~mask) | bits);
}

void MergeTree::BitMemcpy(const BitWeaverNavigator& source, BitWeaverNavigator& target,
                          int64_t source_begin, int64_t source_end,
                          int64_t target_begin) {
  int64_t num_bits = source_end - source_begin;
  if (num_bits == 0) {
    return;
  }

  int64_t target_end = target_begin + num_bits;
  int64_t target_word_begin = target_begin / 64;
  int64_t target_word_end = (target_end - 1) / 64 + 1;
  int64_t target_offset = target_begin % 64;

  // Process the first and the last target word.
  //
  if (target_word_end - target_word_begin == 1) {
    // There is only one output word
    //
    uint64_t input = GetWordUnaligned(source, source_begin, static_cast<int>(num_bits));
    UpdateWord(target, target_begin, static_cast<int>(num_bits), input);
    return;
  } else {
    // First output word
    //
    int num_bits_first_word = static_cast<int>(64 - target_offset);
    uint64_t input = GetWordUnaligned(source, source_begin, num_bits_first_word);
    UpdateWord(target, target_begin, num_bits_first_word, input);

    // Last output word
    //
    int num_bits_last_word = (target_end % 64 == 0) ? 64 : (target_end % 64);
    input = GetWordUnaligned(source, source_end - num_bits_last_word, num_bits_last_word);
    UpdateWord(target, target_end - num_bits_last_word, num_bits_last_word, input);
  }

  // Index of source word containing the last bit that needs to be copied to
  // the first target word.
  //
  int64_t source_word_begin =
      (source_begin + (target_word_begin * 64 + 63) - target_begin) / 64;

  // The case of aligned bit sequences
  //
  if (target_offset == (source_begin % 64)) {
    for (int64_t target_word = target_word_begin + 1; target_word < target_word_end - 1;
         ++target_word) {
      int64_t source_word = source_word_begin + (target_word - target_word_begin);
      target.SetWord(target_word, source.GetWord(source_word));
    }
    return;
  }

  int64_t first_unprocessed_source_bit = source_begin + (64 - target_offset);

  // Number of bits from a single input word carried from one output word to
  // the next
  //
  int num_carry_bits = 64 - first_unprocessed_source_bit % 64;
  ARROW_DCHECK(num_carry_bits > 0 && num_carry_bits < 64);

  // Carried bits
  //
  uint64_t carry = GetWordUnaligned(source, first_unprocessed_source_bit, num_carry_bits);

  // Process target words between the first and the last.
  //
  for (int64_t target_word = target_word_begin + 1; target_word < target_word_end - 1;
       ++target_word) {
    int64_t source_word = source_word_begin + (target_word - target_word_begin);
    uint64_t input = source.GetWord(source_word);
    uint64_t output = carry | (input << num_carry_bits);
    target.SetWord(target_word, output);
    carry = input >> (64 - num_carry_bits);
  }
}

void MergeTree::GetChildrenBoundaries(const BitWeaverNavigator& split_bits,
                                      int64_t num_source_nodes,
                                      int64_t* source_node_begins,
                                      int64_t* target_node_begins) {
  for (int64_t source_node_index = 0; source_node_index < num_source_nodes;
       ++source_node_index) {
    int64_t node_begin = source_node_begins[source_node_index];
    int64_t node_end = source_node_begins[source_node_index + 1];
    target_node_begins[2 * source_node_index + 0] = node_begin;
    if (node_begin == node_end) {
      target_node_begins[2 * source_node_index + 1] = node_begin;
    } else {
      int64_t num_bits_1 =
          split_bits.RankNext(node_end - 1) - split_bits.Rank(node_begin);
      int64_t num_bits_0 = (node_end - node_begin) - num_bits_1;
      target_node_begins[2 * source_node_index + 1] = node_begin + num_bits_0;
    }
  }
  int64_t num_target_nodes = 2 * num_source_nodes;
  target_node_begins[num_target_nodes] = source_node_begins[num_source_nodes];
}

void MergeTree::BuildUpperSliceMorsel(int level_begin, int64_t* permutation_of_X,
                                      int64_t* temp_permutation_of_X,
                                      int64_t morsel_index, ThreadContext& thread_ctx) {
  int64_t morsel_length = 1LL << morsel_loglen_;
  int64_t morsel_begin = morsel_index * morsel_length;
  int64_t morsel_end = std::min(length_, morsel_begin + morsel_length);

  ARROW_DCHECK((morsel_begin & (BitVectorWithCounts::kBitsPerBlock - 1)) == 0);
  ARROW_DCHECK((morsel_end & (BitVectorWithCounts::kBitsPerBlock - 1)) == 0 ||
               morsel_end == length_);
  ARROW_DCHECK(morsel_end > morsel_begin);

  int level_end = morsel_loglen_;
  ARROW_DCHECK(level_begin > level_end);

  std::vector<int64_t> node_begins[2];
  // Begin level may have multiple nodes but the morsel is contained in
  // just one.
  //
  node_begins[0].resize(2);
  node_begins[0][0] = morsel_begin;
  node_begins[0][1] = morsel_end;

  for (int level = level_begin; level > level_end; --level) {
    // Setup pointers to ping-pong buffers (for permutation of X).
    //
    int64_t* source_Xs;
    int64_t* target_Xs;
    if ((level_begin - level) % 2 == 0) {
      source_Xs = permutation_of_X;
      target_Xs = temp_permutation_of_X;
    } else {
      source_Xs = temp_permutation_of_X;
      target_Xs = permutation_of_X;
    }

    // Fill the bit vector
    //
    for (int64_t word_index = morsel_begin / 64;
         word_index < bit_util::CeilDiv(morsel_end, 64); ++word_index) {
      uint64_t word = 0;
      int num_bits = (word_index == (morsel_end / 64)) ? (morsel_end % 64) : 64;
      for (int i = 0; i < num_bits; ++i) {
        int64_t X = source_Xs[word_index * 64 + i];
        uint64_t bit = ((X >> (level - 1)) & 1ULL);
        word |= (bit << i);
      }
      bit_matrix_upper_slices_.GetMutableRow(level).SetWord(word_index, word);
    }

    // Fill the population counters
    //
    int64_t block_index_begin =
        (morsel_begin >> BitVectorWithCountsBase::kLogBitsPerBlock);
    int64_t block_index_end =
        ((morsel_end - 1) >> BitVectorWithCountsBase::kLogBitsPerBlock) + 1;
    for (int64_t block_index = block_index_begin; block_index < block_index_end;
         ++block_index) {
      bit_matrix_upper_slices_.GetMutableRow(level).BuildMidCounts(block_index);
    }
    bit_matrix_upper_slices_.GetMutableRow(level).BuildTopCounts(block_index_begin,
                                                                 block_index_end);

    // Setup pointers to ping-pong buffers (for node boundaries from previous
    // and current level).
    //
    int64_t num_source_nodes = (1LL << (level_begin - level));
    int64_t num_target_nodes = 2 * num_source_nodes;
    int64_t* source_node_begins;
    int64_t* target_node_begins;
    if ((level_begin - level) % 2 == 0) {
      source_node_begins = node_begins[0].data();
      node_begins[1].resize(num_target_nodes + 1);
      target_node_begins = node_begins[1].data();
    } else {
      source_node_begins = node_begins[1].data();
      node_begins[0].resize(num_target_nodes + 1);
      target_node_begins = node_begins[0].data();
    }

    // Compute boundaries of the children nodes (cummulative sum of children
    // sizes).
    //
    GetChildrenBoundaries(bit_matrix_upper_slices_.GetRow(level), num_source_nodes,
                          source_node_begins, target_node_begins);

    // Split vector of Xs, one parent node at a time.
    // Each parent node gets split into two children nodes.
    // Parent and child nodes can have arbitrary sizes, including zero.
    //
    for (int64_t source_node_index = 0; source_node_index < num_source_nodes;
         ++source_node_index) {
      SplitSubsetImp<int64_t, false>(
          bit_matrix_upper_slices_.GetRow(level), level, source_Xs, target_Xs,
          source_node_begins[source_node_index],
          source_node_begins[source_node_index + 1],
          target_node_begins[2 * source_node_index + 0],
          target_node_begins[2 * source_node_index + 1], thread_ctx);
    }
  }
}

void MergeTree::CombineUpperSlicesMorsel(int level_begin, int64_t output_morsel,
                                         int64_t* input_permutation_of_X,
                                         int64_t* output_permutation_of_X,
                                         ThreadContext& thread_ctx) {
  int level_end = morsel_loglen_;
  ARROW_DCHECK(level_begin > level_end);

  int64_t morsel_length = 1LL << morsel_loglen_;
  int64_t output_morsel_begin = output_morsel * morsel_length;
  int64_t output_morsel_end = std::min(length_, output_morsel_begin + morsel_length);

  int64_t begin_level_node_length = (1LL << level_begin);

  // Copy bits for begin level bit vector.
  //
  ARROW_DCHECK(output_morsel_begin % 64 == 0);
  for (int64_t word_index = output_morsel_begin / 64;
       word_index <= (output_morsel_end - 1) / 64; ++word_index) {
    bit_matrix_.GetMutableRow(level_begin)
        .SetWord(word_index,
                 bit_matrix_upper_slices_.GetRow(level_begin).GetWord(word_index));
  }

  // For each node of the top level
  // (every input morsel is contained in one such node):
  //
  for (int64_t begin_level_node = 0;
       begin_level_node < bit_util::CeilDiv(length_, begin_level_node_length);
       ++begin_level_node) {
    int64_t begin_level_node_begin = begin_level_node * begin_level_node_length;
    int64_t begin_level_node_end =
        std::min(length_, begin_level_node_begin + begin_level_node_length);

    int64_t num_input_morsels =
        bit_util::CeilDiv(begin_level_node_end - begin_level_node_begin, morsel_length);

    std::vector<int64_t> slice_node_begins[2];
    for (int64_t input_morsel = 0; input_morsel < num_input_morsels; ++input_morsel) {
      slice_node_begins[0].push_back(begin_level_node_begin +
                                     input_morsel * morsel_length);
    }
    slice_node_begins[0].push_back(begin_level_node_end);

    for (int level = level_begin - 1; level >= level_end; --level) {
      std::vector<int64_t>* parent_node_begins;
      std::vector<int64_t>* child_node_begins;
      if ((level_begin - level) % 2 == 1) {
        parent_node_begins = &slice_node_begins[0];
        child_node_begins = &slice_node_begins[1];
      } else {
        parent_node_begins = &slice_node_begins[1];
        child_node_begins = &slice_node_begins[0];
      }
      child_node_begins->resize((parent_node_begins->size() - 1) * 2 + 1);

      GetChildrenBoundaries(bit_matrix_upper_slices_.GetRow(level + 1),
                            static_cast<int64_t>(parent_node_begins->size()) - 1,
                            parent_node_begins->data(), child_node_begins->data());

      // Scan all output nodes and all input nodes for each of them.
      //
      // Filter to the subset of input-output node pairs that cross the output
      // morsel boundary.
      //
      int64_t num_output_nodes = (1LL << (level_begin - level));
      for (int64_t output_node = 0; output_node < num_output_nodes; ++output_node) {
        int64_t output_node_length = 1LL << level;
        int64_t output_begin = begin_level_node_begin + output_node * output_node_length;
        for (int64_t input_morsel = 0; input_morsel < num_input_morsels; ++input_morsel) {
          // Boundaries of the input node for a given input morsel and a given
          // output node.
          //
          int64_t input_begin =
              (*child_node_begins)[input_morsel * num_output_nodes + output_node];
          int64_t input_end =
              (*child_node_begins)[input_morsel * num_output_nodes + output_node + 1];
          int64_t input_length = input_end - input_begin;
          if (output_morsel_end > output_begin &&
              output_morsel_begin < output_begin + input_length) {
            // Clamp the copy request to have the output range within the output
            // morsel.
            //
            int64_t target_begin = std::max(output_morsel_begin, output_begin);
            int64_t target_end = std::min(output_morsel_end, output_begin + input_length);

            if (level == level_end) {
              // Reorder chunks of vector of X for level_end.
              //
              memcpy(output_permutation_of_X + target_begin,
                     input_permutation_of_X + input_begin + (target_begin - output_begin),
                     (target_end - target_begin) * sizeof(input_permutation_of_X[0]));
            } else {
              // Reorder bits in the split bit vector for all levels above
              // level_end.
              //
              BitMemcpy(bit_matrix_upper_slices_.GetRow(level),
                        bit_matrix_.GetMutableRow(level),
                        input_begin + (target_begin - output_begin),
                        input_begin + (target_end - output_begin), target_begin);
            }
          }

          // Advance write cursor
          //
          output_begin += input_length;
        }
      }
    }
  }

  // Fill the mid level population counters for bit vectors.
  //
  // Top level population counters will get initialized in a single-threaded
  // section at the end of the build process.
  //
  ARROW_DCHECK(output_morsel_begin % (BitVectorWithCounts::kBitsPerBlock) == 0);
  int64_t block_index_begin = (output_morsel_begin / BitVectorWithCounts::kBitsPerBlock);
  int64_t block_index_end =
      ((output_morsel_end - 1) / BitVectorWithCounts::kBitsPerBlock) + 1;

  for (int level = level_begin; level > level_end; --level) {
    for (int64_t block_index = block_index_begin; block_index < block_index_end;
         ++block_index) {
      bit_matrix_.GetMutableRow(level).BuildMidCounts(block_index);
    }
  }
}

void MergeTree::BuildLower(int level_begin, int64_t morsel_index,
                           int64_t* begin_permutation_of_X,
                           int64_t* temp_permutation_of_X, ThreadContext& thread_ctx) {
  int64_t morsel_length = 1LL << morsel_loglen_;
  int64_t morsel_begin = morsel_index * morsel_length;
  int64_t morsel_end = std::min(length_, morsel_begin + morsel_length);
  int64_t begin_level_node_length = 1LL << level_begin;
  ARROW_DCHECK(morsel_begin % begin_level_node_length == 0 &&
               (morsel_end % begin_level_node_length == 0 || morsel_end == length_));

  int64_t* permutation_of_X[2];
  permutation_of_X[0] = begin_permutation_of_X;
  permutation_of_X[1] = temp_permutation_of_X;

  for (int level = level_begin; level > 0; --level) {
    int selector = (level_begin - level) % 2;
    const int64_t* input_X = permutation_of_X[selector];
    int64_t* output_X = permutation_of_X[1 - selector];

    // Populate bit vector for current level based on (level - 1) bits of X in
    // the input vector.
    //
    ARROW_DCHECK(morsel_begin % 64 == 0);
    uint64_t word = 0ULL;
    for (int64_t i = morsel_begin; i < morsel_end; ++i) {
      word |= ((input_X[i] >> (level - 1)) & 1ULL) << (i % 64);
      if (i % 64 == 63) {
        bit_matrix_.GetMutableRow(level).SetWord(i / 64, word);
        word = 0ULL;
      }
    }
    if (morsel_end % 64 > 0) {
      bit_matrix_.GetMutableRow(level).SetWord(morsel_end / 64, word);
    }

    // Fille population counters for bit vector.
    //
    constexpr int64_t block_size = BitVectorWithCounts::kBitsPerBlock;
    int64_t block_index_begin = morsel_begin / block_size;
    int64_t block_index_end = (morsel_end - 1) / block_size + 1;
    for (int64_t block_index = block_index_begin; block_index < block_index_end;
         ++block_index) {
      bit_matrix_.GetMutableRow(level).BuildMidCounts(block_index);
    }
    bit_matrix_.GetMutableRow(level).BuildTopCounts(block_index_begin, block_index_end,
                                                    morsel_begin / 2);

    // Split X based on the generated bit vector.
    //
    SplitSubset(level, input_X, output_X, morsel_begin, morsel_end, thread_ctx);
  }
}

Status MergeTree::Build(int64_t length, int level_begin, int64_t* permutation_of_X,
                        ParallelForStream& parallel_fors) {
  morsel_loglen_ = kMinMorselLoglen;
  length_ = length;
  temp_permutation_of_X_.resize(length);

  // Allocate matrix bits.
  //
  int upper_slices_level_end = morsel_loglen_;
  int num_upper_levels = std::max(0, level_begin - upper_slices_level_end);
  bit_matrix_.Init(kBitMatrixBandSize, length);
  for (int level = 1; level <= level_begin; ++level) {
    bit_matrix_.AddRow(level);
  }
  bit_matrix_upper_slices_.Init(kBitMatrixBandSize, length);
  for (int level = upper_slices_level_end + 1; level <= level_begin; ++level) {
    bit_matrix_upper_slices_.AddRow(level);
  }

  int64_t num_morsels = bit_util::CeilDiv(length_, 1LL << morsel_loglen_);

  // Upper slices of merge tree are generated for levels for which the size of
  // each node is greater than a single morsel.
  //
  // If there are such level, then add parallel for loops that create upper
  // slices and then combine them.
  //
  if (num_upper_levels > 0) {
    parallel_fors.InsertParallelFor(
        num_morsels,
        [this, level_begin, permutation_of_X](int64_t morsel_index,
                                              ThreadContext& thread_context) -> Status {
          BuildUpperSliceMorsel(level_begin, permutation_of_X,
                                temp_permutation_of_X_.data(), morsel_index,
                                thread_context);
          return Status::OK();
        });
    parallel_fors.InsertParallelFor(
        num_morsels,
        [this, level_begin, num_upper_levels, permutation_of_X](
            int64_t morsel_index, ThreadContext& thread_context) -> Status {
          CombineUpperSlicesMorsel(
              level_begin, morsel_index,
              (num_upper_levels % 2 == 0) ? permutation_of_X
                                          : temp_permutation_of_X_.data(),
              (num_upper_levels % 2 == 0) ? temp_permutation_of_X_.data()
                                          : permutation_of_X,
              thread_context);
          return Status::OK();
        });
  }
  parallel_fors.InsertParallelFor(
      num_morsels,
      [this, level_begin, num_upper_levels, upper_slices_level_end, permutation_of_X](
          int64_t morsel_index, ThreadContext& thread_context) -> Status {
        BuildLower(std::min(level_begin, upper_slices_level_end), morsel_index,
                   (num_upper_levels > 0 && (num_upper_levels % 2 == 0))
                       ? temp_permutation_of_X_.data()
                       : permutation_of_X,
                   (num_upper_levels > 0 && (num_upper_levels % 2 == 0))
                       ? permutation_of_X
                       : temp_permutation_of_X_.data(),
                   thread_context);
        return Status::OK();
      });
  parallel_fors.InsertTaskSingle(
      [this, level_begin](int64_t morsel_index, ThreadContext& thread_context) -> Status {
        // Fill the top level population counters for upper level bit vectors.
        //
        int level_end = morsel_loglen_;
        int64_t num_blocks =
            bit_util::CeilDiv(length_, BitVectorWithCountsBase::kBitsPerBlock);
        for (int level = level_begin; level > level_end; --level) {
          bit_matrix_.GetMutableRow(level).BuildTopCounts(0, num_blocks, 0);
        }

        // Release the pair of temporary vectors representing permutation of
        // X.
        //
        std::vector<int64_t>().swap(temp_permutation_of_X_);

        return Status::OK();
      });

  return Status::OK();
}

void MergeTree::BoxQuery(const BoxQueryRequest& queries, ThreadContext& thread_ctx) {
  auto temp_vector_stack = thread_ctx.temp_vector_stack;  // For TEMP_VECTOR
  TEMP_VECTOR(int64_t, partial_results0);
  TEMP_VECTOR(int64_t, partial_results1);
  TEMP_VECTOR(int64_t, y_ends_copy);

  int64_t child_cursors[5];
  child_cursors[4] = kEmptyRange;

  // Split processing into mini batches, in order to use small buffers on
  // the stack (and in CPU cache) for intermediate vectors.
  //
  BEGIN_MINI_BATCH_FOR(batch_begin, batch_length, queries.num_queries)

  // Preserve initial state, that is the upper bound on y coordinate.
  // It will be overwritten for each range of the frame, during tree traversal.
  //
  if (queries.num_x_ranges > 1) {
    for (int64_t i = 0; i < batch_length; ++i) {
      y_ends_copy[i] = queries.states[batch_begin + i].ends[0];
    }
  }

  for (int x_range_index = 0; x_range_index < queries.num_x_ranges; ++x_range_index) {
    const int64_t* xbegins = queries.xbegins[x_range_index];
    const int64_t* xends = queries.xends[x_range_index];

    // Restore the initial state for ranges after the first one.
    // Every range during its processing overwrites it.
    //
    if (x_range_index > 0) {
      for (int64_t i = 0; i < batch_length; ++i) {
        queries.states[batch_begin + i].ends[0] = y_ends_copy[i];
        queries.states[batch_begin + i].ends[1] = MergeTree::kEmptyRange;
      }
    }

    if (queries.level_begin == num_levels() - 1 && num_levels() == 1) {
      // Check if the entire top level node is in X range
      //
      for (int i = 0; i < batch_length; ++i) {
        partial_results0[i] = partial_results1[i] = kEmptyRange;
      }
      for (int64_t query_index = batch_begin; query_index < batch_begin + batch_length;
           ++query_index) {
        auto& state = queries.states[query_index];
        ARROW_DCHECK(state.ends[1] == kEmptyRange);
        int64_t xbegin = xbegins[query_index];
        int64_t xend = xends[query_index];
        if (state.ends[0] != kEmptyRange) {
          if (NodeIntersect(num_levels() - 1, state.ends[0] - 1, xbegin, xend) ==
              NodeSubsetType::FULL) {
            partial_results0[query_index - batch_begin] = state.ends[0];
          }
        }
      }
      queries.report_results_callback_(num_levels() - 1, batch_begin,
                                       batch_begin + batch_length, partial_results0,
                                       partial_results1, thread_ctx);
    }

    for (int level = queries.level_begin; level > queries.level_end; --level) {
      for (int64_t query_index = batch_begin; query_index < batch_begin + batch_length;
           ++query_index) {
        auto& state = queries.states[query_index];
        int64_t xbegin = xbegins[query_index];
        int64_t xend = xends[query_index];

        // Predication: kEmptyRange is replaced with special constants,
        // which are always a valid input, in order to avoid conditional
        // branches.
        //
        // We will later correct values returned by called functions for
        // kEmptyRange inputs.
        //
        constexpr int64_t kCascadeReplacement = static_cast<int64_t>(1);
        constexpr int64_t kIntersectReplacement = static_cast<int64_t>(0);

        // Use fractional cascading to traverse one level down the tree
        //
        for (int i = 0; i < 2; ++i) {
          CascadeEnd(level,
                     state.ends[i] == kEmptyRange ? kCascadeReplacement : state.ends[i],
                     &child_cursors[2 * i + 0], &child_cursors[2 * i + 1]);
        }

        // For each child node check:
        // a) if it should be rejected (outside of specified range of X),
        // b) if it should be included in the reported results (fully inside
        // of specified range of X).
        //
        int node_intersects_flags = 0;
        int node_inside_flags = 0;
        for (int i = 0; i < 4; ++i) {
          child_cursors[i] =
              state.ends[i / 2] == kEmptyRange ? kEmptyRange : child_cursors[i];
          auto intersection =
              NodeIntersect(level - 1,
                            child_cursors[i] == kEmptyRange ? kIntersectReplacement
                                                            : child_cursors[i] - 1,
                            xbegin, xend);
          intersection =
              child_cursors[i] == kEmptyRange ? NodeSubsetType::EMPTY : intersection;
          node_intersects_flags |= (intersection == NodeSubsetType::PARTIAL ? 1 : 0) << i;
          node_inside_flags |= (intersection == NodeSubsetType::FULL ? 1 : 0) << i;
        }

        // We shouldn't have more than two bits set in each intersection bit
        // masks.
        //
        ARROW_DCHECK(ARROW_POPCOUNT64(node_intersects_flags) <= 2);
        ARROW_DCHECK(ARROW_POPCOUNT64(node_inside_flags) <= 2);

        // Shuffle generated child node cursors based on X range
        // intersection results.
        //
        static constexpr uint8_t kNil = 4;
        uint8_t source_shuffle_index[16][2] = {
            {kNil, kNil}, {0, kNil},    {1, kNil},    {0, 1},
            {2, kNil},    {0, 2},       {1, 2},       {kNil, kNil},
            {3, kNil},    {0, 3},       {1, 3},       {kNil, kNil},
            {2, 3},       {kNil, kNil}, {kNil, kNil}, {kNil, kNil}};
        state.ends[0] = child_cursors[source_shuffle_index[node_intersects_flags][0]];
        state.ends[1] = child_cursors[source_shuffle_index[node_intersects_flags][1]];
        partial_results0[query_index - batch_begin] =
            child_cursors[source_shuffle_index[node_inside_flags][0]];
        partial_results1[query_index - batch_begin] =
            child_cursors[source_shuffle_index[node_inside_flags][1]];
      }

      // Report partial query results.
      //
      queries.report_results_callback_(level - 1, batch_begin, batch_begin + batch_length,
                                       partial_results0, partial_results1, thread_ctx);
    }
  }

  END_MINI_BATCH_FOR
}

void MergeTree::BoxCountQuery(int64_t num_queries, int num_x_ranges_per_query,
                              const int64_t** x_begins, const int64_t** x_ends,
                              const int64_t* y_ends, int64_t* results,
                              ThreadContext& thread_context) {
  // Callback function that updates the final count based on node prefixes
  // representing subsets that satisfy query constraints.
  //
  // There is one callback call per batch per level.
  //
  auto callback = [results](int level, int64_t batch_begin, int64_t batch_end,
                            const int64_t* partial_results0,
                            const int64_t* partial_results1,
                            ThreadContext& thread_context) {
    // Mask used to separate node offset from the offset within the node at
    // the current level.
    //
    int64_t mask = (1LL << level) - 1LL;
    for (int64_t query_index = batch_begin; query_index < batch_end; ++query_index) {
      int64_t partial_result0 = partial_results0[query_index];
      int64_t partial_result1 = partial_results1[query_index];

      // We may have between 0 and 2 node prefixes that satisfy query
      // constraints for each query.
      //
      // To find out their number we need to check if each of the two reported
      // indices is equal to kEmptyRange.
      //
      // For a valid node prefix, the index reported represents the position
      // one after the last element in the node prefix.
      //
      if (partial_result0 != kEmptyRange) {
        results[query_index] += ((partial_result0 - 1) & mask) + 1;
      }
      if (partial_result1 != kEmptyRange) {
        results[query_index] += ((partial_result1 - 1) & mask) + 1;
      }
    }
  };

  auto temp_vector_stack = thread_context.temp_vector_stack;
  TEMP_VECTOR(MergeTree::BoxQueryState, states);

  BEGIN_MINI_BATCH_FOR(batch_begin, batch_length, num_queries)

  // Populate BoxQueryRequest structure.
  //
  MergeTree::BoxQueryRequest request;
  request.report_results_callback_ = callback;
  request.num_queries = batch_length;
  request.num_x_ranges = num_x_ranges_per_query;
  for (int range_index = 0; range_index < num_x_ranges_per_query; ++range_index) {
    request.xbegins[range_index] = x_begins[range_index] + batch_begin;
    request.xends[range_index] = x_ends[range_index] + batch_begin;
  }
  request.level_begin = num_levels() - 1;
  request.level_end = 0;
  request.states = states;
  for (int64_t i = 0; i < num_queries; ++i) {
    int64_t y_end = y_ends[batch_begin + i];
    states[i].ends[0] = (y_end == 0) ? MergeTree::kEmptyRange : y_end;
    states[i].ends[1] = MergeTree::kEmptyRange;
  }

  BoxQuery(request, thread_context);

  END_MINI_BATCH_FOR
}

bool MergeTree::NOutOfBounds(const NthQueryRequest& queries, int64_t query_index) {
  int64_t num_elements = 0;
  for (int y_range_index = 0; y_range_index < queries.num_y_ranges; ++y_range_index) {
    int64_t ybegin = queries.ybegins[y_range_index][query_index];
    int64_t yend = queries.yends[y_range_index][query_index];
    num_elements += yend - ybegin;
  }
  int64_t N = queries.states[query_index].pos;
  return N < 0 || N >= num_elements;
}

void MergeTree::NthQuery(const NthQueryRequest& queries, ThreadContext& thread_ctx) {
  ARROW_DCHECK(queries.num_y_ranges >= 1 && queries.num_y_ranges <= 3);

  auto temp_vector_stack = thread_ctx.temp_vector_stack;  // For TEMP_VECTOR
  TEMP_VECTOR(int64_t, pos);
  TEMP_VECTOR(int64_t, ybegins0);
  TEMP_VECTOR(int64_t, yends0);
  TEMP_VECTOR(int64_t, ybegins1);
  TEMP_VECTOR(int64_t, yends1);
  TEMP_VECTOR(int64_t, ybegins2);
  TEMP_VECTOR(int64_t, yends2);
  int64_t* ybegins[3];
  int64_t* yends[3];
  ybegins[0] = ybegins0;
  ybegins[1] = ybegins1;
  ybegins[2] = ybegins2;
  yends[0] = yends0;
  yends[1] = yends1;
  yends[2] = yends2;

  // Split processing into mini batches, in order to use small buffers on
  // the stack (and in CPU cache) for intermediate vectors.
  //
  BEGIN_MINI_BATCH_FOR(batch_begin, batch_length, queries.num_queries)

  // Filter out queries with N out of bounds.
  //
  int64_t num_batch_queries = 0;
  for (int64_t batch_query_index = 0; batch_query_index < batch_length;
       ++batch_query_index) {
    int64_t query_index = batch_begin + batch_query_index;
    for (int y_range_index = 0; y_range_index < queries.num_y_ranges; ++y_range_index) {
      int64_t ybegin = queries.ybegins[y_range_index][query_index];
      int64_t yend = queries.yends[y_range_index][query_index];
      // Set range boundaries to kEmptyRange for all empty ranges in
      // queries.
      //
      ybegin = (yend == ybegin) ? kEmptyRange : ybegin;
      yend = (yend == ybegin) ? kEmptyRange : yend;
      ybegins[y_range_index][num_batch_queries] = ybegin;
      yends[y_range_index][num_batch_queries] = yend;
    }
    pos[num_batch_queries] = queries.states[query_index].pos;
    num_batch_queries += NOutOfBounds(queries, query_index) ? 0 : 1;
  }

  for (int level = num_levels() - 1; level > 0; --level) {
    // For all  batch queries post filtering
    //
    for (int64_t batch_query_index = 0; batch_query_index < num_batch_queries;
         ++batch_query_index) {
      // Predication: kEmptyRange is replaced with special constants, which
      // are always a valid input, in order to avoid conditional branches.
      //
      // We will later correct values returned by called functions for
      // kEmptyRange inputs.
      //
      constexpr int64_t kBeginReplacement = static_cast<int64_t>(0);
      constexpr int64_t kEndReplacement = static_cast<int64_t>(1);
      int64_t ybegin[6];
      int64_t yend[6];
      int64_t num_elements_in_left = 0;
      for (int y_range_index = 0; y_range_index < queries.num_y_ranges; ++y_range_index) {
        int64_t ybegin_parent = ybegins[y_range_index][batch_query_index];
        int64_t yend_parent = yends[y_range_index][batch_query_index];

        // Use fractional cascading to map range of elements in parent node
        // to corresponding ranges of elements in two child nodes.
        //
        CascadeBegin(level,
                     ybegin_parent == kEmptyRange ? kBeginReplacement : ybegin_parent,
                     &ybegin[y_range_index], &ybegin[3 + y_range_index]);
        CascadeEnd(level, yend_parent == kEmptyRange ? kEndReplacement : yend_parent,
                   &yend[y_range_index], &yend[y_range_index + 3]);

        // Check if any of the resulting ranges in child nodes is empty and
        // update boundaries accordingly.
        //
        bool empty_parent = ybegin_parent == kEmptyRange || yend_parent == kEmptyRange;
        for (int i = 0; i < 2; ++i) {
          int child_range_index = y_range_index + 3 * i;
          bool empty_range = ybegin[child_range_index] == kEmptyRange ||
                             yend[child_range_index] == kEmptyRange ||
                             ybegin[child_range_index] == yend[child_range_index];
          ybegin[child_range_index] =
              (empty_parent || empty_range) ? kEmptyRange : ybegin[child_range_index];
          yend[child_range_index] =
              (empty_parent || empty_range) ? kEmptyRange : yend[child_range_index];
        }

        // Update the number of elements in all ranges in left child.
        //
        num_elements_in_left += yend[y_range_index] - ybegin[y_range_index];
      }

      // Decide whether to traverse down to the left or to the right child.
      //
      int64_t N = pos[batch_query_index] - NodeBegin(level, pos[batch_query_index]);
      int child_index = N < num_elements_in_left ? 0 : 1;

      // Update range boundaries for the selected child node.
      //
      for (int y_range_index = 0; y_range_index < queries.num_y_ranges; ++y_range_index) {
        ybegins[y_range_index][batch_query_index] =
            ybegin[y_range_index + 3 * child_index];
        yends[y_range_index][batch_query_index] = ybegin[y_range_index + 3 * child_index];
      }

      // Update node index and N for the selected child node.
      //
      int64_t child_node_length = 1LL << (level - 1);
      pos[batch_query_index] +=
          child_node_length * child_index - num_elements_in_left * child_index;
    }
  }

  // Expand results of filtered batch queries to update the array of all
  // query results and fill the remaining query results in this batch with
  // kOutOfBounds constant.
  //
  num_batch_queries = 0;
  for (int64_t batch_query_index = 0; batch_query_index < batch_length;
       ++batch_query_index) {
    int64_t query_index = batch_begin + batch_query_index;
    int valid_query = NOutOfBounds(queries, query_index) ? 0 : 1;
    queries.states[query_index].pos = valid_query ? pos[num_batch_queries] : kOutOfBounds;
    num_batch_queries += valid_query;
  }

  END_MINI_BATCH_FOR
}

void MergeTree::DebugPrintToFile(const char* filename) const {
  FILE* fout;
#if defined(_MSC_VER) && _MSC_VER >= 1400
  fopen_s(&fout, filename, "wt");
#else
  fout = fopen(filename, "wt");
#endif
  if (!fout) {
    return;
  }

  for (int level = num_levels() - 1; level > 0; --level) {
    for (int64_t i = 0; i < length_; ++i) {
      fprintf(fout, "%s", bit_matrix_.GetRow(level).GetBit(i) ? "1" : "0");
    }
    fprintf(fout, "\n");
  }

  fprintf(fout, "\n");

  for (int level = num_levels() - 1; level > 0; --level) {
    auto bits = bit_matrix_.GetRow(level);
    bits.DebugPrintCountersToFile(fout);
  }

  fclose(fout);
}

}  // namespace compute
}  // namespace arrow
