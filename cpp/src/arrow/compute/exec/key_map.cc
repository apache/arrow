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

#include "arrow/compute/exec/key_map.h"

#include <memory.h>

#include <algorithm>
#include <cstdint>

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/ubsan.h"

namespace arrow {

using BitUtil::CountLeadingZeros;

namespace compute {

constexpr uint64_t kHighBitOfEachByte = 0x8080808080808080ULL;

// Search status bytes inside a block of 8 slots (64-bit word).
// Try to find a slot that contains a 7-bit stamp matching the one provided.
// There are three possible outcomes:
// 1. A matching slot is found.
// -> Return its index between 0 and 7 and set match found flag.
// 2. A matching slot is not found and there is an empty slot in the block.
// -> Return the index of the first empty slot and clear match found flag.
// 3. A matching slot is not found and there are no empty slots in the block.
// -> Return 8 as the output slot index and clear match found flag.
//
// Optionally an index of the first slot to start the search from can be specified.
// In this case slots before it will be ignored.
//
template <bool use_start_slot>
inline void SwissTable::search_block(uint64_t block, int stamp, int start_slot,
                                     int* out_slot, int* out_match_found) {
  // Filled slot bytes have the highest bit set to 0 and empty slots are equal to 0x80.
  uint64_t block_high_bits = block & kHighBitOfEachByte;

  // Replicate 7-bit stamp to all non-empty slots, leaving zeroes for empty slots.
  uint64_t stamp_pattern = stamp * ((block_high_bits ^ kHighBitOfEachByte) >> 7);

  // If we xor this pattern with block status bytes we get in individual bytes:
  // a) 0x00, for filled slots matching the stamp,
  // b) 0x00 < x < 0x80, for filled slots not matching the stamp,
  // c) 0x80, for empty slots.
  uint64_t block_xor_pattern = block ^ stamp_pattern;

  // If we then add 0x7f to every byte, we get:
  // a) 0x7F
  // b) 0x80 <= x < 0xFF
  // c) 0xFF
  uint64_t match_base = block_xor_pattern + ~kHighBitOfEachByte;

  // The highest bit now tells us if we have a match (0) or not (1).
  // We will negate the bits so that match is represented by a set bit.
  uint64_t matches = ~match_base;

  // Clear 7 non-relevant bits in each byte.
  // Also clear bytes that correspond to slots that we were supposed to
  // skip due to provided start slot index.
  // Note: the highest byte corresponds to the first slot.
  if (use_start_slot) {
    matches &= kHighBitOfEachByte >> (8 * start_slot);
  } else {
    matches &= kHighBitOfEachByte;
  }

  // We get 0 if there are no matches
  *out_match_found = (matches == 0 ? 0 : 1);

  // Now if we or with the highest bits of the block and scan zero bits in reverse,
  // we get 8x slot index that we were looking for.
  // This formula works in all three cases a), b) and c).
  *out_slot = static_cast<int>(CountLeadingZeros(matches | block_high_bits) >> 3);
}

// This call follows the call to search_block.
// The input slot index is the output returned by it, which is a value from 0 to 8,
// with 8 indicating that both: no match was found and there were no empty slots.
//
// If the slot corresponds to a non-empty slot return a group id associated with it.
// Otherwise return any group id from any of the slots or
// zero, which is the default value stored in empty slots.
//
inline uint64_t SwissTable::extract_group_id(const uint8_t* block_ptr, int slot,
                                             uint64_t group_id_mask) {
  // Input slot can be equal to 8, in which case we need to output any valid group id
  // value, so we take the one from slot 0 in the block.
  int clamped_slot = slot & 7;

  // Group id values for all 8 slots in the block are bit-packed and follow the status
  // bytes. We assume here that the number of bits is rounded up to 8, 16, 32 or 64. In
  // that case we can extract group id using aligned 64-bit word access.
  int num_groupid_bits = static_cast<int>(ARROW_POPCOUNT64(group_id_mask));
  ARROW_DCHECK(num_groupid_bits == 8 || num_groupid_bits == 16 ||
               num_groupid_bits == 32 || num_groupid_bits == 64);

  int bit_offset = clamped_slot * num_groupid_bits;
  const uint64_t* group_id_bytes =
      reinterpret_cast<const uint64_t*>(block_ptr) + 1 + (bit_offset >> 6);
  uint64_t group_id = (*group_id_bytes >> (bit_offset & 63)) & group_id_mask;

  return group_id;
}

// Return global slot id (the index including the information about the block)
// where the search should continue if the first comparison fails.
// This function always follows search_block and receives the slot id returned by it.
//
inline uint64_t SwissTable::next_slot_to_visit(uint64_t block_index, int slot,
                                               int match_found) {
  // The result should be taken modulo the number of all slots in all blocks,
  // but here we allow it to take a value one above the last slot index.
  // Modulo operation is postponed to later.
  return block_index * 8 + slot + match_found;
}

// Implements first (fast-path, optimistic) lookup.
// Searches for a match only within the start block and
// trying only the first slot with a matching stamp.
//
// Comparison callback needed for match verification is done outside of this function.
// Match bit vector filled by it only indicates finding a matching stamp in a slot.
//
template <bool use_selection>
void SwissTable::lookup_1(const uint16_t* selection, const int num_keys,
                          const uint32_t* hashes, uint8_t* out_match_bitvector,
                          uint32_t* out_groupids, uint32_t* out_slot_ids) {
  // Clear the output bit vector
  memset(out_match_bitvector, 0, (num_keys + 7) / 8);

  // Based on the size of the table, prepare bit number constants.
  uint32_t stamp_mask = (1 << bits_stamp_) - 1;
  int num_groupid_bits = num_groupid_bits_from_log_blocks(log_blocks_);
  uint32_t groupid_mask = (1 << num_groupid_bits) - 1;

  for (int i = 0; i < num_keys; ++i) {
    int id;
    if (use_selection) {
      id = util::SafeLoad(&selection[i]);
    } else {
      id = i;
    }

    // Extract from hash: block index and stamp
    //
    uint32_t hash = hashes[id];
    uint32_t iblock = hash >> (bits_hash_ - bits_stamp_ - log_blocks_);
    uint32_t stamp = iblock & stamp_mask;
    iblock >>= bits_stamp_;

    uint32_t num_block_bytes = num_groupid_bits + 8;
    const uint8_t* blockbase = reinterpret_cast<const uint8_t*>(blocks_) +
                               static_cast<uint64_t>(iblock) * num_block_bytes;
    uint64_t block = util::SafeLoadAs<uint64_t>(blockbase);

    // Call helper functions to obtain the output triplet:
    // - match (of a stamp) found flag
    // - group id for key comparison
    // - slot to resume search from in case of no match or false positive
    int match_found;
    int islot_in_block;
    search_block<false>(block, stamp, 0, &islot_in_block, &match_found);
    uint64_t groupid = extract_group_id(blockbase, islot_in_block, groupid_mask);
    ARROW_DCHECK(groupid < num_inserted_ || num_inserted_ == 0);
    uint64_t islot = next_slot_to_visit(iblock, islot_in_block, match_found);

    out_match_bitvector[id / 8] |= match_found << (id & 7);
    util::SafeStore(&out_groupids[id], static_cast<uint32_t>(groupid));
    util::SafeStore(&out_slot_ids[id], static_cast<uint32_t>(islot));
  }
}

// How many groups we can keep in the hash table without the need for resizing.
// When we reach this limit, we need to break processing of any further rows and resize.
//
uint64_t SwissTable::num_groups_for_resize() const {
  // Resize small hash tables when 50% full (up to 12KB).
  // Resize large hash tables when 75% full.
  constexpr int log_blocks_small_ = 9;
  uint64_t num_slots = 1ULL << (log_blocks_ + 3);
  if (log_blocks_ <= log_blocks_small_) {
    return num_slots / 2;
  } else {
    return num_slots * 3 / 4;
  }
}

uint64_t SwissTable::wrap_global_slot_id(uint64_t global_slot_id) {
  uint64_t global_slot_id_mask = (1 << (log_blocks_ + 3)) - 1;
  return global_slot_id & global_slot_id_mask;
}

// Run a single round of slot search - comparison / insert - filter unprocessed.
// Update selection vector to reflect which items have been processed.
// Ids in selection vector do not have to be sorted.
//
Status SwissTable::lookup_2(const uint32_t* hashes, uint32_t* inout_num_selected,
                            uint16_t* inout_selection, bool* out_need_resize,
                            uint32_t* out_group_ids, uint32_t* inout_next_slot_ids) {
  auto num_groups_limit = num_groups_for_resize();
  ARROW_DCHECK(num_inserted_ < num_groups_limit);

  // Temporary arrays are of limited size.
  // The input needs to be split into smaller portions if it exceeds that limit.
  //
  ARROW_DCHECK(*inout_num_selected <= static_cast<uint32_t>(1 << log_minibatch_));

  // We will split input row ids into three categories:
  // - needing to visit next block [0]
  // - needing comparison [1]
  // - inserted [2]
  //
  auto ids_inserted_buf =
      util::TempVectorHolder<uint16_t>(temp_stack_, *inout_num_selected);
  auto ids_for_comparison_buf =
      util::TempVectorHolder<uint16_t>(temp_stack_, *inout_num_selected);
  constexpr int category_nomatch = 0;
  constexpr int category_cmp = 1;
  constexpr int category_inserted = 2;
  int num_ids[3];
  num_ids[0] = num_ids[1] = num_ids[2] = 0;
  uint16_t* ids[3]{inout_selection, ids_for_comparison_buf.mutable_data(),
                   ids_inserted_buf.mutable_data()};
  auto push_id = [&num_ids, &ids](int category, int id) {
    util::SafeStore(&ids[category][num_ids[category]++], static_cast<uint16_t>(id));
  };

  uint64_t num_groupid_bits = num_groupid_bits_from_log_blocks(log_blocks_);
  uint64_t groupid_mask = (1ULL << num_groupid_bits) - 1;
  constexpr uint64_t stamp_mask = 0x7f;
  uint64_t num_block_bytes = (8 + num_groupid_bits);

  uint32_t num_processed;
  for (num_processed = 0;
       // Second condition in for loop:
       // We need to break processing and have the caller of this function
       // resize hash table if we reach the limit of the number of groups present.
       num_processed < *inout_num_selected &&
       num_inserted_ + num_ids[category_inserted] < num_groups_limit;
       ++num_processed) {
    // row id in original batch
    int id = util::SafeLoad(&inout_selection[num_processed]);

    uint64_t slot_id = wrap_global_slot_id(util::SafeLoad(&inout_next_slot_ids[id]));
    uint64_t block_id = slot_id >> 3;
    uint32_t hash = hashes[id];
    uint8_t* blockbase = blocks_ + num_block_bytes * block_id;
    uint64_t block = *reinterpret_cast<uint64_t*>(blockbase);
    uint64_t stamp = (hash >> (bits_hash_ - log_blocks_ - bits_stamp_)) & stamp_mask;
    int start_slot = (slot_id & 7);

    bool isempty = (blockbase[7 - start_slot] == 0x80);
    if (isempty) {
      // If we reach the empty slot we insert key for new group

      blockbase[7 - start_slot] = static_cast<uint8_t>(stamp);
      uint32_t group_id = num_inserted_ + num_ids[category_inserted];
      int groupid_bit_offset = static_cast<int>(start_slot * num_groupid_bits);

      // We assume here that the number of bits is rounded up to 8, 16, 32 or 64.
      // In that case we can insert group id value using aligned 64-bit word access.
      ARROW_DCHECK(num_groupid_bits == 8 || num_groupid_bits == 16 ||
                   num_groupid_bits == 32 || num_groupid_bits == 64);
      uint64_t* ptr =
          &reinterpret_cast<uint64_t*>(blockbase + 8)[groupid_bit_offset >> 6];
      util::SafeStore(ptr, util::SafeLoad(ptr) | (static_cast<uint64_t>(group_id)
                                                  << (groupid_bit_offset & 63)));

      hashes_[slot_id] = hash;
      util::SafeStore(&out_group_ids[id], group_id);
      push_id(category_inserted, id);
    } else {
      // We search for a slot with a matching stamp within a single block.
      // We append row id to the appropriate sequence of ids based on
      // whether the match has been found or not.

      int new_match_found;
      int new_slot;
      search_block<true>(block, static_cast<int>(stamp), start_slot, &new_slot,
                         &new_match_found);
      auto new_groupid =
          static_cast<uint32_t>(extract_group_id(blockbase, new_slot, groupid_mask));
      ARROW_DCHECK(new_groupid < num_inserted_ + num_ids[category_inserted]);
      new_slot =
          static_cast<int>(next_slot_to_visit(block_id, new_slot, new_match_found));
      util::SafeStore(&inout_next_slot_ids[id], new_slot);
      util::SafeStore(&out_group_ids[id], new_groupid);
      push_id(new_match_found, id);
    }
  }

  // Copy keys for newly inserted rows using callback
  RETURN_NOT_OK(append_impl_(num_ids[category_inserted], ids[category_inserted]));
  num_inserted_ += num_ids[category_inserted];

  // Evaluate comparisons and append ids of rows that failed it to the non-match set.
  uint32_t num_not_equal;
  equal_impl_(num_ids[category_cmp], ids[category_cmp], out_group_ids, &num_not_equal,
              ids[category_nomatch] + num_ids[category_nomatch]);
  num_ids[category_nomatch] += num_not_equal;

  // Append ids of any unprocessed entries if we aborted processing due to the need
  // to resize.
  if (num_processed < *inout_num_selected) {
    memmove(ids[category_nomatch] + num_ids[category_nomatch],
            inout_selection + num_processed,
            sizeof(uint16_t) * (*inout_num_selected - num_processed));
    num_ids[category_nomatch] += (*inout_num_selected - num_processed);
  }

  *out_need_resize = (num_inserted_ == num_groups_limit);
  *inout_num_selected = num_ids[category_nomatch];
  return Status::OK();
}

// Use hashes and callbacks to find group ids for already existing keys and
// to insert and report newly assigned group ids for new keys.
//
Status SwissTable::map(const int num_keys, const uint32_t* hashes,
                       uint32_t* out_groupids) {
  // Temporary buffers have limited size.
  // Caller is responsible for splitting larger input arrays into smaller chunks.
  ARROW_DCHECK(num_keys <= (1 << log_minibatch_));

  // Allocate temporary buffers with a lifetime of this function
  auto match_bitvector_buf = util::TempVectorHolder<uint8_t>(temp_stack_, num_keys);
  uint8_t* match_bitvector = match_bitvector_buf.mutable_data();
  auto slot_ids_buf = util::TempVectorHolder<uint32_t>(temp_stack_, num_keys);
  uint32_t* slot_ids = slot_ids_buf.mutable_data();
  auto ids_buf = util::TempVectorHolder<uint16_t>(temp_stack_, num_keys);
  uint16_t* ids = ids_buf.mutable_data();
  uint32_t num_ids;

  // First-pass processing.
  // Optimistically use simplified lookup involving only a start block to find
  // a single group id candidate for every input.
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags_ & arrow::internal::CpuInfo::AVX2) {
    if (log_blocks_ <= 4) {
      int tail = num_keys % 32;
      int delta = num_keys - tail;
      lookup_1_avx2_x32(num_keys - tail, hashes, match_bitvector, out_groupids, slot_ids);
      lookup_1_avx2_x8(tail, hashes + delta, match_bitvector + delta / 8,
                       out_groupids + delta, slot_ids + delta);
    } else {
      lookup_1_avx2_x8(num_keys, hashes, match_bitvector, out_groupids, slot_ids);
    }
  } else {
#endif
    lookup_1<false>(nullptr, num_keys, hashes, match_bitvector, out_groupids, slot_ids);
#if defined(ARROW_HAVE_AVX2)
  }
#endif

  int64_t num_matches =
      arrow::internal::CountSetBits(match_bitvector, /*offset=*/0, num_keys);

  // After the first-pass processing count rows with matches (based on stamp comparison)
  // and decide based on their percentage whether to call dense or sparse comparison
  // function. Dense comparison means evaluating it for all inputs, even if the matching
  // stamp was not found. It may be cheaper to evaluate comparison for all inputs if the
  // extra cost of filtering is higher than the wasted processing of rows with no match.
  //
  // Dense comparison can only be used if there is at least one inserted key,
  // because otherwise there is no key to compare to.
  //
  if (num_inserted_ > 0 && num_matches > 0 && num_matches > 3 * num_keys / 4) {
    // Dense comparisons
    equal_impl_(num_keys, nullptr, out_groupids, &num_ids, ids);
  } else {
    // Sparse comparisons that involve filtering the input set of keys
    auto ids_cmp_buf = util::TempVectorHolder<uint16_t>(temp_stack_, num_keys);
    uint16_t* ids_cmp = ids_cmp_buf.mutable_data();
    int num_ids_result;
    util::BitUtil::bits_split_indexes(hardware_flags_, num_keys, match_bitvector,
                                      &num_ids_result, ids, ids_cmp);
    num_ids = num_ids_result;
    uint32_t num_not_equal;
    equal_impl_(num_keys - num_ids, ids_cmp, out_groupids, &num_not_equal, ids + num_ids);
    num_ids += num_not_equal;
  }

  do {
    // A single round of slow-pass (robust) lookup or insert.
    // A single round ends with either a single comparison verifying the match candidate
    // or inserting a new key. A single round of slow-pass may return early if we reach
    // the limit of the number of groups due to inserts of new keys. In that case we need
    // to resize and recalculating starting global slot ids for new bigger hash table.
    bool out_of_capacity;
    RETURN_NOT_OK(
        lookup_2(hashes, &num_ids, ids, &out_of_capacity, out_groupids, slot_ids));
    if (out_of_capacity) {
      RETURN_NOT_OK(grow_double());
      // Reset start slot ids for still unprocessed input keys.
      //
      for (uint32_t i = 0; i < num_ids; ++i) {
        // First slot in the new starting block
        const int16_t id = util::SafeLoad(&ids[i]);
        util::SafeStore(&slot_ids[id], (hashes[id] >> (bits_hash_ - log_blocks_)) * 8);
      }
    }
  } while (num_ids > 0);

  return Status::OK();
}

Status SwissTable::grow_double() {
  // Before and after metadata
  int num_group_id_bits_before = num_groupid_bits_from_log_blocks(log_blocks_);
  int num_group_id_bits_after = num_groupid_bits_from_log_blocks(log_blocks_ + 1);
  uint64_t group_id_mask_before = ~0ULL >> (64 - num_group_id_bits_before);
  int log_blocks_before = log_blocks_;
  int log_blocks_after = log_blocks_ + 1;
  uint64_t block_size_before = (8 + num_group_id_bits_before);
  uint64_t block_size_after = (8 + num_group_id_bits_after);
  uint64_t block_size_total_before = (block_size_before << log_blocks_before) + padding_;
  uint64_t block_size_total_after = (block_size_after << log_blocks_after) + padding_;
  uint64_t hashes_size_total_before =
      (bits_hash_ / 8 * (1 << (log_blocks_before + 3))) + padding_;
  uint64_t hashes_size_total_after =
      (bits_hash_ / 8 * (1 << (log_blocks_after + 3))) + padding_;
  constexpr uint32_t stamp_mask = (1 << bits_stamp_) - 1;

  // Allocate new buffers
  uint8_t* blocks_new;
  RETURN_NOT_OK(pool_->Allocate(block_size_total_after, &blocks_new));
  memset(blocks_new, 0, block_size_total_after);
  uint8_t* hashes_new_8B;
  uint32_t* hashes_new;
  RETURN_NOT_OK(pool_->Allocate(hashes_size_total_after, &hashes_new_8B));
  hashes_new = reinterpret_cast<uint32_t*>(hashes_new_8B);

  // First pass over all old blocks.
  // Reinsert entries that were not in the overflow block
  // (block other than selected by hash bits corresponding to the entry).
  for (int i = 0; i < (1 << log_blocks_); ++i) {
    // How many full slots in this block
    uint8_t* block_base = blocks_ + i * block_size_before;
    uint8_t* double_block_base_new = blocks_new + 2 * i * block_size_after;
    uint64_t block = *reinterpret_cast<const uint64_t*>(block_base);

    auto full_slots =
        static_cast<int>(CountLeadingZeros(block & kHighBitOfEachByte) >> 3);
    int full_slots_new[2];
    full_slots_new[0] = full_slots_new[1] = 0;
    util::SafeStore(double_block_base_new, kHighBitOfEachByte);
    util::SafeStore(double_block_base_new + block_size_after, kHighBitOfEachByte);

    for (int j = 0; j < full_slots; ++j) {
      uint64_t slot_id = i * 8 + j;
      uint32_t hash = hashes_[slot_id];
      uint64_t block_id_new = hash >> (bits_hash_ - log_blocks_after);
      bool is_overflow_entry = ((block_id_new >> 1) != static_cast<uint64_t>(i));
      if (is_overflow_entry) {
        continue;
      }

      int ihalf = block_id_new & 1;
      uint8_t stamp_new =
          hash >> ((bits_hash_ - log_blocks_after - bits_stamp_)) & stamp_mask;
      uint64_t group_id_bit_offs = j * num_group_id_bits_before;
      uint64_t group_id =
          (util::SafeLoadAs<uint64_t>(block_base + 8 + (group_id_bit_offs >> 3)) >>
           (group_id_bit_offs & 7)) &
          group_id_mask_before;

      uint64_t slot_id_new = i * 16 + ihalf * 8 + full_slots_new[ihalf];
      hashes_new[slot_id_new] = hash;
      uint8_t* block_base_new = double_block_base_new + ihalf * block_size_after;
      block_base_new[7 - full_slots_new[ihalf]] = stamp_new;
      int group_id_bit_offs_new = full_slots_new[ihalf] * num_group_id_bits_after;
      uint64_t* ptr =
          reinterpret_cast<uint64_t*>(block_base_new + 8 + (group_id_bit_offs_new >> 3));
      util::SafeStore(ptr,
                      util::SafeLoad(ptr) | (group_id << (group_id_bit_offs_new & 7)));
      full_slots_new[ihalf]++;
    }
  }

  // Second pass over all old blocks.
  // Reinsert entries that were in an overflow block.
  for (int i = 0; i < (1 << log_blocks_); ++i) {
    // How many full slots in this block
    uint8_t* block_base = blocks_ + i * block_size_before;
    uint64_t block = util::SafeLoadAs<uint64_t>(block_base);
    int full_slots = static_cast<int>(CountLeadingZeros(block & kHighBitOfEachByte) >> 3);

    for (int j = 0; j < full_slots; ++j) {
      uint64_t slot_id = i * 8 + j;
      uint32_t hash = hashes_[slot_id];
      uint64_t block_id_new = hash >> (bits_hash_ - log_blocks_after);
      bool is_overflow_entry = ((block_id_new >> 1) != static_cast<uint64_t>(i));
      if (!is_overflow_entry) {
        continue;
      }

      uint64_t group_id_bit_offs = j * num_group_id_bits_before;
      uint64_t group_id =
          (util::SafeLoadAs<uint64_t>(block_base + 8 + (group_id_bit_offs >> 3)) >>
           (group_id_bit_offs & 7)) &
          group_id_mask_before;
      uint8_t stamp_new =
          hash >> ((bits_hash_ - log_blocks_after - bits_stamp_)) & stamp_mask;

      uint8_t* block_base_new = blocks_new + block_id_new * block_size_after;
      uint64_t block_new = util::SafeLoadAs<uint64_t>(block_base_new);
      int full_slots_new =
          static_cast<int>(CountLeadingZeros(block_new & kHighBitOfEachByte) >> 3);
      while (full_slots_new == 8) {
        block_id_new = (block_id_new + 1) & ((1 << log_blocks_after) - 1);
        block_base_new = blocks_new + block_id_new * block_size_after;
        block_new = util::SafeLoadAs<uint64_t>(block_base_new);
        full_slots_new =
            static_cast<int>(CountLeadingZeros(block_new & kHighBitOfEachByte) >> 3);
      }

      hashes_new[block_id_new * 8 + full_slots_new] = hash;
      block_base_new[7 - full_slots_new] = stamp_new;
      int group_id_bit_offs_new = full_slots_new * num_group_id_bits_after;
      uint64_t* ptr =
          reinterpret_cast<uint64_t*>(block_base_new + 8 + (group_id_bit_offs_new >> 3));
      util::SafeStore(ptr,
                      util::SafeLoad(ptr) | (group_id << (group_id_bit_offs_new & 7)));
    }
  }

  pool_->Free(blocks_, block_size_total_before);
  pool_->Free(reinterpret_cast<uint8_t*>(hashes_), hashes_size_total_before);
  log_blocks_ = log_blocks_after;
  blocks_ = blocks_new;
  hashes_ = hashes_new;

  return Status::OK();
}

Status SwissTable::init(int64_t hardware_flags, MemoryPool* pool,
                        util::TempVectorStack* temp_stack, int log_minibatch,
                        EqualImpl equal_impl, AppendImpl append_impl) {
  hardware_flags_ = hardware_flags;
  pool_ = pool;
  temp_stack_ = temp_stack;
  log_minibatch_ = log_minibatch;
  equal_impl_ = equal_impl;
  append_impl_ = append_impl;

  log_blocks_ = 0;
  int num_groupid_bits = num_groupid_bits_from_log_blocks(log_blocks_);
  num_inserted_ = 0;

  const uint64_t block_bytes = 8 + num_groupid_bits;
  const uint64_t slot_bytes = (block_bytes << log_blocks_) + padding_;
  RETURN_NOT_OK(pool_->Allocate(slot_bytes, &blocks_));

  // Make sure group ids are initially set to zero for all slots.
  memset(blocks_, 0, slot_bytes);

  // Initialize all status bytes to represent an empty slot.
  for (uint64_t i = 0; i < (static_cast<uint64_t>(1) << log_blocks_); ++i) {
    util::SafeStore(blocks_ + i * block_bytes, kHighBitOfEachByte);
  }

  uint64_t num_slots = 1ULL << (log_blocks_ + 3);
  const uint64_t hash_size = sizeof(uint32_t);
  const uint64_t hash_bytes = hash_size * num_slots + padding_;
  uint8_t* hashes8;
  RETURN_NOT_OK(pool_->Allocate(hash_bytes, &hashes8));
  hashes_ = reinterpret_cast<uint32_t*>(hashes8);

  return Status::OK();
}

void SwissTable::cleanup() {
  if (blocks_) {
    int num_groupid_bits = num_groupid_bits_from_log_blocks(log_blocks_);
    const uint64_t block_bytes = 8 + num_groupid_bits;
    const uint64_t slot_bytes = (block_bytes << log_blocks_) + padding_;
    pool_->Free(blocks_, slot_bytes);
    blocks_ = nullptr;
  }
  if (hashes_) {
    uint64_t num_slots = 1ULL << (log_blocks_ + 3);
    const uint64_t hash_size = sizeof(uint32_t);
    const uint64_t hash_bytes = hash_size * num_slots + padding_;
    pool_->Free(reinterpret_cast<uint8_t*>(hashes_), hash_bytes);
    hashes_ = nullptr;
  }
  log_blocks_ = 0;
  num_inserted_ = 0;
}

}  // namespace compute
}  // namespace arrow
