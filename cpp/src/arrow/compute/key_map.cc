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

#include "arrow/compute/key_map.h"

#include <algorithm>
#include <cstdint>

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow {

using bit_util::CountLeadingZeros;

namespace compute {

constexpr uint64_t kHighBitOfEachByte = 0x8080808080808080ULL;

// Scan bytes in block in reverse and stop as soon
// as a position of interest is found.
//
// Positions of interest:
// a) slot with a matching stamp is encountered,
// b) first empty slot is encountered,
// c) we reach the end of the block.
//
// Optionally an index of the first slot to start the search from can be specified. In
// this case slots before it will be ignored.
//
template <bool use_start_slot>
inline void SwissTable::search_block(uint64_t block, int stamp, int start_slot,
                                     int* out_slot, int* out_match_found) const {
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

  // In case when there are no matches in slots and the block is full (no empty slots),
  // pretend that there is a match in the last slot.
  //
  matches |= (~block_high_bits & 0x80);

  // We get 0 if there are no matches
  *out_match_found = (matches == 0 ? 0 : 1);

  // Now if we or with the highest bits of the block and scan zero bits in reverse, we get
  // 8x slot index that we were looking for. This formula works in all three cases a), b)
  // and c).
  *out_slot = static_cast<int>(CountLeadingZeros(matches | block_high_bits) >> 3);
}

template <typename T, bool use_selection>
void SwissTable::extract_group_ids_imp(const int num_keys, const uint16_t* selection,
                                       const uint32_t* hashes, const uint8_t* local_slots,
                                       uint32_t* out_group_ids, int element_offset,
                                       int element_multiplier) const {
  const T* elements = reinterpret_cast<const T*>(blocks_->data()) + element_offset;
  if (log_blocks_ == 0) {
    ARROW_DCHECK(sizeof(T) == sizeof(uint8_t));
    for (int i = 0; i < num_keys; ++i) {
      uint32_t id = use_selection ? selection[i] : i;
      uint32_t group_id = blocks()[8 + local_slots[id]];
      out_group_ids[id] = group_id;
    }
  } else {
    for (int i = 0; i < num_keys; ++i) {
      uint32_t id = use_selection ? selection[i] : i;
      uint32_t hash = hashes[id];
      int64_t pos =
          (hash >> (bits_hash_ - log_blocks_)) * element_multiplier + local_slots[id];
      uint32_t group_id = static_cast<uint32_t>(elements[pos]);
      ARROW_DCHECK(group_id < num_inserted_ || num_inserted_ == 0);
      out_group_ids[id] = group_id;
    }
  }
}

void SwissTable::extract_group_ids(const int num_keys, const uint16_t* optional_selection,
                                   const uint32_t* hashes, const uint8_t* local_slots,
                                   uint32_t* out_group_ids) const {
  // Group id values for all 8 slots in the block are bit-packed and follow the status
  // bytes. We assume here that the number of bits is rounded up to 8, 16, 32 or 64. In
  // that case we can extract group id using aligned 64-bit word access.
  int num_group_id_bits = num_groupid_bits_from_log_blocks(log_blocks_);
  ARROW_DCHECK(num_group_id_bits == 8 || num_group_id_bits == 16 ||
               num_group_id_bits == 32);

  int num_processed = 0;

  // Optimistically use simplified lookup involving only a start block to find
  // a single group id candidate for every input.
#if defined(ARROW_HAVE_AVX2)
  int num_group_id_bytes = num_group_id_bits / 8;
  if ((hardware_flags_ & arrow::internal::CpuInfo::AVX2) && !optional_selection) {
    num_processed = extract_group_ids_avx2(num_keys, hashes, local_slots, out_group_ids,
                                           sizeof(uint64_t), 8 + 8 * num_group_id_bytes,
                                           num_group_id_bytes);
  }
#endif
  switch (num_group_id_bits) {
    case 8:
      if (optional_selection) {
        extract_group_ids_imp<uint8_t, true>(num_keys, optional_selection, hashes,
                                             local_slots, out_group_ids, 8, 16);
      } else {
        extract_group_ids_imp<uint8_t, false>(
            num_keys - num_processed, nullptr, hashes + num_processed,
            local_slots + num_processed, out_group_ids + num_processed, 8, 16);
      }
      break;
    case 16:
      if (optional_selection) {
        extract_group_ids_imp<uint16_t, true>(num_keys, optional_selection, hashes,
                                              local_slots, out_group_ids, 4, 12);
      } else {
        extract_group_ids_imp<uint16_t, false>(
            num_keys - num_processed, nullptr, hashes + num_processed,
            local_slots + num_processed, out_group_ids + num_processed, 4, 12);
      }
      break;
    case 32:
      if (optional_selection) {
        extract_group_ids_imp<uint32_t, true>(num_keys, optional_selection, hashes,
                                              local_slots, out_group_ids, 2, 10);
      } else {
        extract_group_ids_imp<uint32_t, false>(
            num_keys - num_processed, nullptr, hashes + num_processed,
            local_slots + num_processed, out_group_ids + num_processed, 2, 10);
      }
      break;
    default:
      ARROW_DCHECK(false);
  }
}

void SwissTable::init_slot_ids(const int num_keys, const uint16_t* selection,
                               const uint32_t* hashes, const uint8_t* local_slots,
                               const uint8_t* match_bitvector,
                               uint32_t* out_slot_ids) const {
  ARROW_DCHECK(selection);
  if (log_blocks_ == 0) {
    for (int i = 0; i < num_keys; ++i) {
      uint16_t id = selection[i];
      uint32_t match = ::arrow::bit_util::GetBit(match_bitvector, id) ? 1 : 0;
      uint32_t slot_id = local_slots[id] + match;
      out_slot_ids[id] = slot_id;
    }
  } else {
    for (int i = 0; i < num_keys; ++i) {
      uint16_t id = selection[i];
      uint32_t hash = hashes[id];
      uint32_t iblock = (hash >> (bits_hash_ - log_blocks_));
      uint32_t match = ::arrow::bit_util::GetBit(match_bitvector, id) ? 1 : 0;
      uint32_t slot_id = iblock * 8 + local_slots[id] + match;
      out_slot_ids[id] = slot_id;
    }
  }
}

void SwissTable::init_slot_ids_for_new_keys(uint32_t num_ids, const uint16_t* ids,
                                            const uint32_t* hashes,
                                            uint32_t* slot_ids) const {
  int num_groupid_bits = num_groupid_bits_from_log_blocks(log_blocks_);
  uint32_t num_block_bytes = num_groupid_bits + 8;
  if (log_blocks_ == 0) {
    uint64_t block = *reinterpret_cast<const uint64_t*>(blocks_->mutable_data());
    uint32_t empty_slot =
        static_cast<uint32_t>(8 - ARROW_POPCOUNT64(block & kHighBitOfEachByte));
    for (uint32_t i = 0; i < num_ids; ++i) {
      int id = ids[i];
      slot_ids[id] = empty_slot;
    }
  } else {
    for (uint32_t i = 0; i < num_ids; ++i) {
      int id = ids[i];
      uint32_t hash = hashes[id];
      uint32_t iblock = hash >> (bits_hash_ - log_blocks_);
      uint64_t block;
      for (;;) {
        block = *reinterpret_cast<const uint64_t*>(blocks_->mutable_data() +
                                                   num_block_bytes * iblock);
        block &= kHighBitOfEachByte;
        if (block) {
          break;
        }
        iblock = (iblock + 1) & ((1 << log_blocks_) - 1);
      }
      uint32_t empty_slot = static_cast<int>(8 - ARROW_POPCOUNT64(block));
      slot_ids[id] = iblock * 8 + empty_slot;
    }
  }
}

// Quickly filter out keys that have no matches based only on hash value and the
// corresponding starting 64-bit block of slot status bytes. May return false positives.
//
void SwissTable::early_filter_imp(const int num_keys, const uint32_t* hashes,
                                  uint8_t* out_match_bitvector,
                                  uint8_t* out_local_slots) const {
  // Clear the output bit vector
  memset(out_match_bitvector, 0, (num_keys + 7) / 8);

  // Based on the size of the table, prepare bit number constants.
  uint32_t stamp_mask = (1 << bits_stamp_) - 1;
  int num_groupid_bits = num_groupid_bits_from_log_blocks(log_blocks_);

  for (int i = 0; i < num_keys; ++i) {
    // Extract from hash: block index and stamp
    //
    uint32_t hash = hashes[i];
    uint32_t iblock = hash >> (bits_hash_ - bits_stamp_ - log_blocks_);
    uint32_t stamp = iblock & stamp_mask;
    iblock >>= bits_stamp_;

    uint32_t num_block_bytes = num_groupid_bits + 8;
    const uint8_t* blockbase =
        blocks_->data() + static_cast<uint64_t>(iblock) * num_block_bytes;
    ARROW_DCHECK(num_block_bytes % sizeof(uint64_t) == 0);
    uint64_t block = *reinterpret_cast<const uint64_t*>(blockbase);

    // Call helper functions to obtain the output triplet:
    // - match (of a stamp) found flag
    // - number of slots to skip before resuming further search, in case of no match or
    // false positive
    int match_found;
    int islot_in_block;
    search_block<false>(block, stamp, 0, &islot_in_block, &match_found);

    out_match_bitvector[i / 8] |= match_found << (i & 7);
    out_local_slots[i] = static_cast<uint8_t>(islot_in_block);
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

uint64_t SwissTable::wrap_global_slot_id(uint64_t global_slot_id) const {
  uint64_t global_slot_id_mask = (1 << (log_blocks_ + 3)) - 1;
  return global_slot_id & global_slot_id_mask;
}

void SwissTable::early_filter(const int num_keys, const uint32_t* hashes,
                              uint8_t* out_match_bitvector,
                              uint8_t* out_local_slots) const {
  // Optimistically use simplified lookup involving only a start block to find
  // a single group id candidate for every input.
  int num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags_ & arrow::internal::CpuInfo::AVX2) {
    if (log_blocks_ <= 4) {
      num_processed = early_filter_imp_avx2_x32(num_keys, hashes, out_match_bitvector,
                                                out_local_slots);
    }
    num_processed += early_filter_imp_avx2_x8(
        num_keys - num_processed, hashes + num_processed,
        out_match_bitvector + num_processed / 8, out_local_slots + num_processed);
  }
#endif
  early_filter_imp(num_keys - num_processed, hashes + num_processed,
                   out_match_bitvector + num_processed / 8,
                   out_local_slots + num_processed);
}

// Input selection may be:
// - a range of all ids from 0 to num_keys - 1
// - a selection vector with list of ids
// - a bit-vector marking ids that are included
// Either selection index vector or selection bit-vector must be provided
// but both cannot be set at the same time (one must be null).
//
// Input and output selection index vectors are allowed to point to the same buffer
// (in-place filtering of ids).
//
// Output selection vector needs to have enough space for num_keys entries.
//
void SwissTable::run_comparisons(const int num_keys,
                                 const uint16_t* optional_selection_ids,
                                 const uint8_t* optional_selection_bitvector,
                                 const uint32_t* groupids, int* out_num_not_equal,
                                 uint16_t* out_not_equal_selection,
                                 const EqualImpl& equal_impl, void* callback_ctx) const {
  ARROW_DCHECK(optional_selection_ids || optional_selection_bitvector);
  ARROW_DCHECK(!optional_selection_ids || !optional_selection_bitvector);

  if (num_keys == 0) {
    *out_num_not_equal = 0;
    return;
  }

  if (!optional_selection_ids && optional_selection_bitvector) {
    // Count rows with matches (based on stamp comparison)
    // and decide based on their percentage whether to call dense or sparse comparison
    // function. Dense comparison means evaluating it for all inputs, even if the
    // matching stamp was not found. It may be cheaper to evaluate comparison for all
    // inputs if the extra cost of filtering is higher than the wasted processing of
    // rows with no match.
    //
    // Dense comparison can only be used if there is at least one inserted key,
    // because otherwise there is no key to compare to.
    //
    int64_t num_matches = arrow::internal::CountSetBits(optional_selection_bitvector,
                                                        /*offset=*/0, num_keys);

    if (num_inserted_ > 0 && num_matches > 0 && num_matches > 3 * num_keys / 4) {
      uint32_t out_num;
      equal_impl(num_keys, nullptr, groupids, &out_num, out_not_equal_selection,
                 callback_ctx);
      *out_num_not_equal = static_cast<int>(out_num);
    } else {
      util::bit_util::bits_to_indexes(1, hardware_flags_, num_keys,
                                      optional_selection_bitvector, out_num_not_equal,
                                      out_not_equal_selection);
      uint32_t out_num;
      equal_impl(*out_num_not_equal, out_not_equal_selection, groupids, &out_num,
                 out_not_equal_selection, callback_ctx);
      *out_num_not_equal = static_cast<int>(out_num);
    }
  } else {
    uint32_t out_num;
    equal_impl(num_keys, optional_selection_ids, groupids, &out_num,
               out_not_equal_selection, callback_ctx);
    *out_num_not_equal = static_cast<int>(out_num);
  }
}

// Given starting slot index, search blocks for a matching stamp
// until one is found or an empty slot is reached.
// If the search stopped on a non-empty slot, output corresponding
// group id from that slot.
//
// Return true if a match was found.
//
bool SwissTable::find_next_stamp_match(const uint32_t hash, const uint32_t in_slot_id,
                                       uint32_t* out_slot_id,
                                       uint32_t* out_group_id) const {
  const uint64_t num_groupid_bits = num_groupid_bits_from_log_blocks(log_blocks_);
  constexpr uint64_t stamp_mask = 0x7f;
  const int stamp =
      static_cast<int>((hash >> (bits_hash_ - log_blocks_ - bits_stamp_)) & stamp_mask);
  uint64_t start_slot_id = wrap_global_slot_id(in_slot_id);
  int match_found;
  int local_slot;
  uint8_t* blockbase;
  for (;;) {
    const uint64_t num_block_bytes = (8 + num_groupid_bits);
    blockbase = blocks_->mutable_data() + num_block_bytes * (start_slot_id >> 3);
    uint64_t block = *reinterpret_cast<uint64_t*>(blockbase);

    search_block<true>(block, stamp, (start_slot_id & 7), &local_slot, &match_found);

    start_slot_id =
        wrap_global_slot_id((start_slot_id & ~7ULL) + local_slot + match_found);

    // Match found can be 1 in two cases:
    // - match was found
    // - match was not found in a full block
    // In the second case search needs to continue in the next block.
    if (match_found == 0 || blockbase[7 - local_slot] == stamp) {
      break;
    }
  }

  const uint64_t groupid_mask = (1ULL << num_groupid_bits) - 1;
  *out_group_id =
      static_cast<uint32_t>(extract_group_id(blockbase, local_slot, groupid_mask));
  *out_slot_id = static_cast<uint32_t>(start_slot_id);

  return match_found;
}

// Find method is the continuation of processing from early_filter.
// Its input consists of hash values and the output of early_filter.
// It updates match bit-vector, clearing it from any false positives
// that might have been left by early_filter.
// It also outputs group ids, which are needed to be able to execute
// key comparisons. The caller may discard group ids if only the
// match flag is of interest.
//
void SwissTable::find(const int num_keys, const uint32_t* hashes,
                      uint8_t* inout_match_bitvector, const uint8_t* local_slots,
                      uint32_t* out_group_ids, util::TempVectorStack* temp_stack,
                      const EqualImpl& equal_impl, void* callback_ctx) const {
  // Temporary selection vector.
  // It will hold ids of keys for which we do not know yet
  // if they have a match in hash table or not.
  //
  // Initially the set of these keys is represented by input
  // match bit-vector. Eventually we switch from this bit-vector
  // to array of ids.
  //
  ARROW_DCHECK(num_keys <= (1 << log_minibatch_));
  auto ids_buf = util::TempVectorHolder<uint16_t>(temp_stack, num_keys);
  uint16_t* ids = ids_buf.mutable_data();
  int num_ids;

  int64_t num_matches = arrow::internal::CountSetBits(inout_match_bitvector,
                                                      /*offset=*/0, num_keys);

  // If there is a high density of selected input rows
  // (majority of them are present in the selection),
  // we may run some computation on all of the input rows ignoring
  // selection and then filter the output of this computation
  // (pre-filtering vs post-filtering).
  //
  bool visit_all = num_matches > 0 && num_matches > 3 * num_keys / 4;
  if (visit_all) {
    extract_group_ids(num_keys, nullptr, hashes, local_slots, out_group_ids);
    run_comparisons(num_keys, nullptr, inout_match_bitvector, out_group_ids, &num_ids,
                    ids, equal_impl, callback_ctx);
  } else {
    util::bit_util::bits_to_indexes(1, hardware_flags_, num_keys, inout_match_bitvector,
                                    &num_ids, ids);
    extract_group_ids(num_ids, ids, hashes, local_slots, out_group_ids);
    run_comparisons(num_ids, ids, nullptr, out_group_ids, &num_ids, ids, equal_impl,
                    callback_ctx);
  }

  if (num_ids == 0) {
    return;
  }

  auto slot_ids_buf = util::TempVectorHolder<uint32_t>(temp_stack, num_keys);
  uint32_t* slot_ids = slot_ids_buf.mutable_data();
  init_slot_ids(num_ids, ids, hashes, local_slots, inout_match_bitvector, slot_ids);

  while (num_ids > 0) {
    int num_ids_last_iteration = num_ids;
    num_ids = 0;
    for (int i = 0; i < num_ids_last_iteration; ++i) {
      int id = ids[i];
      uint32_t next_slot_id;
      bool match_found = find_next_stamp_match(hashes[id], slot_ids[id], &next_slot_id,
                                               &(out_group_ids[id]));
      slot_ids[id] = next_slot_id;
      // If next match was not found then clear match bit in a bit vector
      if (!match_found) {
        ::arrow::bit_util::ClearBit(inout_match_bitvector, id);
      } else {
        ids[num_ids++] = id;
      }
    }

    run_comparisons(num_ids, ids, nullptr, out_group_ids, &num_ids, ids, equal_impl,
                    callback_ctx);
  }
}

// Slow processing of input keys in the most generic case.
// Handles inserting new keys.
// Pre-existing keys will be handled correctly, although the intended use is for this
// call to follow a call to find() method, which would only pass on new keys that were
// not present in the hash table.
//
// Run a single round of slot search - comparison or insert - filter unprocessed.
// Update selection vector to reflect which items have been processed.
// Ids in selection vector do not have to be sorted.
//
Status SwissTable::map_new_keys_helper(
    const uint32_t* hashes, uint32_t* inout_num_selected, uint16_t* inout_selection,
    bool* out_need_resize, uint32_t* out_group_ids, uint32_t* inout_next_slot_ids,
    util::TempVectorStack* temp_stack, const EqualImpl& equal_impl,
    const AppendImpl& append_impl, void* callback_ctx) {
  auto num_groups_limit = num_groups_for_resize();
  ARROW_DCHECK(num_inserted_ < num_groups_limit);

  // Temporary arrays are of limited size.
  // The input needs to be split into smaller portions if it exceeds that limit.
  //
  ARROW_DCHECK(*inout_num_selected <= static_cast<uint32_t>(1 << log_minibatch_));

  size_t num_bytes_for_bits = (*inout_num_selected + 7) / 8 + sizeof(uint64_t);
  auto match_bitvector_buf = util::TempVectorHolder<uint8_t>(
      temp_stack, static_cast<uint32_t>(num_bytes_for_bits));
  uint8_t* match_bitvector = match_bitvector_buf.mutable_data();
  memset(match_bitvector, 0xff, num_bytes_for_bits);

  // Check the alignment of the input selection vector
  ARROW_DCHECK((reinterpret_cast<uint64_t>(inout_selection) & 1) == 0);

  uint32_t num_inserted_new = 0;
  uint32_t num_processed;
  for (num_processed = 0; num_processed < *inout_num_selected; ++num_processed) {
    // row id in original batch
    int id = inout_selection[num_processed];
    bool match_found =
        find_next_stamp_match(hashes[id], inout_next_slot_ids[id],
                              &inout_next_slot_ids[id], &out_group_ids[id]);
    if (!match_found) {
      // If we reach the empty slot we insert key for new group
      //
      out_group_ids[id] = num_inserted_ + num_inserted_new;
      insert_into_empty_slot(inout_next_slot_ids[id], hashes[id], out_group_ids[id]);
      this->hashes()[inout_next_slot_ids[id]] = hashes[id];
      ::arrow::bit_util::ClearBit(match_bitvector, num_processed);
      ++num_inserted_new;

      // We need to break processing and have the caller of this function resize hash
      // table if we reach the limit of the number of groups present.
      //
      if (num_inserted_ + num_inserted_new == num_groups_limit) {
        ++num_processed;
        break;
      }
    }
  }

  auto temp_ids_buffer =
      util::TempVectorHolder<uint16_t>(temp_stack, *inout_num_selected);
  uint16_t* temp_ids = temp_ids_buffer.mutable_data();
  int num_temp_ids = 0;

  // Copy keys for newly inserted rows using callback
  //
  util::bit_util::bits_filter_indexes(0, hardware_flags_, num_processed, match_bitvector,
                                      inout_selection, &num_temp_ids, temp_ids);
  ARROW_DCHECK(static_cast<int>(num_inserted_new) == num_temp_ids);
  RETURN_NOT_OK(append_impl(num_inserted_new, temp_ids, callback_ctx));
  num_inserted_ += num_inserted_new;

  // Evaluate comparisons and append ids of rows that failed it to the non-match set.
  util::bit_util::bits_filter_indexes(1, hardware_flags_, num_processed, match_bitvector,
                                      inout_selection, &num_temp_ids, temp_ids);
  run_comparisons(num_temp_ids, temp_ids, nullptr, out_group_ids, &num_temp_ids, temp_ids,
                  equal_impl, callback_ctx);

  if (num_temp_ids > 0) {
    memcpy(inout_selection, temp_ids, sizeof(uint16_t) * num_temp_ids);
  }
  // Append ids of any unprocessed entries if we aborted processing due to the need
  // to resize.
  if (num_processed < *inout_num_selected) {
    memmove(inout_selection + num_temp_ids, inout_selection + num_processed,
            sizeof(uint16_t) * (*inout_num_selected - num_processed));
  }
  *inout_num_selected = num_temp_ids + (*inout_num_selected - num_processed);

  *out_need_resize = (num_inserted_ == num_groups_limit);
  return Status::OK();
}

// Do inserts and find group ids for a set of new keys (with possible duplicates within
// this set).
//
Status SwissTable::map_new_keys(uint32_t num_ids, uint16_t* ids, const uint32_t* hashes,
                                uint32_t* group_ids, util::TempVectorStack* temp_stack,
                                const EqualImpl& equal_impl,
                                const AppendImpl& append_impl, void* callback_ctx) {
  if (num_ids == 0) {
    return Status::OK();
  }

  uint16_t max_id = ids[0];
  for (uint32_t i = 1; i < num_ids; ++i) {
    max_id = std::max(max_id, ids[i]);
  }

  // Temporary buffers have limited size.
  // Caller is responsible for splitting larger input arrays into smaller chunks.
  ARROW_DCHECK(static_cast<int>(num_ids) <= (1 << log_minibatch_));
  ARROW_DCHECK(static_cast<int>(max_id + 1) <= (1 << log_minibatch_));

  // Allocate temporary buffers for slot ids and intialize them
  auto slot_ids_buf = util::TempVectorHolder<uint32_t>(temp_stack, max_id + 1);
  uint32_t* slot_ids = slot_ids_buf.mutable_data();
  init_slot_ids_for_new_keys(num_ids, ids, hashes, slot_ids);

  do {
    // A single round of slow-pass (robust) lookup or insert.
    // A single round ends with either a single comparison verifying the match
    // candidate or inserting a new key. A single round of slow-pass may return early
    // if we reach the limit of the number of groups due to inserts of new keys. In
    // that case we need to resize and recalculating starting global slot ids for new
    // bigger hash table.
    bool out_of_capacity;
    RETURN_NOT_OK(map_new_keys_helper(hashes, &num_ids, ids, &out_of_capacity, group_ids,
                                      slot_ids, temp_stack, equal_impl, append_impl,
                                      callback_ctx));
    if (out_of_capacity) {
      RETURN_NOT_OK(grow_double());
      // Reset start slot ids for still unprocessed input keys.
      //
      for (uint32_t i = 0; i < num_ids; ++i) {
        // First slot in the new starting block
        const int16_t id = ids[i];
        slot_ids[id] = (hashes[id] >> (bits_hash_ - log_blocks_)) * 8;
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
  int log_blocks_after = log_blocks_ + 1;
  uint64_t block_size_before = (8 + num_group_id_bits_before);
  uint64_t block_size_after = (8 + num_group_id_bits_after);
  uint64_t block_size_total_after = (block_size_after << log_blocks_after) + padding_;
  uint64_t hashes_size_total_after =
      (bits_hash_ / 8 * (1 << (log_blocks_after + 3))) + padding_;
  constexpr uint32_t stamp_mask = (1 << bits_stamp_) - 1;

  // Allocate new buffers
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> blocks_new,
                        AllocateBuffer(block_size_total_after, pool_));
  memset(blocks_new->mutable_data(), 0, block_size_total_after);
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> hashes_new_buffer,
                        AllocateBuffer(hashes_size_total_after, pool_));
  auto hashes_new = reinterpret_cast<uint32_t*>(hashes_new_buffer->mutable_data());

  // First pass over all old blocks.
  // Reinsert entries that were not in the overflow block
  // (block other than selected by hash bits corresponding to the entry).
  for (int i = 0; i < (1 << log_blocks_); ++i) {
    // How many full slots in this block
    uint8_t* block_base = blocks_->mutable_data() + i * block_size_before;
    uint8_t* double_block_base_new =
        blocks_new->mutable_data() + 2 * i * block_size_after;
    uint64_t block = *reinterpret_cast<const uint64_t*>(block_base);

    auto full_slots =
        static_cast<int>(CountLeadingZeros(block & kHighBitOfEachByte) >> 3);
    int full_slots_new[2];
    full_slots_new[0] = full_slots_new[1] = 0;
    util::SafeStore(double_block_base_new, kHighBitOfEachByte);
    util::SafeStore(double_block_base_new + block_size_after, kHighBitOfEachByte);

    for (int j = 0; j < full_slots; ++j) {
      uint64_t slot_id = i * 8 + j;
      uint32_t hash = hashes()[slot_id];
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
    uint8_t* block_base = blocks_->mutable_data() + i * block_size_before;
    uint64_t block = util::SafeLoadAs<uint64_t>(block_base);
    int full_slots = static_cast<int>(CountLeadingZeros(block & kHighBitOfEachByte) >> 3);

    for (int j = 0; j < full_slots; ++j) {
      uint64_t slot_id = i * 8 + j;
      uint32_t hash = hashes()[slot_id];
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

      uint8_t* block_base_new =
          blocks_new->mutable_data() + block_id_new * block_size_after;
      uint64_t block_new = util::SafeLoadAs<uint64_t>(block_base_new);
      int full_slots_new =
          static_cast<int>(CountLeadingZeros(block_new & kHighBitOfEachByte) >> 3);
      while (full_slots_new == 8) {
        block_id_new = (block_id_new + 1) & ((1 << log_blocks_after) - 1);
        block_base_new = blocks_new->mutable_data() + block_id_new * block_size_after;
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

  blocks_ = std::move(blocks_new);
  hashes_ = std::move(hashes_new_buffer);
  log_blocks_ = log_blocks_after;

  return Status::OK();
}

Status SwissTable::init(int64_t hardware_flags, MemoryPool* pool, int log_blocks,
                        bool no_hash_array) {
  hardware_flags_ = hardware_flags;
  pool_ = pool;
  log_minibatch_ = util::MiniBatch::kLogMiniBatchLength;

  log_blocks_ = log_blocks;
  int num_groupid_bits = num_groupid_bits_from_log_blocks(log_blocks_);
  num_inserted_ = 0;

  const uint64_t block_bytes = 8 + num_groupid_bits;
  const uint64_t slot_bytes = (block_bytes << log_blocks_) + padding_;
  ARROW_ASSIGN_OR_RAISE(blocks_, AllocateBuffer(slot_bytes, pool_));

  // Make sure group ids are initially set to zero for all slots.
  memset(blocks_->mutable_data(), 0, slot_bytes);

  // Initialize all status bytes to represent an empty slot.
  uint8_t* blocks_ptr = blocks_->mutable_data();
  for (uint64_t i = 0; i < (static_cast<uint64_t>(1) << log_blocks_); ++i) {
    util::SafeStore(blocks_ptr + i * block_bytes, kHighBitOfEachByte);
  }

  if (no_hash_array) {
    hashes_ = nullptr;
  } else {
    uint64_t num_slots = 1ULL << (log_blocks_ + 3);
    const uint64_t hash_size = sizeof(uint32_t);
    const uint64_t hash_bytes = hash_size * num_slots + padding_;
    ARROW_ASSIGN_OR_RAISE(hashes_, AllocateBuffer(hash_bytes, pool_));
  }

  return Status::OK();
}

void SwissTable::cleanup() {
  if (blocks_) {
    blocks_ = nullptr;
  }
  if (hashes_) {
    hashes_ = nullptr;
  }
  log_blocks_ = 0;
  num_inserted_ = 0;
}

}  // namespace compute
}  // namespace arrow
