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

#include <cassert>
#include <functional>

#include "arrow/compute/util.h"
#include "arrow/compute/util_internal.h"
#include "arrow/compute/visibility.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace compute {

// SwissTable is a variant of a hash table implementation.
// This implementation is vectorized, that is: main interface methods take arrays of input
// values and output arrays of result values.
//
// A detailed explanation of this data structure (including concepts such as blocks,
// slots, stamps) and operations provided by this class is given in the document:
// arrow/acero/doc/key_map.md.
//
class ARROW_COMPUTE_EXPORT SwissTable {
  friend class SwissTableMerge;

 public:
  SwissTable() = default;
  ~SwissTable() { cleanup(); }

  using EqualImpl =
      std::function<void(int num_keys, const uint16_t* selection /* may be null */,
                         const uint32_t* group_ids, uint32_t* out_num_keys_mismatch,
                         uint16_t* out_selection_mismatch, void* callback_ctx)>;
  using AppendImpl =
      std::function<Status(int num_keys, const uint16_t* selection, void* callback_ctx)>;

  Status init(int64_t hardware_flags, MemoryPool* pool, int log_blocks = 0,
              bool no_hash_array = false);

  void cleanup();

  void early_filter(const int num_keys, const uint32_t* hashes,
                    uint8_t* out_match_bitvector, uint8_t* out_local_slots) const;

  void find(const int num_keys, const uint32_t* hashes, uint8_t* inout_match_bitvector,
            const uint8_t* local_slots, uint32_t* out_group_ids,
            util::TempVectorStack* temp_stack, const EqualImpl& equal_impl,
            void* callback_ctx) const;

  Status map_new_keys(uint32_t num_ids, uint16_t* ids, const uint32_t* hashes,
                      uint32_t* group_ids, util::TempVectorStack* temp_stack,
                      const EqualImpl& equal_impl, const AppendImpl& append_impl,
                      void* callback_ctx);

  int minibatch_size() const { return 1 << log_minibatch_; }

  uint32_t num_inserted() const { return num_inserted_; }

  int64_t hardware_flags() const { return hardware_flags_; }

  MemoryPool* pool() const { return pool_; }

  int log_blocks() const { return log_blocks_; }

  void num_inserted(uint32_t i) { num_inserted_ = i; }

  uint32_t* hashes() const {
    return reinterpret_cast<uint32_t*>(hashes_->mutable_data());
  }

  /// \brief Extract group id for a given slot in a given block using aligned 32-bit read
  /// regardless of the number of group id bits.
  /// Note that group_id_mask should be derived from num_group_id_bits. This function
  /// accepts both and does debug checking for performance sake.
  ///
  static uint32_t extract_group_id(const uint8_t* block_ptr, int local_slot,
                                   int num_group_id_bits, uint32_t group_id_mask) {
    assert(group_id_mask_from_num_groupid_bits(num_group_id_bits) == group_id_mask);
    int slot_bit_offset = local_slot * num_group_id_bits;
    const uint32_t* group_id_ptr32 =
        reinterpret_cast<const uint32_t*>(block_ptr + bytes_status_in_block_) +
        (slot_bit_offset >> 5);
    uint32_t group_id = (*group_id_ptr32 >> (slot_bit_offset & 31)) & group_id_mask;
    return group_id;
  }

  inline void insert_into_empty_slot(uint32_t global_slot_id, uint32_t hash,
                                     uint32_t group_id);

  static uint32_t block_id_from_hash(uint32_t hash, int log_blocks) {
    return hash >> (bits_hash_ - log_blocks);
  }

  static uint32_t global_slot_id(uint32_t block_id, uint32_t local_slot_id) {
    return block_id * kSlotsPerBlock + local_slot_id;
  }

  static int num_groupid_bits_from_log_blocks(int log_blocks) {
    assert(log_blocks >= 0);
    int required_bits = log_blocks + kLogSlotsPerBlock;
    assert(required_bits <= 32);
    return required_bits <= 8 ? 8 : required_bits <= 16 ? 16 : 32;
  }

  static int num_block_bytes_from_num_groupid_bits(int num_groupid_bits) {
    return num_groupid_bits + bytes_status_in_block_;
  }

  static uint32_t group_id_mask_from_num_groupid_bits(int num_groupid_bits) {
    // num_groupid_bits could be 32, so using 64-bit shifting.
    return static_cast<uint32_t>((1ULL << num_groupid_bits) - 1ULL);
  }

  const uint8_t* block_data(uint32_t block_id, int num_block_bytes) const {
    return block_data(blocks_->data(), block_id, num_block_bytes);
  }

  uint8_t* mutable_block_data(uint32_t block_id, int num_block_bytes) {
    return mutable_block_data(blocks_->mutable_data(), block_id, num_block_bytes);
  }

  static constexpr int kSlotsPerBlock = 8;

  // Use 32-bit hash for now
  static constexpr int bits_hash_ = 32;

 private:
  static const uint8_t* block_data(const uint8_t* blocks, uint32_t block_id,
                                   int num_block_bytes) {
    return blocks + static_cast<int64_t>(block_id) * num_block_bytes;
  }

  static uint8_t* mutable_block_data(uint8_t* blocks, uint32_t block_id,
                                     int num_block_bytes) {
    return blocks + static_cast<int64_t>(block_id) * num_block_bytes;
  }

  // Lookup helpers

  /// \brief Scan bytes in block in reverse and stop as soon
  /// as a position of interest is found.
  ///
  /// Positions of interest:
  /// a) slot with a matching stamp is encountered,
  /// b) first empty slot is encountered,
  /// c) we reach the end of the block.
  ///
  /// Optionally an index of the first slot to start the search from can be specified.
  /// In this case slots before it will be ignored.
  ///
  /// \param[in] block 8 byte block of hash table
  /// \param[in] stamp 7 bits of hash used as a stamp
  /// \param[in] start_slot Index of the first slot in the block to start search from.  We
  ///            assume that this index always points to a non-empty slot, equivalently
  ///            that it comes before any empty slots.  (Used only by one template
  ///            variant.)
  /// \param[out] out_slot index corresponding to the discovered position of interest (8
  ///            represents end of block).
  /// \param[out] out_match_found an integer flag (0 or 1) indicating if we reached an
  /// empty slot (0) or not (1). Therefore 1 can mean that either actual match was found
  /// (case a) above) or we reached the end of full block (case b) above).
  ///
  template <bool use_start_slot>
  inline void search_block(uint64_t block, int stamp, int start_slot, int* out_slot,
                           int* out_match_found) const;

  void extract_group_ids(const int num_keys, const uint16_t* optional_selection,
                         const uint32_t* hashes, const uint8_t* local_slots,
                         uint32_t* out_group_ids) const;

  template <typename T, bool use_selection>
  void extract_group_ids_imp(const int num_keys, const uint16_t* selection,
                             const uint32_t* hashes, const uint8_t* local_slots,
                             uint32_t* out_group_ids) const;

  static constexpr int kLogSlotsPerBlock = 3;
  static constexpr int kMaxLocalSlot = kSlotsPerBlock - 1;
  static constexpr uint32_t kLocalSlotMask = (1U << kLogSlotsPerBlock) - 1U;

  static int64_t num_slots_from_log_blocks(int log_blocks) {
    return 1LL << (log_blocks + kLogSlotsPerBlock);
  }

  static int64_t num_bytes_total_blocks(int num_block_bytes, int log_blocks) {
    return (static_cast<int64_t>(num_block_bytes) << log_blocks) + padding_;
  }

  inline int64_t num_groups_for_resize() const;

  inline uint32_t wrap_global_slot_id(uint32_t global_slot_id) const;

  void init_slot_ids(const int num_keys, const uint16_t* selection,
                     const uint32_t* hashes, const uint8_t* local_slots,
                     const uint8_t* match_bitvector, uint32_t* out_slot_ids) const;

  void init_slot_ids_for_new_keys(uint32_t num_ids, const uint16_t* ids,
                                  const uint32_t* hashes, uint32_t* slot_ids) const;

  // Quickly filter out keys that have no matches based only on hash value and the
  // corresponding starting 64-bit block of slot status bytes. May return false positives.
  //
  void early_filter_imp(const int num_keys, const uint32_t* hashes,
                        uint8_t* out_match_bitvector, uint8_t* out_local_slots) const;
#if defined(ARROW_HAVE_RUNTIME_AVX2) && defined(ARROW_HAVE_RUNTIME_BMI2)
  // The functions below use BMI2 instructions, be careful before calling!
  int early_filter_imp_avx2_x8(const int num_hashes, const uint32_t* hashes,
                               uint8_t* out_match_bitvector,
                               uint8_t* out_local_slots) const;
  int early_filter_imp_avx2_x32(const int num_hashes, const uint32_t* hashes,
                                uint8_t* out_match_bitvector,
                                uint8_t* out_local_slots) const;
  int extract_group_ids_avx2(const int num_keys, const uint32_t* hashes,
                             const uint8_t* local_slots, uint32_t* out_group_ids) const;
#endif

  void run_comparisons(const int num_keys, const uint16_t* optional_selection_ids,
                       const uint8_t* optional_selection_bitvector,
                       const uint32_t* groupids, int* out_num_not_equal,
                       uint16_t* out_not_equal_selection, const EqualImpl& equal_impl,
                       void* callback_ctx) const;

  inline bool find_next_stamp_match(const uint32_t hash, const uint32_t in_slot_id,
                                    uint32_t* out_slot_id, uint32_t* out_group_id) const;

  // Slow processing of input keys in the most generic case.
  // Handles inserting new keys.
  // Preexisting keys will be handled correctly, although the intended use is for this
  // call to follow a call to find() method, which would only pass on new keys that were
  // not present in the hash table.
  //
  Status map_new_keys_helper(const uint32_t* hashes, uint32_t* inout_num_selected,
                             uint16_t* inout_selection, bool* out_need_resize,
                             uint32_t* out_group_ids, uint32_t* out_next_slot_ids,
                             util::TempVectorStack* temp_stack,
                             const EqualImpl& equal_impl, const AppendImpl& append_impl,
                             void* callback_ctx);

  // Resize small hash tables when 50% full (up to 8KB).
  // Resize large hash tables when 75% full.
  Status grow_double();

  // When log_blocks is greater than 25, there will be overlapping bits between block id
  // and stamp within a 32-bit hash value. So we must check if this is the case when
  // right shifting a hash value to retrieve block id and stamp. The following two
  // functions derive the number of bits to right shift from the given log_blocks.
  static int ComputeBitsShiftForBlockAndStamp(int log_blocks) {
    if (ARROW_PREDICT_FALSE(log_blocks + bits_stamp_ > bits_hash_)) {
      return 0;
    }
    return bits_hash_ - log_blocks - bits_stamp_;
  }
  static int ComputeBitsShiftForBlock(int log_blocks) {
    if (ARROW_PREDICT_FALSE(log_blocks + bits_stamp_ > bits_hash_)) {
      return bits_hash_ - log_blocks;
    }
    return bits_stamp_;
  }

  static constexpr int bytes_status_in_block_ = 8;

  // Number of hash bits stored in slots in a block.
  // The highest bits of hash determine block id.
  // The next set of highest bits is a "stamp" stored in a slot in a block.
  static constexpr int bits_stamp_ = 7;

  // Padding bytes added at the end of buffers for ease of SIMD access
  static constexpr int padding_ = 64;

  int log_minibatch_;
  // Base 2 log of the number of blocks
  int log_blocks_ = 0;
  // The following two variables are derived from log_blocks_ as log_blocks_ changes, and
  // used in tight loops to avoid calling the ComputeXXX functions (introducing a
  // branching on whether log_blocks_ + bits_stamp_ > bits_hash_).
  int bits_shift_for_block_and_stamp_ = ComputeBitsShiftForBlockAndStamp(log_blocks_);
  int bits_shift_for_block_ = ComputeBitsShiftForBlock(log_blocks_);
  // Number of keys inserted into hash table
  uint32_t num_inserted_ = 0;

  // Data for blocks.
  // Each block has 8 status bytes for 8 slots, followed by 8 bit packed group ids for
  // these slots. In 8B status word, the order of bytes is reversed. Group ids are in
  // normal order. There is 64B padding at the end.
  //
  // 0 byte - 7 bucket | 1. byte - 6 bucket | ...
  // ---------------------------------------------------
  // |     Empty bit*   |    Empty bit       |
  // ---------------------------------------------------
  // |   7-bit hash    |    7-bit hash      |
  // ---------------------------------------------------
  // * Empty bucket has value 0x80. Non-empty bucket has highest bit set to 0.
  //
  std::shared_ptr<Buffer> blocks_;

  // Array of hashes of values inserted into slots.
  // Undefined if the corresponding slot is empty.
  // There is 64B padding at the end.
  std::shared_ptr<Buffer> hashes_;

  int64_t hardware_flags_;
  MemoryPool* pool_;
};

void SwissTable::insert_into_empty_slot(uint32_t global_slot_id, uint32_t hash,
                                        uint32_t group_id) {
  const int num_groupid_bits = num_groupid_bits_from_log_blocks(log_blocks_);

  // We assume here that the number of bits is rounded up to 8, 16, 32 or 64.
  // In that case we can insert group id value using aligned 64-bit word access.
  assert(num_groupid_bits == 8 || num_groupid_bits == 16 || num_groupid_bits == 32 ||
         num_groupid_bits == 64);

  const int num_block_bytes = num_block_bytes_from_num_groupid_bits(num_groupid_bits);
  constexpr uint32_t stamp_mask = 0x7f;

  int start_slot = (global_slot_id & kLocalSlotMask);
  int stamp = (hash >> bits_shift_for_block_and_stamp_) & stamp_mask;
  uint32_t block_id = global_slot_id >> kLogSlotsPerBlock;
  uint8_t* blockbase = mutable_block_data(block_id, num_block_bytes);

  blockbase[kMaxLocalSlot - start_slot] = static_cast<uint8_t>(stamp);
  int groupid_bit_offset = start_slot * num_groupid_bits;

  // Block status bytes should start at an address aligned to 8 bytes
  assert((reinterpret_cast<uint64_t>(blockbase) & 7) == 0);
  uint64_t* ptr = reinterpret_cast<uint64_t*>(blockbase + bytes_status_in_block_) +
                  (groupid_bit_offset >> 6);
  *ptr |= (static_cast<uint64_t>(group_id) << (groupid_bit_offset & 63));
}

}  // namespace compute
}  // namespace arrow
