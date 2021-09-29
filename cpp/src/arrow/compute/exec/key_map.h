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

#include <functional>

#include "arrow/compute/exec/util.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace compute {

class SwissTable {
 public:
  SwissTable() = default;
  ~SwissTable() { cleanup(); }

  using EqualImpl =
      std::function<void(int num_keys, const uint16_t* selection /* may be null */,
                         const uint32_t* group_ids, uint32_t* out_num_keys_mismatch,
                         uint16_t* out_selection_mismatch)>;
  using AppendImpl = std::function<Status(int num_keys, const uint16_t* selection)>;

  Status init(int64_t hardware_flags, MemoryPool* pool, util::TempVectorStack* temp_stack,
              int log_minibatch, EqualImpl equal_impl, AppendImpl append_impl);

  void cleanup();

  void early_filter(const int num_keys, const uint32_t* hashes,
                    uint8_t* out_match_bitvector, uint8_t* out_local_slots) const;

  void find(const int num_keys, const uint32_t* hashes, uint8_t* inout_match_bitvector,
            const uint8_t* local_slots, uint32_t* out_group_ids) const;

  Status map_new_keys(uint32_t num_ids, uint16_t* ids, const uint32_t* hashes,
                      uint32_t* group_ids);

 private:
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

  /// \brief Extract group id for a given slot in a given block.
  ///
  inline uint64_t extract_group_id(const uint8_t* block_ptr, int slot,
                                   uint64_t group_id_mask) const;
  void extract_group_ids(const int num_keys, const uint16_t* optional_selection,
                         const uint32_t* hashes, const uint8_t* local_slots,
                         uint32_t* out_group_ids) const;

  template <typename T, bool use_selection>
  void extract_group_ids_imp(const int num_keys, const uint16_t* selection,
                             const uint32_t* hashes, const uint8_t* local_slots,
                             uint32_t* out_group_ids, int elements_offset,
                             int element_mutltiplier) const;

  inline uint64_t next_slot_to_visit(uint64_t block_index, int slot,
                                     int match_found) const;

  inline uint64_t num_groups_for_resize() const;

  inline uint64_t wrap_global_slot_id(uint64_t global_slot_id) const;

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
#if defined(ARROW_HAVE_AVX2)
  void early_filter_imp_avx2_x8(const int num_hashes, const uint32_t* hashes,
                                uint8_t* out_match_bitvector,
                                uint8_t* out_local_slots) const;
  void early_filter_imp_avx2_x32(const int num_hashes, const uint32_t* hashes,
                                 uint8_t* out_match_bitvector,
                                 uint8_t* out_local_slots) const;
  void extract_group_ids_avx2(const int num_keys, const uint32_t* hashes,
                              const uint8_t* local_slots, uint32_t* out_group_ids,
                              int byte_offset, int byte_multiplier, int byte_size) const;
#endif

  void run_comparisons(const int num_keys, const uint16_t* optional_selection_ids,
                       const uint8_t* optional_selection_bitvector,
                       const uint32_t* groupids, int* out_num_not_equal,
                       uint16_t* out_not_equal_selection) const;

  inline bool find_next_stamp_match(const uint32_t hash, const uint32_t in_slot_id,
                                    uint32_t* out_slot_id, uint32_t* out_group_id) const;

  inline void insert_into_empty_slot(uint32_t slot_id, uint32_t hash, uint32_t group_id);

  // Slow processing of input keys in the most generic case.
  // Handles inserting new keys.
  // Pre-existing keys will be handled correctly, although the intended use is for this
  // call to follow a call to find() method, which would only pass on new keys that were
  // not present in the hash table.
  //
  Status map_new_keys_helper(const uint32_t* hashes, uint32_t* inout_num_selected,
                             uint16_t* inout_selection, bool* out_need_resize,
                             uint32_t* out_group_ids, uint32_t* out_next_slot_ids);

  // Resize small hash tables when 50% full (up to 8KB).
  // Resize large hash tables when 75% full.
  Status grow_double();

  static int num_groupid_bits_from_log_blocks(int log_blocks) {
    int required_bits = log_blocks + 3;
    return required_bits <= 8 ? 8
                              : required_bits <= 16 ? 16 : required_bits <= 32 ? 32 : 64;
  }

  // Use 32-bit hash for now
  static constexpr int bits_hash_ = 32;

  // Number of hash bits stored in slots in a block.
  // The highest bits of hash determine block id.
  // The next set of highest bits is a "stamp" stored in a slot in a block.
  static constexpr int bits_stamp_ = 7;

  // Padding bytes added at the end of buffers for ease of SIMD access
  static constexpr int padding_ = 64;

  int log_minibatch_;
  // Base 2 log of the number of blocks
  int log_blocks_ = 0;
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
  uint8_t* blocks_;

  // Array of hashes of values inserted into slots.
  // Undefined if the corresponding slot is empty.
  // There is 64B padding at the end.
  uint32_t* hashes_;

  int64_t hardware_flags_;
  MemoryPool* pool_;
  util::TempVectorStack* temp_stack_;

  EqualImpl equal_impl_;
  AppendImpl append_impl_;
};

}  // namespace compute
}  // namespace arrow
