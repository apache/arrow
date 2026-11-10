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

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "arrow/compute/key_map_internal.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/cpu_info.h"

namespace arrow {

using internal::CpuInfo;

namespace compute {

// With 32-bit group ids a block takes 40 bytes, so the byte offsets of the blocks from
// id ceil(2^32 / 40) on do not fit in 32 bits.
TEST(SwissTable, LARGE_MEMORY_TEST(EarlyFilterOver4GB)) {
  if constexpr (sizeof(void*) == 4) {
    GTEST_SKIP() << "Test only works on 64-bit platforms";
  }

  // 2^27 blocks of 40 bytes take 5GB.
  constexpr int kLogBlocks = 27;
  constexpr uint32_t kFirstBlockOver4GB = 107374183;
  constexpr int kNumHashes = 16;

  // One hash for each of the 8 blocks below and the 8 blocks above the 4GB boundary,
  // all with a non-zero stamp.
  std::vector<uint32_t> hashes(kNumHashes);
  for (int i = 0; i < kNumHashes; ++i) {
    uint32_t block_id = kFirstBlockOver4GB - kNumHashes / 2 + i;
    hashes[i] = (block_id << (SwissTable::bits_hash_ - kLogBlocks)) | 1;
  }

  for (int64_t hardware_flags : GetSupportedHardwareFlags({CpuInfo::AVX2})) {
    ARROW_SCOPED_TRACE("hardware_flags = ", hardware_flags);
    SwissTable table;
    ASSERT_OK(table.init(hardware_flags, default_memory_pool(), kLogBlocks,
                         /*no_hash_array=*/true));
    // Insert every other hash into the first slot of its block, leaving the other blocks
    // empty.
    for (int i = 0; i < kNumHashes; i += 2) {
      uint32_t block_id = SwissTable::block_id_from_hash(hashes[i], kLogBlocks);
      table.insert_into_empty_slot(SwissTable::global_slot_id(block_id, 0), hashes[i],
                                   /*group_id=*/i);
    }

    uint8_t match_bitvector[kNumHashes / 8];
    uint8_t local_slots[kNumHashes];
    table.early_filter(kNumHashes, hashes.data(), match_bitvector, local_slots);
    for (int i = 0; i < kNumHashes; ++i) {
      ARROW_SCOPED_TRACE("block_id = ",
                         SwissTable::block_id_from_hash(hashes[i], kLogBlocks));
      // Inserted hashes match in the first slot, the others hit an empty block whose
      // first slot is empty.
      ASSERT_EQ(bit_util::GetBit(match_bitvector, i), i % 2 == 0);
      ASSERT_EQ(local_slots[i], 0);
    }
  }
}

// When growing to a table over 4GB, entries that have to move past a full block must
// still land in the right block.
TEST(SwissTable, LARGE_MEMORY_TEST(GrowOver4GB)) {
  if constexpr (sizeof(void*) == 4) {
    GTEST_SKIP() << "Test only works on 64-bit platforms";
  }

  // Grow from 2^26 to 2^27 blocks.
  constexpr int kLogBlocks = 26;
  constexpr uint32_t kBlockId = (1u << kLogBlocks) - 2;
  constexpr int kNumHashes = SwissTable::kSlotsPerBlock + 1;

  // All these hashes map to block kBlockId before growing and to block 2 * kBlockId
  // after. The first 8 fill these blocks, so the last one is in block kBlockId + 1 before
  // growing and has to move to block 2 * kBlockId + 1, which is over 4GB.
  std::vector<uint32_t> hashes(kNumHashes);
  for (int i = 0; i < kNumHashes; ++i) {
    hashes[i] = (kBlockId << (SwissTable::bits_hash_ - kLogBlocks)) | i;
  }

  // Key i is equal to group id i.
  SwissTable::EqualImpl equal_impl =
      [](int num_keys, const uint16_t* selection, const uint32_t* group_ids,
         uint32_t* out_num_keys_mismatch, uint16_t* out_selection_mismatch, void*) {
        *out_num_keys_mismatch = 0;
        for (int i = 0; i < num_keys; ++i) {
          uint16_t id = selection ? selection[i] : static_cast<uint16_t>(i);
          if (group_ids[id] != id) {
            out_selection_mismatch[(*out_num_keys_mismatch)++] = id;
          }
        }
      };
  SwissTable::AppendImpl append_impl = [](int, const uint16_t*, void*) {
    return Status::OK();
  };

  for (int64_t hardware_flags : GetSupportedHardwareFlags({CpuInfo::AVX2})) {
    ARROW_SCOPED_TRACE("hardware_flags = ", hardware_flags);
    SwissTable table;
    ASSERT_OK(table.init(hardware_flags, default_memory_pool(), kLogBlocks));
    for (int i = 0; i < kNumHashes; ++i) {
      uint32_t slot_id = SwissTable::global_slot_id(kBlockId, 0) + i;
      table.insert_into_empty_slot(slot_id, hashes[i], /*group_id=*/i);
      table.hashes()[slot_id] = hashes[i];
    }

    // The table grows when 75% of its slots are used. Pretend that it is one key short
    // of that and insert one more key.
    table.num_inserted(
        static_cast<uint32_t>((int64_t{1} << (kLogBlocks + 3)) * 3 / 4 - 1));
    util::TempVectorStack temp_stack;
    ASSERT_OK(temp_stack.Init(default_memory_pool(), 64 * table.minibatch_size()));
    uint16_t new_key_id = 0;
    uint32_t new_key_hash = 0;
    uint32_t new_group_id;
    ASSERT_OK(table.map_new_keys(/*num_ids=*/1, &new_key_id, &new_key_hash, &new_group_id,
                                 &temp_stack, equal_impl, append_impl,
                                 /*callback_ctx=*/nullptr));
    ASSERT_EQ(table.log_blocks(), kLogBlocks + 1);

    uint8_t match_bitvector[(kNumHashes + 7) / 8];
    uint8_t local_slots[kNumHashes];
    uint32_t group_ids[kNumHashes];
    table.early_filter(kNumHashes, hashes.data(), match_bitvector, local_slots);
    table.find(kNumHashes, hashes.data(), match_bitvector, local_slots, group_ids,
               &temp_stack, equal_impl, /*callback_ctx=*/nullptr);
    for (int i = 0; i < kNumHashes; ++i) {
      ARROW_SCOPED_TRACE("key = ", i);
      ASSERT_TRUE(bit_util::GetBit(match_bitvector, i));
      ASSERT_EQ(group_ids[i], i);
    }
  }
}

}  // namespace compute
}  // namespace arrow
