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

}  // namespace compute
}  // namespace arrow
