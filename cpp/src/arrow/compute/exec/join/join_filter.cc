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

#include "arrow/compute/exec/join/join_filter.h"

#include "arrow/compute/exec/util.h"

namespace arrow {
namespace compute {

constexpr int ApproximateMembershipTest::BitMasksGenerator::bit_width_;
constexpr int ApproximateMembershipTest::BitMasksGenerator::min_bits_set_;
constexpr int ApproximateMembershipTest::BitMasksGenerator::max_bits_set_;
constexpr int ApproximateMembershipTest::BitMasksGenerator::log_num_masks_;
constexpr int ApproximateMembershipTest::BitMasksGenerator::num_masks_;
constexpr int ApproximateMembershipTest::BitMasksGenerator::num_masks_less_one_;

ApproximateMembershipTest::BitMasksGenerator::BitMasksGenerator() {
  memset(masks_, 0, (num_masks_ + 7) / 8 + sizeof(uint64_t));
  util::Random64Bit rnd;
  int num_bits_set = rnd.from_range(min_bits_set_, max_bits_set_);
  for (int i = 0; i < num_bits_set; ++i) {
    for (;;) {
      int bit_pos = rnd.from_range(0, bit_width_ - 1);
      if (!BitUtil::GetBit(masks_, bit_pos)) {
        BitUtil::SetBit(masks_, bit_pos);
        break;
      }
    }
  }
  for (int next_bit = bit_width_; next_bit < num_masks_ + 64; ++next_bit) {
    if (BitUtil::GetBit(masks_, next_bit - bit_width_) && num_bits_set == min_bits_set_) {
      // Next bit has to be 1
      BitUtil::SetBit(masks_, next_bit);
    } else if (!BitUtil::GetBit(masks_, next_bit - bit_width_) &&
               num_bits_set == max_bits_set_) {
      // Next bit has to be 0
    } else {
      // Next bit can be random
      if ((rnd.next() % 2) == 0) {
        BitUtil::SetBit(masks_, next_bit);
        ++num_bits_set;
      }
      if (BitUtil::GetBit(masks_, next_bit - bit_width_)) {
        --num_bits_set;
      }
    }
  }
}

void ApproximateMembershipTest::MayHaveHash(int64_t hardware_flags, int64_t num_rows,
                                            const uint32_t* hashes,
                                            uint8_t* result) const {
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    MayHaveHash_avx2(num_rows, hashes, result);
    return;
  }
#endif
  for (int64_t i = 0; i < num_rows; ++i) {
    result[i] = MayHaveHash(hashes[i]) ? 0xFF : 0;
  }
}

ApproximateMembershipTest::BitMasksGenerator ApproximateMembershipTest::bit_masks_;

}  // namespace compute
}  // namespace arrow