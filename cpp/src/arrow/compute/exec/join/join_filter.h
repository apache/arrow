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
#include <memory>
#include <vector>

#include "arrow/util/bit_util.h"
#include "arrow/util/ubsan.h"

/* Implementation of Bloom-like approximate membership test for use in hash join. */

namespace arrow {
namespace compute {

// Only supports single-threaded build. TODO: add parallel build implementation.
//
class ApproximateMembershipTest {
 public:
  void StartBuild(int64_t num_hashes) {
    num_bits_ = num_hashes * 8;
    ceil_log_num_bits_ = 0;
    while (num_bits_ > static_cast<int64_t>(1ULL << ceil_log_num_bits_)) {
      ++ceil_log_num_bits_;
    }
    hash_mask_num_bits_ = (1ULL << ceil_log_num_bits_) - 1;
    int64_t num_bytes = (1 << ceil_log_num_bits_) / 8;
    bits_.resize(num_bytes + sizeof(uint64_t));
    memset(bits_.data(), 0, num_bytes + sizeof(uint64_t));
  }

  inline void InsertHash(uint64_t hash) {
    uint64_t mask;
    int64_t byte_offset;
    Prepare(hash, &mask, &byte_offset);
    util::SafeStore(bits_.data() + byte_offset, mask);
  }

  void FinishBuild() {
    uint64_t first_word = util::SafeLoadAs<uint64_t>(bits_.data());
    uint64_t last_word = util::SafeLoadAs<uint64_t>(bits_.data() + num_bits_ / 8);
    util::SafeStore(bits_.data(), first_word | last_word);
    util::SafeStore(bits_.data() + num_bits_ / 8, first_word | last_word);
  }

  inline bool MayHaveHash(uint64_t hash) const {
    uint64_t mask;
    int64_t byte_offset;
    Prepare(hash, &mask, &byte_offset);
    return (util::SafeLoadAs<uint64_t>(bits_.data() + byte_offset) & mask) == mask;
  }

  void MayHaveHash(int64_t hardware_flags, int64_t num_rows, const uint32_t* hashes,
                   uint8_t* result) const;

  class BitMasksGenerator {
   public:
    // In each consecutive "bit_width_" bits, there must be between "min_bits_set_" and
    // "max_bits_set_" bits set.
    BitMasksGenerator();

    static constexpr int bit_width_ = 57;
    static constexpr int min_bits_set_ = 4;
    static constexpr int max_bits_set_ = 5;

    static constexpr int log_num_masks_ = 10;
    static constexpr int num_masks_ = 1 << log_num_masks_;
    static constexpr int num_masks_less_one_ = num_masks_ - 1;
    uint8_t masks_[(num_masks_ + 7) / 8 + sizeof(uint64_t)];
  };

 private:
  inline void Prepare(uint64_t hash, uint64_t* mask, int64_t* byte_offset) const {
    int64_t bit_offset0 = hash & (BitMasksGenerator::num_masks_ - 1);
    constexpr uint64_t mask_mask = (1ULL << BitMasksGenerator::bit_width_) - 1;
    *mask = (util::SafeLoadAs<uint64_t>(bit_masks_.masks_ + bit_offset0 / 8) >>
             (bit_offset0 % 8)) &
            mask_mask;
    int64_t bit_offset =
        (hash >> (BitMasksGenerator::log_num_masks_)) & hash_mask_num_bits_;
    // bit_offset = bit_offset * num_bits_ >> ceil_log_num_bits_;
    *mask <<= (bit_offset % 8);
    *byte_offset = bit_offset / 8;
  }

#if defined(ARROW_HAVE_AVX2)
  template <typename hash_type>
  void MayHaveHash_imp_avx2(int64_t num_rows, const hash_type* hashes,
                            uint8_t* result) const;
  void MayHaveHash_avx2(int64_t num_rows, const uint32_t* hashes, uint8_t* result) const;
  void MayHaveHash_avx2(int64_t num_rows, const uint64_t* hashes, uint8_t* result) const;
#endif

  static BitMasksGenerator bit_masks_;
  int64_t num_bits_;
  int64_t ceil_log_num_bits_;
  int64_t hash_mask_num_bits_;
  std::vector<uint8_t> bits_;
};

}  // namespace compute
}  // namespace arrow
