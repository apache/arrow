// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "codegen/bitmap_accumulator.h"

#include <vector>

namespace gandiva {

void BitMapAccumulator::ComputeResult(uint8_t *dst_bitmap) {
  int num_records = eval_batch_.num_records();

  if (all_invalid_) {
    // set all bits to 0.
    memset(dst_bitmap, 0, arrow::BitUtil::BytesForBits(num_records));
  } else {
    IntersectBitMaps(dst_bitmap, src_maps_, num_records);
  }
}

/// Compute the intersection of multiple bitmaps.
void BitMapAccumulator::IntersectBitMaps(uint8_t *dst_map,
                                         const std::vector<uint8_t *> &src_maps,
                                         int num_records) {
  uint64_t *dst_map64 = reinterpret_cast<uint64_t *>(dst_map);
  int num_words = (num_records + 63) / 64; // aligned to 8-byte.
  int num_bytes = num_words * 8;
  int nmaps = src_maps.size();

  switch (nmaps) {
    case 0: {
      // no src_maps_ bitmap. simply set all bits
      memset(dst_map, 0xff, num_bytes);
      break;
    }

    case 1: {
      // one src_maps_ bitmap. copy to dst_map
      memcpy(dst_map, src_maps[0], num_bytes);
      break;
    }

    case 2: {
      // two src_maps bitmaps. do 64-bit ANDs
      uint64_t *src_maps0_64 = reinterpret_cast<uint64_t *>(src_maps[0]);
      uint64_t *src_maps1_64 = reinterpret_cast<uint64_t *>(src_maps[1]);
      for (int i = 0; i < num_words; ++i) {
        dst_map64[i] = src_maps0_64[i] & src_maps1_64[i];
      }
      break;
    }

    default: {
      /* > 2 src_maps bitmaps. do 64-bit ANDs */
      uint64_t *src_maps0_64 = reinterpret_cast<uint64_t *>(src_maps[0]);
      memcpy(dst_map64, src_maps0_64, num_bytes);
      for (int m = 1; m < nmaps; ++m) {
        for (int i = 0; i < num_words; ++i) {
          uint64_t *src_mapsm_64 = reinterpret_cast<uint64_t *>(src_maps[m]);
          dst_map64[i] &= src_mapsm_64[i];
        }
      }

      break;
    }
  }
}

} // namespace gandiva
