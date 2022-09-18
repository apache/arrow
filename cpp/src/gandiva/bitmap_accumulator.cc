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

#include "gandiva/bitmap_accumulator.h"

#include <vector>

#include "arrow/util/bitmap_ops.h"

namespace gandiva {

void BitMapAccumulator::ComputeResult(uint8_t* dst_bitmap) {
  int64_t num_records = eval_batch_.num_records();

  if (all_invalid_) {
    // set all bits to 0.
    memset(dst_bitmap, 0, arrow::bit_util::BytesForBits(num_records));
  } else {
    IntersectBitMaps(dst_bitmap, src_maps_, src_map_offsets_, num_records);
  }
}

/// Compute the intersection of multiple bitmaps.
void BitMapAccumulator::IntersectBitMaps(uint8_t* dst_map,
                                         const std::vector<const uint8_t*>& src_maps,
                                         const std::vector<int64_t>& src_map_offsets,
                                         int64_t num_records) {
  int64_t num_words = (num_records + 63) / 64;  // aligned to 8-byte.
  int64_t num_bytes = num_words * 8;
  int64_t nmaps = src_maps.size();

  switch (nmaps) {
    case 0: {
      // no src_maps_ bitmap. simply set all bits
      memset(dst_map, 0xff, num_bytes);
      break;
    }

    case 1: {
      // one src_maps_ bitmap. copy to dst_map
      arrow::internal::CopyBitmap(src_maps[0], src_map_offsets[0], num_records, dst_map,
                                  0);
      break;
    }

    default: {
      // src_maps bitmaps ANDs
      arrow::internal::BitmapAnd(src_maps[0], src_map_offsets[0], src_maps[1],
                                 src_map_offsets[1], num_records, /*offset=*/0, dst_map);
      for (int64_t m = 2; m < nmaps; ++m) {
        arrow::internal::BitmapAnd(dst_map, 0, src_maps[m], src_map_offsets[m],
                                   num_records,
                                   /*offset=*/0, dst_map);
      }

      break;
    }
  }
}

}  // namespace gandiva
