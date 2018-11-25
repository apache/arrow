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

#ifndef GANDIVA_LOCAL_BITMAPS_HOLDER_H
#define GANDIVA_LOCAL_BITMAPS_HOLDER_H

#include <memory>
#include <utility>
#include <vector>

#include <arrow/util/logging.h>
#include "gandiva/arrow.h"
#include "gandiva/gandiva_aliases.h"

namespace gandiva {

/// \brief The buffers corresponding to one batch of records, used for
/// expression evaluation.
class LocalBitMapsHolder {
 public:
  LocalBitMapsHolder(int64_t num_records, int num_local_bitmaps);

  int GetNumLocalBitMaps() const { return static_cast<int>(local_bitmaps_vec_.size()); }

  int64_t GetLocalBitMapSize() const { return local_bitmap_size_; }

  uint8_t** GetLocalBitMapArray() const { return local_bitmaps_array_.get(); }

  uint8_t* GetLocalBitMap(int idx) const {
    DCHECK(idx <= GetNumLocalBitMaps());
    return local_bitmaps_array_.get()[idx];
  }

 private:
  /// number of records in the current batch.
  int64_t num_records_;

  /// A container of 'local_bitmaps_', each sized to accomodate 'num_records'.
  std::vector<std::unique_ptr<uint8_t>> local_bitmaps_vec_;

  /// An array of the local bitmaps.
  std::unique_ptr<uint8_t*> local_bitmaps_array_;

  int64_t local_bitmap_size_;
};

inline LocalBitMapsHolder::LocalBitMapsHolder(int64_t num_records, int num_local_bitmaps)
    : num_records_(num_records) {
  // alloc an array for the pointers to the bitmaps.
  if (num_local_bitmaps > 0) {
    local_bitmaps_array_.reset(new uint8_t*[num_local_bitmaps]);
  }

  // 64-bit aligned bitmaps.
  int64_t roundUp64Multiple = (num_records_ + 63) >> 6;
  local_bitmap_size_ = roundUp64Multiple * 8;

  // Alloc 'num_local_bitmaps_' number of bitmaps, each of capacity 'num_records_'.
  for (int i = 0; i < num_local_bitmaps; ++i) {
    // TODO : round-up to a slab friendly multiple.
    std::unique_ptr<uint8_t> bitmap(new uint8_t[local_bitmap_size_]);

    // keep pointer to the bitmap in the array.
    (local_bitmaps_array_.get())[i] = bitmap.get();

    // pre-fill with 1s (assuming that the probability of is_valid is higher).
    memset(bitmap.get(), 0xff, local_bitmap_size_);
    local_bitmaps_vec_.push_back(std::move(bitmap));
  }
}

}  // namespace gandiva

#endif  // GANDIVA_LOCAL_BITMAPS_HOLDER_H
