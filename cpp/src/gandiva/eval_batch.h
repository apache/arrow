/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef GANDIVA_EXPR_EVALBATCH_H
#define GANDIVA_EXPR_EVALBATCH_H

#include "gandiva/gandiva_aliases.h"

namespace gandiva {

/// \brief The buffers corresponding to one batch of records, used for
/// expression evaluation.
class EvalBatch {
 public:
  explicit EvalBatch(int num_records,
                     int num_buffers,
                     int num_local_bitmaps)
    : num_records_(num_records),
      num_buffers_(num_buffers),
      num_local_bitmaps_(num_local_bitmaps) {

    buffers_ = new uint8_t *[num_buffers];
    AllocLocalBitMaps();
  }

  ~EvalBatch() {
    FreeLocalBitMaps();
    delete [] buffers_;
  }

  int num_records() const { return num_records_; }

  uint8_t **buffers() const { return buffers_; }

  int num_buffers() const { return num_buffers_; }

  uint8_t *GetBuffer(int idx) const {
    DCHECK(idx <= num_buffers_);
    return buffers_[idx];
  }

  void SetBuffer(int idx, uint8_t *buffer) {
    DCHECK(idx <= num_buffers_);
    buffers_[idx] = buffer;
  }

  uint8_t **local_bitmaps() const { return local_bitmaps_; }

  int num_local_bitmaps() const { return num_local_bitmaps_; }

  uint8_t *GetLocalBitMap(int idx) const {
    DCHECK(idx <= num_local_bitmaps_);
    return local_bitmaps_[idx];
  }

 private:
  /// Alloc 'num_local_bitmaps_' number of bitmaps, each of capacity 'num_records_'.
  void AllocLocalBitMaps();

  /// Free up local bitmaps, if any.
  void FreeLocalBitMaps();

  /// number of records in the current batch.
  int num_records_;

  /// An array of 'num_buffers_', each containing a buffer. The buffer
  /// sizes depends on the data type, but all of them have the same
  /// number of slots (equal to num_records_).
  uint8_t **buffers_;
  int num_buffers_;

  /// An array of 'local_bitmaps_', each sized to accomodate 'num_records'.
  uint8_t **local_bitmaps_;
  int num_local_bitmaps_;
};

inline void EvalBatch::AllocLocalBitMaps() {
  if (num_local_bitmaps_ == 0) {
    local_bitmaps_ = nullptr;
    return;
  }

  // 64-bit aligned bitmaps.
  int bitmap_sz = arrow::BitUtil::RoundUpNumi64(num_records_) * 8;

  local_bitmaps_ = new uint8_t *[num_local_bitmaps_];
  for (int i = 0; i < num_local_bitmaps_; ++i) {
    // TODO : round-up to a slab friendly multiple.
    local_bitmaps_[i] = new uint8_t[bitmap_sz];

    // pre-fill with 1s (assuming that the probability of is_valid is higher).
    memset(local_bitmaps_[i], 0xff, bitmap_sz);
  }
}

inline void EvalBatch::FreeLocalBitMaps() {
  for (int i = 0; i < num_local_bitmaps_; ++i) {
    delete [] local_bitmaps_[i];
  }
  delete [] local_bitmaps_;
}

} // namespace gandiva

#endif //GANDIVA_EXPR_EVALBATCH_H
