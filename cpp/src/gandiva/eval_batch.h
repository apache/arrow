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

#include <memory>

#include "arrow/util/logging.h"

#include "gandiva/arrow.h"
#include "gandiva/execution_context.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/local_bitmaps_holder.h"

namespace gandiva {

/// \brief The buffers corresponding to one batch of records, used for
/// expression evaluation.
class EvalBatch {
 public:
  explicit EvalBatch(int64_t num_records, int num_buffers, int num_local_bitmaps)
      : num_records_(num_records), num_buffers_(num_buffers) {
    if (num_buffers > 0) {
      buffers_array_.reset(new uint8_t*[num_buffers]);
      buffer_offsets_array_.reset(new int64_t[num_buffers]);
    }
    local_bitmaps_holder_.reset(new LocalBitMapsHolder(num_records, num_local_bitmaps));
    execution_context_.reset(new ExecutionContext());
  }

  int64_t num_records() const { return num_records_; }

  const uint8_t* const* GetBufferArray() const { return buffers_array_.get(); }
  uint8_t** GetBufferArray() { return buffers_array_.get(); }

  const int64_t* GetBufferOffsetArray() const { return buffer_offsets_array_.get(); }
  int64_t* GetBufferOffsetArray() { return buffer_offsets_array_.get(); }

  int GetNumBuffers() const { return num_buffers_; }

  const uint8_t* GetBuffer(int idx) const {
    DCHECK(idx <= num_buffers_);
    return (buffers_array_.get())[idx];
  }

  uint8_t* GetBuffer(int idx) {
    DCHECK(idx <= num_buffers_);
    return (buffers_array_.get())[idx];
  }

  int64_t GetBufferOffset(int idx) const {
    DCHECK(idx <= num_buffers_);
    return (buffer_offsets_array_.get())[idx];
  }

  void SetBuffer(int idx, uint8_t* buffer, int64_t offset) {
    DCHECK(idx <= num_buffers_);
    (buffers_array_.get())[idx] = buffer;
    (buffer_offsets_array_.get())[idx] = offset;
  }

  int GetNumLocalBitMaps() const { return local_bitmaps_holder_->GetNumLocalBitMaps(); }

  int64_t GetLocalBitmapSize() const {
    return local_bitmaps_holder_->GetLocalBitMapSize();
  }

  const uint8_t* GetLocalBitMap(int idx) const {
    DCHECK(idx <= GetNumLocalBitMaps());
    return local_bitmaps_holder_->GetLocalBitMap(idx);
  }
  uint8_t* GetLocalBitMap(int idx) {
    DCHECK(idx <= GetNumLocalBitMaps());
    return local_bitmaps_holder_->GetLocalBitMap(idx);
  }

  const uint8_t* const* GetLocalBitMapArray() const {
    return local_bitmaps_holder_->GetLocalBitMapArray();
  }
  uint8_t** GetLocalBitMapArray() { return local_bitmaps_holder_->GetLocalBitMapArray(); }

  const ExecutionContext* GetExecutionContext() const { return execution_context_.get(); }

 private:
  /// number of records in the current batch.
  int64_t num_records_;

  // number of buffers.
  int num_buffers_;

  /// An array of 'num_buffers_', each containing a buffer. The buffer
  /// sizes depends on the data type, but all of them have the same
  /// number of slots (equal to num_records_).
  std::unique_ptr<uint8_t*[]> buffers_array_;

  /// An array of 'num_buffers_', each containing the offset for
  /// corresponding buffer.
  std::unique_ptr<int64_t[]> buffer_offsets_array_;

  std::unique_ptr<LocalBitMapsHolder> local_bitmaps_holder_;

  std::unique_ptr<ExecutionContext> execution_context_;
};

}  // namespace gandiva
