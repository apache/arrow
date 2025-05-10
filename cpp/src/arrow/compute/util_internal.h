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

#include "arrow/compute/visibility.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {

template <typename T>
void CheckAlignment(const void* ptr) {
  ARROW_DCHECK(reinterpret_cast<uint64_t>(ptr) % sizeof(T) == 0);
}

/// Storage used to allocate temporary vectors of a batch size.
/// Temporary vectors should resemble allocating temporary variables on the stack
/// but in the context of vectorized processing where we need to store a vector of
/// temporaries instead of a single value.
class ARROW_COMPUTE_EXPORT TempVectorStack {
  template <typename>
  friend class TempVectorHolder;

 public:
  TempVectorStack() = default;
  ~TempVectorStack();

  ARROW_DISALLOW_COPY_AND_ASSIGN(TempVectorStack);

  ARROW_DEFAULT_MOVE_AND_ASSIGN(TempVectorStack);

  Status Init(MemoryPool* pool, int64_t size);

  int64_t AllocatedSize() const { return top_; }

 private:
  static int64_t EstimatedAllocationSize(int64_t size) {
    return PaddedAllocationSize(size) + /*two guards*/ 2 * sizeof(uint64_t);
  }

  static int64_t PaddedAllocationSize(int64_t num_bytes);

  void alloc(uint32_t num_bytes, uint8_t** data, int* id);
  void release(int id, uint32_t num_bytes);
  static constexpr uint64_t kGuard1 = 0x3141592653589793ULL;
  static constexpr uint64_t kGuard2 = 0x0577215664901532ULL;
  static constexpr int64_t kPadding = 64;
  int num_vectors_;
  int64_t top_;
  std::unique_ptr<Buffer> buffer_;
  int64_t buffer_size_;

  friend class TempVectorStackTest;
};

template <typename T>
class TempVectorHolder {
  friend class TempVectorStack;

 public:
  ~TempVectorHolder() { stack_->release(id_, num_elements_ * sizeof(T)); }
  T* mutable_data() { return reinterpret_cast<T*>(data_); }
  TempVectorHolder(TempVectorStack* stack, uint32_t num_elements) {
    stack_ = stack;
    num_elements_ = num_elements;
    stack_->alloc(num_elements * sizeof(T), &data_, &id_);
  }

 private:
  TempVectorStack* stack_;
  uint8_t* data_;
  int id_;
  uint32_t num_elements_;
};

}  // namespace util
}  // namespace arrow
