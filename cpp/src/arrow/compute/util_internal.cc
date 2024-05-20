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

#include "arrow/compute/util_internal.h"

#include "arrow/compute/util.h"
#include "arrow/memory_pool.h"

#ifdef ADDRESS_SANITIZER
#include <sanitizer/asan_interface.h>
#endif

namespace arrow {
namespace util {

TempVectorStack::~TempVectorStack() {
#ifdef ADDRESS_SANITIZER
  if (buffer_) {
    ASAN_UNPOISON_MEMORY_REGION(buffer_->mutable_data(), buffer_size_);
  }
#endif
}

Status TempVectorStack::Init(MemoryPool* pool, int64_t size) {
  num_vectors_ = 0;
  top_ = 0;
  buffer_size_ = EstimatedAllocationSize(size);
  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(buffer_size_, pool));
#ifdef ADDRESS_SANITIZER
  ASAN_POISON_MEMORY_REGION(buffer->mutable_data(), buffer_size_);
#endif
  buffer_ = std::move(buffer);
  return Status::OK();
}

int64_t TempVectorStack::PaddedAllocationSize(int64_t num_bytes) {
  // Round up allocation size to multiple of 8 bytes
  // to avoid returning temp vectors with unaligned address.
  //
  // Also add padding at the end to facilitate loads and stores
  // using SIMD when number of vector elements is not divisible
  // by the number of SIMD lanes.
  //
  return ::arrow::bit_util::RoundUp(num_bytes, sizeof(int64_t)) + kPadding;
}

void TempVectorStack::alloc(uint32_t num_bytes, uint8_t** data, int* id) {
  int64_t estimated_alloc_size = EstimatedAllocationSize(num_bytes);
  int64_t new_top = top_ + estimated_alloc_size;
  // Stack overflow check (see GH-39582).
  // XXX cannot return a regular Status because most consumers do not either.
  ARROW_CHECK_LE(new_top, buffer_size_)
      << "TempVectorStack::alloc overflow: allocating " << estimated_alloc_size
      << " on top of " << top_ << " in stack of size " << buffer_size_;
#ifdef ADDRESS_SANITIZER
  ASAN_UNPOISON_MEMORY_REGION(buffer_->mutable_data() + top_, estimated_alloc_size);
#endif
  *data = buffer_->mutable_data() + top_ + /*one guard*/ sizeof(uint64_t);
#ifndef NDEBUG
  // We set 8 bytes before the beginning of the allocated range and
  // 8 bytes after the end to check for stack overflow (which would
  // result in those known bytes being corrupted).
  reinterpret_cast<uint64_t*>(buffer_->mutable_data() + top_)[0] = kGuard1;
  reinterpret_cast<uint64_t*>(buffer_->mutable_data() + new_top)[-1] = kGuard2;
#endif
  *id = num_vectors_++;
  top_ = new_top;
}

void TempVectorStack::release(int id, uint32_t num_bytes) {
  ARROW_DCHECK(num_vectors_ == id + 1);
  int64_t size = EstimatedAllocationSize(num_bytes);
  ARROW_DCHECK(reinterpret_cast<const uint64_t*>(buffer_->mutable_data() + top_)[-1] ==
               kGuard2);
  ARROW_DCHECK(top_ >= size);
  top_ -= size;
  ARROW_DCHECK(reinterpret_cast<const uint64_t*>(buffer_->mutable_data() + top_)[0] ==
               kGuard1);
#ifdef ADDRESS_SANITIZER
  ASAN_POISON_MEMORY_REGION(buffer_->mutable_data() + top_, size);
#endif
  --num_vectors_;
}

}  // namespace util
}  // namespace arrow
