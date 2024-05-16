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

#include <sanitizer/asan_interface.h>

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
  buffer_size_ = PaddedAllocationSize(size);
  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(size, pool));
#ifdef ADDRESS_SANITIZER
  ASAN_POISON_MEMORY_REGION(buffer->mutable_data(), size);
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
  int64_t alloc_size = PaddedAllocationSize(num_bytes);
  int64_t new_top = top_ + alloc_size;
  // Stack overflow check (see GH-39582).
  // XXX cannot return a regular Status because most consumers do not either.
  ARROW_CHECK_LE(new_top, buffer_size_)
      << "TempVectorStack::alloc overflow: allocating " << alloc_size << " on top of "
      << top_ << " in stack of size " << buffer_size_;
  *data = buffer_->mutable_data() + top_;
#ifdef ADDRESS_SANITIZER
  ASAN_UNPOISON_MEMORY_REGION(*data, alloc_size);
#endif
  *id = num_vectors_++;
  top_ = new_top;
}

void TempVectorStack::release(int id, uint32_t num_bytes) {
  ARROW_DCHECK(num_vectors_ == id + 1);
  int64_t size = PaddedAllocationSize(num_bytes);
  ARROW_DCHECK(top_ >= size);
  top_ -= size;
#ifdef ADDRESS_SANITIZER
  ASAN_POISON_MEMORY_REGION(buffer_->mutable_data() + top_, size);
#endif
  --num_vectors_;
}

}  // namespace util
}  // namespace arrow
