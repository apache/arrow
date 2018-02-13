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

#include "arrow/buffer.h"

#include <cstdint>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"

namespace arrow {

Status Buffer::Copy(const int64_t start, const int64_t nbytes, MemoryPool* pool,
                    std::shared_ptr<Buffer>* out) const {
  // Sanity checks
  DCHECK_LT(start, size_);
  DCHECK_LE(nbytes, size_ - start);

  auto new_buffer = std::make_shared<PoolBuffer>(pool);
  RETURN_NOT_OK(new_buffer->Resize(nbytes));

  std::memcpy(new_buffer->mutable_data(), data() + start, static_cast<size_t>(nbytes));

  *out = new_buffer;
  return Status::OK();
}

Status Buffer::Copy(const int64_t start, const int64_t nbytes,
                    std::shared_ptr<Buffer>* out) const {
  return Copy(start, nbytes, default_memory_pool(), out);
}

bool Buffer::Equals(const Buffer& other, const int64_t nbytes) const {
  return this == &other || (size_ >= nbytes && other.size_ >= nbytes &&
                            (data_ == other.data_ ||
                             !memcmp(data_, other.data_, static_cast<size_t>(nbytes))));
}

bool Buffer::Equals(const Buffer& other) const {
  return this == &other || (size_ == other.size_ &&
                            (data_ == other.data_ ||
                             !memcmp(data_, other.data_, static_cast<size_t>(size_))));
}

Status Buffer::FromString(const std::string& data, MemoryPool* pool,
                          std::shared_ptr<Buffer>* out) {
  auto size = static_cast<int64_t>(data.size());
  RETURN_NOT_OK(AllocateBuffer(pool, size, out));
  std::copy(data.c_str(), data.c_str() + size, (*out)->mutable_data());
  return Status::OK();
}

Status Buffer::FromString(const std::string& data, std::shared_ptr<Buffer>* out) {
  return FromString(data, default_memory_pool(), out);
}

PoolBuffer::PoolBuffer(MemoryPool* pool) : ResizableBuffer(nullptr, 0) {
  if (pool == nullptr) {
    pool = default_memory_pool();
  }
  pool_ = pool;
}

PoolBuffer::~PoolBuffer() {
  if (mutable_data_ != nullptr) {
    pool_->Free(mutable_data_, capacity_);
  }
}

Status PoolBuffer::Reserve(const int64_t capacity) {
  if (!mutable_data_ || capacity > capacity_) {
    uint8_t* new_data;
    int64_t new_capacity = BitUtil::RoundUpToMultipleOf64(capacity);
    if (mutable_data_) {
      RETURN_NOT_OK(pool_->Reallocate(capacity_, new_capacity, &mutable_data_));
    } else {
      RETURN_NOT_OK(pool_->Allocate(new_capacity, &new_data));
      mutable_data_ = new_data;
    }
    data_ = mutable_data_;
    capacity_ = new_capacity;
  }
  return Status::OK();
}

Status PoolBuffer::Resize(const int64_t new_size, bool shrink_to_fit) {
  if (!shrink_to_fit || (new_size > size_)) {
    RETURN_NOT_OK(Reserve(new_size));
  } else {
    // Buffer is not growing, so shrink to the requested size without
    // excess space.
    int64_t new_capacity = BitUtil::RoundUpToMultipleOf64(new_size);
    if (capacity_ != new_capacity) {
      // Buffer hasn't got yet the requested size.
      if (new_size == 0) {
        pool_->Free(mutable_data_, capacity_);
        capacity_ = 0;
        mutable_data_ = nullptr;
        data_ = nullptr;
      } else {
        RETURN_NOT_OK(pool_->Reallocate(capacity_, new_capacity, &mutable_data_));
        data_ = mutable_data_;
        capacity_ = new_capacity;
      }
    }
  }
  size_ = new_size;
  return Status::OK();
}

std::shared_ptr<Buffer> SliceMutableBuffer(const std::shared_ptr<Buffer>& buffer,
                                           const int64_t offset, const int64_t length) {
  return std::make_shared<MutableBuffer>(buffer, offset, length);
}

MutableBuffer::MutableBuffer(const std::shared_ptr<Buffer>& parent, const int64_t offset,
                             const int64_t size)
    : MutableBuffer(parent->mutable_data() + offset, size) {
  DCHECK(parent->is_mutable()) << "Must pass mutable buffer";
  parent_ = parent;
}

Status AllocateBuffer(MemoryPool* pool, const int64_t size,
                      std::shared_ptr<Buffer>* out) {
  auto buffer = std::make_shared<PoolBuffer>(pool);
  RETURN_NOT_OK(buffer->Resize(size));
  *out = buffer;
  return Status::OK();
}

Status AllocateResizableBuffer(MemoryPool* pool, const int64_t size,
                               std::shared_ptr<ResizableBuffer>* out) {
  auto buffer = std::make_shared<PoolBuffer>(pool);
  RETURN_NOT_OK(buffer->Resize(size));
  *out = buffer;
  return Status::OK();
}

}  // namespace arrow
