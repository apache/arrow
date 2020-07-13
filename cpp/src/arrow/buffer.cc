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

#include <algorithm>
#include <cstdint>
#include <utility>

#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/int_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"

namespace arrow {

Result<std::shared_ptr<Buffer>> Buffer::CopySlice(const int64_t start,
                                                  const int64_t nbytes,
                                                  MemoryPool* pool) const {
  // Sanity checks
  ARROW_CHECK_LE(start, size_);
  ARROW_CHECK_LE(nbytes, size_ - start);
  DCHECK_GE(nbytes, 0);

  ARROW_ASSIGN_OR_RAISE(auto new_buffer, AllocateResizableBuffer(nbytes, pool));
  std::memcpy(new_buffer->mutable_data(), data() + start, static_cast<size_t>(nbytes));
  return std::move(new_buffer);
}

namespace {

Status CheckBufferSlice(const Buffer& buffer, int64_t offset, int64_t length) {
  return internal::CheckSliceParams(buffer.size(), offset, length, "buffer");
}

Status CheckBufferSlice(const Buffer& buffer, int64_t offset) {
  if (ARROW_PREDICT_FALSE(offset < 0)) {
    // Avoid UBSAN in subtraction below
    return Status::Invalid("Negative buffer slice offset");
  }
  return CheckBufferSlice(buffer, offset, buffer.size() - offset);
}

}  // namespace

Result<std::shared_ptr<Buffer>> SliceBufferSafe(const std::shared_ptr<Buffer>& buffer,
                                                int64_t offset) {
  RETURN_NOT_OK(CheckBufferSlice(*buffer, offset));
  return SliceBuffer(buffer, offset);
}

Result<std::shared_ptr<Buffer>> SliceBufferSafe(const std::shared_ptr<Buffer>& buffer,
                                                int64_t offset, int64_t length) {
  RETURN_NOT_OK(CheckBufferSlice(*buffer, offset, length));
  return SliceBuffer(buffer, offset, length);
}

Result<std::shared_ptr<Buffer>> SliceMutableBufferSafe(
    const std::shared_ptr<Buffer>& buffer, int64_t offset) {
  RETURN_NOT_OK(CheckBufferSlice(*buffer, offset));
  return SliceMutableBuffer(buffer, offset);
}

Result<std::shared_ptr<Buffer>> SliceMutableBufferSafe(
    const std::shared_ptr<Buffer>& buffer, int64_t offset, int64_t length) {
  RETURN_NOT_OK(CheckBufferSlice(*buffer, offset, length));
  return SliceMutableBuffer(buffer, offset, length);
}

std::string Buffer::ToHexString() {
  return HexEncode(data(), static_cast<size_t>(size()));
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

std::string Buffer::ToString() const {
  return std::string(reinterpret_cast<const char*>(data_), static_cast<size_t>(size_));
}

void Buffer::CheckMutable() const { DCHECK(is_mutable()) << "buffer not mutable"; }

void Buffer::CheckCPU() const {
  DCHECK(is_cpu()) << "not a CPU buffer (device: " << device()->ToString() << ")";
}

Result<std::shared_ptr<io::RandomAccessFile>> Buffer::GetReader(
    std::shared_ptr<Buffer> buf) {
  return buf->memory_manager_->GetBufferReader(buf);
}

Result<std::shared_ptr<io::OutputStream>> Buffer::GetWriter(std::shared_ptr<Buffer> buf) {
  if (!buf->is_mutable()) {
    return Status::Invalid("Expected mutable buffer");
  }
  return buf->memory_manager_->GetBufferWriter(buf);
}

Result<std::shared_ptr<Buffer>> Buffer::Copy(std::shared_ptr<Buffer> source,
                                             const std::shared_ptr<MemoryManager>& to) {
  return MemoryManager::CopyBuffer(source, to);
}

Result<std::shared_ptr<Buffer>> Buffer::View(std::shared_ptr<Buffer> source,
                                             const std::shared_ptr<MemoryManager>& to) {
  return MemoryManager::ViewBuffer(source, to);
}

Result<std::shared_ptr<Buffer>> Buffer::ViewOrCopy(
    std::shared_ptr<Buffer> source, const std::shared_ptr<MemoryManager>& to) {
  auto maybe_buffer = MemoryManager::ViewBuffer(source, to);
  if (maybe_buffer.ok()) {
    return maybe_buffer;
  }
  return MemoryManager::CopyBuffer(source, to);
}

class StlStringBuffer : public Buffer {
 public:
  explicit StlStringBuffer(std::string data)
      : Buffer(nullptr, 0), input_(std::move(data)) {
    data_ = reinterpret_cast<const uint8_t*>(input_.c_str());
    size_ = static_cast<int64_t>(input_.size());
    capacity_ = size_;
  }

 private:
  std::string input_;
};

std::shared_ptr<Buffer> Buffer::FromString(std::string data) {
  return std::make_shared<StlStringBuffer>(std::move(data));
}

std::shared_ptr<Buffer> SliceMutableBuffer(const std::shared_ptr<Buffer>& buffer,
                                           const int64_t offset, const int64_t length) {
  return std::make_shared<MutableBuffer>(buffer, offset, length);
}

MutableBuffer::MutableBuffer(const std::shared_ptr<Buffer>& parent, const int64_t offset,
                             const int64_t size)
    : MutableBuffer(reinterpret_cast<uint8_t*>(parent->mutable_address()) + offset,
                    size) {
  DCHECK(parent->is_mutable()) << "Must pass mutable buffer";
  parent_ = parent;
}

// -----------------------------------------------------------------------
// Pool buffer and allocation

/// A Buffer whose lifetime is tied to a particular MemoryPool
class PoolBuffer : public ResizableBuffer {
 public:
  explicit PoolBuffer(std::shared_ptr<MemoryManager> mm, MemoryPool* pool)
      : ResizableBuffer(nullptr, 0, std::move(mm)), pool_(pool) {}

  ~PoolBuffer() override {
    if (mutable_data_ != nullptr) {
      pool_->Free(mutable_data_, capacity_);
    }
  }

  Status Reserve(const int64_t capacity) override {
    if (capacity < 0) {
      return Status::Invalid("Negative buffer capacity: ", capacity);
    }
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

  Status Resize(const int64_t new_size, bool shrink_to_fit = true) override {
    if (new_size < 0) {
      return Status::Invalid("Negative buffer resize: ", new_size);
    }
    if (mutable_data_ && shrink_to_fit && new_size <= size_) {
      // Buffer is non-null and is not growing, so shrink to the requested size without
      // excess space.
      int64_t new_capacity = BitUtil::RoundUpToMultipleOf64(new_size);
      if (capacity_ != new_capacity) {
        // Buffer hasn't got yet the requested size.
        RETURN_NOT_OK(pool_->Reallocate(capacity_, new_capacity, &mutable_data_));
        data_ = mutable_data_;
        capacity_ = new_capacity;
      }
    } else {
      RETURN_NOT_OK(Reserve(new_size));
    }
    size_ = new_size;

    return Status::OK();
  }

  static std::shared_ptr<PoolBuffer> MakeShared(MemoryPool* pool) {
    std::shared_ptr<MemoryManager> mm;
    if (pool == nullptr) {
      pool = default_memory_pool();
      mm = default_cpu_memory_manager();
    } else {
      mm = CPUDevice::memory_manager(pool);
    }
    return std::make_shared<PoolBuffer>(std::move(mm), pool);
  }

  static std::unique_ptr<PoolBuffer> MakeUnique(MemoryPool* pool) {
    std::shared_ptr<MemoryManager> mm;
    if (pool == nullptr) {
      pool = default_memory_pool();
      mm = default_cpu_memory_manager();
    } else {
      mm = CPUDevice::memory_manager(pool);
    }
    return std::unique_ptr<PoolBuffer>(new PoolBuffer(std::move(mm), pool));
  }

 private:
  MemoryPool* pool_;
};

namespace {
// A utility that does most of the work of the `AllocateBuffer` and
// `AllocateResizableBuffer` methods. The argument `buffer` should be a smart pointer to
// a PoolBuffer.
template <typename BufferPtr, typename PoolBufferPtr>
inline Result<BufferPtr> ResizePoolBuffer(PoolBufferPtr&& buffer, const int64_t size) {
  RETURN_NOT_OK(buffer->Resize(size));
  buffer->ZeroPadding();
  return std::move(buffer);
}

}  // namespace

Result<std::unique_ptr<Buffer>> AllocateBuffer(const int64_t size, MemoryPool* pool) {
  return ResizePoolBuffer<std::unique_ptr<Buffer>>(PoolBuffer::MakeUnique(pool), size);
}

Result<std::unique_ptr<ResizableBuffer>> AllocateResizableBuffer(const int64_t size,
                                                                 MemoryPool* pool) {
  return ResizePoolBuffer<std::unique_ptr<ResizableBuffer>>(PoolBuffer::MakeUnique(pool),
                                                            size);
}

Result<std::shared_ptr<Buffer>> AllocateBitmap(int64_t length, MemoryPool* pool) {
  return AllocateBuffer(BitUtil::BytesForBits(length), pool);
}

Result<std::shared_ptr<Buffer>> AllocateEmptyBitmap(int64_t length, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto buf, AllocateBitmap(length, pool));
  memset(buf->mutable_data(), 0, static_cast<size_t>(buf->size()));
  return buf;
}

Status AllocateEmptyBitmap(int64_t length, std::shared_ptr<Buffer>* out) {
  return AllocateEmptyBitmap(length).Value(out);
}

Result<std::shared_ptr<Buffer>> ConcatenateBuffers(
    const std::vector<std::shared_ptr<Buffer>>& buffers, MemoryPool* pool) {
  int64_t out_length = 0;
  for (const auto& buffer : buffers) {
    out_length += buffer->size();
  }
  ARROW_ASSIGN_OR_RAISE(auto out, AllocateBuffer(out_length, pool));
  auto out_data = out->mutable_data();
  for (const auto& buffer : buffers) {
    std::memcpy(out_data, buffer->data(), buffer->size());
    out_data += buffer->size();
  }
  return std::move(out);
}

}  // namespace arrow
