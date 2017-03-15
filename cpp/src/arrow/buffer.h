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

#ifndef ARROW_BUFFER_H
#define ARROW_BUFFER_H

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>

#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;
class Status;

// ----------------------------------------------------------------------
// Buffer classes

/// Immutable API for a chunk of bytes which may or may not be owned by the
/// class instance.
///
/// Buffers have two related notions of length: size and capacity. Size is
/// the number of bytes that might have valid data. Capacity is the number
/// of bytes that where allocated for the buffer in total.
///
/// The following invariant is always true: Size < Capacity
class ARROW_EXPORT Buffer : public std::enable_shared_from_this<Buffer> {
 public:
  Buffer(const uint8_t* data, int64_t size)
      : is_mutable_(false), data_(data), size_(size), capacity_(size) {}
  virtual ~Buffer();

  /// An offset into data that is owned by another buffer, but we want to be
  /// able to retain a valid pointer to it even after other shared_ptr's to the
  /// parent buffer have been destroyed
  ///
  /// This method makes no assertions about alignment or padding of the buffer but
  /// in general we expected buffers to be aligned and padded to 64 bytes.  In the future
  /// we might add utility methods to help determine if a buffer satisfies this contract.
  Buffer(const std::shared_ptr<Buffer>& parent, int64_t offset, int64_t size);

  std::shared_ptr<Buffer> get_shared_ptr() { return shared_from_this(); }

  bool is_mutable() const { return is_mutable_; }

  /// Return true if both buffers are the same size and contain the same bytes
  /// up to the number of compared bytes
  bool Equals(const Buffer& other, int64_t nbytes) const;
  bool Equals(const Buffer& other) const;

  /// Copy a section of the buffer into a new Buffer.
  Status Copy(int64_t start, int64_t nbytes, MemoryPool* pool,
      std::shared_ptr<Buffer>* out) const;

  /// Copy a section of the buffer using the default memory pool into a new Buffer.
  Status Copy(int64_t start, int64_t nbytes, std::shared_ptr<Buffer>* out) const;

  int64_t capacity() const { return capacity_; }
  const uint8_t* data() const { return data_; }

  int64_t size() const { return size_; }

  std::shared_ptr<Buffer> parent() const { return parent_; }

 protected:
  bool is_mutable_;
  const uint8_t* data_;
  int64_t size_;
  int64_t capacity_;

  // nullptr by default, but may be set
  std::shared_ptr<Buffer> parent_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Buffer);
};

/// Construct a view on passed buffer at the indicated offset and length. This
/// function cannot fail and does not error checking (except in debug builds)
std::shared_ptr<Buffer> ARROW_EXPORT SliceBuffer(
    const std::shared_ptr<Buffer>& buffer, int64_t offset, int64_t length);

/// A Buffer whose contents can be mutated. May or may not own its data.
class ARROW_EXPORT MutableBuffer : public Buffer {
 public:
  MutableBuffer(uint8_t* data, int64_t size) : Buffer(data, size) {
    is_mutable_ = true;
    mutable_data_ = data;
  }

  uint8_t* mutable_data() { return mutable_data_; }

  /// Get a read-only view of this buffer
  std::shared_ptr<Buffer> GetImmutableView();

 protected:
  MutableBuffer() : Buffer(nullptr, 0), mutable_data_(nullptr) {}

  uint8_t* mutable_data_;
};

class ARROW_EXPORT ResizableBuffer : public MutableBuffer {
 public:
  /// Change buffer reported size to indicated size, allocating memory if
  /// necessary.  This will ensure that the capacity of the buffer is a multiple
  /// of 64 bytes as defined in Layout.md.
  ///
  /// @param shrink_to_fit On deactivating this option, the capacity of the Buffer won't
  /// decrease.
  virtual Status Resize(int64_t new_size, bool shrink_to_fit = true) = 0;

  /// Ensure that buffer has enough memory allocated to fit the indicated
  /// capacity (and meets the 64 byte padding requirement in Layout.md).
  /// It does not change buffer's reported size.
  virtual Status Reserve(int64_t new_capacity) = 0;

 protected:
  ResizableBuffer(uint8_t* data, int64_t size) : MutableBuffer(data, size) {}
};

/// A Buffer whose lifetime is tied to a particular MemoryPool
class ARROW_EXPORT PoolBuffer : public ResizableBuffer {
 public:
  explicit PoolBuffer(MemoryPool* pool = nullptr);
  virtual ~PoolBuffer();

  Status Resize(int64_t new_size, bool shrink_to_fit = true) override;
  Status Reserve(int64_t new_capacity) override;

 private:
  MemoryPool* pool_;
};

class ARROW_EXPORT BufferBuilder {
 public:
  explicit BufferBuilder(MemoryPool* pool)
      : pool_(pool), data_(nullptr), capacity_(0), size_(0) {}

  /// Resizes the buffer to the nearest multiple of 64 bytes per Layout.md
  Status Resize(int64_t elements) {
    // Resize(0) is a no-op
    if (elements == 0) { return Status::OK(); }
    if (capacity_ == 0) { buffer_ = std::make_shared<PoolBuffer>(pool_); }
    RETURN_NOT_OK(buffer_->Resize(elements));
    capacity_ = buffer_->capacity();
    data_ = buffer_->mutable_data();
    return Status::OK();
  }

  Status Append(const uint8_t* data, int64_t length) {
    if (capacity_ < length + size_) { RETURN_NOT_OK(Resize(length + size_)); }
    UnsafeAppend(data, length);
    return Status::OK();
  }

  // Advance pointer and zero out memory
  Status Advance(int64_t length) {
    if (capacity_ < length + size_) { RETURN_NOT_OK(Resize(length + size_)); }
    memset(data_ + size_, 0, static_cast<size_t>(length));
    size_ += length;
    return Status::OK();
  }

  template <typename T>
  Status Append(T arithmetic_value) {
    static_assert(std::is_arithmetic<T>::value,
        "Convenience buffer append only supports arithmetic types");
    return Append(reinterpret_cast<uint8_t*>(&arithmetic_value), sizeof(T));
  }

  template <typename T>
  Status Append(const T* arithmetic_values, int64_t num_elements) {
    static_assert(std::is_arithmetic<T>::value,
        "Convenience buffer append only supports arithmetic types");
    return Append(
        reinterpret_cast<const uint8_t*>(arithmetic_values), num_elements * sizeof(T));
  }

  // Unsafe methods don't check existing size
  void UnsafeAppend(const uint8_t* data, int64_t length) {
    memcpy(data_ + size_, data, static_cast<size_t>(length));
    size_ += length;
  }

  template <typename T>
  void UnsafeAppend(T arithmetic_value) {
    static_assert(std::is_arithmetic<T>::value,
        "Convenience buffer append only supports arithmetic types");
    UnsafeAppend(reinterpret_cast<uint8_t*>(&arithmetic_value), sizeof(T));
  }

  template <typename T>
  void UnsafeAppend(const T* arithmetic_values, int64_t num_elements) {
    static_assert(std::is_arithmetic<T>::value,
        "Convenience buffer append only supports arithmetic types");
    UnsafeAppend(
        reinterpret_cast<const uint8_t*>(arithmetic_values), num_elements * sizeof(T));
  }

  std::shared_ptr<Buffer> Finish() {
    auto result = buffer_;
    buffer_ = nullptr;
    capacity_ = size_ = 0;
    return result;
  }
  int64_t capacity() { return capacity_; }
  int64_t length() { return size_; }

 private:
  std::shared_ptr<PoolBuffer> buffer_;
  MemoryPool* pool_;
  uint8_t* data_;
  int64_t capacity_;
  int64_t size_;
};

/// Allocate a new mutable buffer from a memory pool
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer with padding
///
/// \return Status message
Status ARROW_EXPORT AllocateBuffer(
    MemoryPool* pool, int64_t size, std::shared_ptr<MutableBuffer>* out);

Status ARROW_EXPORT AllocateResizableBuffer(
    MemoryPool* pool, int64_t size, std::shared_ptr<ResizableBuffer>* out);

}  // namespace arrow

#endif  // ARROW_BUFFER_H
