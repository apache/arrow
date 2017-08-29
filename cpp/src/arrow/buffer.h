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
#include <string>

#include "arrow/status.h"
#include "arrow/util/bit-util.h"
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
class ARROW_EXPORT Buffer {
 public:
  Buffer(const uint8_t* data, int64_t size)
      : is_mutable_(false), data_(data), size_(size), capacity_(size) {}

  virtual ~Buffer() = default;

  /// An offset into data that is owned by another buffer, but we want to be
  /// able to retain a valid pointer to it even after other shared_ptr's to the
  /// parent buffer have been destroyed
  ///
  /// This method makes no assertions about alignment or padding of the buffer but
  /// in general we expected buffers to be aligned and padded to 64 bytes.  In the future
  /// we might add utility methods to help determine if a buffer satisfies this contract.
  Buffer(const std::shared_ptr<Buffer>& parent, const int64_t offset, const int64_t size)
      : Buffer(parent->data() + offset, size) {
    parent_ = parent;
  }

  bool is_mutable() const { return is_mutable_; }

  /// Return true if both buffers are the same size and contain the same bytes
  /// up to the number of compared bytes
  bool Equals(const Buffer& other, int64_t nbytes) const;
  bool Equals(const Buffer& other) const;

  /// Copy a section of the buffer into a new Buffer.
  Status Copy(const int64_t start, const int64_t nbytes, MemoryPool* pool,
              std::shared_ptr<Buffer>* out) const;

  /// Copy a section of the buffer using the default memory pool into a new Buffer.
  Status Copy(const int64_t start, const int64_t nbytes,
              std::shared_ptr<Buffer>* out) const;

  int64_t capacity() const { return capacity_; }
  const uint8_t* data() const { return data_; }
  uint8_t* mutable_data() { return mutable_data_; }

  int64_t size() const { return size_; }

  std::shared_ptr<Buffer> parent() const { return parent_; }

 protected:
  bool is_mutable_;
  const uint8_t* data_;
  uint8_t* mutable_data_;
  int64_t size_;
  int64_t capacity_;

  // nullptr by default, but may be set
  std::shared_ptr<Buffer> parent_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Buffer);
};

/// \brief Create Buffer referencing std::string memory
///
/// Warning: string instance must stay alive
///
/// \param str std::string instance
/// \return std::shared_ptr<Buffer>
static inline std::shared_ptr<Buffer> GetBufferFromString(const std::string& str) {
  return std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(str.c_str()),
                                  static_cast<int64_t>(str.size()));
}

/// Construct a view on passed buffer at the indicated offset and length. This
/// function cannot fail and does not error checking (except in debug builds)
static inline std::shared_ptr<Buffer> SliceBuffer(const std::shared_ptr<Buffer>& buffer,
                                                  const int64_t offset,
                                                  const int64_t length) {
  return std::make_shared<Buffer>(buffer, offset, length);
}

/// Construct a mutable buffer slice. If the parent buffer is not mutable, this
/// will abort in debug builds
ARROW_EXPORT
std::shared_ptr<Buffer> SliceMutableBuffer(const std::shared_ptr<Buffer>& buffer,
                                           const int64_t offset, const int64_t length);

/// A Buffer whose contents can be mutated. May or may not own its data.
class ARROW_EXPORT MutableBuffer : public Buffer {
 public:
  MutableBuffer(uint8_t* data, const int64_t size) : Buffer(data, size) {
    mutable_data_ = data;
    is_mutable_ = true;
  }

  MutableBuffer(const std::shared_ptr<Buffer>& parent, const int64_t offset,
                const int64_t size);

 protected:
  MutableBuffer() : Buffer(nullptr, 0) {}
};

class ARROW_EXPORT ResizableBuffer : public MutableBuffer {
 public:
  /// Change buffer reported size to indicated size, allocating memory if
  /// necessary.  This will ensure that the capacity of the buffer is a multiple
  /// of 64 bytes as defined in Layout.md.
  ///
  /// @param shrink_to_fit On deactivating this option, the capacity of the Buffer won't
  /// decrease.
  virtual Status Resize(const int64_t new_size, bool shrink_to_fit = true) = 0;

  /// Ensure that buffer has enough memory allocated to fit the indicated
  /// capacity (and meets the 64 byte padding requirement in Layout.md).
  /// It does not change buffer's reported size.
  virtual Status Reserve(const int64_t new_capacity) = 0;

  template <class T>
  Status TypedResize(const int64_t new_nb_elements, bool shrink_to_fit = true) {
    return Resize(sizeof(T) * new_nb_elements, shrink_to_fit);
  }

  template <class T>
  Status TypedReserve(const int64_t new_nb_elements) {
    return Reserve(sizeof(T) * new_nb_elements);
  }

 protected:
  ResizableBuffer(uint8_t* data, int64_t size) : MutableBuffer(data, size) {}
};

/// A Buffer whose lifetime is tied to a particular MemoryPool
class ARROW_EXPORT PoolBuffer : public ResizableBuffer {
 public:
  explicit PoolBuffer(MemoryPool* pool = nullptr);
  virtual ~PoolBuffer();

  Status Resize(const int64_t new_size, bool shrink_to_fit = true) override;
  Status Reserve(const int64_t new_capacity) override;

 private:
  MemoryPool* pool_;
};

class ARROW_EXPORT BufferBuilder {
 public:
  explicit BufferBuilder(MemoryPool* pool)
      : pool_(pool), data_(nullptr), capacity_(0), size_(0) {}

  /// Resizes the buffer to the nearest multiple of 64 bytes per Layout.md
  Status Resize(const int64_t elements) {
    // Resize(0) is a no-op
    if (elements == 0) {
      return Status::OK();
    }
    if (capacity_ == 0) {
      buffer_ = std::make_shared<PoolBuffer>(pool_);
    }
    int64_t old_capacity = capacity_;
    RETURN_NOT_OK(buffer_->Resize(elements));
    capacity_ = buffer_->capacity();
    data_ = buffer_->mutable_data();
    if (capacity_ > old_capacity) {
      memset(data_ + old_capacity, 0, capacity_ - old_capacity);
    }
    return Status::OK();
  }

  Status Append(const uint8_t* data, int64_t length) {
    if (capacity_ < length + size_) {
      int64_t new_capacity = BitUtil::NextPower2(length + size_);
      RETURN_NOT_OK(Resize(new_capacity));
    }
    UnsafeAppend(data, length);
    return Status::OK();
  }

  // Advance pointer and zero out memory
  Status Advance(const int64_t length) {
    if (capacity_ < length + size_) {
      int64_t new_capacity = BitUtil::NextPower2(length + size_);
      RETURN_NOT_OK(Resize(new_capacity));
    }
    memset(data_ + size_, 0, static_cast<size_t>(length));
    size_ += length;
    return Status::OK();
  }

  // Unsafe methods don't check existing size
  void UnsafeAppend(const uint8_t* data, int64_t length) {
    memcpy(data_ + size_, data, static_cast<size_t>(length));
    size_ += length;
  }

  Status Finish(std::shared_ptr<Buffer>* out) {
    // Do not shrink to fit to avoid unneeded realloc
    if (size_ > 0) {
      RETURN_NOT_OK(buffer_->Resize(size_, false));
    }
    *out = buffer_;
    Reset();
    return Status::OK();
  }

  void Reset() {
    buffer_ = nullptr;
    capacity_ = size_ = 0;
  }

  int64_t capacity() const { return capacity_; }
  int64_t length() const { return size_; }
  const uint8_t* data() const { return data_; }

 protected:
  std::shared_ptr<PoolBuffer> buffer_;
  MemoryPool* pool_;
  uint8_t* data_;
  int64_t capacity_;
  int64_t size_;
};

template <typename T>
class ARROW_EXPORT TypedBufferBuilder : public BufferBuilder {
 public:
  explicit TypedBufferBuilder(MemoryPool* pool) : BufferBuilder(pool) {}

  Status Append(T arithmetic_value) {
    static_assert(std::is_arithmetic<T>::value,
                  "Convenience buffer append only supports arithmetic types");
    return BufferBuilder::Append(reinterpret_cast<uint8_t*>(&arithmetic_value),
                                 sizeof(T));
  }

  Status Append(const T* arithmetic_values, int64_t num_elements) {
    static_assert(std::is_arithmetic<T>::value,
                  "Convenience buffer append only supports arithmetic types");
    return BufferBuilder::Append(reinterpret_cast<const uint8_t*>(arithmetic_values),
                                 num_elements * sizeof(T));
  }

  void UnsafeAppend(T arithmetic_value) {
    static_assert(std::is_arithmetic<T>::value,
                  "Convenience buffer append only supports arithmetic types");
    BufferBuilder::UnsafeAppend(reinterpret_cast<uint8_t*>(&arithmetic_value), sizeof(T));
  }

  void UnsafeAppend(const T* arithmetic_values, int64_t num_elements) {
    static_assert(std::is_arithmetic<T>::value,
                  "Convenience buffer append only supports arithmetic types");
    BufferBuilder::UnsafeAppend(reinterpret_cast<const uint8_t*>(arithmetic_values),
                                num_elements * sizeof(T));
  }

  const T* data() const { return reinterpret_cast<const T*>(data_); }
  int64_t length() const { return size_ / sizeof(T); }
};

/// \brief Allocate a fixed size mutable buffer from a memory pool
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(MemoryPool* pool, const int64_t size, std::shared_ptr<Buffer>* out);

/// Allocate resizeable buffer from a memory pool
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(MemoryPool* pool, const int64_t size,
                               std::shared_ptr<ResizableBuffer>* out);

#ifndef ARROW_NO_DEPRECATED_API
/// \deprecated Since 0.7.0
ARROW_EXPORT
Status AllocateBuffer(MemoryPool* pool, const int64_t size,
                      std::shared_ptr<MutableBuffer>* out);
#endif

}  // namespace arrow

#endif  // ARROW_BUFFER_H
