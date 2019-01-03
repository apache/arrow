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

#ifndef ARROW_BUFFER_BUILDER_H
#define ARROW_BUFFER_BUILDER_H

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

// ----------------------------------------------------------------------
// Buffer builder classes

/// \class BufferBuilder
/// \brief A class for incrementally building a contiguous chunk of in-memory data
class ARROW_EXPORT BufferBuilder {
 public:
  explicit BufferBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : pool_(pool), data_(NULLPTR), capacity_(0), size_(0) {}

  /// \brief Resize the buffer to the nearest multiple of 64 bytes
  ///
  /// \param elements the new capacity of the of the builder. Will be rounded
  /// up to a multiple of 64 bytes for padding
  /// \param shrink_to_fit if new capacity is smaller than the existing size,
  /// reallocate internal buffer. Set to false to avoid reallocations when
  /// shrinking the builder.
  /// \return Status
  Status Resize(const int64_t elements, bool shrink_to_fit = true) {
    // Resize(0) is a no-op
    if (elements == 0) {
      return Status::OK();
    }
    int64_t old_capacity = capacity_;

    if (buffer_ == NULLPTR) {
      ARROW_RETURN_NOT_OK(AllocateResizableBuffer(pool_, elements, &buffer_));
    } else {
      ARROW_RETURN_NOT_OK(buffer_->Resize(elements, shrink_to_fit));
    }
    capacity_ = buffer_->capacity();
    data_ = buffer_->mutable_data();
    if (capacity_ > old_capacity) {
      memset(data_ + old_capacity, 0, capacity_ - old_capacity);
    }
    return Status::OK();
  }

  /// \brief Ensure that builder can accommodate the additional number of bytes
  /// without the need to perform allocations
  ///
  /// \param size number of additional bytes to make space for
  /// \return Status
  Status Reserve(const int64_t size) { return Resize(size_ + size, false); }

  /// \brief Append the given data to the buffer
  ///
  /// The buffer is automatically expanded if necessary.
  Status Append(const void* data, int64_t length) {
    if (capacity_ < length + size_) {
      int64_t new_capacity = BitUtil::NextPower2(length + size_);
      ARROW_RETURN_NOT_OK(Resize(new_capacity));
    }
    UnsafeAppend(data, length);
    return Status::OK();
  }

  /// \brief Append the given data to the buffer
  ///
  /// The buffer is automatically expanded if necessary.
  template <size_t NBYTES>
  Status Append(const std::array<uint8_t, NBYTES>& data) {
    constexpr auto nbytes = static_cast<int64_t>(NBYTES);
    if (capacity_ < nbytes + size_) {
      int64_t new_capacity = BitUtil::NextPower2(nbytes + size_);
      ARROW_RETURN_NOT_OK(Resize(new_capacity));
    }

    if (nbytes > 0) {
      std::copy(data.cbegin(), data.cend(), data_ + size_);
      size_ += nbytes;
    }
    return Status::OK();
  }

  // Advance pointer and zero out memory
  Status Advance(const int64_t length) {
    if (capacity_ < length + size_) {
      int64_t new_capacity = BitUtil::NextPower2(length + size_);
      ARROW_RETURN_NOT_OK(Resize(new_capacity));
    }
    if (length > 0) {
      memset(data_ + size_, 0, static_cast<size_t>(length));
      size_ += length;
    }
    return Status::OK();
  }

  // Unsafe methods don't check existing size
  void UnsafeAppend(const void* data, int64_t length) {
    if (length > 0) {
      memcpy(data_ + size_, data, static_cast<size_t>(length));
      size_ += length;
    }
  }

  /// \brief Return result of builder as a Buffer object.
  ///
  /// The builder is reset and can be reused afterwards.
  ///
  /// \param[out] out the finalized Buffer object
  /// \param shrink_to_fit if the buffer size is smaller than its capacity,
  /// reallocate to fit more tightly in memory. Set to false to avoid
  /// a reallocation, at the expense of potentially more memory consumption.
  /// \return Status
  Status Finish(std::shared_ptr<Buffer>* out, bool shrink_to_fit = true) {
    ARROW_RETURN_NOT_OK(Resize(size_, shrink_to_fit));
    *out = buffer_;
    Reset();
    return Status::OK();
  }

  void Reset() {
    buffer_ = NULLPTR;
    capacity_ = size_ = 0;
  }

  int64_t capacity() const { return capacity_; }
  int64_t length() const { return size_; }
  const uint8_t* data() const { return data_; }

 protected:
  std::shared_ptr<ResizableBuffer> buffer_;
  MemoryPool* pool_;
  uint8_t* data_;
  int64_t capacity_;
  int64_t size_;
};

/// \brief A BufferBuilder subclass with convenience methods to append typed data
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
  int64_t capacity() const { return capacity_ / sizeof(T); }
};

}  // namespace arrow

#endif  // ARROW_BUFFER_BUILDER_H
