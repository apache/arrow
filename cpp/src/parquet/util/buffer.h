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

#ifndef PARQUET_UTIL_BUFFER_H
#define PARQUET_UTIL_BUFFER_H

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include "parquet/util/macros.h"
#include "parquet/util/mem-allocator.h"

namespace parquet_cpp {

// ----------------------------------------------------------------------
// Buffer classes

// Immutable API for a chunk of bytes which may or may not be owned by the
// class instance
class Buffer : public std::enable_shared_from_this<Buffer> {
 public:
  Buffer(const uint8_t* data, int64_t size) :
      data_(data),
      size_(size) {}

  // An offset into data that is owned by another buffer, but we want to be
  // able to retain a valid pointer to it even after other shared_ptr's to the
  // parent buffer have been destroyed
  Buffer(const std::shared_ptr<Buffer>& parent, int64_t offset, int64_t size);

  std::shared_ptr<Buffer> get_shared_ptr() {
    return shared_from_this();
  }

  // Return true if both buffers are the same size and contain the same bytes
  // up to the number of compared bytes
  bool Equals(const Buffer& other, int64_t nbytes) const {
    return this == &other ||
      (size_ >= nbytes && other.size_ >= nbytes &&
          !memcmp(data_, other.data_, nbytes));
  }

  bool Equals(const Buffer& other) const {
    return this == &other ||
      (size_ == other.size_ && !memcmp(data_, other.data_, size_));
  }

  const uint8_t* data() const {
    return data_;
  }

  int64_t size() const {
    return size_;
  }

  // Returns true if this Buffer is referencing memory (possibly) owned by some
  // other buffer
  bool is_shared() const {
    return static_cast<bool>(parent_);
  }

  const std::shared_ptr<Buffer> parent() const {
    return parent_;
  }

 protected:
  const uint8_t* data_;
  int64_t size_;

  // nullptr by default, but may be set
  std::shared_ptr<Buffer> parent_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Buffer);
};

// A Buffer whose contents can be mutated. May or may not own its data.
class MutableBuffer : public Buffer {
 public:
  MutableBuffer(uint8_t* data, int64_t size) :
      Buffer(data, size) {
    mutable_data_ = data;
  }

  uint8_t* mutable_data() {
    return mutable_data_;
  }

  // Get a read-only view of this buffer
  std::shared_ptr<Buffer> GetImmutableView();

 protected:
  MutableBuffer() :
      Buffer(nullptr, 0),
      mutable_data_(nullptr) {}

  uint8_t* mutable_data_;
};

class ResizableBuffer : public MutableBuffer {
 public:
  virtual void Resize(int64_t new_size) = 0;

 protected:
  ResizableBuffer(uint8_t* data, int64_t size) :
      MutableBuffer(data, size), capacity_(size) {}
  int64_t capacity_;
};

// A ResizableBuffer whose memory is owned by the class instance. For example,
// for reading data out of files that you want to deallocate when this class is
// garbage-collected
class OwnedMutableBuffer : public ResizableBuffer {
 public:
  explicit OwnedMutableBuffer(int64_t size = 0,
      MemoryAllocator* allocator = default_allocator());
  virtual ~OwnedMutableBuffer();
  void Resize(int64_t new_size) override;
  void Reserve(int64_t new_capacity);
  uint8_t& operator[](int64_t i);

 private:
  // TODO: aligned allocations
  MemoryAllocator* allocator_;

  DISALLOW_COPY_AND_ASSIGN(OwnedMutableBuffer);
};

template <class T>
class Vector {
 public:
  explicit Vector(int64_t size, MemoryAllocator* allocator);
  void Resize(int64_t new_size);
  void Reserve(int64_t new_capacity);
  void Assign(int64_t size, const T val);
  void Swap(Vector<T>& v);
  inline T& operator[](int64_t i) {
    return data_[i];
  }

 private:
  std::unique_ptr<OwnedMutableBuffer> buffer_;
  int64_t size_;
  int64_t capacity_;
  T* data_;

  DISALLOW_COPY_AND_ASSIGN(Vector);
};

} // namespace parquet_cpp

#endif // PARQUET_UTIL_BUFFER_H
