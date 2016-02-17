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

#ifndef ARROW_UTIL_BUFFER_H
#define ARROW_UTIL_BUFFER_H

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include "arrow/util/macros.h"

namespace arrow {

class Status;

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

// A MutableBuffer whose memory is owned by the class instance. For example,
// for reading data out of files that you want to deallocate when this class is
// garbage-collected
class OwnedMutableBuffer : public MutableBuffer {
 public:
  OwnedMutableBuffer();
  Status Resize(int64_t new_size);

 private:
  // TODO: aligned allocations
  std::vector<uint8_t> buffer_owner_;
};

} // namespace arrow

#endif // ARROW_UTIL_BUFFER_H
