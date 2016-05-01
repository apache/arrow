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

// Initially imported from Impala on 2016-02-23

#ifndef PARQUET_UTIL_BUFFER_BUILDER_H
#define PARQUET_UTIL_BUFFER_BUILDER_H

#include <stdlib.h>
#include <cstdint>

namespace parquet {

/// Utility class to build an in-memory buffer.
class BufferBuilder {
 public:
  BufferBuilder(uint8_t* dst_buffer, int dst_len)
      : buffer_(dst_buffer), capacity_(dst_len), size_(0) {}

  BufferBuilder(char* dst_buffer, int dst_len)
      : buffer_(reinterpret_cast<uint8_t*>(dst_buffer)), capacity_(dst_len), size_(0) {}

  inline void Append(const void* buffer, int len) {
    memcpy(buffer_ + size_, buffer, len);
    size_ += len;
  }

  template <typename T>
  inline void Append(const T& v) {
    Append(&v, sizeof(T));
  }

  int capacity() const { return capacity_; }
  int size() const { return size_; }

 private:
  uint8_t* buffer_;
  int capacity_;
  int size_;
};

}  // namespace parquet

#endif  // PARQUET_UTIL_BUFFER_BUILDER_H
