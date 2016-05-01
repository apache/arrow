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

#ifndef PARQUET_UTIL_OUTPUT_H
#define PARQUET_UTIL_OUTPUT_H

#include <cstdint>
#include <memory>

#include "parquet/util/macros.h"
#include "parquet/util/mem-allocator.h"

namespace parquet {

class Buffer;
class ResizableBuffer;

// ----------------------------------------------------------------------
// Output stream classes

// Abstract output stream
class OutputStream {
 public:
  // Close the output stream
  virtual void Close() = 0;

  // Return the current position in the output stream relative to the start
  virtual int64_t Tell() = 0;

  // Copy bytes into the output stream
  virtual void Write(const uint8_t* data, int64_t length) = 0;
};

static constexpr int64_t IN_MEMORY_DEFAULT_CAPACITY = 1024;

// An output stream that is an in-memory
class InMemoryOutputStream : public OutputStream {
 public:
  explicit InMemoryOutputStream(int64_t initial_capacity = IN_MEMORY_DEFAULT_CAPACITY,
      MemoryAllocator* allocator = default_allocator());

  // Close is currently a no-op with the in-memory stream
  virtual void Close() {}

  virtual int64_t Tell();

  virtual void Write(const uint8_t* data, int64_t length);

  // Return complete stream as Buffer
  std::shared_ptr<Buffer> GetBuffer();

 private:
  // Mutable pointer to the current write position in the stream
  uint8_t* Head();

  std::shared_ptr<ResizableBuffer> buffer_;
  int64_t size_;
  int64_t capacity_;

  DISALLOW_COPY_AND_ASSIGN(InMemoryOutputStream);
};

}  // namespace parquet

#endif  // PARQUET_UTIL_OUTPUT_H
