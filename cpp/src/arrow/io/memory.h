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

// Public API for different memory sharing / IO mechanisms

#ifndef ARROW_IO_MEMORY_H
#define ARROW_IO_MEMORY_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "arrow/io/interfaces.h"

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

static constexpr int64_t BYTES_IN_MB = 1 << 20;

namespace arrow {

class Buffer;
class ResizableBuffer;
class Status;

namespace io {

class Memcopy {
public:
  virtual void memcopy(uint8_t* dst, const uint8_t* src, uint64_t nbytes) = 0;
};

class SerialMemcopy : public Memcopy {
public:
  void memcopy(uint8_t* dst, const uint8_t* src, uint64_t nbytes) override;
};

// A helper class for doing memcpy with multiple threads. This is required
// to saturate the memory bandwidth of modern cpus.
class ParallelMemcopy : public Memcopy {
 public:
  explicit ParallelMemcopy(uint64_t block_size, int threadpool_size)
      : block_size_(block_size),
        threadpool_(threadpool_size) {}

  void memcopy(uint8_t* dst, const uint8_t* src, uint64_t nbytes) override;

  virtual ~ParallelMemcopy() {
    // Join threadpool threads just in case they are still running.
    for (auto& t : threadpool_) {
      if (t.joinable()) { t.join(); }
    }
  }

 private:
  // Specifies the desired alignment in bytes, as a power of 2.
  uint64_t block_size_;
  // Internal threadpool to be used in the fork/join pattern.
  std::vector<std::thread> threadpool_;

  void memcopy_aligned(
      uint8_t* dst, const uint8_t* src, uint64_t nbytes, uint64_t block_size);
};

// An output stream that writes to a MutableBuffer, such as one obtained from a
// memory map
class ARROW_EXPORT BufferOutputStream : public OutputStream {
 public:
  explicit BufferOutputStream(const std::shared_ptr<ResizableBuffer>& buffer,
    std::unique_ptr<Memcopy> memcopy = nullptr);

  static Status Create(int64_t initial_capacity, MemoryPool* pool,
      std::shared_ptr<BufferOutputStream>* out);

  ~BufferOutputStream();

  // Implement the OutputStream interface
  Status Close() override;
  Status Tell(int64_t* position) override;
  Status Write(const uint8_t* data, int64_t nbytes) override;

  /// Close the stream and return the buffer
  Status Finish(std::shared_ptr<Buffer>* result);

 private:
  // Ensures there is sufficient space available to write nbytes
  Status Reserve(int64_t nbytes);

  std::shared_ptr<ResizableBuffer> buffer_;
  int64_t capacity_;
  int64_t position_;
  uint8_t* mutable_data_;
  std::unique_ptr<Memcopy> memcopy_;
};

/// \brief Enables random writes into a fixed-size mutable buffer
///
class ARROW_EXPORT FixedSizeBufferWriter : public WriteableFile {
 public:
  /// Input buffer must be mutable, will abort if not
  explicit FixedSizeBufferWriter(const std::shared_ptr<Buffer>& buffer,
    std::unique_ptr<Memcopy> memcopy = nullptr);
  ~FixedSizeBufferWriter();

  Status Close() override;
  Status Seek(int64_t position) override;
  Status Tell(int64_t* position) override;
  Status Write(const uint8_t* data, int64_t nbytes) override;
  Status WriteAt(int64_t position, const uint8_t* data, int64_t nbytes) override;

 private:
  std::mutex lock_;
  std::shared_ptr<Buffer> buffer_;
  uint8_t* mutable_data_;
  int64_t size_;
  int64_t position_;
  std::unique_ptr<Memcopy> memcopy_;
};

class ARROW_EXPORT BufferReader : public RandomAccessFile {
 public:
  explicit BufferReader(const std::shared_ptr<Buffer>& buffer);
  BufferReader(const uint8_t* data, int64_t size);
  virtual ~BufferReader();

  Status Close() override;
  Status Tell(int64_t* position) override;

  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) override;

  // Zero copy read
  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  Status GetSize(int64_t* size) override;
  Status Seek(int64_t position) override;

  bool supports_zero_copy() const override;

  std::shared_ptr<Buffer> buffer() const { return buffer_; }

 private:
  std::shared_ptr<Buffer> buffer_;
  const uint8_t* data_;
  int64_t size_;
  int64_t position_;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_MEMORY_H
