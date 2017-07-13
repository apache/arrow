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

#include "arrow/io/interfaces.h"

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class ResizableBuffer;
class Status;

namespace io {

// An output stream that writes to a MutableBuffer, such as one obtained from a
// memory map
class ARROW_EXPORT BufferOutputStream : public OutputStream {
 public:
  explicit BufferOutputStream(const std::shared_ptr<ResizableBuffer>& buffer);

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
};

// A helper class to tracks the size of allocations
class ARROW_EXPORT MockOutputStream : public OutputStream {
 public:
  MockOutputStream() : extent_bytes_written_(0) {}

  // Implement the OutputStream interface
  Status Close() override;
  Status Tell(int64_t* position) override;
  Status Write(const uint8_t* data, int64_t nbytes) override;

  int64_t GetExtentBytesWritten() const { return extent_bytes_written_; }

 private:
  int64_t extent_bytes_written_;
};

/// \brief Enables random writes into a fixed-size mutable buffer
///
class ARROW_EXPORT FixedSizeBufferWriter : public WriteableFile {
 public:
  /// Input buffer must be mutable, will abort if not
  explicit FixedSizeBufferWriter(const std::shared_ptr<Buffer>& buffer);
  ~FixedSizeBufferWriter();

  Status Close() override;
  Status Seek(int64_t position) override;
  Status Tell(int64_t* position) override;
  Status Write(const uint8_t* data, int64_t nbytes) override;
  Status WriteAt(int64_t position, const uint8_t* data, int64_t nbytes) override;

  void set_memcopy_threads(int num_threads);
  void set_memcopy_blocksize(int64_t blocksize);
  void set_memcopy_threshold(int64_t threshold);

 private:
  std::mutex lock_;
  std::shared_ptr<Buffer> buffer_;
  uint8_t* mutable_data_;
  int64_t size_;
  int64_t position_;

  int memcopy_num_threads_;
  int64_t memcopy_blocksize_;
  int64_t memcopy_threshold_;
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
