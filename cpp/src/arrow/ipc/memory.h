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

// Public API for different interprocess memory sharing mechanisms

#ifndef ARROW_IPC_MEMORY_H
#define ARROW_IPC_MEMORY_H

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MutableBuffer;
class Status;

namespace ipc {

// Abstract output stream
class OutputStream {
 public:
  virtual ~OutputStream() {}
  // Close the output stream
  virtual Status Close() = 0;

  // The current position in the output stream
  virtual int64_t Tell() const = 0;

  // Write bytes to the stream
  virtual Status Write(const uint8_t* data, int64_t length) = 0;
};

// An output stream that writes to a MutableBuffer, such as one obtained from a
// memory map
class BufferOutputStream : public OutputStream {
 public:
  explicit BufferOutputStream(const std::shared_ptr<MutableBuffer>& buffer)
      : buffer_(buffer) {}

  // Implement the OutputStream interface
  Status Close() override;
  int64_t Tell() const override;
  Status Write(const uint8_t* data, int64_t length) override;

  // Returns the number of bytes remaining in the buffer
  int64_t bytes_remaining() const;

 private:
  std::shared_ptr<MutableBuffer> buffer_;
  int64_t capacity_;
  int64_t position_;
};

class ARROW_EXPORT MemorySource {
 public:
  // Indicates the access permissions of the memory source
  enum AccessMode { READ_ONLY, READ_WRITE };

  virtual ~MemorySource();

  // Retrieve a buffer of memory from the source of the indicates size and at
  // the indicated location
  // @returns: arrow::Status indicating success / failure. The buffer is set
  // into the *out argument
  virtual Status ReadAt(
      int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) = 0;

  virtual Status Close() = 0;

  virtual Status Write(int64_t position, const uint8_t* data, int64_t nbytes) = 0;

  // @return: the size in bytes of the memory source
  virtual int64_t Size() const = 0;

 protected:
  explicit MemorySource(AccessMode access_mode = AccessMode::READ_WRITE);

  AccessMode access_mode_;

 private:
  DISALLOW_COPY_AND_ASSIGN(MemorySource);
};

// A memory source that uses memory-mapped files for memory interactions
class ARROW_EXPORT MemoryMappedSource : public MemorySource {
 public:
  static Status Open(const std::string& path, AccessMode access_mode,
      std::shared_ptr<MemoryMappedSource>* out);

  Status Close() override;

  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  Status Write(int64_t position, const uint8_t* data, int64_t nbytes) override;

  // @return: the size in bytes of the memory source
  int64_t Size() const override;

 private:
  explicit MemoryMappedSource(AccessMode access_mode);
  // Hide the internal details of this class for now
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// A MemorySource that tracks the size of allocations from a memory source
class MockMemorySource : public MemorySource {
 public:
  explicit MockMemorySource(int64_t size);

  Status Close() override;

  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  Status Write(int64_t position, const uint8_t* data, int64_t nbytes) override;

  int64_t Size() const override;

  // @return: the smallest number of bytes containing the modified region of the
  // MockMemorySource
  int64_t GetExtentBytesWritten() const;

 private:
  int64_t size_;
  int64_t extent_bytes_written_;
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_MEMORY_H
