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
#include <string>

#include "arrow/io/interfaces.h"

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MutableBuffer;
class Status;

namespace io {

// An output stream that writes to a MutableBuffer, such as one obtained from a
// memory map
//
// TODO(wesm): Implement this class
class ARROW_EXPORT BufferOutputStream : public OutputStream {
 public:
  explicit BufferOutputStream(const std::shared_ptr<MutableBuffer>& buffer)
      : buffer_(buffer) {}

  // Implement the OutputStream interface
  Status Close() override;
  Status Tell(int64_t* position) override;
  Status Write(const uint8_t* data, int64_t length) override;

  // Returns the number of bytes remaining in the buffer
  int64_t bytes_remaining() const;

 private:
  std::shared_ptr<MutableBuffer> buffer_;
  int64_t capacity_;
  int64_t position_;
};

// A memory source that uses memory-mapped files for memory interactions
class ARROW_EXPORT MemoryMappedFile : public ReadWriteFileInterface {
 public:
  static Status Open(const std::string& path, FileMode::type mode,
      std::shared_ptr<MemoryMappedFile>* out);

  Status Close() override;

  Status Tell(int64_t* position) override;

  Status Seek(int64_t position) override;

  // Required by ReadableFileInterface, copies memory into out
  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) override;

  Status ReadAt(
      int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* out) override;

  // Read into a buffer, zero copy if possible
  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  bool supports_zero_copy() const override;

  Status Write(const uint8_t* data, int64_t nbytes) override;

  Status WriteAt(int64_t position, const uint8_t* data, int64_t nbytes) override;

  // @return: the size in bytes of the memory source
  Status GetSize(int64_t* size) override;

 private:
  explicit MemoryMappedFile(FileMode::type mode);

  Status WriteInternal(const uint8_t* data, int64_t nbytes);

  // Hide the internal details of this class for now
  class MemoryMappedFileImpl;
  std::unique_ptr<MemoryMappedFileImpl> impl_;
};

class ARROW_EXPORT BufferReader : public ReadableFileInterface {
 public:
  BufferReader(const uint8_t* buffer, int buffer_size)
      : buffer_(buffer), buffer_size_(buffer_size), position_(0) {}

  Status Close() override;
  Status Tell(int64_t* position) override;

  Status ReadAt(
      int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) override;
  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) override;
  Status GetSize(int64_t* size) override;
  Status Seek(int64_t position) override;

  bool supports_zero_copy() const override;

 private:
  const uint8_t* buffer_;
  int buffer_size_;
  int64_t position_;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_MEMORY_H
