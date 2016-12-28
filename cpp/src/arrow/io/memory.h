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
class ResizableBuffer;
class Status;

namespace io {

// An output stream that writes to a MutableBuffer, such as one obtained from a
// memory map
class ARROW_EXPORT BufferOutputStream : public OutputStream {
 public:
  explicit BufferOutputStream(const std::shared_ptr<ResizableBuffer>& buffer);

  // Implement the OutputStream interface
  Status Close() override;
  Status Tell(int64_t* position) override;
  Status Write(const uint8_t* data, int64_t nbytes) override;

 private:
  // Ensures there is sufficient space available to write nbytes
  Status Reserve(int64_t nbytes);

  std::shared_ptr<ResizableBuffer> buffer_;
  int64_t capacity_;
  int64_t position_;
  uint8_t* mutable_data_;
};

// A memory source that uses memory-mapped files for memory interactions
class ARROW_EXPORT MemoryMappedFile : public ReadWriteFileInterface {
 public:
  ~MemoryMappedFile();

  static Status Open(const std::string& path, FileMode::type mode,
      std::shared_ptr<MemoryMappedFile>* out);

  Status Close() override;

  Status Tell(int64_t* position) override;

  Status Seek(int64_t position) override;

  // Required by ReadableFileInterface, copies memory into out
  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) override;

  // Zero copy read
  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  bool supports_zero_copy() const override;

  Status Write(const uint8_t* data, int64_t nbytes) override;

  Status WriteAt(int64_t position, const uint8_t* data, int64_t nbytes) override;

  // @return: the size in bytes of the memory source
  Status GetSize(int64_t* size) override;

 private:
  explicit MemoryMappedFile(FileMode::type mode);

  Status WriteInternal(const uint8_t* data, int64_t nbytes);

  // Hide the internal details of this class for now
  class ARROW_NO_EXPORT MemoryMappedFileImpl;
  std::unique_ptr<MemoryMappedFileImpl> impl_;
};

class ARROW_EXPORT BufferReader : public ReadableFileInterface {
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

 private:
  std::shared_ptr<Buffer> buffer_;
  const uint8_t* data_;
  int64_t size_;
  int64_t position_;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_MEMORY_H
