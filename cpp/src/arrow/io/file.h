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

// IO interface implementations for OS files

#ifndef ARROW_IO_FILE_H
#define ARROW_IO_FILE_H

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/io/interfaces.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;
class Status;

namespace io {

class ARROW_EXPORT FileOutputStream : public OutputStream {
 public:
  ~FileOutputStream();

  // When opening a new file, any existing file with the indicated path is
  // truncated to 0 bytes, deleting any existing memory
  static Status Open(const std::string& path, std::shared_ptr<FileOutputStream>* file);

  static Status Open(
      const std::string& path, bool append, std::shared_ptr<FileOutputStream>* file);

  // OutputStream interface
  Status Close() override;
  Status Tell(int64_t* position) override;
  Status Write(const uint8_t* data, int64_t nbytes) override;

  int file_descriptor() const;

 private:
  FileOutputStream();

  class ARROW_NO_EXPORT FileOutputStreamImpl;
  std::unique_ptr<FileOutputStreamImpl> impl_;
};

// Operating system file
class ARROW_EXPORT ReadableFile : public ReadableFileInterface {
 public:
  ~ReadableFile();

  // Open file, allocate memory (if needed) from default memory pool
  static Status Open(const std::string& path, std::shared_ptr<ReadableFile>* file);

  // Open file with one's own memory pool for memory allocations
  static Status Open(const std::string& path, MemoryPool* memory_pool,
      std::shared_ptr<ReadableFile>* file);

  Status Close() override;
  Status Tell(int64_t* position) override;

  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) override;
  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  Status GetSize(int64_t* size) override;
  Status Seek(int64_t position) override;

  bool supports_zero_copy() const override;

  int file_descriptor() const;

 private:
  explicit ReadableFile(MemoryPool* pool);

  class ARROW_NO_EXPORT ReadableFileImpl;
  std::unique_ptr<ReadableFileImpl> impl_;
};

// A file interface that uses memory-mapped files for memory interactions,
// supporting zero copy reads. The same class is used for both reading and
// writing.
//
// If opening a file in a writeable mode, it is not truncated first as with
// FileOutputStream
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

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_FILE_H
