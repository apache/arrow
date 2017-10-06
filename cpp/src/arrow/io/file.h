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
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;
class Status;

namespace io {

class ARROW_EXPORT FileOutputStream : public OutputStream {
 public:
  ~FileOutputStream();

  /// \brief Open a local file for writing, truncating any existing file
  /// \param[in] path with UTF8 encoding
  /// \param[out] file a FileOutputStream instance
  ///
  /// When opening a new file, any existing file with the indicated path is
  /// truncated to 0 bytes, deleting any existing memory
  static Status Open(const std::string& path, std::shared_ptr<FileOutputStream>* file);

  /// \brief Open a local file for writing
  /// \param[in] path with UTF8 encoding
  /// \param[in] append append to existing file, otherwise truncate to 0 bytes
  /// \param[out] file a FileOutputStream instance
  static Status Open(const std::string& path, bool append,
                     std::shared_ptr<FileOutputStream>* file);

  // OutputStream interface
  Status Close() override;
  Status Tell(int64_t* position) const override;

  // Write bytes to the stream. Thread-safe
  Status Write(const uint8_t* data, int64_t nbytes) override;

  int file_descriptor() const;

 private:
  FileOutputStream();

  class ARROW_NO_EXPORT FileOutputStreamImpl;
  std::unique_ptr<FileOutputStreamImpl> impl_;
};

// Operating system file
class ARROW_EXPORT ReadableFile : public RandomAccessFile {
 public:
  ~ReadableFile();

  /// \brief Open a local file for reading
  /// \param[in] path with UTF8 encoding
  /// \param[out] file ReadableFile instance
  /// Open file, allocate memory (if needed) from default memory pool
  static Status Open(const std::string& path, std::shared_ptr<ReadableFile>* file);

  /// \brief Open a local file for reading
  /// \param[in] path with UTF8 encoding
  /// \param[in] pool a MemoryPool for memory allocations
  /// \param[out] file ReadableFile instance
  /// Open file with one's own memory pool for memory allocations
  static Status Open(const std::string& path, MemoryPool* pool,
                     std::shared_ptr<ReadableFile>* file);

  Status Close() override;
  Status Tell(int64_t* position) const override;

  // Read bytes from the file. Thread-safe
  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) override;
  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  /// \brief Thread-safe implementation of ReadAt
  Status ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                uint8_t* out) override;

  /// \brief Thread-safe implementation of ReadAt
  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override;

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

  /// Create new file with indicated size, return in read/write mode
  static Status Create(const std::string& path, int64_t size,
                       std::shared_ptr<MemoryMappedFile>* out);

  static Status Open(const std::string& path, FileMode::type mode,
                     std::shared_ptr<MemoryMappedFile>* out);

  Status Close() override;

  Status Tell(int64_t* position) const override;

  Status Seek(int64_t position) override;

  // Required by RandomAccessFile, copies memory into out. Not thread-safe
  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) override;

  // Zero copy read. Not thread-safe
  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  Status ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                uint8_t* out) override;

  /// Default implementation is thread-safe
  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  bool supports_zero_copy() const override;

  /// Write data at the current position in the file. Thread-safe
  Status Write(const uint8_t* data, int64_t nbytes) override;

  /// Write data at a particular position in the file. Thread-safe
  Status WriteAt(int64_t position, const uint8_t* data, int64_t nbytes) override;

  // @return: the size in bytes of the memory source
  Status GetSize(int64_t* size) override;

  int file_descriptor() const;

 private:
  MemoryMappedFile();

  Status WriteInternal(const uint8_t* data, int64_t nbytes);

  class ARROW_NO_EXPORT MemoryMap;
  std::shared_ptr<MemoryMap> memory_map_;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_FILE_H
