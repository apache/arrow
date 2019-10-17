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

#include "arrow/io/concurrency.h"
#include "arrow/io/interfaces.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;
class Status;

namespace io {

/// \brief An operating system file open in write-only mode.
class ARROW_EXPORT FileOutputStream : public OutputStream {
 public:
  ~FileOutputStream() override;

  /// \brief Open a local file for writing, truncating any existing file
  /// \param[in] path with UTF8 encoding
  /// \param[out] out a base interface OutputStream instance
  ///
  /// When opening a new file, any existing file with the indicated path is
  /// truncated to 0 bytes, deleting any existing data
  static Status Open(const std::string& path, std::shared_ptr<OutputStream>* out);

  /// \brief Open a local file for writing
  /// \param[in] path with UTF8 encoding
  /// \param[in] append append to existing file, otherwise truncate to 0 bytes
  /// \param[out] out a base interface OutputStream instance
  static Status Open(const std::string& path, bool append,
                     std::shared_ptr<OutputStream>* out);

  /// \brief Open a file descriptor for writing.  The underlying file isn't
  /// truncated.
  /// \param[in] fd file descriptor
  /// \param[out] out a base interface OutputStream instance
  ///
  /// The file descriptor becomes owned by the OutputStream, and will be closed
  /// on Close() or destruction.
  static Status Open(int fd, std::shared_ptr<OutputStream>* out);

  /// \brief Open a local file for writing, truncating any existing file
  /// \param[in] path with UTF8 encoding
  /// \param[out] file a FileOutputStream instance
  ///
  /// When opening a new file, any existing file with the indicated path is
  /// truncated to 0 bytes, deleting any existing data
  static Status Open(const std::string& path, std::shared_ptr<FileOutputStream>* file);

  /// \brief Open a local file for writing
  /// \param[in] path with UTF8 encoding
  /// \param[in] append append to existing file, otherwise truncate to 0 bytes
  /// \param[out] file a FileOutputStream instance
  static Status Open(const std::string& path, bool append,
                     std::shared_ptr<FileOutputStream>* file);

  /// \brief Open a file descriptor for writing.  The underlying file isn't
  /// truncated.
  /// \param[in] fd file descriptor
  /// \param[out] out a FileOutputStream instance
  ///
  /// The file descriptor becomes owned by the OutputStream, and will be closed
  /// on Close() or destruction.
  static Status Open(int fd, std::shared_ptr<FileOutputStream>* out);

  // OutputStream interface
  Status Close() override;
  bool closed() const override;
  Status Tell(int64_t* position) const override;

  // Write bytes to the stream. Thread-safe
  Status Write(const void* data, int64_t nbytes) override;
  /// \cond FALSE
  using Writable::Write;
  /// \endcond

  int file_descriptor() const;

 private:
  FileOutputStream();

  class ARROW_NO_EXPORT FileOutputStreamImpl;
  std::unique_ptr<FileOutputStreamImpl> impl_;
};

/// \brief An operating system file open in read-only mode.
///
/// Reads through this implementation are unbuffered.  If many small reads
/// need to be issued, it is recommended to use a buffering layer for good
/// performance.
class ARROW_EXPORT ReadableFile
    : public internal::RandomAccessFileConcurrencyWrapper<ReadableFile> {
 public:
  ~ReadableFile() override;

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

  /// \brief Open a local file for reading
  /// \param[in] fd file descriptor
  /// \param[out] file ReadableFile instance
  /// Open file with one's own memory pool for memory allocations
  ///
  /// The file descriptor becomes owned by the ReadableFile, and will be closed
  /// on Close() or destruction.
  static Status Open(int fd, std::shared_ptr<ReadableFile>* file);

  /// \brief Open a local file for reading
  /// \param[in] fd file descriptor
  /// \param[in] pool a MemoryPool for memory allocations
  /// \param[out] file ReadableFile instance
  /// Open file with one's own memory pool for memory allocations
  ///
  /// The file descriptor becomes owned by the ReadableFile, and will be closed
  /// on Close() or destruction.
  static Status Open(int fd, MemoryPool* pool, std::shared_ptr<ReadableFile>* file);

  bool closed() const override;

  int file_descriptor() const;

 private:
  friend RandomAccessFileConcurrencyWrapper<ReadableFile>;

  explicit ReadableFile(MemoryPool* pool);

  Status DoClose();
  Status DoTell(int64_t* position) const;
  Status DoRead(int64_t nbytes, int64_t* bytes_read, void* buffer);
  Status DoRead(int64_t nbytes, std::shared_ptr<Buffer>* out);

  /// \brief Thread-safe implementation of ReadAt
  Status DoReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out);

  /// \brief Thread-safe implementation of ReadAt
  Status DoReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out);

  Status DoGetSize(int64_t* size);
  Status DoSeek(int64_t position);

  class ARROW_NO_EXPORT ReadableFileImpl;
  std::unique_ptr<ReadableFileImpl> impl_;
};

/// \brief A file interface that uses memory-mapped files for memory interactions
///
/// This implementation supports zero-copy reads. The same class is used
/// for both reading and writing.
///
/// If opening a file in a writable mode, it is not truncated first as with
/// FileOutputStream.
class ARROW_EXPORT MemoryMappedFile : public ReadWriteFileInterface {
 public:
  ~MemoryMappedFile() override;

  /// Create new file with indicated size, return in read/write mode
  static Status Create(const std::string& path, int64_t size,
                       std::shared_ptr<MemoryMappedFile>* out);

  // mmap() with whole file
  static Status Open(const std::string& path, FileMode::type mode,
                     std::shared_ptr<MemoryMappedFile>* out);

  // mmap() with a region of file, the offset must be a multiple of the page size
  static Status Open(const std::string& path, FileMode::type mode, const int64_t offset,
                     const int64_t length, std::shared_ptr<MemoryMappedFile>* out);

  Status Close() override;

  bool closed() const override;

  Status Tell(int64_t* position) const override;

  Status Seek(int64_t position) override;

  // Required by RandomAccessFile, copies memory into out. Not thread-safe
  Status Read(int64_t nbytes, int64_t* bytes_read, void* out) override;

  // Zero copy read, moves position pointer. Not thread-safe
  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  // Zero-copy read, leaves position unchanged. Acquires a reader lock
  // for the duration of slice creation (typically very short). Is thread-safe.
  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  // Raw copy of the memory at specified position. Thread-safe, but
  // locks out other readers for the duration of memcpy. Prefer the
  // zero copy method
  Status ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                void* out) override;

  bool supports_zero_copy() const override;

  /// Write data at the current position in the file. Thread-safe
  Status Write(const void* data, int64_t nbytes) override;
  /// \cond FALSE
  using Writable::Write;
  /// \endcond

  /// Set the size of the map to new_size.
  Status Resize(int64_t new_size);

  /// Write data at a particular position in the file. Thread-safe
  Status WriteAt(int64_t position, const void* data, int64_t nbytes) override;

  // @return: the size in bytes of the memory source
  Status GetSize(int64_t* size) const;

  // @return: the size in bytes of the memory source
  Status GetSize(int64_t* size) override;

  int file_descriptor() const;

 private:
  MemoryMappedFile();

  Status WriteInternal(const void* data, int64_t nbytes);

  class ARROW_NO_EXPORT MemoryMap;
  std::shared_ptr<MemoryMap> memory_map_;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_FILE_H
