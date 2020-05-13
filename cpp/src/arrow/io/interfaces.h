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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/io/type_fwd.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"
#include "arrow/util/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace io {

struct ReadRange {
  int64_t offset;
  int64_t length;

  friend bool operator==(const ReadRange& left, const ReadRange& right) {
    return (left.offset == right.offset && left.length == right.length);
  }
  friend bool operator!=(const ReadRange& left, const ReadRange& right) {
    return !(left == right);
  }

  bool Contains(const ReadRange& other) const {
    return (offset <= other.offset && offset + length >= other.offset + other.length);
  }
};

// EXPERIMENTAL
struct ARROW_EXPORT AsyncContext {
  ::arrow::internal::Executor* executor;

  // Set `executor` to a global IO-specific thread pool.
  AsyncContext();
  explicit AsyncContext(::arrow::internal::Executor* executor);
};

class ARROW_EXPORT FileInterface {
 public:
  virtual ~FileInterface() = 0;

  /// \brief Close the stream cleanly
  ///
  /// For writable streams, this will attempt to flush any pending data
  /// before releasing the underlying resource.
  ///
  /// After Close() is called, closed() returns true and the stream is not
  /// available for further operations.
  virtual Status Close() = 0;

  /// \brief Close the stream abruptly
  ///
  /// This method does not guarantee that any pending data is flushed.
  /// It merely releases any underlying resource used by the stream for
  /// its operation.
  ///
  /// After Abort() is called, closed() returns true and the stream is not
  /// available for further operations.
  virtual Status Abort();

  /// \brief Return the position in this stream
  virtual Result<int64_t> Tell() const = 0;

  /// \brief Return whether the stream is closed
  virtual bool closed() const = 0;

  FileMode::type mode() const { return mode_; }

 protected:
  FileInterface() : mode_(FileMode::READ) {}
  FileMode::type mode_;
  void set_mode(FileMode::type mode) { mode_ = mode; }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(FileInterface);
};

class ARROW_EXPORT Seekable {
 public:
  virtual ~Seekable() = default;
  virtual Status Seek(int64_t position) = 0;
};

class ARROW_EXPORT Writable {
 public:
  virtual ~Writable() = default;

  /// \brief Write the given data to the stream
  ///
  /// This method always processes the bytes in full.  Depending on the
  /// semantics of the stream, the data may be written out immediately,
  /// held in a buffer, or written asynchronously.  In the case where
  /// the stream buffers the data, it will be copied.  To avoid potentially
  /// large copies, use the Write variant that takes an owned Buffer.
  virtual Status Write(const void* data, int64_t nbytes) = 0;

  /// \brief Write the given data to the stream
  ///
  /// Since the Buffer owns its memory, this method can avoid a copy if
  /// buffering is required.  See Write(const void*, int64_t) for details.
  virtual Status Write(const std::shared_ptr<Buffer>& data);

  /// \brief Flush buffered bytes, if any
  virtual Status Flush();

  Status Write(const std::string& data);
};

class ARROW_EXPORT Readable {
 public:
  virtual ~Readable() = default;

  /// \brief Read data from current file position.
  ///
  /// Read at most `nbytes` from the current file position into `out`.
  /// The number of bytes read is returned.
  virtual Result<int64_t> Read(int64_t nbytes, void* out) = 0;

  /// \brief Read data from current file position.
  ///
  /// Read at most `nbytes` from the current file position. Less bytes may
  /// be read if EOF is reached. This method updates the current file position.
  ///
  /// In some cases (e.g. a memory-mapped file), this method may avoid a
  /// memory copy.
  virtual Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) = 0;
};

class ARROW_EXPORT OutputStream : virtual public FileInterface, public Writable {
 protected:
  OutputStream() = default;
};

class ARROW_EXPORT InputStream : virtual public FileInterface, virtual public Readable {
 public:
  /// \brief Advance or skip stream indicated number of bytes
  /// \param[in] nbytes the number to move forward
  /// \return Status
  Status Advance(int64_t nbytes);

  /// \brief Return zero-copy string_view to upcoming bytes.
  ///
  /// Do not modify the stream position.  The view becomes invalid after
  /// any operation on the stream.  May trigger buffering if the requested
  /// size is larger than the number of buffered bytes.
  ///
  /// May return NotImplemented on streams that don't support it.
  ///
  /// \param[in] nbytes the maximum number of bytes to see
  virtual Result<util::string_view> Peek(int64_t nbytes);

  /// \brief Return true if InputStream is capable of zero copy Buffer reads
  ///
  /// Zero copy reads imply the use of Buffer-returning Read() overloads.
  virtual bool supports_zero_copy() const;

 protected:
  InputStream() = default;
};

class ARROW_EXPORT RandomAccessFile
    : public std::enable_shared_from_this<RandomAccessFile>,
      public InputStream,
      public Seekable {
 public:
  /// Necessary because we hold a std::unique_ptr
  ~RandomAccessFile() override;

  /// \brief Create an isolated InputStream that reads a segment of a
  /// RandomAccessFile. Multiple such stream can be created and used
  /// independently without interference
  /// \param[in] file a file instance
  /// \param[in] file_offset the starting position in the file
  /// \param[in] nbytes the extent of bytes to read. The file should have
  /// sufficient bytes available
  static std::shared_ptr<InputStream> GetStream(std::shared_ptr<RandomAccessFile> file,
                                                int64_t file_offset, int64_t nbytes);

  /// \brief Return the total file size in bytes.
  ///
  /// This method does not read or move the current file position, so is safe
  /// to call concurrently with e.g. ReadAt().
  virtual Result<int64_t> GetSize() = 0;

  /// \brief Read data from given file position.
  ///
  /// At most `nbytes` bytes are read.  The number of bytes read is returned
  /// (it can be less than `nbytes` if EOF is reached).
  ///
  /// This method can be safely called from multiple threads concurrently.
  /// It is unspecified whether this method updates the file position or not.
  ///
  /// The default RandomAccessFile-provided implementation uses Seek() and Read(),
  /// but subclasses may override it with a more efficient implementation
  /// that doesn't depend on implicit file positioning.
  ///
  /// \param[in] position Where to read bytes from
  /// \param[in] nbytes The number of bytes to read
  /// \param[out] out The buffer to read bytes into
  /// \return The number of bytes read, or an error
  virtual Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out);

  /// \brief Read data from given file position.
  ///
  /// At most `nbytes` bytes are read, but it can be less if EOF is reached.
  ///
  /// \param[in] position Where to read bytes from
  /// \param[in] nbytes The number of bytes to read
  /// \return A buffer containing the bytes read, or an error
  virtual Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes);

  // EXPERIMENTAL
  virtual Future<std::shared_ptr<Buffer>> ReadAsync(const AsyncContext&, int64_t position,
                                                    int64_t nbytes);

 protected:
  RandomAccessFile();

 private:
  struct ARROW_NO_EXPORT RandomAccessFileImpl;
  std::unique_ptr<RandomAccessFileImpl> interface_impl_;
};

class ARROW_EXPORT WritableFile : public OutputStream, public Seekable {
 public:
  virtual Status WriteAt(int64_t position, const void* data, int64_t nbytes) = 0;

 protected:
  WritableFile() = default;
};

class ARROW_EXPORT ReadWriteFileInterface : public RandomAccessFile, public WritableFile {
 protected:
  ReadWriteFileInterface() { RandomAccessFile::set_mode(FileMode::READWRITE); }
};

/// \brief Return an iterator on an input stream
///
/// The iterator yields a fixed-size block on each Next() call, except the
/// last block in the stream which may be smaller.
/// Once the end of stream is reached, Next() returns nullptr
/// (unlike InputStream::Read() which returns an empty buffer).
ARROW_EXPORT
Result<Iterator<std::shared_ptr<Buffer>>> MakeInputStreamIterator(
    std::shared_ptr<InputStream> stream, int64_t block_size);

}  // namespace io
}  // namespace arrow
