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

// Buffered stream implementations

#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

#include "arrow/buffer.h"
#include "arrow/io/concurrency.h"
#include "arrow/io/interfaces.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;
class Status;

namespace io {

class ARROW_EXPORT BufferedOutputStream : public OutputStream {
 public:
  ~BufferedOutputStream() override;

  /// \brief Create a buffered output stream wrapping the given output stream.
  /// \param[in] buffer_size the size of the temporary write buffer
  /// \param[in] pool a MemoryPool to use for allocations
  /// \param[in] raw another OutputStream
  /// \return the created BufferedOutputStream
  static Result<std::shared_ptr<BufferedOutputStream>> Create(
      int64_t buffer_size, MemoryPool* pool, std::shared_ptr<OutputStream> raw);

  /// \brief Resize internal buffer
  /// \param[in] new_buffer_size the new buffer size
  /// \return Status
  Status SetBufferSize(int64_t new_buffer_size);

  /// \brief Return the current size of the internal buffer
  int64_t buffer_size() const;

  /// \brief Return the number of remaining bytes that have not been flushed to
  /// the raw OutputStream
  int64_t bytes_buffered() const;

  /// \brief Flush any buffered writes and release the raw
  /// OutputStream. Further operations on this object are invalid
  /// \return the underlying OutputStream
  Result<std::shared_ptr<OutputStream>> Detach();

  // OutputStream interface

  /// \brief Close the buffered output stream.  This implicitly closes the
  /// underlying raw output stream.
  Status Close() override;
  Status Abort() override;
  bool closed() const override;

  Result<int64_t> Tell() const override;
  // Write bytes to the stream. Thread-safe
  Status Write(const void* data, int64_t nbytes) override;
  Status Write(const std::shared_ptr<Buffer>& data) override;

  Status Flush() override;

  /// \brief Return the underlying raw output stream.
  std::shared_ptr<OutputStream> raw() const;

 private:
  explicit BufferedOutputStream(std::shared_ptr<OutputStream> raw, MemoryPool* pool);

  class ARROW_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
};

/// \class BufferedInputStream
/// \brief An InputStream that performs buffered reads from an unbuffered
/// InputStream, which can mitigate the overhead of many small reads in some
/// cases
class ARROW_EXPORT BufferedInputStream
    : public internal::InputStreamConcurrencyWrapper<BufferedInputStream> {
 public:
  ~BufferedInputStream() override;

  /// \brief Create a BufferedInputStream from a raw InputStream
  /// \param[in] buffer_size the size of the temporary read buffer
  /// \param[in] pool a MemoryPool to use for allocations
  /// \param[in] raw a raw InputStream
  /// \param[in] raw_read_bound a bound on the maximum number of bytes
  /// to read from the raw input stream. The default -1 indicates that
  /// it is unbounded
  /// \return the created BufferedInputStream
  static Result<std::shared_ptr<BufferedInputStream>> Create(
      int64_t buffer_size, MemoryPool* pool, std::shared_ptr<InputStream> raw,
      int64_t raw_read_bound = -1);

  /// \brief Resize internal read buffer; calls to Read(...) will read at least
  /// \param[in] new_buffer_size the new read buffer size
  /// \return Status
  Status SetBufferSize(int64_t new_buffer_size);

  /// \brief Return the number of remaining bytes in the read buffer
  int64_t bytes_buffered() const;

  /// \brief Return the current size of the internal buffer
  int64_t buffer_size() const;

  /// \brief Release the raw InputStream. Any data buffered will be
  /// discarded. Further operations on this object are invalid
  /// \return raw the underlying InputStream
  std::shared_ptr<InputStream> Detach();

  /// \brief Return the unbuffered InputStream
  std::shared_ptr<InputStream> raw() const;

  // InputStream APIs

  bool closed() const override;
  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() override;
  Future<std::shared_ptr<const KeyValueMetadata>> ReadMetadataAsync(
      const IOContext& io_context) override;

 private:
  friend InputStreamConcurrencyWrapper<BufferedInputStream>;

  explicit BufferedInputStream(std::shared_ptr<InputStream> raw, MemoryPool* pool,
                               int64_t raw_total_bytes_bound);

  Status DoClose();
  Status DoAbort() override;

  /// \brief Returns the position of the buffered stream, though the position
  /// of the unbuffered stream may be further advanced.
  Result<int64_t> DoTell() const;

  Result<int64_t> DoRead(int64_t nbytes, void* out);

  /// \brief Read into buffer.
  Result<std::shared_ptr<Buffer>> DoRead(int64_t nbytes);

  /// \brief Return a zero-copy string view referencing buffered data,
  /// but do not advance the position of the stream. Buffers data and
  /// expands the buffer size if necessary
  Result<std::string_view> DoPeek(int64_t nbytes) override;

  class ARROW_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
};

/// \class ChunkBufferedInputStream
/// \brief An ChunkBufferedInputStream that performs buffered reads from an
/// unbuffered InputStream, which can mitigate the overhead of many small
/// reads in some cases.
///
/// When an actual io request occurs, read ranges will be coalesced if the
/// distance between them is less than io_merge_threshold, and the actual size
/// in one io request will be limited by the buffer_size.
///
/// \attention It is important to note that since the data returned by the Read
/// interface is a reference to the Buffer, the caller must ensure that the data
/// returned by the Read interface is processed completely before calling the
/// Read interface again. Otherwise, fatal errors may occur due to data in the
/// Buffer being overwritten.
class ARROW_EXPORT ChunkBufferedInputStream : public InputStream {
 public:
  static Result<std::shared_ptr<ChunkBufferedInputStream>> Create(
      int64_t start, int64_t length, std::shared_ptr<RandomAccessFile> impl,
      std::vector<ReadRange> read_ranges, int64_t buffer_size, int32_t io_merge_threshold,
      MemoryPool* pool_ = ::arrow::default_memory_pool());

  // InputStream interface
  Status Advance(int64_t nbytes) override;

  /// \brief Peek some data without advancing the read position.
  Result<std::string_view> Peek(int64_t nbytes) override;

  // Readable interface
  Result<int64_t> Read(int64_t nbytes, void* out) override;

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override;

  // FileInterface
  Status Close() override;

  Result<int64_t> Tell() const override;

  bool closed() const override;

 private:
  explicit ChunkBufferedInputStream(int64_t start, int64_t length,
                                    std::shared_ptr<RandomAccessFile> impl,
                                    std::vector<ReadRange> read_ranges,
                                    int64_t buffer_size, int32_t io_merge_threshold,
                                    std::shared_ptr<ResizableBuffer> buffer);

  inline Status CheckClosed() const {
    if (ARROW_PREDICT_TRUE(is_open_)) {
      return Status::OK();
    }

    return Status::IOError("Operation forbidden on closed file input stream");
  }

  inline Status CheckReadRange(int64_t nbytes) {
    // Upon reaching the end of the current read range, if there is a next read
    // range, current_range_available_bytes_ will be set to the length of the
    // next read range, and this will be ensured by ConsumeBuffer;
    if (ARROW_PREDICT_TRUE(current_range_available_bytes_ >= nbytes)) {
      return Status::OK();
    }

    if (current_range_available_bytes_ > 0 && current_range_available_bytes_ < nbytes) {
      if (read_ranges_[read_ranges_idx_].length > nbytes) {
        return Status::IOError("Read length is illegal");
      }

      // At the beginning of a new read range and required bytes is more than
      // read range length, we think it's ok because there are some tentative
      // read requests when getting next page data.
      return Status::OK();
    }

    if (read_ranges_idx_ != read_ranges_.size()) {
      // Should not be here
      return Status::IOError("Unexpected status");
    }
    return Status::OK();
  }

  inline Status CheckPosition(int64_t length) const {
    if (ARROW_PREDICT_TRUE(raw_pos_ + length <= raw_end_)) {
      return Status::OK();
    }

    return Status::IOError("Read position is out of range");
  }

  inline Status EnsureBufferCapacity(int64_t size) const {
    if (ARROW_PREDICT_FALSE(size > buffer_->size())) {
      RETURN_NOT_OK(buffer_->Resize(size));
    }

    return Status::OK();
  }

  inline void ConsumeBuffer(int64_t nbytes) {
    current_range_available_bytes_ -= nbytes;
    buffer_pos_ += nbytes;
    bytes_buffered_ -= nbytes;
    if (current_range_available_bytes_ == 0) {
      if (++read_gaps_idx_ != read_gaps_.size()) {
        buffer_pos_ += read_gaps_[read_gaps_idx_];
        bytes_buffered_ -= read_gaps_[read_gaps_idx_];
      }
      if (++read_ranges_idx_ != read_ranges_.size()) {
        current_range_available_bytes_ = read_ranges_[read_ranges_idx_].length;
      }
    }
  }

  /// \brief Prefetch data from the underlying file if remaining data in buffer
  /// is less than the required nbytes. Return the number of bytes that can be
  /// read from the buffer.
  Status BufferIfNeeded(int64_t nbytes);

  bool is_open_;

  // Every instance of FileInputStream should keep its own position
  // info to make sure the owner can acquire data on demand, in
  // consideration of other readers may change the position of
  // the impl_.
  int64_t raw_pos_{0};
  int64_t raw_end_{0};
  std::shared_ptr<RandomAccessFile> raw_;

  std::vector<ReadRange> read_ranges_;
  size_t read_ranges_idx_{0};

  // Keep the distance between two read ranges when coalescing them
  std::vector<int32_t> read_gaps_;
  size_t read_gaps_idx_{0};

  int64_t buffer_pos_{0};
  int64_t bytes_buffered_{0};

  // Buffer may contain multiple read ranges, this value keeps the
  // remaining bytes for current range being read.
  int64_t current_range_available_bytes_{0};

  // Threshold to limit prefetch data size.
  int64_t buffer_size_{0};
  // Coalesce two read ranges if the distance between them is less
  // then the threshold.
  int32_t io_merge_threshold_{0};

  // Used for saving the data when invoking Peek to make data can
  // be accessed safely outside.
  std::shared_ptr<ResizableBuffer> buffer_;
};

}  // namespace io
}  // namespace arrow
