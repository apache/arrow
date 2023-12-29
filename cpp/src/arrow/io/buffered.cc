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

#include "arrow/io/buffered.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <string_view>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/io/util_internal.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace io {

// ----------------------------------------------------------------------
// BufferedOutputStream implementation

class BufferedBase {
 public:
  explicit BufferedBase(MemoryPool* pool)
      : pool_(pool),
        is_open_(true),
        buffer_data_(nullptr),
        buffer_pos_(0),
        buffer_size_(0),
        raw_pos_(-1) {}

  bool closed() const {
    std::lock_guard<std::mutex> guard(lock_);
    return !is_open_;
  }

  Status ResetBuffer() {
    if (!buffer_) {
      // On first invocation, or if the buffer has been released, we allocate a
      // new buffer
      ARROW_ASSIGN_OR_RAISE(buffer_, AllocateResizableBuffer(buffer_size_, pool_));
    } else if (buffer_->size() != buffer_size_) {
      RETURN_NOT_OK(buffer_->Resize(buffer_size_));
    }
    buffer_data_ = buffer_->mutable_data();
    return Status::OK();
  }

  Status ResizeBuffer(int64_t new_buffer_size) {
    buffer_size_ = new_buffer_size;
    return ResetBuffer();
  }

  void AppendToBuffer(const void* data, int64_t nbytes) {
    DCHECK_LE(buffer_pos_ + nbytes, buffer_size_);
    std::memcpy(buffer_data_ + buffer_pos_, data, nbytes);
    buffer_pos_ += nbytes;
  }

  int64_t buffer_size() const { return buffer_size_; }

  int64_t buffer_pos() const { return buffer_pos_; }

 protected:
  MemoryPool* pool_;
  bool is_open_;

  std::shared_ptr<ResizableBuffer> buffer_;
  uint8_t* buffer_data_;
  int64_t buffer_pos_;
  int64_t buffer_size_;

  mutable int64_t raw_pos_;
  mutable std::mutex lock_;
};

class BufferedOutputStream::Impl : public BufferedBase {
 public:
  explicit Impl(std::shared_ptr<OutputStream> raw, MemoryPool* pool)
      : BufferedBase(pool), raw_(std::move(raw)) {}

  Status Close() {
    std::lock_guard<std::mutex> guard(lock_);
    if (is_open_) {
      Status st = FlushUnlocked();
      is_open_ = false;
      RETURN_NOT_OK(raw_->Close());
      return st;
    }
    return Status::OK();
  }

  Status Abort() {
    std::lock_guard<std::mutex> guard(lock_);
    if (is_open_) {
      is_open_ = false;
      return raw_->Abort();
    }
    return Status::OK();
  }

  Result<int64_t> Tell() const {
    std::lock_guard<std::mutex> guard(lock_);
    if (raw_pos_ == -1) {
      ARROW_ASSIGN_OR_RAISE(raw_pos_, raw_->Tell());
      DCHECK_GE(raw_pos_, 0);
    }
    return raw_pos_ + buffer_pos_;
  }

  Status Write(const void* data, int64_t nbytes) { return DoWrite(data, nbytes); }

  Status Write(const std::shared_ptr<Buffer>& buffer) {
    return DoWrite(buffer->data(), buffer->size(), buffer);
  }

  Status DoWrite(const void* data, int64_t nbytes,
                 const std::shared_ptr<Buffer>& buffer = nullptr) {
    std::lock_guard<std::mutex> guard(lock_);
    if (nbytes < 0) {
      return Status::Invalid("write count should be >= 0");
    }
    if (nbytes == 0) {
      return Status::OK();
    }
    if (nbytes + buffer_pos_ >= buffer_size_) {
      RETURN_NOT_OK(FlushUnlocked());
      DCHECK_EQ(buffer_pos_, 0);
      if (nbytes >= buffer_size_) {
        // Invalidate cached raw pos
        raw_pos_ = -1;
        // Direct write
        if (buffer) {
          return raw_->Write(buffer);
        } else {
          return raw_->Write(data, nbytes);
        }
      }
    }
    AppendToBuffer(data, nbytes);
    return Status::OK();
  }

  Status FlushUnlocked() {
    if (buffer_pos_ > 0) {
      // Invalidate cached raw pos
      raw_pos_ = -1;
      RETURN_NOT_OK(raw_->Write(buffer_data_, buffer_pos_));
      buffer_pos_ = 0;
    }
    return Status::OK();
  }

  Status Flush() {
    std::lock_guard<std::mutex> guard(lock_);
    return FlushUnlocked();
  }

  Result<std::shared_ptr<OutputStream>> Detach() {
    std::lock_guard<std::mutex> guard(lock_);
    RETURN_NOT_OK(FlushUnlocked());
    is_open_ = false;
    return std::move(raw_);
  }

  Status SetBufferSize(int64_t new_buffer_size) {
    std::lock_guard<std::mutex> guard(lock_);
    if (new_buffer_size <= 0) {
      return Status::Invalid("Buffer size should be positive");
    }
    if (buffer_pos_ >= new_buffer_size) {
      // If the buffer is shrinking, first flush to the raw OutputStream
      RETURN_NOT_OK(FlushUnlocked());
    }
    return ResizeBuffer(new_buffer_size);
  }

  std::shared_ptr<OutputStream> raw() const { return raw_; }

 private:
  std::shared_ptr<OutputStream> raw_;
};

BufferedOutputStream::BufferedOutputStream(std::shared_ptr<OutputStream> raw,
                                           MemoryPool* pool) {
  impl_.reset(new Impl(std::move(raw), pool));
}

Result<std::shared_ptr<BufferedOutputStream>> BufferedOutputStream::Create(
    int64_t buffer_size, MemoryPool* pool, std::shared_ptr<OutputStream> raw) {
  auto result = std::shared_ptr<BufferedOutputStream>(
      new BufferedOutputStream(std::move(raw), pool));
  RETURN_NOT_OK(result->SetBufferSize(buffer_size));
  return result;
}

BufferedOutputStream::~BufferedOutputStream() { internal::CloseFromDestructor(this); }

Status BufferedOutputStream::SetBufferSize(int64_t new_buffer_size) {
  return impl_->SetBufferSize(new_buffer_size);
}

int64_t BufferedOutputStream::buffer_size() const { return impl_->buffer_size(); }

int64_t BufferedOutputStream::bytes_buffered() const { return impl_->buffer_pos(); }

Result<std::shared_ptr<OutputStream>> BufferedOutputStream::Detach() {
  return impl_->Detach();
}

Status BufferedOutputStream::Close() { return impl_->Close(); }

Status BufferedOutputStream::Abort() { return impl_->Abort(); }

bool BufferedOutputStream::closed() const { return impl_->closed(); }

Result<int64_t> BufferedOutputStream::Tell() const { return impl_->Tell(); }

Status BufferedOutputStream::Write(const void* data, int64_t nbytes) {
  return impl_->Write(data, nbytes);
}

Status BufferedOutputStream::Write(const std::shared_ptr<Buffer>& data) {
  return impl_->Write(data);
}

Status BufferedOutputStream::Flush() { return impl_->Flush(); }

std::shared_ptr<OutputStream> BufferedOutputStream::raw() const { return impl_->raw(); }

// ----------------------------------------------------------------------
// BufferedInputStream implementation

class BufferedInputStream::Impl : public BufferedBase {
 public:
  Impl(std::shared_ptr<InputStream> raw, MemoryPool* pool, int64_t raw_total_bytes_bound)
      : BufferedBase(pool),
        raw_(std::move(raw)),
        raw_read_total_(0),
        raw_read_bound_(raw_total_bytes_bound),
        bytes_buffered_(0) {}

  Status Close() {
    if (is_open_) {
      is_open_ = false;
      return raw_->Close();
    }
    return Status::OK();
  }

  Status Abort() {
    if (is_open_) {
      is_open_ = false;
      return raw_->Abort();
    }
    return Status::OK();
  }

  Result<int64_t> Tell() const {
    if (raw_pos_ == -1) {
      ARROW_ASSIGN_OR_RAISE(raw_pos_, raw_->Tell());
      DCHECK_GE(raw_pos_, 0);
    }
    // Shift by bytes_buffered to return semantic stream position
    return raw_pos_ - bytes_buffered_;
  }

  Status SetBufferSize(int64_t new_buffer_size) {
    if (new_buffer_size <= 0) {
      return Status::Invalid("Buffer size should be positive");
    }
    if ((buffer_pos_ + bytes_buffered_) >= new_buffer_size) {
      return Status::Invalid("Cannot shrink read buffer if buffered data remains");
    }
    return ResizeBuffer(new_buffer_size);
  }

  Result<std::string_view> Peek(int64_t nbytes) {
    if (raw_read_bound_ >= 0) {
      // Do not try to peek more than the total remaining number of bytes.
      nbytes = std::min(nbytes, bytes_buffered_ + (raw_read_bound_ - raw_read_total_));
    }

    if (bytes_buffered_ == 0 && nbytes < buffer_size_) {
      // Pre-buffer for small reads
      RETURN_NOT_OK(BufferIfNeeded());
    }

    // Increase the buffer size if needed.
    if (nbytes > buffer_->size() - buffer_pos_) {
      RETURN_NOT_OK(SetBufferSize(nbytes + buffer_pos_));
      DCHECK(buffer_->size() - buffer_pos_ >= nbytes);
    }
    // Read more data when buffer has insufficient left
    if (nbytes > bytes_buffered_) {
      int64_t additional_bytes_to_read = nbytes - bytes_buffered_;
      if (raw_read_bound_ >= 0) {
        additional_bytes_to_read =
            std::min(additional_bytes_to_read, raw_read_bound_ - raw_read_total_);
      }
      ARROW_ASSIGN_OR_RAISE(
          int64_t bytes_read,
          raw_->Read(additional_bytes_to_read,
                     buffer_->mutable_data() + buffer_pos_ + bytes_buffered_));
      bytes_buffered_ += bytes_read;
      raw_read_total_ += bytes_read;
      nbytes = bytes_buffered_;
    }
    DCHECK(nbytes <= bytes_buffered_);  // Enough bytes available
    return std::string_view(reinterpret_cast<const char*>(buffer_data_ + buffer_pos_),
                            static_cast<size_t>(nbytes));
  }

  int64_t bytes_buffered() const { return bytes_buffered_; }

  int64_t buffer_size() const { return buffer_size_; }

  std::shared_ptr<InputStream> Detach() {
    is_open_ = false;
    return std::move(raw_);
  }

  void RewindBuffer() {
    // Invalidate buffered data, as with a Seek or large Read
    buffer_pos_ = bytes_buffered_ = 0;
  }

  Status DoBuffer() {
    // Fill buffer
    if (!buffer_) {
      RETURN_NOT_OK(ResetBuffer());
    }

    int64_t bytes_to_buffer = buffer_size_;
    if (raw_read_bound_ >= 0) {
      bytes_to_buffer = std::min(buffer_size_, raw_read_bound_ - raw_read_total_);
    }
    ARROW_ASSIGN_OR_RAISE(bytes_buffered_, raw_->Read(bytes_to_buffer, buffer_data_));
    buffer_pos_ = 0;
    raw_read_total_ += bytes_buffered_;

    // Do not make assumptions about the raw stream position
    raw_pos_ = -1;
    return Status::OK();
  }

  Status BufferIfNeeded() {
    if (bytes_buffered_ == 0) {
      return DoBuffer();
    }
    return Status::OK();
  }

  void ConsumeBuffer(int64_t nbytes) {
    buffer_pos_ += nbytes;
    bytes_buffered_ -= nbytes;
  }

  Result<int64_t> Read(int64_t nbytes, void* out) {
    if (ARROW_PREDICT_FALSE(nbytes < 0)) {
      return Status::Invalid("Bytes to read must be positive. Received:", nbytes);
    }

    // 1. First consume pre-buffered data.
    int64_t pre_buffer_copy_bytes = std::min(nbytes, bytes_buffered_);
    if (pre_buffer_copy_bytes > 0) {
      memcpy(out, buffer_data_ + buffer_pos_, pre_buffer_copy_bytes);
      ConsumeBuffer(pre_buffer_copy_bytes);
    }
    int64_t remaining_bytes = nbytes - pre_buffer_copy_bytes;
    if (raw_read_bound_ >= 0) {
      remaining_bytes = std::min(remaining_bytes, raw_read_bound_ - raw_read_total_);
    }
    if (remaining_bytes == 0) {
      return pre_buffer_copy_bytes;
    }
    DCHECK_EQ(0, bytes_buffered_);

    // 2. Read from storage.
    if (remaining_bytes >= buffer_size_) {
      // 2.1. If read is larger than buffer size, read directly from storage.
      ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                            raw_->Read(remaining_bytes, reinterpret_cast<uint8_t*>(out) +
                                                            pre_buffer_copy_bytes));
      raw_read_total_ += bytes_read;
      RewindBuffer();
      return pre_buffer_copy_bytes + bytes_read;
    } else {
      // 2.2. If read is smaller than buffer size, fill buffer and copy from buffer.
      RETURN_NOT_OK(DoBuffer());
      int64_t bytes_copy_after_buffer = std::min(bytes_buffered_, remaining_bytes);
      memcpy(reinterpret_cast<uint8_t*>(out) + pre_buffer_copy_bytes,
             buffer_data_ + buffer_pos_, bytes_copy_after_buffer);
      ConsumeBuffer(bytes_copy_after_buffer);
      return pre_buffer_copy_bytes + bytes_copy_after_buffer;
    }
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(nbytes, pool_));

    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));

    if (bytes_read < nbytes) {
      // Change size but do not reallocate internal capacity
      RETURN_NOT_OK(buffer->Resize(bytes_read, false /* shrink_to_fit */));
      buffer->ZeroPadding();
    }
    return std::move(buffer);
  }

  // For providing access to the raw file handles
  std::shared_ptr<InputStream> raw() const { return raw_; }

 private:
  std::shared_ptr<InputStream> raw_;
  int64_t raw_read_total_;
  int64_t raw_read_bound_;

  // Number of remaining bytes in the buffer, to be reduced on each read from
  // the buffer
  int64_t bytes_buffered_;
};

BufferedInputStream::BufferedInputStream(std::shared_ptr<InputStream> raw,
                                         MemoryPool* pool,
                                         int64_t raw_total_bytes_bound) {
  impl_ = std::make_unique<Impl>(std::move(raw), pool, raw_total_bytes_bound);
}

BufferedInputStream::~BufferedInputStream() { internal::CloseFromDestructor(this); }

Result<std::shared_ptr<BufferedInputStream>> BufferedInputStream::Create(
    int64_t buffer_size, MemoryPool* pool, std::shared_ptr<InputStream> raw,
    int64_t raw_total_bytes_bound) {
  auto result = std::shared_ptr<BufferedInputStream>(
      new BufferedInputStream(std::move(raw), pool, raw_total_bytes_bound));
  RETURN_NOT_OK(result->SetBufferSize(buffer_size));
  return result;
}

Status BufferedInputStream::DoClose() { return impl_->Close(); }

Status BufferedInputStream::DoAbort() { return impl_->Abort(); }

bool BufferedInputStream::closed() const { return impl_->closed(); }

std::shared_ptr<InputStream> BufferedInputStream::Detach() { return impl_->Detach(); }

std::shared_ptr<InputStream> BufferedInputStream::raw() const { return impl_->raw(); }

Result<int64_t> BufferedInputStream::DoTell() const { return impl_->Tell(); }

Result<std::string_view> BufferedInputStream::DoPeek(int64_t nbytes) {
  return impl_->Peek(nbytes);
}

Status BufferedInputStream::SetBufferSize(int64_t new_buffer_size) {
  return impl_->SetBufferSize(new_buffer_size);
}

int64_t BufferedInputStream::bytes_buffered() const { return impl_->bytes_buffered(); }

int64_t BufferedInputStream::buffer_size() const { return impl_->buffer_size(); }

Result<int64_t> BufferedInputStream::DoRead(int64_t nbytes, void* out) {
  return impl_->Read(nbytes, out);
}

Result<std::shared_ptr<Buffer>> BufferedInputStream::DoRead(int64_t nbytes) {
  return impl_->Read(nbytes);
}

Result<std::shared_ptr<const KeyValueMetadata>> BufferedInputStream::ReadMetadata() {
  return impl_->raw()->ReadMetadata();
}

Future<std::shared_ptr<const KeyValueMetadata>> BufferedInputStream::ReadMetadataAsync(
    const IOContext& io_context) {
  return impl_->raw()->ReadMetadataAsync(io_context);
}

// ----------------------------------------------------------------------
// ChunkBufferedInputStream implementation

ChunkBufferedInputStream::ChunkBufferedInputStream(
    int64_t start, int64_t length, std::shared_ptr<RandomAccessFile> raw,
    std::vector<ReadRange> read_ranges, int64_t buffer_size, int32_t io_merge_threshold,
    std::shared_ptr<ResizableBuffer> buffer)
    : is_open_(true),
      raw_pos_(start),
      raw_end_(start + length),
      raw_(std::move(raw)),
      read_ranges_(std::move(read_ranges)),
      current_range_available_bytes_(read_ranges_[read_ranges_idx_].length),
      buffer_size_(buffer_size),
      io_merge_threshold_(io_merge_threshold),
      buffer_(std::move(buffer)) {}

Result<std::shared_ptr<ChunkBufferedInputStream>> ChunkBufferedInputStream::Create(
    int64_t start, int64_t length, std::shared_ptr<RandomAccessFile> impl,
    std::vector<ReadRange> read_ranges, int64_t buffer_size, int32_t io_merge_threshold,
    MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto buffer, ::arrow::AllocateResizableBuffer(0, pool));
  return std::shared_ptr<ChunkBufferedInputStream>(
      new ChunkBufferedInputStream(start, length, std::move(impl), std::move(read_ranges),
                                   buffer_size, io_merge_threshold, std::move(buffer)));
}

Status ChunkBufferedInputStream::Advance(int64_t nbytes) {
  RETURN_NOT_OK(BufferIfNeeded(nbytes));
  auto bytes_read = std::min(current_range_available_bytes_, nbytes);
  ConsumeBuffer(bytes_read);
  return Status::OK();
}

Result<std::string_view> ChunkBufferedInputStream::Peek(int64_t nbytes) {
  RETURN_NOT_OK(BufferIfNeeded(nbytes));
  auto bytes_read = std::min(current_range_available_bytes_, nbytes);
  return std::string_view(reinterpret_cast<const char*>(buffer_->data() + buffer_pos_),
                          bytes_read);
}

Result<int64_t> ChunkBufferedInputStream::Read(int64_t nbytes, void* out) {
  RETURN_NOT_OK(BufferIfNeeded(nbytes));
  auto bytes_read = std::min(current_range_available_bytes_, nbytes);
  std::memcpy(out, buffer_->data() + buffer_pos_, bytes_read);
  ConsumeBuffer(bytes_read);
  return nbytes;
}

Result<std::shared_ptr<Buffer>> ChunkBufferedInputStream::Read(int64_t nbytes) {
  RETURN_NOT_OK(BufferIfNeeded(nbytes));
  auto bytes_read = std::min(current_range_available_bytes_, nbytes);
  auto buffer = SliceBuffer(buffer_, buffer_pos_, bytes_read);
  ConsumeBuffer(bytes_read);
  return std::move(buffer);
}

Status ChunkBufferedInputStream::Close() {
  is_open_ = false;
  return Status::OK();
}

Result<int64_t> ChunkBufferedInputStream::Tell() const {
  RETURN_NOT_OK(CheckClosed());
  return raw_pos_;
}

bool ChunkBufferedInputStream::closed() const { return !is_open_; }

Status ChunkBufferedInputStream::BufferIfNeeded(int64_t nbytes) {
  RETURN_NOT_OK(CheckClosed());
  RETURN_NOT_OK(CheckReadRange(nbytes));

  if (ARROW_PREDICT_TRUE(bytes_buffered_ > 0)) {
    return Status::OK();
  }

  if (read_ranges_idx_ == read_ranges_.size()) {
    ARROW_DCHECK(current_range_available_bytes_ == 0);
    return Status::OK();
  }

  read_gaps_.clear();
  read_gaps_idx_ = 0;
  buffer_pos_ = 0;

  // Read range has been checked, so get read range by idx is safe
  auto next_read_range = read_ranges_[read_ranges_idx_];
  raw_pos_ = next_read_range.offset;
  int64_t to_read = next_read_range.length;
  read_gaps_.push_back(0);

  // Try to coalesce read ranges according to io_merge_threshold_ and buffer_size_
  int64_t next_raw_pos = raw_pos_ + to_read;
  for (size_t i = read_ranges_idx_ + 1; i < read_ranges_.size(); i++) {
    next_read_range = read_ranges_[i];
    if (next_raw_pos == next_read_range.offset) {
      // For consecutive read ranges, we can merge them if the total bytes to read
      // is less than buffer_size_
      if (to_read + next_read_range.length > buffer_size_) {
        break;
      }

      to_read += next_read_range.length;
      next_raw_pos += next_read_range.length;
      read_gaps_.push_back(0);
    } else if (next_read_range.offset - next_raw_pos < io_merge_threshold_) {
      // For non-consecutive read ranges, we can merge them if the gap between them
      // is less than io_merge_threshold_ and the total bytes to read is less than
      // buffer_size_
      auto gap = next_read_range.offset - next_raw_pos;
      if (to_read + gap + next_read_range.length > buffer_size_) {
        break;
      }

      to_read += (gap + next_read_range.length);
      next_raw_pos += (gap + next_read_range.length);
      read_gaps_.push_back(static_cast<int32_t>(gap));
    } else {
      break;
    }
  }

  ARROW_DCHECK(to_read > 0);
  RETURN_NOT_OK(CheckPosition(to_read));
  RETURN_NOT_OK(EnsureBufferCapacity(to_read));
  ARROW_ASSIGN_OR_RAISE(auto bytes_read,
                        raw_->ReadAt(raw_pos_, to_read, buffer_->mutable_data()));
  ARROW_CHECK(bytes_read == to_read);
  raw_pos_ += bytes_read;
  bytes_buffered_ = bytes_read;
  current_range_available_bytes_ = read_ranges_[read_ranges_idx_].length;

  return Status::OK();
}

}  // namespace io
}  // namespace arrow
