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

#include "arrow/io/memory.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/io/util_internal.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/future.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/memory.h"

namespace arrow {
namespace io {

// ----------------------------------------------------------------------
// OutputStream that writes to resizable buffer

static constexpr int64_t kBufferMinimumSize = 256;

BufferOutputStream::BufferOutputStream()
    : is_open_(false), capacity_(0), position_(0), mutable_data_(nullptr) {}

BufferOutputStream::BufferOutputStream(const std::shared_ptr<ResizableBuffer>& buffer)
    : buffer_(buffer),
      is_open_(true),
      capacity_(buffer->size()),
      position_(0),
      mutable_data_(buffer->mutable_data()) {}

Result<std::shared_ptr<BufferOutputStream>> BufferOutputStream::Create(
    int64_t initial_capacity, MemoryPool* pool) {
  // ctor is private, so cannot use make_shared
  auto ptr = std::shared_ptr<BufferOutputStream>(new BufferOutputStream);
  RETURN_NOT_OK(ptr->Reset(initial_capacity, pool));
  return ptr;
}

Status BufferOutputStream::Reset(int64_t initial_capacity, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(buffer_, AllocateResizableBuffer(initial_capacity, pool));
  is_open_ = true;
  capacity_ = initial_capacity;
  position_ = 0;
  mutable_data_ = buffer_->mutable_data();
  return Status::OK();
}

BufferOutputStream::~BufferOutputStream() {
  if (buffer_) {
    internal::CloseFromDestructor(this);
  }
}

Status BufferOutputStream::Close() {
  if (is_open_) {
    is_open_ = false;
    if (position_ < capacity_) {
      RETURN_NOT_OK(buffer_->Resize(position_, false));
    }
  }
  return Status::OK();
}

bool BufferOutputStream::closed() const { return !is_open_; }

Result<std::shared_ptr<Buffer>> BufferOutputStream::Finish() {
  RETURN_NOT_OK(Close());
  buffer_->ZeroPadding();
  is_open_ = false;
  return std::move(buffer_);
}

Result<int64_t> BufferOutputStream::Tell() const { return position_; }

Status BufferOutputStream::Write(const void* data, int64_t nbytes) {
  if (ARROW_PREDICT_FALSE(!is_open_)) {
    return Status::IOError("OutputStream is closed");
  }
  DCHECK(buffer_);
  if (ARROW_PREDICT_TRUE(nbytes > 0)) {
    if (ARROW_PREDICT_FALSE(position_ + nbytes >= capacity_)) {
      RETURN_NOT_OK(Reserve(nbytes));
    }
    memcpy(mutable_data_ + position_, data, nbytes);
    position_ += nbytes;
  }
  return Status::OK();
}

Status BufferOutputStream::Reserve(int64_t nbytes) {
  // Always overallocate by doubling.  It seems that it is a better growth
  // strategy, at least for memory_benchmark.cc.
  // This may be because it helps match the allocator's allocation buckets
  // more exactly.  Or perhaps it hits a sweet spot in jemalloc.
  int64_t new_capacity = std::max(kBufferMinimumSize, capacity_);
  while (new_capacity < position_ + nbytes) {
    new_capacity = new_capacity * 2;
  }
  if (new_capacity > capacity_) {
    RETURN_NOT_OK(buffer_->Resize(new_capacity));
    capacity_ = new_capacity;
    mutable_data_ = buffer_->mutable_data();
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// OutputStream that doesn't write anything

Status MockOutputStream::Close() {
  is_open_ = false;
  return Status::OK();
}

bool MockOutputStream::closed() const { return !is_open_; }

Result<int64_t> MockOutputStream::Tell() const { return extent_bytes_written_; }

Status MockOutputStream::Write(const void* data, int64_t nbytes) {
  extent_bytes_written_ += nbytes;
  return Status::OK();
}

// ----------------------------------------------------------------------
// In-memory buffer writer

static constexpr int kMemcopyDefaultNumThreads = 1;
static constexpr int64_t kMemcopyDefaultBlocksize = 64;
static constexpr int64_t kMemcopyDefaultThreshold = 1024 * 1024;

class FixedSizeBufferWriter::FixedSizeBufferWriterImpl {
 public:
  /// Input buffer must be mutable, will abort if not

  /// Input buffer must be mutable, will abort if not
  explicit FixedSizeBufferWriterImpl(const std::shared_ptr<Buffer>& buffer)
      : is_open_(true),
        memcopy_num_threads_(kMemcopyDefaultNumThreads),
        memcopy_blocksize_(kMemcopyDefaultBlocksize),
        memcopy_threshold_(kMemcopyDefaultThreshold) {
    buffer_ = buffer;
    ARROW_CHECK(buffer->is_mutable()) << "Must pass mutable buffer";
    mutable_data_ = buffer->mutable_data();
    size_ = buffer->size();
    position_ = 0;
  }

  Status Close() {
    is_open_ = false;
    return Status::OK();
  }

  bool closed() const { return !is_open_; }

  Status Seek(int64_t position) {
    if (position < 0 || position > size_) {
      return Status::IOError("Seek out of bounds");
    }
    position_ = position;
    return Status::OK();
  }

  Result<int64_t> Tell() { return position_; }

  Status Write(const void* data, int64_t nbytes) {
    RETURN_NOT_OK(internal::ValidateWriteRange(position_, nbytes, size_));
    if (nbytes > memcopy_threshold_ && memcopy_num_threads_ > 1) {
      ::arrow::internal::parallel_memcopy(mutable_data_ + position_,
                                          reinterpret_cast<const uint8_t*>(data), nbytes,
                                          memcopy_blocksize_, memcopy_num_threads_);
    } else {
      memcpy(mutable_data_ + position_, data, nbytes);
    }
    position_ += nbytes;
    return Status::OK();
  }

  Status WriteAt(int64_t position, const void* data, int64_t nbytes) {
    std::lock_guard<std::mutex> guard(lock_);
    RETURN_NOT_OK(internal::ValidateWriteRange(position, nbytes, size_));
    RETURN_NOT_OK(Seek(position));
    return Write(data, nbytes);
  }

  void set_memcopy_threads(int num_threads) { memcopy_num_threads_ = num_threads; }

  void set_memcopy_blocksize(int64_t blocksize) { memcopy_blocksize_ = blocksize; }

  void set_memcopy_threshold(int64_t threshold) { memcopy_threshold_ = threshold; }

 private:
  std::mutex lock_;
  std::shared_ptr<Buffer> buffer_;
  uint8_t* mutable_data_;
  int64_t size_;
  int64_t position_;
  bool is_open_;

  int memcopy_num_threads_;
  int64_t memcopy_blocksize_;
  int64_t memcopy_threshold_;
};

FixedSizeBufferWriter::FixedSizeBufferWriter(const std::shared_ptr<Buffer>& buffer)
    : impl_(new FixedSizeBufferWriterImpl(buffer)) {}

FixedSizeBufferWriter::~FixedSizeBufferWriter() = default;

Status FixedSizeBufferWriter::Close() { return impl_->Close(); }

bool FixedSizeBufferWriter::closed() const { return impl_->closed(); }

Status FixedSizeBufferWriter::Seek(int64_t position) { return impl_->Seek(position); }

Result<int64_t> FixedSizeBufferWriter::Tell() const { return impl_->Tell(); }

Status FixedSizeBufferWriter::Write(const void* data, int64_t nbytes) {
  return impl_->Write(data, nbytes);
}

Status FixedSizeBufferWriter::WriteAt(int64_t position, const void* data,
                                      int64_t nbytes) {
  return impl_->WriteAt(position, data, nbytes);
}

void FixedSizeBufferWriter::set_memcopy_threads(int num_threads) {
  impl_->set_memcopy_threads(num_threads);
}

void FixedSizeBufferWriter::set_memcopy_blocksize(int64_t blocksize) {
  impl_->set_memcopy_blocksize(blocksize);
}

void FixedSizeBufferWriter::set_memcopy_threshold(int64_t threshold) {
  impl_->set_memcopy_threshold(threshold);
}

// ----------------------------------------------------------------------
// In-memory buffer reader

BufferReader::BufferReader(std::shared_ptr<Buffer> buffer)
    : buffer_(std::move(buffer)),
      data_(buffer_ ? buffer_->data() : reinterpret_cast<const uint8_t*>("")),
      size_(buffer_ ? buffer_->size() : 0),
      position_(0),
      is_open_(true) {}

BufferReader::BufferReader(const uint8_t* data, int64_t size)
    : buffer_(nullptr), data_(data), size_(size), position_(0), is_open_(true) {}

BufferReader::BufferReader(const Buffer& buffer)
    : BufferReader(buffer.data(), buffer.size()) {}

BufferReader::BufferReader(std::string_view data)
    : BufferReader(reinterpret_cast<const uint8_t*>(data.data()),
                   static_cast<int64_t>(data.size())) {}

Status BufferReader::DoClose() {
  is_open_ = false;
  return Status::OK();
}

bool BufferReader::closed() const { return !is_open_; }

Result<int64_t> BufferReader::DoTell() const {
  RETURN_NOT_OK(CheckClosed());
  return position_;
}

Result<std::string_view> BufferReader::DoPeek(int64_t nbytes) {
  RETURN_NOT_OK(CheckClosed());

  const int64_t bytes_available = std::min(nbytes, size_ - position_);
  return std::string_view(reinterpret_cast<const char*>(data_) + position_,
                          static_cast<size_t>(bytes_available));
}

bool BufferReader::supports_zero_copy() const { return true; }

Status BufferReader::WillNeed(const std::vector<ReadRange>& ranges) {
  using ::arrow::internal::MemoryRegion;

  RETURN_NOT_OK(CheckClosed());

  std::vector<MemoryRegion> regions(ranges.size());
  for (size_t i = 0; i < ranges.size(); ++i) {
    const auto& range = ranges[i];
    ARROW_ASSIGN_OR_RAISE(auto size,
                          internal::ValidateReadRange(range.offset, range.length, size_));
    regions[i] = {const_cast<uint8_t*>(data_ + range.offset), static_cast<size_t>(size)};
  }
  const auto st = ::arrow::internal::MemoryAdviseWillNeed(regions);
  if (st.IsIOError()) {
    // Ignore any system-level errors, in case the memory area isn't madvise()-able
    return Status::OK();
  }
  return st;
}

Future<std::shared_ptr<Buffer>> BufferReader::ReadAsync(const IOContext&,
                                                        int64_t position,
                                                        int64_t nbytes) {
  return Future<std::shared_ptr<Buffer>>::MakeFinished(DoReadAt(position, nbytes));
}

Result<int64_t> BufferReader::DoReadAt(int64_t position, int64_t nbytes, void* buffer) {
  RETURN_NOT_OK(CheckClosed());

  ARROW_ASSIGN_OR_RAISE(nbytes, internal::ValidateReadRange(position, nbytes, size_));
  DCHECK_GE(nbytes, 0);
  if (nbytes) {
    memcpy(buffer, data_ + position, nbytes);
  }
  return nbytes;
}

Result<std::shared_ptr<Buffer>> BufferReader::DoReadAt(int64_t position, int64_t nbytes) {
  RETURN_NOT_OK(CheckClosed());

  ARROW_ASSIGN_OR_RAISE(nbytes, internal::ValidateReadRange(position, nbytes, size_));
  DCHECK_GE(nbytes, 0);

  // Arrange for data to be paged in
  // RETURN_NOT_OK(::arrow::internal::MemoryAdviseWillNeed(
  //     {{const_cast<uint8_t*>(data_ + position), static_cast<size_t>(nbytes)}}));

  if (nbytes > 0 && buffer_ != nullptr) {
    return SliceBuffer(buffer_, position, nbytes);
  } else {
    return std::make_shared<Buffer>(data_ + position, nbytes);
  }
}

Result<int64_t> BufferReader::DoRead(int64_t nbytes, void* out) {
  RETURN_NOT_OK(CheckClosed());
  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, DoReadAt(position_, nbytes, out));
  position_ += bytes_read;
  return bytes_read;
}

Result<std::shared_ptr<Buffer>> BufferReader::DoRead(int64_t nbytes) {
  RETURN_NOT_OK(CheckClosed());
  ARROW_ASSIGN_OR_RAISE(auto buffer, DoReadAt(position_, nbytes));
  position_ += buffer->size();
  return buffer;
}

Result<int64_t> BufferReader::DoGetSize() {
  RETURN_NOT_OK(CheckClosed());
  return size_;
}

Status BufferReader::DoSeek(int64_t position) {
  RETURN_NOT_OK(CheckClosed());

  if (position < 0 || position > size_) {
    return Status::IOError("Seek out of bounds");
  }

  position_ = position;
  return Status::OK();
}

}  // namespace io
}  // namespace arrow
