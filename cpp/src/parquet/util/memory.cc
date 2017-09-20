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

#include "parquet/util/memory.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <utility>

#include "arrow/status.h"
#include "arrow/util/bit-util.h"

#include "parquet/exception.h"
#include "parquet/types.h"
#include "parquet/util/logging.h"

using arrow::MemoryPool;

namespace parquet {

template <class T>
Vector<T>::Vector(int64_t size, MemoryPool* pool)
    : buffer_(AllocateUniqueBuffer(pool, size * sizeof(T))),
      size_(size),
      capacity_(size) {
  if (size > 0) {
    data_ = reinterpret_cast<T*>(buffer_->mutable_data());
  } else {
    data_ = nullptr;
  }
}

template <class T>
void Vector<T>::Reserve(int64_t new_capacity) {
  if (new_capacity > capacity_) {
    PARQUET_THROW_NOT_OK(buffer_->Resize(new_capacity * sizeof(T)));
    data_ = reinterpret_cast<T*>(buffer_->mutable_data());
    capacity_ = new_capacity;
  }
}

template <class T>
void Vector<T>::Resize(int64_t new_size) {
  Reserve(new_size);
  size_ = new_size;
}

template <class T>
void Vector<T>::Assign(int64_t size, const T val) {
  Resize(size);
  for (int64_t i = 0; i < size_; i++) {
    data_[i] = val;
  }
}

template <class T>
void Vector<T>::Swap(Vector<T>& v) {
  buffer_.swap(v.buffer_);
  std::swap(size_, v.size_);
  std::swap(capacity_, v.capacity_);
  std::swap(data_, v.data_);
}

template class Vector<int32_t>;
template class Vector<int64_t>;
template class Vector<bool>;
template class Vector<float>;
template class Vector<double>;
template class Vector<Int96>;
template class Vector<ByteArray>;
template class Vector<FixedLenByteArray>;

const int ChunkedAllocator::INITIAL_CHUNK_SIZE;
const int ChunkedAllocator::MAX_CHUNK_SIZE;

ChunkedAllocator::ChunkedAllocator(MemoryPool* pool)
    : current_chunk_idx_(-1),
      next_chunk_size_(INITIAL_CHUNK_SIZE),
      total_allocated_bytes_(0),
      peak_allocated_bytes_(0),
      total_reserved_bytes_(0),
      pool_(pool) {}

ChunkedAllocator::ChunkInfo::ChunkInfo(int64_t size, uint8_t* buf)
    : data(buf), size(size), allocated_bytes(0) {}

ChunkedAllocator::~ChunkedAllocator() {
  int64_t total_bytes_released = 0;
  for (size_t i = 0; i < chunks_.size(); ++i) {
    total_bytes_released += chunks_[i].size;
    pool_->Free(chunks_[i].data, chunks_[i].size);
  }

  DCHECK(chunks_.empty()) << "Must call FreeAll() or AcquireData() for this pool";
}

void ChunkedAllocator::ReturnPartialAllocation(int byte_size) {
  DCHECK_GE(byte_size, 0);
  DCHECK(current_chunk_idx_ != -1);
  ChunkInfo& info = chunks_[current_chunk_idx_];
  DCHECK_GE(info.allocated_bytes, byte_size);
  info.allocated_bytes -= byte_size;
  total_allocated_bytes_ -= byte_size;
}

template <bool CHECK_LIMIT_FIRST>
uint8_t* ChunkedAllocator::Allocate(int size) {
  if (size == 0) {
    return nullptr;
  }

  int64_t num_bytes = ::arrow::BitUtil::RoundUp(size, 8);
  if (current_chunk_idx_ == -1 ||
      num_bytes + chunks_[current_chunk_idx_].allocated_bytes >
          chunks_[current_chunk_idx_].size) {
    // If we couldn't allocate a new chunk, return nullptr.
    if (ARROW_PREDICT_FALSE(!FindChunk(num_bytes))) {
      return nullptr;
    }
  }
  ChunkInfo& info = chunks_[current_chunk_idx_];
  uint8_t* result = info.data + info.allocated_bytes;
  DCHECK_LE(info.allocated_bytes + num_bytes, info.size);
  info.allocated_bytes += num_bytes;
  total_allocated_bytes_ += num_bytes;
  DCHECK_LE(current_chunk_idx_, static_cast<int>(chunks_.size()) - 1);
  peak_allocated_bytes_ = std::max(total_allocated_bytes_, peak_allocated_bytes_);
  return result;
}

uint8_t* ChunkedAllocator::Allocate(int size) { return Allocate<false>(size); }

void ChunkedAllocator::Clear() {
  current_chunk_idx_ = -1;
  for (auto chunk = chunks_.begin(); chunk != chunks_.end(); ++chunk) {
    chunk->allocated_bytes = 0;
  }
  total_allocated_bytes_ = 0;
  DCHECK(CheckIntegrity(false));
}

void ChunkedAllocator::FreeAll() {
  int64_t total_bytes_released = 0;
  for (size_t i = 0; i < chunks_.size(); ++i) {
    total_bytes_released += chunks_[i].size;
    pool_->Free(chunks_[i].data, chunks_[i].size);
  }
  chunks_.clear();
  next_chunk_size_ = INITIAL_CHUNK_SIZE;
  current_chunk_idx_ = -1;
  total_allocated_bytes_ = 0;
  total_reserved_bytes_ = 0;
}

bool ChunkedAllocator::FindChunk(int64_t min_size) {
  // Try to allocate from a free chunk. The first free chunk, if any, will be immediately
  // after the current chunk.
  int first_free_idx = current_chunk_idx_ + 1;
  // (cast size() to signed int in order to avoid everything else being cast to
  // unsigned long, in particular -1)
  while (++current_chunk_idx_ < static_cast<int>(chunks_.size())) {
    // we found a free chunk
    DCHECK_EQ(chunks_[current_chunk_idx_].allocated_bytes, 0);

    if (chunks_[current_chunk_idx_].size >= min_size) {
      // This chunk is big enough.  Move it before the other free chunks.
      if (current_chunk_idx_ != first_free_idx) {
        std::swap(chunks_[current_chunk_idx_], chunks_[first_free_idx]);
        current_chunk_idx_ = first_free_idx;
      }
      break;
    }
  }

  if (current_chunk_idx_ == static_cast<int>(chunks_.size())) {
    // need to allocate new chunk.
    int64_t chunk_size;
    DCHECK_GE(next_chunk_size_, INITIAL_CHUNK_SIZE);
    DCHECK_LE(next_chunk_size_, MAX_CHUNK_SIZE);

    chunk_size = std::max<int64_t>(min_size, next_chunk_size_);

    // Allocate a new chunk. Return early if malloc fails.
    uint8_t* buf = nullptr;
    PARQUET_THROW_NOT_OK(pool_->Allocate(chunk_size, &buf));
    if (ARROW_PREDICT_FALSE(buf == nullptr)) {
      DCHECK_EQ(current_chunk_idx_, static_cast<int>(chunks_.size()));
      current_chunk_idx_ = static_cast<int>(chunks_.size()) - 1;
      return false;
    }

    // If there are no free chunks put it at the end, otherwise before the first free.
    if (first_free_idx == static_cast<int>(chunks_.size())) {
      chunks_.push_back(ChunkInfo(chunk_size, buf));
    } else {
      current_chunk_idx_ = first_free_idx;
      auto insert_chunk = chunks_.begin() + current_chunk_idx_;
      chunks_.insert(insert_chunk, ChunkInfo(chunk_size, buf));
    }
    total_reserved_bytes_ += chunk_size;
    // Don't increment the chunk size until the allocation succeeds: if an attempted
    // large allocation fails we don't want to increase the chunk size further.
    next_chunk_size_ =
        static_cast<int>(std::min<int64_t>(chunk_size * 2, MAX_CHUNK_SIZE));
  }

  DCHECK_LT(current_chunk_idx_, static_cast<int>(chunks_.size()));
  DCHECK(CheckIntegrity(true));
  return true;
}

void ChunkedAllocator::AcquireData(ChunkedAllocator* src, bool keep_current) {
  DCHECK(src->CheckIntegrity(false));
  int num_acquired_chunks;
  if (keep_current) {
    num_acquired_chunks = src->current_chunk_idx_;
  } else if (src->GetFreeOffset() == 0) {
    // nothing in the last chunk
    num_acquired_chunks = src->current_chunk_idx_;
  } else {
    num_acquired_chunks = src->current_chunk_idx_ + 1;
  }

  if (num_acquired_chunks <= 0) {
    if (!keep_current) src->FreeAll();
    return;
  }

  auto end_chunk = src->chunks_.begin() + num_acquired_chunks;
  int64_t total_transfered_bytes = 0;
  for (auto i = src->chunks_.begin(); i != end_chunk; ++i) {
    total_transfered_bytes += i->size;
  }
  src->total_reserved_bytes_ -= total_transfered_bytes;
  total_reserved_bytes_ += total_transfered_bytes;

  // insert new chunks after current_chunk_idx_
  auto insert_chunk = chunks_.begin() + (current_chunk_idx_ + 1);
  chunks_.insert(insert_chunk, src->chunks_.begin(), end_chunk);
  src->chunks_.erase(src->chunks_.begin(), end_chunk);
  current_chunk_idx_ += num_acquired_chunks;

  if (keep_current) {
    src->current_chunk_idx_ = 0;
    DCHECK(src->chunks_.size() == 1 || src->chunks_[1].allocated_bytes == 0);
    total_allocated_bytes_ += src->total_allocated_bytes_ - src->GetFreeOffset();
    src->total_allocated_bytes_ = src->GetFreeOffset();
  } else {
    src->current_chunk_idx_ = -1;
    total_allocated_bytes_ += src->total_allocated_bytes_;
    src->total_allocated_bytes_ = 0;
  }
  peak_allocated_bytes_ = std::max(total_allocated_bytes_, peak_allocated_bytes_);

  if (!keep_current) src->FreeAll();
  DCHECK(CheckIntegrity(false));
}

std::string ChunkedAllocator::DebugString() {
  std::stringstream out;
  char str[16];
  out << "ChunkedAllocator(#chunks=" << chunks_.size() << " [";
  for (size_t i = 0; i < chunks_.size(); ++i) {
    sprintf(str, "0x%zx=", reinterpret_cast<size_t>(chunks_[i].data));  // NOLINT
    out << (i > 0 ? " " : "") << str << chunks_[i].size << "/"
        << chunks_[i].allocated_bytes;
  }
  out << "] current_chunk=" << current_chunk_idx_
      << " total_sizes=" << GetTotalChunkSizes()
      << " total_alloc=" << total_allocated_bytes_ << ")";
  return out.str();
}

int64_t ChunkedAllocator::GetTotalChunkSizes() const {
  int64_t result = 0;
  for (size_t i = 0; i < chunks_.size(); ++i) {
    result += chunks_[i].size;
  }
  return result;
}

bool ChunkedAllocator::CheckIntegrity(bool current_chunk_empty) {
  // check that current_chunk_idx_ points to the last chunk with allocated data
  DCHECK_LT(current_chunk_idx_, static_cast<int>(chunks_.size()));
  int64_t total_allocated = 0;
  for (int i = 0; i < static_cast<int>(chunks_.size()); ++i) {
    DCHECK_GT(chunks_[i].size, 0);
    if (i < current_chunk_idx_) {
      DCHECK_GT(chunks_[i].allocated_bytes, 0);
    } else if (i == current_chunk_idx_) {
      if (current_chunk_empty) {
        DCHECK_EQ(chunks_[i].allocated_bytes, 0);
      } else {
        DCHECK_GT(chunks_[i].allocated_bytes, 0);
      }
    } else {
      DCHECK_EQ(chunks_[i].allocated_bytes, 0);
    }
    total_allocated += chunks_[i].allocated_bytes;
  }
  DCHECK_EQ(total_allocated, total_allocated_bytes_);
  return true;
}

// ----------------------------------------------------------------------
// Arrow IO wrappers

void ArrowFileMethods::Close() {
  // Closing the file is the responsibility of the owner of the handle
  return;
}

// Return the current position in the output stream relative to the start
int64_t ArrowFileMethods::Tell() {
  int64_t position = 0;
  PARQUET_THROW_NOT_OK(file_interface()->Tell(&position));
  return position;
}

ArrowInputFile::ArrowInputFile(
    const std::shared_ptr<::arrow::io::ReadableFileInterface>& file)
    : file_(file) {}

::arrow::io::FileInterface* ArrowInputFile::file_interface() { return file_.get(); }

int64_t ArrowInputFile::Size() const {
  int64_t size;
  PARQUET_THROW_NOT_OK(file_->GetSize(&size));
  return size;
}

// Returns bytes read
int64_t ArrowInputFile::Read(int64_t nbytes, uint8_t* out) {
  int64_t bytes_read = 0;
  PARQUET_THROW_NOT_OK(file_->Read(nbytes, &bytes_read, out));
  return bytes_read;
}

std::shared_ptr<Buffer> ArrowInputFile::Read(int64_t nbytes) {
  std::shared_ptr<Buffer> out;
  PARQUET_THROW_NOT_OK(file_->Read(nbytes, &out));
  return out;
}

std::shared_ptr<Buffer> ArrowInputFile::ReadAt(int64_t position, int64_t nbytes) {
  std::shared_ptr<Buffer> out;
  PARQUET_THROW_NOT_OK(file_->ReadAt(position, nbytes, &out));
  return out;
}

int64_t ArrowInputFile::ReadAt(int64_t position, int64_t nbytes, uint8_t* out) {
  int64_t bytes_read = 0;
  PARQUET_THROW_NOT_OK(file_->ReadAt(position, nbytes, &bytes_read, out));
  return bytes_read;
}

ArrowOutputStream::ArrowOutputStream(
    const std::shared_ptr<::arrow::io::OutputStream> file)
    : file_(file) {}

::arrow::io::FileInterface* ArrowOutputStream::file_interface() { return file_.get(); }

// Copy bytes into the output stream
void ArrowOutputStream::Write(const uint8_t* data, int64_t length) {
  PARQUET_THROW_NOT_OK(file_->Write(data, length));
}

// ----------------------------------------------------------------------
// InMemoryInputStream

InMemoryInputStream::InMemoryInputStream(const std::shared_ptr<Buffer>& buffer)
    : buffer_(buffer), offset_(0) {
  len_ = buffer_->size();
}

InMemoryInputStream::InMemoryInputStream(RandomAccessSource* source, int64_t start,
                                         int64_t num_bytes)
    : offset_(0) {
  buffer_ = source->ReadAt(start, num_bytes);
  if (buffer_->size() < num_bytes) {
    throw ParquetException("Unable to read column chunk data");
  }
  len_ = buffer_->size();
}

const uint8_t* InMemoryInputStream::Peek(int64_t num_to_peek, int64_t* num_bytes) {
  *num_bytes = std::min(static_cast<int64_t>(num_to_peek), len_ - offset_);
  return buffer_->data() + offset_;
}

const uint8_t* InMemoryInputStream::Read(int64_t num_to_read, int64_t* num_bytes) {
  const uint8_t* result = Peek(num_to_read, num_bytes);
  offset_ += *num_bytes;
  return result;
}

void InMemoryInputStream::Advance(int64_t num_bytes) { offset_ += num_bytes; }

// ----------------------------------------------------------------------
// In-memory output stream

InMemoryOutputStream::InMemoryOutputStream(MemoryPool* pool, int64_t initial_capacity)
    : size_(0), capacity_(initial_capacity) {
  if (initial_capacity == 0) {
    initial_capacity = kInMemoryDefaultCapacity;
  }
  buffer_ = AllocateBuffer(pool, initial_capacity);
}

InMemoryOutputStream::~InMemoryOutputStream() {}

uint8_t* InMemoryOutputStream::Head() { return buffer_->mutable_data() + size_; }

void InMemoryOutputStream::Write(const uint8_t* data, int64_t length) {
  if (size_ + length > capacity_) {
    int64_t new_capacity = capacity_ * 2;
    while (new_capacity < size_ + length) {
      new_capacity *= 2;
    }
    PARQUET_THROW_NOT_OK(buffer_->Resize(new_capacity));
    capacity_ = new_capacity;
  }
  memcpy(Head(), data, length);
  size_ += length;
}

int64_t InMemoryOutputStream::Tell() { return size_; }

std::shared_ptr<Buffer> InMemoryOutputStream::GetBuffer() {
  PARQUET_THROW_NOT_OK(buffer_->Resize(size_));
  std::shared_ptr<Buffer> result = buffer_;
  buffer_ = nullptr;
  return result;
}

// ----------------------------------------------------------------------
// BufferedInputStream

BufferedInputStream::BufferedInputStream(MemoryPool* pool, int64_t buffer_size,
                                         RandomAccessSource* source, int64_t start,
                                         int64_t num_bytes)
    : source_(source), stream_offset_(start), stream_end_(start + num_bytes) {
  buffer_ = AllocateBuffer(pool, buffer_size);
  buffer_size_ = buffer_->size();
  // Required to force a lazy read
  buffer_offset_ = buffer_size_;
}

const uint8_t* BufferedInputStream::Peek(int64_t num_to_peek, int64_t* num_bytes) {
  *num_bytes = std::min(num_to_peek, stream_end_ - stream_offset_);
  // increase the buffer size if needed
  if (*num_bytes > buffer_size_) {
    PARQUET_THROW_NOT_OK(buffer_->Resize(*num_bytes));
    buffer_size_ = buffer_->size();
    DCHECK(buffer_size_ >= *num_bytes);
  }
  // Read more data when buffer has insufficient left or when resized
  if (*num_bytes > (buffer_size_ - buffer_offset_)) {
    buffer_size_ = std::min(buffer_size_, stream_end_ - stream_offset_);
    int64_t bytes_read =
        source_->ReadAt(stream_offset_, buffer_size_, buffer_->mutable_data());
    if (bytes_read < *num_bytes) {
      throw ParquetException("Failed reading column data from source");
    }
    buffer_offset_ = 0;
  }
  return buffer_->data() + buffer_offset_;
}

const uint8_t* BufferedInputStream::Read(int64_t num_to_read, int64_t* num_bytes) {
  const uint8_t* result = Peek(num_to_read, num_bytes);
  stream_offset_ += *num_bytes;
  buffer_offset_ += *num_bytes;
  return result;
}

void BufferedInputStream::Advance(int64_t num_bytes) {
  stream_offset_ += num_bytes;
  buffer_offset_ += num_bytes;
}

std::shared_ptr<PoolBuffer> AllocateBuffer(MemoryPool* pool, int64_t size) {
  auto result = std::make_shared<PoolBuffer>(pool);
  if (size > 0) {
    PARQUET_THROW_NOT_OK(result->Resize(size));
  }
  return result;
}

std::unique_ptr<PoolBuffer> AllocateUniqueBuffer(MemoryPool* pool, int64_t size) {
  std::unique_ptr<PoolBuffer> result(new PoolBuffer(pool));
  if (size > 0) {
    PARQUET_THROW_NOT_OK(result->Resize(size));
  }
  return result;
}

}  // namespace parquet
