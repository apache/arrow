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

#include "arrow/util/compression.h"
#include "arrow/util/logging.h"
#include "parquet/exception.h"
#include "parquet/types.h"

using arrow::MemoryPool;
using arrow::util::Codec;

namespace parquet {

std::unique_ptr<Codec> GetCodecFromArrow(Compression::type codec) {
  std::unique_ptr<Codec> result;
  switch (codec) {
    case Compression::UNCOMPRESSED:
      break;
    case Compression::SNAPPY:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::SNAPPY, &result));
      break;
    case Compression::GZIP:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::GZIP, &result));
      break;
    case Compression::LZO:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::LZO, &result));
      break;
    case Compression::BROTLI:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::BROTLI, &result));
      break;
    case Compression::LZ4:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::LZ4, &result));
      break;
    case Compression::ZSTD:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::ZSTD, &result));
      break;
    default:
      break;
  }
  return result;
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

ArrowInputFile::ArrowInputFile(const std::shared_ptr<::arrow::io::RandomAccessFile>& file)
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
  // If length == 0, data may be null
  if (length > 0) {
    memcpy(Head(), data, length);
    size_ += length;
  }
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
  buffer_offset_ = 0;
  buffer_end_ = 0;
}

int64_t BufferedInputStream::remaining_in_buffer() const {
  return buffer_end_ - buffer_offset_;
}

const uint8_t* BufferedInputStream::Peek(int64_t num_to_peek, int64_t* num_bytes) {
  int64_t buffer_avail = buffer_end_ - buffer_offset_;
  int64_t stream_avail = stream_end_ - stream_offset_;
  // Do not try to peek more than the total remaining number of bytes.
  *num_bytes = std::min(num_to_peek, buffer_avail + stream_avail);
  // Increase the buffer size if needed
  if (*num_bytes > buffer_->size() - buffer_offset_) {
    // XXX Should adopt a shrinking heuristic if buffer_offset_ is close
    // to buffer_end_.
    PARQUET_THROW_NOT_OK(buffer_->Resize(*num_bytes + buffer_offset_));
    DCHECK(buffer_->size() - buffer_offset_ >= *num_bytes);
  }
  // Read more data when buffer has insufficient left
  if (*num_bytes > buffer_avail) {
    // Read as much as possible to fill the buffer, but not past stream end
    int64_t read_size = std::min(buffer_->size() - buffer_end_, stream_avail);
    int64_t bytes_read =
        source_->ReadAt(stream_offset_, read_size, buffer_->mutable_data() + buffer_end_);
    stream_offset_ += bytes_read;
    buffer_end_ += bytes_read;
    if (bytes_read < read_size) {
      throw ParquetException("Failed reading column data from source");
    }
  }
  DCHECK(*num_bytes <= buffer_end_ - buffer_offset_);  // Enough bytes available
  return buffer_->data() + buffer_offset_;
}

const uint8_t* BufferedInputStream::Read(int64_t num_to_read, int64_t* num_bytes) {
  const uint8_t* result = Peek(num_to_read, num_bytes);
  buffer_offset_ += *num_bytes;
  if (buffer_offset_ == buffer_end_) {
    // Rewind pointer to reuse beginning of buffer
    buffer_offset_ = 0;
    buffer_end_ = 0;
  }
  return result;
}

void BufferedInputStream::Advance(int64_t num_bytes) {
  buffer_offset_ += num_bytes;
  if (buffer_offset_ == buffer_end_) {
    // Rewind pointer to reuse beginning of buffer
    buffer_offset_ = 0;
    buffer_end_ = 0;
  }
}

std::shared_ptr<ResizableBuffer> AllocateBuffer(MemoryPool* pool, int64_t size) {
  std::shared_ptr<ResizableBuffer> result;
  PARQUET_THROW_NOT_OK(arrow::AllocateResizableBuffer(pool, size, &result));
  return result;
}

}  // namespace parquet
