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

template <class T>
Vector<T>::Vector(int64_t size, MemoryPool* pool)
    : buffer_(AllocateBuffer(pool, size * sizeof(T))), size_(size), capacity_(size) {
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

std::shared_ptr<ResizableBuffer> AllocateBuffer(MemoryPool* pool, int64_t size) {
  std::shared_ptr<ResizableBuffer> result;
  PARQUET_THROW_NOT_OK(arrow::AllocateResizableBuffer(pool, size, &result));
  return result;
}

}  // namespace parquet
