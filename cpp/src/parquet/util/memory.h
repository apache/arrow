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

#ifndef PARQUET_UTIL_MEMORY_H
#define PARQUET_UTIL_MEMORY_H

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/memory_pool.h"

#include "parquet/exception.h"
#include "parquet/types.h"
#include "parquet/util/macros.h"
#include "parquet/util/visibility.h"

namespace arrow {
namespace util {

class Codec;

}  // namespace util
}  // namespace arrow

namespace parquet {

PARQUET_EXPORT
std::unique_ptr<::arrow::util::Codec> GetCodecFromArrow(Compression::type codec);

static constexpr int64_t kInMemoryDefaultCapacity = 1024;

using Buffer = ::arrow::Buffer;
using MutableBuffer = ::arrow::MutableBuffer;
using ResizableBuffer = ::arrow::ResizableBuffer;
using ResizableBuffer = ::arrow::ResizableBuffer;

template <class T>
class PARQUET_EXPORT Vector {
 public:
  explicit Vector(int64_t size, ::arrow::MemoryPool* pool);
  void Resize(int64_t new_size);
  void Reserve(int64_t new_capacity);
  void Assign(int64_t size, const T val);
  void Swap(Vector<T>& v);
  inline T& operator[](int64_t i) const { return data_[i]; }

  T* data() { return data_; }
  const T* data() const { return data_; }

 private:
  std::shared_ptr<ResizableBuffer> buffer_;
  int64_t size_;
  int64_t capacity_;
  T* data_;

  PARQUET_DISALLOW_COPY_AND_ASSIGN(Vector);
};

// File input and output interfaces that translate arrow::Status to exceptions

class PARQUET_EXPORT FileInterface {
 public:
  virtual ~FileInterface() = default;

  // Close the file
  virtual void Close() = 0;

  // Return the current position in the file relative to the start
  virtual int64_t Tell() = 0;
};

/// It is the responsibility of implementations to mind threadsafety of shared
/// resources
class PARQUET_EXPORT RandomAccessSource : virtual public FileInterface {
 public:
  virtual ~RandomAccessSource() = default;

  virtual int64_t Size() const = 0;

  // Returns bytes read
  virtual int64_t Read(int64_t nbytes, uint8_t* out) = 0;

  virtual std::shared_ptr<Buffer> Read(int64_t nbytes) = 0;

  virtual std::shared_ptr<Buffer> ReadAt(int64_t position, int64_t nbytes) = 0;

  /// Returns bytes read
  virtual int64_t ReadAt(int64_t position, int64_t nbytes, uint8_t* out) = 0;
};

class PARQUET_EXPORT OutputStream : virtual public FileInterface {
 public:
  virtual ~OutputStream() = default;

  // Copy bytes into the output stream
  virtual void Write(const uint8_t* data, int64_t length) = 0;
};

class PARQUET_EXPORT ArrowFileMethods : virtual public FileInterface {
 public:
  // No-op. Closing the file is the responsibility of the owner of the handle
  void Close() override;

  int64_t Tell() override;

 protected:
  virtual ::arrow::io::FileInterface* file_interface() = 0;
};

// Suppress C4250 warning caused by diamond inheritance
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4250)
#endif

/// This interface depends on the threadsafety of the underlying Arrow file interface
class PARQUET_EXPORT ArrowInputFile : public ArrowFileMethods, public RandomAccessSource {
 public:
  explicit ArrowInputFile(
      const std::shared_ptr<::arrow::io::ReadableFileInterface>& file);

  int64_t Size() const override;

  // Returns bytes read
  int64_t Read(int64_t nbytes, uint8_t* out) override;

  std::shared_ptr<Buffer> Read(int64_t nbytes) override;

  std::shared_ptr<Buffer> ReadAt(int64_t position, int64_t nbytes) override;

  /// Returns bytes read
  int64_t ReadAt(int64_t position, int64_t nbytes, uint8_t* out) override;

  std::shared_ptr<::arrow::io::ReadableFileInterface> file() const { return file_; }

  // Diamond inheritance
  using ArrowFileMethods::Close;
  using ArrowFileMethods::Tell;

 private:
  ::arrow::io::FileInterface* file_interface() override;
  std::shared_ptr<::arrow::io::ReadableFileInterface> file_;
};

class PARQUET_EXPORT ArrowOutputStream : public ArrowFileMethods, public OutputStream {
 public:
  explicit ArrowOutputStream(const std::shared_ptr<::arrow::io::OutputStream> file);

  // Copy bytes into the output stream
  void Write(const uint8_t* data, int64_t length) override;

  std::shared_ptr<::arrow::io::OutputStream> file() { return file_; }

  // Diamond inheritance
  using ArrowFileMethods::Close;
  using ArrowFileMethods::Tell;

 private:
  ::arrow::io::FileInterface* file_interface() override;
  std::shared_ptr<::arrow::io::OutputStream> file_;
};

// Pop C4250 pragma
#ifdef _MSC_VER
#pragma warning(pop)
#endif

class PARQUET_EXPORT InMemoryOutputStream : public OutputStream {
 public:
  explicit InMemoryOutputStream(
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool(),
      int64_t initial_capacity = kInMemoryDefaultCapacity);

  virtual ~InMemoryOutputStream();

  // Close is currently a no-op with the in-memory stream
  virtual void Close() {}

  virtual int64_t Tell();

  virtual void Write(const uint8_t* data, int64_t length);

  // Clears the stream
  void Clear() { size_ = 0; }

  // Get pointer to the underlying buffer
  const Buffer& GetBufferRef() const { return *buffer_; }

  // Return complete stream as Buffer
  std::shared_ptr<Buffer> GetBuffer();

 private:
  // Mutable pointer to the current write position in the stream
  uint8_t* Head();

  std::shared_ptr<ResizableBuffer> buffer_;
  int64_t size_;
  int64_t capacity_;

  PARQUET_DISALLOW_COPY_AND_ASSIGN(InMemoryOutputStream);
};

// ----------------------------------------------------------------------
// Streaming input interfaces

// Interface for the column reader to get the bytes. The interface is a stream
// interface, meaning the bytes in order and once a byte is read, it does not
// need to be read again.
class PARQUET_EXPORT InputStream {
 public:
  // Returns the next 'num_to_peek' without advancing the current position.
  // *num_bytes will contain the number of bytes returned which can only be
  // less than num_to_peek at end of stream cases.
  // Since the position is not advanced, calls to this function are idempotent.
  // The buffer returned to the caller is still owned by the input stream and must
  // stay valid until the next call to Peek() or Read().
  virtual const uint8_t* Peek(int64_t num_to_peek, int64_t* num_bytes) = 0;

  // Identical to Peek(), except the current position in the stream is advanced by
  // *num_bytes.
  virtual const uint8_t* Read(int64_t num_to_read, int64_t* num_bytes) = 0;

  // Advance the stream without reading
  virtual void Advance(int64_t num_bytes) = 0;

  virtual ~InputStream() {}

 protected:
  InputStream() {}
};

// Implementation of an InputStream when all the bytes are in memory.
class PARQUET_EXPORT InMemoryInputStream : public InputStream {
 public:
  InMemoryInputStream(RandomAccessSource* source, int64_t start, int64_t end);
  explicit InMemoryInputStream(const std::shared_ptr<Buffer>& buffer);
  virtual const uint8_t* Peek(int64_t num_to_peek, int64_t* num_bytes);
  virtual const uint8_t* Read(int64_t num_to_read, int64_t* num_bytes);

  virtual void Advance(int64_t num_bytes);

 private:
  std::shared_ptr<Buffer> buffer_;
  int64_t len_;
  int64_t offset_;
};

// Implementation of an InputStream when only some of the bytes are in memory.
class PARQUET_EXPORT BufferedInputStream : public InputStream {
 public:
  BufferedInputStream(::arrow::MemoryPool* pool, int64_t buffer_size,
                      RandomAccessSource* source, int64_t start, int64_t end);
  virtual const uint8_t* Peek(int64_t num_to_peek, int64_t* num_bytes);
  virtual const uint8_t* Read(int64_t num_to_read, int64_t* num_bytes);

  virtual void Advance(int64_t num_bytes);

 private:
  std::shared_ptr<ResizableBuffer> buffer_;
  RandomAccessSource* source_;
  int64_t stream_offset_;
  int64_t stream_end_;
  int64_t buffer_offset_;
  int64_t buffer_size_;
};

std::shared_ptr<ResizableBuffer> PARQUET_EXPORT AllocateBuffer(
    ::arrow::MemoryPool* pool = ::arrow::default_memory_pool(), int64_t size = 0);

}  // namespace parquet

#endif  // PARQUET_UTIL_MEMORY_H
