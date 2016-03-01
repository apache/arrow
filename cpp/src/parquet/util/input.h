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

#ifndef PARQUET_UTIL_INPUT_H
#define PARQUET_UTIL_INPUT_H

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

namespace parquet_cpp {

class Buffer;

// ----------------------------------------------------------------------
// Random access input (e.g. file-like)

// Random
class RandomAccessSource {
 public:
  virtual ~RandomAccessSource() {}

  virtual int64_t Size() const = 0;

  virtual void Close() = 0;
  virtual int64_t Tell() const = 0;
  virtual void Seek(int64_t pos) = 0;

  // Returns actual number of bytes read
  virtual int64_t Read(int64_t nbytes, uint8_t* out) = 0;

  virtual std::shared_ptr<Buffer> Read(int64_t nbytes) = 0;
  std::shared_ptr<Buffer> ReadAt(int64_t pos, int64_t nbytes);

 protected:
  int64_t size_;
};


class LocalFileSource : public RandomAccessSource {
 public:
  LocalFileSource() : file_(nullptr), is_open_(false) {}
  virtual ~LocalFileSource();

  virtual void Open(const std::string& path);

  virtual void Close();
  virtual int64_t Size() const;
  virtual int64_t Tell() const;
  virtual void Seek(int64_t pos);

  // Returns actual number of bytes read
  virtual int64_t Read(int64_t nbytes, uint8_t* out);

  virtual std::shared_ptr<Buffer> Read(int64_t nbytes);

  bool is_open() const { return is_open_;}
  const std::string& path() const { return path_;}

  // Return the integer file descriptor
  int file_descriptor() const;

 protected:
  void CloseFile();
  void SeekFile(int64_t pos, int origin = SEEK_SET);

  std::string path_;
  FILE* file_;
  bool is_open_;
};

class MemoryMapSource : public LocalFileSource {
 public:
  MemoryMapSource() :
      LocalFileSource(),
      data_(nullptr),
      pos_(0) {}

  virtual ~MemoryMapSource();

  virtual void Close();
  virtual void Open(const std::string& path);

  virtual int64_t Tell() const;
  virtual void Seek(int64_t pos);

  // Copy data from memory map into out (must be already allocated memory)
  // @returns: actual number of bytes read
  virtual int64_t Read(int64_t nbytes, uint8_t* out);

  // Return a buffer referencing memory-map (no copy)
  virtual std::shared_ptr<Buffer> Read(int64_t nbytes);

 private:
  void CloseFile();

  uint8_t* data_;
  int64_t pos_;
};

// ----------------------------------------------------------------------
// A file-like object that reads from virtual address space

class BufferReader : public RandomAccessSource {
 public:
  explicit BufferReader(const std::shared_ptr<Buffer>& buffer);
  virtual void Close() {}
  virtual int64_t Tell() const;
  virtual void Seek(int64_t pos);
  virtual int64_t Size() const;

  virtual int64_t Read(int64_t nbytes, uint8_t* out);

  virtual std::shared_ptr<Buffer> Read(int64_t nbytes);

 protected:
  const uint8_t* Head() {
    return data_ + pos_;
  }

  std::shared_ptr<Buffer> buffer_;
  const uint8_t* data_;
  int64_t pos_;
};

// ----------------------------------------------------------------------
// Streaming input interfaces

// Interface for the column reader to get the bytes. The interface is a stream
// interface, meaning the bytes in order and once a byte is read, it does not
// need to be read again.
class InputStream {
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
class InMemoryInputStream : public InputStream {
 public:
  explicit InMemoryInputStream(const std::shared_ptr<Buffer>& buffer);
  virtual const uint8_t* Peek(int64_t num_to_peek, int64_t* num_bytes);
  virtual const uint8_t* Read(int64_t num_to_read, int64_t* num_bytes);

  virtual void Advance(int64_t num_bytes);

 private:
  std::shared_ptr<Buffer> buffer_;
  int64_t len_;
  int64_t offset_;
};

} // namespace parquet_cpp

#endif // PARQUET_UTIL_INPUT_H
