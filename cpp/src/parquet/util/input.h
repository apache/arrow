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
#include <memory>
#include <string>
#include <vector>

namespace parquet_cpp {

// ----------------------------------------------------------------------
// Random access input (e.g. file-like)

// Random
class RandomAccessSource {
 public:
  virtual ~RandomAccessSource() {}

  virtual void Close() = 0;
  virtual size_t Size() = 0;
  virtual size_t Tell() = 0;
  virtual void Seek(size_t pos) = 0;

  // Returns actual number of bytes read
  virtual size_t Read(size_t nbytes, uint8_t* out) = 0;
};


class LocalFileSource : public RandomAccessSource {
 public:
  LocalFileSource() : file_(nullptr), is_open_(false) {}
  virtual ~LocalFileSource();

  void Open(const std::string& path);

  virtual void Close();
  virtual size_t Size();
  virtual size_t Tell();
  virtual void Seek(size_t pos);

  // Returns actual number of bytes read
  virtual size_t Read(size_t nbytes, uint8_t* out);

  bool is_open() const { return is_open_;}
  const std::string& path() const { return path_;}

 private:
  void CloseFile();

  std::string path_;
  FILE* file_;
  bool is_open_;
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

  virtual ~InputStream() {}

 protected:
  InputStream() {}
};

// Implementation of an InputStream when all the bytes are in memory.
class InMemoryInputStream : public InputStream {
 public:
  InMemoryInputStream(const uint8_t* buffer, int64_t len);
  virtual const uint8_t* Peek(int64_t num_to_peek, int64_t* num_bytes);
  virtual const uint8_t* Read(int64_t num_to_read, int64_t* num_bytes);

 private:
  const uint8_t* buffer_;
  int64_t len_;
  int64_t offset_;
};


// A wrapper for InMemoryInputStream to manage the memory.
class ScopedInMemoryInputStream : public InputStream {
 public:
  explicit ScopedInMemoryInputStream(int64_t len);
  uint8_t* data();
  int64_t size();
  virtual const uint8_t* Peek(int64_t num_to_peek, int64_t* num_bytes);
  virtual const uint8_t* Read(int64_t num_to_read, int64_t* num_bytes);

 private:
  std::vector<uint8_t> buffer_;
  std::unique_ptr<InMemoryInputStream> stream_;
};

} // namespace parquet_cpp

#endif // PARQUET_UTIL_INPUT_H
