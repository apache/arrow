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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/io/file.h>
#include <arrow/io/memory.h>

namespace cpp11 {
template <> inline std::string r6_class_name<arrow::io::MemoryMappedFile>(const std::shared_ptr<arrow::io::MemoryMappedFile>& x) {
  return "MemoryMappedFile";
}
template <> inline std::string r6_class_name<arrow::io::ReadableFile>(const std::shared_ptr<arrow::io::ReadableFile>& x) {
  return "ReadableFile";
}
template <> inline std::string r6_class_name<arrow::io::BufferReader>(const std::shared_ptr<arrow::io::BufferReader>& x) {
  return "BufferReader";
}
template <> inline std::string r6_class_name<arrow::io::FileOutputStream>(const std::shared_ptr<arrow::io::FileOutputStream>& x) {
  return "FileOutputStream";
}
template <> inline std::string r6_class_name<arrow::io::BufferOutputStream>(const std::shared_ptr<arrow::io::BufferOutputStream>& x) {
  return "BufferOutputStream";
}
}

// ------ arrow::io::Readable

// [[arrow::export]]
R6 io___Readable__Read(const std::shared_ptr<arrow::io::Readable>& x, int64_t nbytes) {
  return ValueOrStop(x->Read(nbytes));
}

// ------ arrow::io::InputStream

// [[arrow::export]]
void io___InputStream__Close(const std::shared_ptr<arrow::io::InputStream>& x) {
  StopIfNotOk(x->Close());
}

// ------ arrow::io::OutputStream

// [[arrow::export]]
void io___OutputStream__Close(const std::shared_ptr<arrow::io::OutputStream>& x) {
  StopIfNotOk(x->Close());
}

// ------ arrow::io::RandomAccessFile

// [[arrow::export]]
int64_t io___RandomAccessFile__GetSize(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  return ValueOrStop(x->GetSize());
}

// [[arrow::export]]
bool io___RandomAccessFile__supports_zero_copy(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  return x->supports_zero_copy();
}

// [[arrow::export]]
void io___RandomAccessFile__Seek(const std::shared_ptr<arrow::io::RandomAccessFile>& x,
                                 int64_t position) {
  StopIfNotOk(x->Seek(position));
}

// [[arrow::export]]
int64_t io___RandomAccessFile__Tell(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  return ValueOrStop(x->Tell());
}

// [[arrow::export]]
R6 io___RandomAccessFile__Read0(const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  int64_t current = ValueOrStop(x->Tell());

  int64_t n = ValueOrStop(x->GetSize());

  return ValueOrStop(x->Read(n - current));
}

// [[arrow::export]]
R6 io___RandomAccessFile__ReadAt(const std::shared_ptr<arrow::io::RandomAccessFile>& x,
                                 int64_t position, int64_t nbytes) {
  return ValueOrStop(x->ReadAt(position, nbytes));
}

// ------ arrow::io::MemoryMappedFile

// [[arrow::export]]
R6 io___MemoryMappedFile__Create(const std::string& path, int64_t size) {
  return ValueOrStop(arrow::io::MemoryMappedFile::Create(path, size));
}

// [[arrow::export]]
R6 io___MemoryMappedFile__Open(const std::string& path, arrow::io::FileMode::type mode) {
  return ValueOrStop(arrow::io::MemoryMappedFile::Open(path, mode));
}

// [[arrow::export]]
void io___MemoryMappedFile__Resize(const std::shared_ptr<arrow::io::MemoryMappedFile>& x,
                                   int64_t size) {
  StopIfNotOk(x->Resize(size));
}

// ------ arrow::io::ReadableFile

// [[arrow::export]]
R6 io___ReadableFile__Open(const std::string& path) {
  return ValueOrStop(arrow::io::ReadableFile::Open(path, gc_memory_pool()));
}

// ------ arrow::io::BufferReader

// [[arrow::export]]
R6 io___BufferReader__initialize(const std::shared_ptr<arrow::Buffer>& buffer) {
  return std::make_shared<arrow::io::BufferReader>(buffer);
}

// ------- arrow::io::Writable

// [[arrow::export]]
void io___Writable__write(const std::shared_ptr<arrow::io::Writable>& stream,
                          const std::shared_ptr<arrow::Buffer>& buf) {
  StopIfNotOk(stream->Write(buf->data(), buf->size()));
}

// ------- arrow::io::OutputStream

// [[arrow::export]]
int64_t io___OutputStream__Tell(const std::shared_ptr<arrow::io::OutputStream>& stream) {
  return ValueOrStop(stream->Tell());
}

// ------ arrow::io::FileOutputStream

// [[arrow::export]]
R6 io___FileOutputStream__Open(const std::string& path) {
  return ValueOrStop(arrow::io::FileOutputStream::Open(path));
}

// ------ arrow::BufferOutputStream

// [[arrow::export]]
R6 io___BufferOutputStream__Create(int64_t initial_capacity) {
  return ValueOrStop(
      arrow::io::BufferOutputStream::Create(initial_capacity, gc_memory_pool()));
}

// [[arrow::export]]
int64_t io___BufferOutputStream__capacity(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream) {
  return stream->capacity();
}

// [[arrow::export]]
R6 io___BufferOutputStream__Finish(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream) {
  return ValueOrStop(stream->Finish());
}

// [[arrow::export]]
int64_t io___BufferOutputStream__Tell(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream) {
  return ValueOrStop(stream->Tell());
}

// [[arrow::export]]
void io___BufferOutputStream__Write(
    const std::shared_ptr<arrow::io::BufferOutputStream>& stream, cpp11::raws bytes) {
  StopIfNotOk(stream->Write(RAW(bytes), bytes.size()));
}

#endif
