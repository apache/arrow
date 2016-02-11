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

#include "parquet/util/input.h"

#include <algorithm>
#include <string>

#include "parquet/exception.h"

namespace parquet_cpp {

// ----------------------------------------------------------------------
// LocalFileSource

LocalFileSource::~LocalFileSource() {
  CloseFile();
}

void LocalFileSource::Open(const std::string& path) {
  path_ = path;
  file_ = fopen(path_.c_str(), "r");
  is_open_ = true;
}

void LocalFileSource::Close() {
  // Pure virtual
  CloseFile();
}

void LocalFileSource::CloseFile() {
  if (is_open_) {
    fclose(file_);
    is_open_ = false;
  }
}

size_t LocalFileSource::Size() {
  fseek(file_, 0L, SEEK_END);
  return Tell();
}

void LocalFileSource::Seek(size_t pos) {
  fseek(file_, pos, SEEK_SET);
}

size_t LocalFileSource::Tell() {
  return ftell(file_);
}

size_t LocalFileSource::Read(size_t nbytes, uint8_t* buffer) {
  return fread(buffer, 1, nbytes, file_);
}

// ----------------------------------------------------------------------
// InMemoryInputStream

InMemoryInputStream::InMemoryInputStream(const uint8_t* buffer, int64_t len) :
    buffer_(buffer), len_(len), offset_(0) {}

const uint8_t* InMemoryInputStream::Peek(int64_t num_to_peek, int64_t* num_bytes) {
  *num_bytes = std::min(static_cast<int64_t>(num_to_peek), len_ - offset_);
  return buffer_ + offset_;
}

const uint8_t* InMemoryInputStream::Read(int64_t num_to_read, int64_t* num_bytes) {
  const uint8_t* result = Peek(num_to_read, num_bytes);
  offset_ += *num_bytes;
  return result;
}

// ----------------------------------------------------------------------
// ScopedInMemoryInputStream:: like InMemoryInputStream but owns its memory

ScopedInMemoryInputStream::ScopedInMemoryInputStream(int64_t len) {
  buffer_.resize(len);
  stream_.reset(new InMemoryInputStream(buffer_.data(), buffer_.size()));
}

uint8_t* ScopedInMemoryInputStream::data() {
  return buffer_.data();
}

int64_t ScopedInMemoryInputStream::size() {
  return buffer_.size();
}

const uint8_t* ScopedInMemoryInputStream::Peek(int64_t num_to_peek, int64_t* num_bytes) {
  return stream_->Peek(num_to_peek, num_bytes);
}

const uint8_t* ScopedInMemoryInputStream::Read(int64_t num_to_read, int64_t* num_bytes) {
  return stream_->Read(num_to_read, num_bytes);
}

} // namespace parquet_cpp
