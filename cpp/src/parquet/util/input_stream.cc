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

#include "parquet/util/input_stream.h"

#include <algorithm>

#include "parquet/exception.h"

namespace parquet_cpp {

InMemoryInputStream::InMemoryInputStream(const uint8_t* buffer, int64_t len) :
    buffer_(buffer), len_(len), offset_(0) {}

const uint8_t* InMemoryInputStream::Peek(int num_to_peek, int* num_bytes) {
  *num_bytes = std::min(static_cast<int64_t>(num_to_peek), len_ - offset_);
  return buffer_ + offset_;
}

const uint8_t* InMemoryInputStream::Read(int num_to_read, int* num_bytes) {
  const uint8_t* result = Peek(num_to_read, num_bytes);
  offset_ += *num_bytes;
  return result;
}

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

const uint8_t* ScopedInMemoryInputStream::Peek(int num_to_peek,
                                               int* num_bytes) {
  return stream_->Peek(num_to_peek, num_bytes);
}

const uint8_t* ScopedInMemoryInputStream::Read(int num_to_read,
                                               int* num_bytes) {
  return stream_->Read(num_to_read, num_bytes);
}

} // namespace parquet_cpp
