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

#include "parquet/util/output.h"

#include <cstring>
#include <memory>

#include "parquet/exception.h"
#include "parquet/util/buffer.h"

namespace parquet_cpp {

// ----------------------------------------------------------------------
// In-memory output stream

static constexpr int64_t IN_MEMORY_DEFAULT_CAPACITY = 1024;

InMemoryOutputStream::InMemoryOutputStream(int64_t initial_capacity) :
    size_(0),
    capacity_(initial_capacity) {
  if (initial_capacity == 0) {
    initial_capacity = IN_MEMORY_DEFAULT_CAPACITY;
  }
  buffer_.reset(new OwnedMutableBuffer());
  buffer_->Resize(initial_capacity);
}

InMemoryOutputStream::InMemoryOutputStream() :
    InMemoryOutputStream(IN_MEMORY_DEFAULT_CAPACITY) {}

uint8_t* InMemoryOutputStream::Head() {
  return buffer_->mutable_data() + size_;
}

void InMemoryOutputStream::Write(const uint8_t* data, int64_t length) {
  if (size_ + length > capacity_) {
    int64_t new_capacity = capacity_ * 2;
    while (new_capacity < size_ + length) {
      new_capacity *= 2;
    }
    buffer_->Resize(new_capacity);
    capacity_ = new_capacity;
  }
  memcpy(Head(), data, length);
  size_ += length;
}

int64_t InMemoryOutputStream::Tell() {
  return size_;
}

std::shared_ptr<Buffer> InMemoryOutputStream::GetBuffer() {
  buffer_->Resize(size_);
  std::shared_ptr<Buffer> result = buffer_;
  buffer_ = nullptr;
  return result;
}

} // namespace parquet_cpp
