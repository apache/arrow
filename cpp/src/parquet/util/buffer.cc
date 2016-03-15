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

#include "parquet/util/buffer.h"

#include <cstdint>

#include "parquet/exception.h"

namespace parquet_cpp {

Buffer::Buffer(const std::shared_ptr<Buffer>& parent, int64_t offset,
    int64_t size) {
  data_ = parent->data() + offset;
  size_ = size;
  parent_ = parent;
}

std::shared_ptr<Buffer> MutableBuffer::GetImmutableView() {
  return std::make_shared<Buffer>(this->get_shared_ptr(), 0, size());
}

OwnedMutableBuffer::OwnedMutableBuffer() :
    ResizableBuffer(nullptr, 0) {}

void OwnedMutableBuffer::Resize(int64_t new_size) {
  try {
    buffer_owner_.resize(new_size);
  } catch (const std::bad_alloc& e) {
    throw ParquetException("OOM: resize failed");
  }
  size_ = new_size;
  data_ = buffer_owner_.data();
  mutable_data_ = buffer_owner_.data();
}

} // namespace parquet_cpp
