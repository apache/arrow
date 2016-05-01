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

#include <algorithm>
#include <cstdint>

#include "parquet/exception.h"
#include "parquet/types.h"

namespace parquet {

Buffer::Buffer(const std::shared_ptr<Buffer>& parent, int64_t offset, int64_t size) {
  data_ = parent->data() + offset;
  size_ = size;
  parent_ = parent;
}

std::shared_ptr<Buffer> MutableBuffer::GetImmutableView() {
  return std::make_shared<Buffer>(this->get_shared_ptr(), 0, size());
}

OwnedMutableBuffer::OwnedMutableBuffer(int64_t size, MemoryAllocator* allocator)
    : ResizableBuffer(nullptr, 0), allocator_(allocator) {
  Resize(size);
}

OwnedMutableBuffer::~OwnedMutableBuffer() {
  if (mutable_data_) { allocator_->Free(mutable_data_, capacity_); }
}

void OwnedMutableBuffer::Reserve(int64_t new_capacity) {
  if (!mutable_data_ || new_capacity > capacity_) {
    if (mutable_data_) {
      uint8_t* new_data = allocator_->Malloc(new_capacity);
      memcpy(new_data, mutable_data_, size_);
      allocator_->Free(mutable_data_, capacity_);
      mutable_data_ = new_data;
    } else {
      mutable_data_ = allocator_->Malloc(new_capacity);
    }
    data_ = mutable_data_;
    capacity_ = new_capacity;
  }
}

void OwnedMutableBuffer::Resize(int64_t new_size) {
  Reserve(new_size);
  size_ = new_size;
}

uint8_t& OwnedMutableBuffer::operator[](int64_t i) {
  return mutable_data_[i];
}

template <class T>
Vector<T>::Vector(int64_t size, MemoryAllocator* allocator)
    : buffer_(new OwnedMutableBuffer(size * sizeof(T), allocator)),
      size_(size),
      capacity_(size) {
  if (size > 0) {
    data_ = reinterpret_cast<T*>(buffer_->mutable_data());
  } else {
    data_ = nullptr;
  }
}

template <class T>
void Vector<T>::Reserve(int64_t new_capacity) {
  if (new_capacity > capacity_) {
    buffer_->Resize(new_capacity * sizeof(T));
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

}  // namespace parquet
