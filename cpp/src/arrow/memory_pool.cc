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

#include "arrow/memory_pool.h"

#include <algorithm>
#include <cstdlib>
#include <mutex>
#include <sstream>
#include <stdlib.h>
#include <iostream>

#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {

namespace {
// Allocate memory according to the alignment requirements for Arrow
// (as of May 2016 64 bytes)
Status AllocateAligned(int64_t size, uint8_t** out) {
  // TODO(emkornfield) find something compatible with windows
  constexpr size_t kAlignment = 64;
#ifdef _MSC_VER
  // Special code path for MSVC
  *out =
      reinterpret_cast<uint8_t*>(_aligned_malloc(static_cast<size_t>(size), kAlignment));
  if (!*out) {
    std::stringstream ss;
    ss << "malloc of size " << size << " failed";
    return Status::OutOfMemory(ss.str());
  }
#else
  const int result = posix_memalign(
      reinterpret_cast<void**>(out), kAlignment, static_cast<size_t>(size));
  if (result == ENOMEM) {
    std::stringstream ss;
    ss << "malloc of size " << size << " failed";
    return Status::OutOfMemory(ss.str());
  }

  if (result == EINVAL) {
    std::stringstream ss;
    ss << "invalid alignment parameter: " << kAlignment;
    return Status::Invalid(ss.str());
  }
#endif
  return Status::OK();
}
}  // namespace

MemoryPool::MemoryPool() {}

MemoryPool::~MemoryPool() {}

int64_t MemoryPool::max_memory() const {
  return -1;
}

DefaultMemoryPool::DefaultMemoryPool() : bytes_allocated_(0) {
  max_memory_ = 0;
}

Status DefaultMemoryPool::Allocate(int64_t size, uint8_t** out) {
  RETURN_NOT_OK(AllocateAligned(size, out));
  bytes_allocated_ += size;

  {
    std::lock_guard<std::mutex> guard(lock_);
    if (bytes_allocated_ > max_memory_) { max_memory_ = bytes_allocated_.load(); }
  }
  return Status::OK();
}

Status DefaultMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  // Note: We cannot use realloc() here as it doesn't guarantee alignment.

  // Allocate new chunk
  uint8_t* out;
  RETURN_NOT_OK(AllocateAligned(new_size, &out));
  // Copy contents and release old memory chunk
  memcpy(out, *ptr, static_cast<size_t>(std::min(new_size, old_size)));
#ifdef _MSC_VER
  _aligned_free(*ptr);
#else
  std::free(*ptr);
#endif
  *ptr = out;

  bytes_allocated_ += new_size - old_size;
  {
    std::lock_guard<std::mutex> guard(lock_);
    if (bytes_allocated_ > max_memory_) { max_memory_ = bytes_allocated_.load(); }
  }

  return Status::OK();
}

int64_t DefaultMemoryPool::bytes_allocated() const {
  return bytes_allocated_.load();
}

void DefaultMemoryPool::Free(uint8_t* buffer, int64_t size) {
  DCHECK_GE(bytes_allocated_, size);
#ifdef _MSC_VER
  _aligned_free(buffer);
#else
  std::free(buffer);
#endif
  bytes_allocated_ -= size;
}

int64_t DefaultMemoryPool::max_memory() const {
  return max_memory_.load();
}

DefaultMemoryPool::~DefaultMemoryPool() {}

MemoryPool* default_memory_pool() {
  static DefaultMemoryPool default_memory_pool_;
  return &default_memory_pool_;
}

LoggingMemoryPool::LoggingMemoryPool(MemoryPool* pool) : pool_(pool) {}

Status LoggingMemoryPool::Allocate(int64_t size, uint8_t** out) {
  Status s = pool_->Allocate(size, out);
  std::cout << "Allocate: size = " << size << " - out = " << *out << std::endl;
  return s;
}

Status LoggingMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  Status s = pool_->Reallocate(old_size, new_size, ptr);
  std::cout << "Reallocate: old_size = " << old_size << " - new_size = " << new_size
            << " - ptr = " << *ptr << std::endl;
  return s;
}

void LoggingMemoryPool::Free(uint8_t* buffer, int64_t size) {
  pool_->Free(buffer, size);
  std::cout << "Free: buffer = " << buffer << " - size = " << size << std::endl;
}

int64_t LoggingMemoryPool::bytes_allocated() const {
  int64_t nb_bytes = pool_->bytes_allocated();
  std::cout << "bytes_allocated: " << nb_bytes << std::endl;
  return nb_bytes;
}

int64_t LoggingMemoryPool::max_memory() const {
  int64_t mem = pool_->max_memory();
  std::cout << "max_memory: " << mem << std::endl;
  return mem;
}
}  // namespace arrow
