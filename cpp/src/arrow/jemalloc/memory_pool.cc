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

#include "arrow/jemalloc/memory_pool.h"

#include <sstream>

// Needed to support jemalloc 3 and 4
#define JEMALLOC_MANGLE
#include <jemalloc/jemalloc.h>

#include "arrow/status.h"

constexpr size_t kAlignment = 64;

namespace arrow {
namespace jemalloc {

MemoryPool* MemoryPool::default_pool() {
  static MemoryPool pool;
  return &pool;
}

MemoryPool::MemoryPool() : allocated_size_(0) {}

MemoryPool::~MemoryPool() {}

Status MemoryPool::Allocate(int64_t size, uint8_t** out) {
  *out = reinterpret_cast<uint8_t*>(mallocx(size, MALLOCX_ALIGN(kAlignment)));
  if (*out == NULL) {
    std::stringstream ss;
    ss << "malloc of size " << size << " failed";
    return Status::OutOfMemory(ss.str());
  }
  allocated_size_ += size;
  return Status::OK();
}

Status MemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  *ptr = reinterpret_cast<uint8_t*>(rallocx(*ptr, new_size, MALLOCX_ALIGN(kAlignment)));
  if (*ptr == NULL) {
    std::stringstream ss;
    ss << "realloc of size " << new_size << " failed";
    return Status::OutOfMemory(ss.str());
  }

  allocated_size_ += new_size - old_size;

  return Status::OK();
}

void MemoryPool::Free(uint8_t* buffer, int64_t size) {
  allocated_size_ -= size;
  free(buffer);
}

int64_t MemoryPool::bytes_allocated() const {
  return allocated_size_.load();
}

}  // namespace jemalloc
}  // namespace arrow
