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

#include "arrow/util/memory-pool.h"

#include <stdlib.h>
#include <cstdlib>
#include <mutex>
#include <sstream>

#include "arrow/util/status.h"

namespace arrow {

namespace {
// Allocate memory according to the alignment requirements for Arrow
// (as of May 2016 64 bytes)
Status AllocateAligned(int64_t size, uint8_t** out) {
  // TODO(emkornfield) find something compatible with windows
  constexpr size_t kAlignment = 64;
  const int result = posix_memalign(reinterpret_cast<void**>(out), kAlignment, size);
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
  return Status::OK();
}
}  // namespace

MemoryPool::~MemoryPool() {}

class InternalMemoryPool : public MemoryPool {
 public:
  InternalMemoryPool() : bytes_allocated_(0) {}
  virtual ~InternalMemoryPool();

  Status Allocate(int64_t size, uint8_t** out) override;

  void Free(uint8_t* buffer, int64_t size) override;

  int64_t bytes_allocated() const override;

 private:
  mutable std::mutex pool_lock_;
  int64_t bytes_allocated_;
};

Status InternalMemoryPool::Allocate(int64_t size, uint8_t** out) {
  std::lock_guard<std::mutex> guard(pool_lock_);
  RETURN_NOT_OK(AllocateAligned(size, out));
  bytes_allocated_ += size;

  return Status::OK();
}

int64_t InternalMemoryPool::bytes_allocated() const {
  std::lock_guard<std::mutex> guard(pool_lock_);
  return bytes_allocated_;
}

void InternalMemoryPool::Free(uint8_t* buffer, int64_t size) {
  std::lock_guard<std::mutex> guard(pool_lock_);
  std::free(buffer);
  bytes_allocated_ -= size;
}

InternalMemoryPool::~InternalMemoryPool() {}

MemoryPool* default_memory_pool() {
  static InternalMemoryPool default_memory_pool_;
  return &default_memory_pool_;
}

}  // namespace arrow
