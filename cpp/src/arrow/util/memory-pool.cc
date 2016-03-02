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

#include <cstdlib>
#include <sstream>
#include <mutex>

#include "arrow/util/status.h"

namespace arrow {

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
  *out = static_cast<uint8_t*>(std::malloc(size));
  if (*out == nullptr) {
    std::stringstream ss;
    ss << "malloc of size " << size << " failed";
    return Status::OutOfMemory(ss.str());
  }

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

MemoryPool* GetDefaultMemoryPool() {
  static InternalMemoryPool default_memory_pool;
  return &default_memory_pool;
}

} // namespace arrow
