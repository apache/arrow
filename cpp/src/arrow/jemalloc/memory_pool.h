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

// Public API for the jemalloc-based allocator

#ifndef ARROW_JEMALLOC_MEMORY_POOL_H
#define ARROW_JEMALLOC_MEMORY_POOL_H

#include "arrow/memory_pool.h"

#include <atomic>

namespace arrow {

class Status;

namespace jemalloc {

class ARROW_EXPORT MemoryPool : public ::arrow::MemoryPool {
 public:
  static MemoryPool* default_pool();

  MemoryPool(MemoryPool const&) = delete;
  MemoryPool& operator=(MemoryPool const&) = delete;

  virtual ~MemoryPool();

  Status Allocate(int64_t size, uint8_t** out) override;
  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override;
  void Free(uint8_t* buffer, int64_t size) override;

  int64_t bytes_allocated() const override;

 private:
  MemoryPool();

  std::atomic<int64_t> allocated_size_;
};

}  // namespace jemalloc
}  // namespace arrow

#endif  // ARROW_JEMALLOC_MEMORY_POOL_H
