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

#include <arrow/memory_pool.h>
#include <arrow/util/mutex.h>
#include "./arrow_types.h"

class GcMemoryPool : public arrow::MemoryPool {
 public:
  GcMemoryPool() : pool_(arrow::default_memory_pool()) {}

  using MemoryPool::Allocate;
  using MemoryPool::Free;
  using MemoryPool::Reallocate;

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
    return GcAndTryAgain([&] { return pool_->Allocate(size, alignment, out); });
  }

  arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                           uint8_t** ptr) override {
    return GcAndTryAgain(
        [&] { return pool_->Reallocate(old_size, new_size, alignment, ptr); });
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
    pool_->Free(buffer, size, alignment);
  }

  int64_t bytes_allocated() const override { return pool_->bytes_allocated(); }

  int64_t max_memory() const override { return pool_->max_memory(); }

  int64_t total_bytes_allocated() const override {
    return pool_->total_bytes_allocated();
  }

  int64_t num_allocations() const override { return pool_->num_allocations(); }

  std::string backend_name() const override { return pool_->backend_name(); }

 private:
  template <typename Call>
  arrow::Status GcAndTryAgain(const Call& call) {
    if (call().ok()) {
      return arrow::Status::OK();
    } else {
      auto lock = mutex_.Lock();

      // ARROW-10080: Allocation may fail spuriously since the garbage collector is lazy.
      // Force it to run then try again in case any reusable allocations have been freed.
      static cpp11::function gc = cpp11::package("base")["gc"];
      gc();
    }
    return call();
  }

  arrow::util::Mutex mutex_;
  arrow::MemoryPool* pool_;
};

static GcMemoryPool g_pool;

arrow::MemoryPool* gc_memory_pool() { return &g_pool; }

// [[arrow::export]]
std::shared_ptr<arrow::MemoryPool> MemoryPool__default() {
  return std::shared_ptr<arrow::MemoryPool>(&g_pool, [](...) {});
}

// [[arrow::export]]
double MemoryPool__bytes_allocated(const std::shared_ptr<arrow::MemoryPool>& pool) {
  return pool->bytes_allocated();
}

// [[arrow::export]]
double MemoryPool__max_memory(const std::shared_ptr<arrow::MemoryPool>& pool) {
  return pool->max_memory();
}

// [[arrow::export]]
std::string MemoryPool__backend_name(const std::shared_ptr<arrow::MemoryPool>& pool) {
  return pool->backend_name();
}

// [[arrow::export]]
std::vector<std::string> supported_memory_backends() {
  return arrow::SupportedMemoryBackendNames();
}
