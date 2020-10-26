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

#include "./arrow_types.h"
#if defined(ARROW_R_WITH_ARROW)
#include <arrow/memory_pool.h>

class GcMemoryPool : public arrow::MemoryPool {
 public:
  GcMemoryPool()
      : pool_(arrow::default_memory_pool()), gc_(cpp11::package("base")["gc"]) {}

  arrow::Status Allocate(int64_t size, uint8_t** out) override {
    return GcAndTryAgain([&] { return pool_->Allocate(size, out); });
  }

  arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    return GcAndTryAgain([&] { return pool_->Reallocate(old_size, new_size, ptr); });
  }

  void Free(uint8_t* buffer, int64_t size) override { pool_->Free(buffer, size); }

  int64_t bytes_allocated() const override { return pool_->bytes_allocated(); }

  int64_t max_memory() const override { return pool_->max_memory(); }

  std::string backend_name() const override { return pool_->backend_name() + "-gc"; }

 private:
  template <typename Call>
  arrow::Status GcAndTryAgain(const Call& call) {
    if (call().ok()) {
      return arrow::Status::OK();
    }
    // ARROW-10080
    gc_();
    return call();
  }

  arrow::MemoryPool* pool_;
  cpp11::function gc_;
};

// [[arrow::export]]
std::shared_ptr<arrow::MemoryPool> MemoryPool__default() {
  return std::make_shared<GcMemoryPool>();
}

// [[arrow::export]]
int MemoryPool__bytes_allocated(const std::shared_ptr<arrow::MemoryPool>& pool) {
  return pool->bytes_allocated();
}

// [[arrow::export]]
int MemoryPool__max_memory(const std::shared_ptr<arrow::MemoryPool>& pool) {
  return pool->max_memory();
}

#endif
