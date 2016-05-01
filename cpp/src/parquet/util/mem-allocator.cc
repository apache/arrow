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

#include "parquet/util/mem-allocator.h"

#include <cstdlib>

#include "parquet/exception.h"

namespace parquet {

MemoryAllocator::~MemoryAllocator() {}

uint8_t* TrackingAllocator::Malloc(int64_t size) {
  if (0 == size) { return nullptr; }

  uint8_t* p = static_cast<uint8_t*>(std::malloc(size));
  if (!p) { throw ParquetException("OOM: memory allocation failed"); }
  total_memory_ += size;
  if (total_memory_ > max_memory_) { max_memory_ = total_memory_; }
  return p;
}

void TrackingAllocator::Free(uint8_t* p, int64_t size) {
  if (nullptr != p && size > 0) {
    if (total_memory_ < size) {
      throw ParquetException("Attempting to free too much memory");
    }
    total_memory_ -= size;
    std::free(p);
  }
}

TrackingAllocator::~TrackingAllocator() {}

MemoryAllocator* default_allocator() {
  static TrackingAllocator default_allocator;
  return &default_allocator;
}

}  // namespace parquet
