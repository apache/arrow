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

#ifndef PARQUET_UTIL_MEMORY_POOL_H
#define PARQUET_UTIL_MEMORY_POOL_H

#include "parquet/util/logging.h"
#include "parquet/util/bit-util.h"

namespace parquet {

class MemoryAllocator {
 public:
  virtual ~MemoryAllocator();

  // Returns nullptr if size is 0
  virtual uint8_t* Malloc(int64_t size) = 0;
  virtual void Free(uint8_t* p, int64_t size) = 0;
};

MemoryAllocator* default_allocator();

class TrackingAllocator: public MemoryAllocator {
 public:
  TrackingAllocator() : total_memory_(0), max_memory_(0) {}
  virtual ~TrackingAllocator();

  uint8_t* Malloc(int64_t size) override;
  void Free(uint8_t* p, int64_t size) override;

  int64_t TotalMemory() {
    return total_memory_;
  }

  int64_t MaxMemory() {
    return max_memory_;
  }

 private:
  int64_t total_memory_;
  int64_t max_memory_;
};

} // namespace parquet

#endif // PARQUET_UTIL_MEMORY_POOL_H
