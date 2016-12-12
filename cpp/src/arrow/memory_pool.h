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

#ifndef ARROW_UTIL_MEMORY_POOL_H
#define ARROW_UTIL_MEMORY_POOL_H

#include <cstdint>

#include "arrow/util/visibility.h"

namespace arrow {

class Status;

class ARROW_EXPORT MemoryPool {
 public:
  virtual ~MemoryPool();

  virtual Status Allocate(int64_t size, uint8_t** out) = 0;
  virtual void Free(uint8_t* buffer, int64_t size) = 0;

  virtual int64_t bytes_allocated() const = 0;
};

ARROW_EXPORT MemoryPool* default_memory_pool();

}  // namespace arrow

#endif  // ARROW_UTIL_MEMORY_POOL_H
