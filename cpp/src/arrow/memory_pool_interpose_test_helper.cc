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

// Test helper for memory_pool_interpose_test.
// This program performs some allocations using Arrow's memory pool
// and exits. It is meant to be run with LD_PRELOAD to test interposition.

#include <cstdlib>
#include <iostream>

#include "arrow/memory_pool.h"
#include "arrow/status.h"

int main(int argc, char** argv) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  std::cout << "Backend: " << pool->backend_name() << std::endl;

  // Skip test if not using mimalloc
  if (pool->backend_name() != "mimalloc") {
    std::cout << "SKIP: not using mimalloc backend" << std::endl;
    return 0;
  }

  // Perform some allocations
  uint8_t* buf1 = nullptr;
  uint8_t* buf2 = nullptr;
  uint8_t* buf3 = nullptr;

  auto status = pool->Allocate(1024, &buf1);
  if (!status.ok()) {
    std::cerr << "Allocate failed: " << status.ToString() << std::endl;
    return 1;
  }

  status = pool->Allocate(2048, &buf2);
  if (!status.ok()) {
    std::cerr << "Allocate failed: " << status.ToString() << std::endl;
    return 1;
  }

  // Reallocate buf1
  status = pool->Reallocate(1024, 4096, &buf1);
  if (!status.ok()) {
    std::cerr << "Reallocate failed: " << status.ToString() << std::endl;
    return 1;
  }

  status = pool->Allocate(512, &buf3);
  if (!status.ok()) {
    std::cerr << "Allocate failed: " << status.ToString() << std::endl;
    return 1;
  }

  // Free all buffers
  pool->Free(buf1, 4096);
  pool->Free(buf2, 2048);
  pool->Free(buf3, 512);

  std::cout << "SUCCESS: allocations completed" << std::endl;
  return 0;
}
