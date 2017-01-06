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

#include <cstdint>
#include <limits>

#include "gtest/gtest.h"

#include "arrow/jemalloc/memory_pool.h"
#include "arrow/memory_pool-test.h"

namespace arrow {
namespace jemalloc {
namespace test {

class TestJemallocMemoryPool : public ::arrow::test::TestMemoryPoolBase {
 public:
  ::arrow::MemoryPool* memory_pool() override {
    return ::arrow::jemalloc::MemoryPool::default_pool();
  }
};

TEST_F(TestJemallocMemoryPool, MemoryTracking) {
  this->TestMemoryTracking();
}

TEST_F(TestJemallocMemoryPool, OOM) {
  this->TestOOM();
}

TEST_F(TestJemallocMemoryPool, Reallocate) {
  this->TestReallocate();
}

}  // namespace test
}  // namespace jemalloc
}  // namespace arrow
