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

#include "gandiva/simple_arena.h"

#include <gtest/gtest.h>

#include "arrow/memory_pool.h"

namespace gandiva {

class TestSimpleArena : public ::testing::Test {};

TEST_F(TestSimpleArena, TestAlloc) {
  int64_t chunk_size = 4096;
  SimpleArena arena(arrow::default_memory_pool(), chunk_size);

  // Small allocations should come from the same chunk.
  int64_t small_size = 100;
  for (int64_t i = 0; i < 20; ++i) {
    auto p = arena.Allocate(small_size);
    EXPECT_NE(p, nullptr);

    EXPECT_EQ(arena.total_bytes(), chunk_size);
    EXPECT_EQ(arena.avail_bytes(), chunk_size - (i + 1) * small_size);
  }

  // large allocations require separate chunks
  int64_t large_size = 100 * chunk_size;
  auto p = arena.Allocate(large_size);
  EXPECT_NE(p, nullptr);
  EXPECT_EQ(arena.total_bytes(), chunk_size + large_size);
  EXPECT_EQ(arena.avail_bytes(), 0);
}

// small followed by big, then reset
TEST_F(TestSimpleArena, TestReset1) {
  int64_t chunk_size = 4096;
  SimpleArena arena(arrow::default_memory_pool(), chunk_size);

  int64_t small_size = 100;
  auto p = arena.Allocate(small_size);
  EXPECT_NE(p, nullptr);

  int64_t large_size = 100 * chunk_size;
  p = arena.Allocate(large_size);
  EXPECT_NE(p, nullptr);

  EXPECT_EQ(arena.total_bytes(), chunk_size + large_size);
  EXPECT_EQ(arena.avail_bytes(), 0);
  arena.Reset();
  EXPECT_EQ(arena.total_bytes(), chunk_size);
  EXPECT_EQ(arena.avail_bytes(), chunk_size);

  // should re-use buffer after reset.
  p = arena.Allocate(small_size);
  EXPECT_NE(p, nullptr);
  EXPECT_EQ(arena.total_bytes(), chunk_size);
  EXPECT_EQ(arena.avail_bytes(), chunk_size - small_size);
}

// big followed by small, then reset
TEST_F(TestSimpleArena, TestReset2) {
  int64_t chunk_size = 4096;
  SimpleArena arena(arrow::default_memory_pool(), chunk_size);

  int64_t large_size = 100 * chunk_size;
  auto p = arena.Allocate(large_size);
  EXPECT_NE(p, nullptr);

  int64_t small_size = 100;
  p = arena.Allocate(small_size);
  EXPECT_NE(p, nullptr);

  EXPECT_EQ(arena.total_bytes(), chunk_size + large_size);
  EXPECT_EQ(arena.avail_bytes(), chunk_size - small_size);
  arena.Reset();
  EXPECT_EQ(arena.total_bytes(), large_size);
  EXPECT_EQ(arena.avail_bytes(), large_size);

  // should re-use buffer after reset.
  p = arena.Allocate(small_size);
  EXPECT_NE(p, nullptr);
  EXPECT_EQ(arena.total_bytes(), large_size);
  EXPECT_EQ(arena.avail_bytes(), large_size - small_size);
}

}  // namespace gandiva
