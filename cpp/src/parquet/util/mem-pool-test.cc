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

// Initially imported from Apache Impala on 2016-02-23, and has been modified
// since for parquet-cpp

#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <string>

#include "parquet/util/bit-util.h"
#include "parquet/util/mem-pool.h"

namespace parquet {

// Utility class to call private functions on MemPool.
class MemPoolTest {
 public:
  static bool CheckIntegrity(MemPool* pool, bool current_chunk_empty) {
    return pool->CheckIntegrity(current_chunk_empty);
  }

  static const int INITIAL_CHUNK_SIZE = MemPool::INITIAL_CHUNK_SIZE;
  static const int MAX_CHUNK_SIZE = MemPool::MAX_CHUNK_SIZE;
};

const int MemPoolTest::INITIAL_CHUNK_SIZE;
const int MemPoolTest::MAX_CHUNK_SIZE;

TEST(MemPoolTest, Basic) {
  MemPool p;
  MemPool p2;
  MemPool p3;

  for (int iter = 0; iter < 2; ++iter) {
    // allocate a total of 24K in 32-byte pieces (for which we only request 25 bytes)
    for (int i = 0; i < 768; ++i) {
      // pads to 32 bytes
      p.Allocate(25);
    }
    // we handed back 24K
    EXPECT_EQ(24 * 1024, p.total_allocated_bytes());
    // .. and allocated 28K of chunks (4, 8, 16)
    EXPECT_EQ(28 * 1024, p.GetTotalChunkSizes());

    // we're passing on the first two chunks, containing 12K of data; we're left with
    // one chunk of 16K containing 12K of data
    p2.AcquireData(&p, true);
    EXPECT_EQ(12 * 1024, p.total_allocated_bytes());
    EXPECT_EQ(16 * 1024, p.GetTotalChunkSizes());

    // we allocate 8K, for which there isn't enough room in the current chunk,
    // so another one is allocated (32K)
    p.Allocate(8 * 1024);
    EXPECT_EQ((16 + 32) * 1024, p.GetTotalChunkSizes());

    // we allocate 65K, which doesn't fit into the current chunk or the default
    // size of the next allocated chunk (64K)
    p.Allocate(65 * 1024);
    EXPECT_EQ((12 + 8 + 65) * 1024, p.total_allocated_bytes());
    if (iter == 0) {
      EXPECT_EQ((12 + 8 + 65) * 1024, p.peak_allocated_bytes());
    } else {
      EXPECT_EQ((1 + 120 + 33) * 1024, p.peak_allocated_bytes());
    }
    EXPECT_EQ((16 + 32 + 65) * 1024, p.GetTotalChunkSizes());

    // Clear() resets allocated data, but doesn't remove any chunks
    p.Clear();
    EXPECT_EQ(0, p.total_allocated_bytes());
    if (iter == 0) {
      EXPECT_EQ((12 + 8 + 65) * 1024, p.peak_allocated_bytes());
    } else {
      EXPECT_EQ((1 + 120 + 33) * 1024, p.peak_allocated_bytes());
    }
    EXPECT_EQ((16 + 32 + 65) * 1024, p.GetTotalChunkSizes());

    // next allocation reuses existing chunks
    p.Allocate(1024);
    EXPECT_EQ(1024, p.total_allocated_bytes());
    if (iter == 0) {
      EXPECT_EQ((12 + 8 + 65) * 1024, p.peak_allocated_bytes());
    } else {
      EXPECT_EQ((1 + 120 + 33) * 1024, p.peak_allocated_bytes());
    }
    EXPECT_EQ((16 + 32 + 65) * 1024, p.GetTotalChunkSizes());

    // ... unless it doesn't fit into any available chunk
    p.Allocate(120 * 1024);
    EXPECT_EQ((1 + 120) * 1024, p.total_allocated_bytes());
    if (iter == 0) {
      EXPECT_EQ((1 + 120) * 1024, p.peak_allocated_bytes());
    } else {
      EXPECT_EQ((1 + 120 + 33) * 1024, p.peak_allocated_bytes());
    }
    EXPECT_EQ((130 + 16 + 32 + 65) * 1024, p.GetTotalChunkSizes());

    // ... Try another chunk that fits into an existing chunk
    p.Allocate(33 * 1024);
    EXPECT_EQ((1 + 120 + 33) * 1024, p.total_allocated_bytes());
    EXPECT_EQ((130 + 16 + 32 + 65) * 1024, p.GetTotalChunkSizes());

    // we're releasing 3 chunks, which get added to p2
    p2.AcquireData(&p, false);
    EXPECT_EQ(0, p.total_allocated_bytes());
    EXPECT_EQ((1 + 120 + 33) * 1024, p.peak_allocated_bytes());
    EXPECT_EQ(0, p.GetTotalChunkSizes());

    p3.AcquireData(&p2, true);  // we're keeping the 65k chunk
    EXPECT_EQ(33 * 1024, p2.total_allocated_bytes());
    EXPECT_EQ(65 * 1024, p2.GetTotalChunkSizes());

    p.FreeAll();
    p2.FreeAll();
    p3.FreeAll();
  }
}

// Test that we can keep an allocated chunk and a free chunk.
// This case verifies that when chunks are acquired by another memory pool the
// remaining chunks are consistent if there were more than one used chunk and some
// free chunks.
TEST(MemPoolTest, Keep) {
  MemPool p;
  p.Allocate(4 * 1024);
  p.Allocate(8 * 1024);
  p.Allocate(16 * 1024);
  EXPECT_EQ((4 + 8 + 16) * 1024, p.total_allocated_bytes());
  EXPECT_EQ((4 + 8 + 16) * 1024, p.GetTotalChunkSizes());
  p.Clear();
  EXPECT_EQ(0, p.total_allocated_bytes());
  EXPECT_EQ((4 + 8 + 16) * 1024, p.GetTotalChunkSizes());
  p.Allocate(1 * 1024);
  p.Allocate(4 * 1024);
  EXPECT_EQ((1 + 4) * 1024, p.total_allocated_bytes());
  EXPECT_EQ((4 + 8 + 16) * 1024, p.GetTotalChunkSizes());

  MemPool p2;
  p2.AcquireData(&p, true);
  EXPECT_EQ(4 * 1024, p.total_allocated_bytes());
  EXPECT_EQ((8 + 16) * 1024, p.GetTotalChunkSizes());
  EXPECT_EQ(1 * 1024, p2.total_allocated_bytes());
  EXPECT_EQ(4 * 1024, p2.GetTotalChunkSizes());

  p.FreeAll();
  p2.FreeAll();
}

// Tests that we can return partial allocations.
TEST(MemPoolTest, ReturnPartial) {
  MemPool p;
  uint8_t* ptr = p.Allocate(1024);
  EXPECT_EQ(1024, p.total_allocated_bytes());
  memset(ptr, 0, 1024);
  p.ReturnPartialAllocation(1024);

  uint8_t* ptr2 = p.Allocate(1024);
  EXPECT_EQ(1024, p.total_allocated_bytes());
  EXPECT_TRUE(ptr == ptr2);
  p.ReturnPartialAllocation(1016);

  ptr2 = p.Allocate(1016);
  EXPECT_EQ(1024, p.total_allocated_bytes());
  EXPECT_TRUE(ptr2 == ptr + 8);
  p.ReturnPartialAllocation(512);
  memset(ptr2, 1, 1016 - 512);

  uint8_t* ptr3 = p.Allocate(512);
  EXPECT_EQ(1024, p.total_allocated_bytes());
  EXPECT_TRUE(ptr3 == ptr + 512);
  memset(ptr3, 2, 512);

  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(0, ptr[i]);
  }
  for (int i = 8; i < 512; ++i) {
    EXPECT_EQ(1, ptr[i]);
  }
  for (int i = 512; i < 1024; ++i) {
    EXPECT_EQ(2, ptr[i]);
  }

  p.FreeAll();
}

// Test that the MemPool overhead is bounded when we make allocations of
// INITIAL_CHUNK_SIZE.
TEST(MemPoolTest, MemoryOverhead) {
  MemPool p;
  const int alloc_size = MemPoolTest::INITIAL_CHUNK_SIZE;
  const int num_allocs = 1000;
  int64_t total_allocated = 0;

  for (int i = 0; i < num_allocs; ++i) {
    uint8_t* mem = p.Allocate(alloc_size);
    ASSERT_TRUE(mem != NULL);
    total_allocated += alloc_size;

    int64_t wasted_memory = p.GetTotalChunkSizes() - total_allocated;
    // The initial chunk fits evenly into MAX_CHUNK_SIZE, so should have at most
    // one empty chunk at the end.
    EXPECT_LE(wasted_memory, MemPoolTest::MAX_CHUNK_SIZE);
    // The chunk doubling algorithm should not allocate chunks larger than the total
    // amount of memory already allocated.
    EXPECT_LE(wasted_memory, total_allocated);
  }

  p.FreeAll();
}

// Test that the MemPool overhead is bounded when we make alternating large and small
// allocations.
TEST(MemPoolTest, FragmentationOverhead) {
  MemPool p;
  const int num_allocs = 100;
  int64_t total_allocated = 0;

  for (int i = 0; i < num_allocs; ++i) {
    int alloc_size = i % 2 == 0 ? 1 : MemPoolTest::MAX_CHUNK_SIZE;
    uint8_t* mem = p.Allocate(alloc_size);
    ASSERT_TRUE(mem != NULL);
    total_allocated += alloc_size;

    int64_t wasted_memory = p.GetTotalChunkSizes() - total_allocated;
    // Fragmentation should not waste more than half of each completed chunk.
    EXPECT_LE(wasted_memory, total_allocated + MemPoolTest::MAX_CHUNK_SIZE);
  }

  p.FreeAll();
}

}  // namespace parquet
