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

#include "gandiva/greedy_dual_size_cache.h"

#include <string>
#include <typeinfo>

#include <gtest/gtest.h>

namespace gandiva {

class GreedyDualSizeCacheKey {
 public:
  explicit GreedyDualSizeCacheKey(int tmp) : tmp_(tmp) {}
  std::size_t Hash() const { return tmp_; }
  bool operator==(const GreedyDualSizeCacheKey& other) const {
    return tmp_ == other.tmp_;
  }

 private:
  int tmp_;
};

class TestGreedyDualSizeCache : public ::testing::Test {
 public:
  TestGreedyDualSizeCache() : cache_(2) {}

 protected:
  GreedyDualSizeCache<GreedyDualSizeCacheKey, std::string> cache_;
};

TEST_F(TestGreedyDualSizeCache, TestEvict) {
  // check if the cache is evicting the items with low priority on cache
  cache_.insert(GreedyDualSizeCacheKey(1), ValueCacheObject<std::string>("1", 1, 1));
  cache_.insert(GreedyDualSizeCacheKey(2), ValueCacheObject<std::string>("2", 10, 10));
  cache_.insert(GreedyDualSizeCacheKey(3), ValueCacheObject<std::string>("3", 20, 20));
  cache_.insert(GreedyDualSizeCacheKey(4), ValueCacheObject<std::string>("4", 15, 15));
  cache_.insert(GreedyDualSizeCacheKey(1), ValueCacheObject<std::string>("5", 1, 1));
  ASSERT_EQ(16, cache_.size());
  // we check initially the values that won't be on the cache, since the get operation
  // may affect the entity costs, which is not the purpose of this test
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(2)), arrow::util::nullopt);
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(3)), arrow::util::nullopt);
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(1))->module, "5");
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(4))->module, "4");
}

TEST_F(TestGreedyDualSizeCache, TestGreedyDualSizeBehavior) {
  // insert 1 and 3 evicting 2 (this eviction will increase the inflation cost by 20)
  cache_.insert(GreedyDualSizeCacheKey(1), ValueCacheObject<std::string>("1", 40, 40));
  cache_.insert(GreedyDualSizeCacheKey(2), ValueCacheObject<std::string>("2", 20, 20));
  cache_.insert(GreedyDualSizeCacheKey(3), ValueCacheObject<std::string>("3", 30, 30));

  // when accessing key 3, its actual cost will be increased by the inflation, so in the
  // next eviction, the key 1 will be evicted, since the key 1 actual cost (original(40))
  // is smaller than key 3 actual increased cost (original(30) + inflation(20))
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(3))->module, "3");

  // try to insert key 2 and expect the eviction of key 1
  cache_.insert(GreedyDualSizeCacheKey(2), ValueCacheObject<std::string>("2", 20, 20));
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(1)), arrow::util::nullopt);

  // when accessing key 2, its original cost should be increased by inflation, so when
  // inserting the key 1 again, now the key 3 should be evicted
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(2))->module, "2");
  cache_.insert(GreedyDualSizeCacheKey(1), ValueCacheObject<std::string>("1", 20, 20));

  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(1))->module, "1");
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(3)), arrow::util::nullopt);
  ASSERT_EQ(20, cache_.size());
}
}  // namespace gandiva
