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
  bool operator<(const GreedyDualSizeCacheKey& other) const { return tmp_ < other.tmp_; }

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
  cache_.insert(GreedyDualSizeCacheKey(1), "bye", 1);
  cache_.insert(GreedyDualSizeCacheKey(2), "bye", 10);
  cache_.insert(GreedyDualSizeCacheKey(1), "bye", 1);
  cache_.insert(GreedyDualSizeCacheKey(3), "bye", 20);
  cache_.insert(GreedyDualSizeCacheKey(4), "bye", 100);
  cache_.insert(GreedyDualSizeCacheKey(1), "bye", 1);
  ASSERT_EQ(2, cache_.size());
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(1)), arrow::util::nullopt);
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(2)), arrow::util::nullopt);
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(3)), "bye");
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(4)), "bye");
}

TEST_F(TestGreedyDualSizeCache, TestGreedyDualSizeBehavior) {
  // should insert key 1 and 2
  cache_.insert(GreedyDualSizeCacheKey(1), "bye", 1);
  cache_.insert(GreedyDualSizeCacheKey(2), "bye", 10);
  cache_.insert(GreedyDualSizeCacheKey(1), "bye", 1);
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(1)), "bye");
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(2)), "bye");

  // should insert key 3 evicting key 1 (because value to order of key 3 is higher)
  cache_.insert(GreedyDualSizeCacheKey(3), "bye", 20);
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(3)), "bye");
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(2)), "bye");
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(1)), arrow::util::nullopt);

  // should insert key 4 evicting key 2 (because value to order of key 4 is higher)
  cache_.insert(GreedyDualSizeCacheKey(4), "bye", 100);
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(3)), "bye");
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(4)), "bye");
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(2)), arrow::util::nullopt);
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(1)), arrow::util::nullopt);

  // should not insert key 1 on cache (because the value to order is lower)
  cache_.insert(GreedyDualSizeCacheKey(1), "bye", 1);
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(3)), "bye");
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(4)), "bye");
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(1)), arrow::util::nullopt);
  ASSERT_EQ(cache_.get(GreedyDualSizeCacheKey(2)), arrow::util::nullopt);
}
}  // namespace gandiva
