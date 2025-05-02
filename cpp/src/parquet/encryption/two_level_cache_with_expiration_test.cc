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

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/concurrent_map.h"

#include "parquet/encryption/two_level_cache_with_expiration.h"

namespace parquet::encryption::test {

using ::arrow::SleepFor;

class TwoLevelCacheWithExpirationTest : public ::testing::Test {
 public:
  void SetUp() {
    // lifetime is 0.2s
    std::shared_ptr<ConcurrentMap<std::string, int>> lifetime1 =
        cache_.GetOrCreateInternalCache("lifetime1", 0.2);
    lifetime1->Insert("item1", 1);
    lifetime1->Insert("item2", 2);

    // lifetime is 0.5s
    std::shared_ptr<ConcurrentMap<std::string, int>> lifetime2 =
        cache_.GetOrCreateInternalCache("lifetime2", 0.5);
    lifetime2->Insert("item21", 21);
    lifetime2->Insert("item22", 22);
  }

 protected:
  void TaskInsert(int thread_no) {
    for (int i = 0; i < 20; i++) {
      std::string token = (i % 2 == 0) ? "lifetime1" : "lifetime2";
      double lifetime = (i % 2 == 0) ? 0.2 : 0.5;
      auto internal_cache = cache_.GetOrCreateInternalCache(token, lifetime);
      std::stringstream ss;
      ss << "item_" << thread_no << "_" << i;
      internal_cache->Insert(ss.str(), i);
      SleepFor(0.005);
    }
  }

  void TaskClean() {
    for (int i = 0; i < 20; i++) {
      cache_.Clear();
      SleepFor(0.008);
    }
  }

  TwoLevelCacheWithExpiration<int> cache_;
};

TEST_F(TwoLevelCacheWithExpirationTest, RemoveExpiration) {
  auto lifetime1_before_expiration = cache_.GetOrCreateInternalCache("lifetime1", 1);
  ASSERT_EQ(lifetime1_before_expiration->size(), 2);

  // wait for 0.3s, we expect:
  // lifetime1 will be expired
  // lifetime2 will not be expired
  SleepFor(0.3);
  // now clear expired items from the cache
  cache_.CheckCacheForExpiredTokens();

  // lifetime1 (with 2 items) is expired and has been removed from the cache.
  // Now the cache create a new object which has no item.
  auto lifetime1 = cache_.GetOrCreateInternalCache("lifetime1", 1);
  ASSERT_EQ(lifetime1->size(), 0);

  // However, lifetime1_before_expiration can still access normally and independently
  // from the one in cache
  lifetime1_before_expiration->Insert("item3", 3);
  ASSERT_EQ(lifetime1_before_expiration->size(), 3);
  ASSERT_EQ(lifetime1->size(), 0);

  // lifetime2 is not expired and still contains 2 items.
  std::shared_ptr<ConcurrentMap<std::string, int>> lifetime2 =
      cache_.GetOrCreateInternalCache("lifetime2", 3);
  ASSERT_EQ(lifetime2->size(), 2);
}

TEST_F(TwoLevelCacheWithExpirationTest, CleanupPeriodOk) {
  // wait for 0.3s, now:
  // lifetime1 is expired
  // lifetime2 isn't expired
  SleepFor(0.3);

  // cleanup_period is 0.2s, less than or equals lifetime of both items, so the expired
  // items will be removed from cache.
  cache_.CheckCacheForExpiredTokens(0.2);

  // lifetime1 (with 2 items) is expired and has been removed from the cache.
  // Now the cache create a new object which has no item.
  auto lifetime1 = cache_.GetOrCreateInternalCache("lifetime1", 1);
  ASSERT_EQ(lifetime1->size(), 0);

  // lifetime2 is not expired and still contains 2 items.
  auto lifetime2 = cache_.GetOrCreateInternalCache("lifetime2", 3);
  ASSERT_EQ(lifetime2->size(), 2);

  // The further process is added to test whether the timestamp is set correctly.
  // CheckCacheForExpiredTokens() should be called at least twice to verify the
  // correctness.
  SleepFor(0.3);
  cache_.CheckCacheForExpiredTokens(0.2);
  lifetime2 = cache_.GetOrCreateInternalCache("lifetime2", 0.5);
  ASSERT_EQ(lifetime2->size(), 0);
}

TEST_F(TwoLevelCacheWithExpirationTest, RemoveByToken) {
  cache_.Remove("lifetime1");

  // lifetime1 (with 2 items) has been removed from the cache.
  // Now the cache create a new object which has no item.
  auto lifetime1 = cache_.GetOrCreateInternalCache("lifetime1", 1);
  ASSERT_EQ(lifetime1->size(), 0);

  // lifetime2 is still contains 2 items.
  auto lifetime2 = cache_.GetOrCreateInternalCache("lifetime2", 3);
  ASSERT_EQ(lifetime2->size(), 2);

  cache_.Remove("lifetime2");
  auto lifetime2_after_removed = cache_.GetOrCreateInternalCache("lifetime2", 3);
  ASSERT_EQ(lifetime2_after_removed->size(), 0);
}

TEST_F(TwoLevelCacheWithExpirationTest, RemoveAllTokens) {
  cache_.Clear();

  // All tokens has been removed from the cache.
  // Now the cache create a new object which has no item.
  auto lifetime1 = cache_.GetOrCreateInternalCache("lifetime1", 1);
  ASSERT_EQ(lifetime1->size(), 0);

  auto lifetime2 = cache_.GetOrCreateInternalCache("lifetime2", 3);
  ASSERT_EQ(lifetime2->size(), 0);
}

TEST_F(TwoLevelCacheWithExpirationTest, Clear) {
  cache_.Clear();

  // All tokens has been removed from the cache.
  // Now the cache create a new object which has no item.
  auto lifetime1 = cache_.GetOrCreateInternalCache("lifetime1", 1);
  ASSERT_EQ(lifetime1->size(), 0);

  auto lifetime2 = cache_.GetOrCreateInternalCache("lifetime2", 3);
  ASSERT_EQ(lifetime2->size(), 0);
}

TEST_F(TwoLevelCacheWithExpirationTest, MultiThread) {
  std::vector<std::thread> insert_threads;
  for (int i = 0; i < 10; i++) {
    insert_threads.emplace_back([this, i]() { this->TaskInsert(i); });
  }
  std::thread clean_thread([this]() { this->TaskClean(); });

  for (auto& th : insert_threads) {
    th.join();
  }
  clean_thread.join();
}

}  // namespace parquet::encryption::test
