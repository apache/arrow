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

#include "arrow/util/concurrent_map.h"

#include "parquet/encryption/two_level_cache_with_expiration.h"

namespace parquet {
namespace encryption {
namespace test {

class TwoLevelCacheWithExpirationTest : public ::testing::Test {
 public:
  void SetUp() {
    // lifetime is 1s
    std::shared_ptr<ConcurrentMap<int>> lifetime_1s =
        cache_.GetOrCreateInternalCache("lifetime1s", 1000);
    lifetime_1s->Insert("item1", 1);
    lifetime_1s->Insert("item2", 2);

    // lifetime is 3s
    std::shared_ptr<ConcurrentMap<int>> lifetime_3s =
        cache_.GetOrCreateInternalCache("lifetime3s", 3000);
    lifetime_3s->Insert("item21", 21);
    lifetime_3s->Insert("item22", 22);
  }

 protected:
  void TaskInsert(int thread_no) {
    for (int i = 0; i < 20; i++) {
      std::string token = i % 2 == 0 ? "lifetime1s" : "lifetime3s";
      uint64_t lifetime_ms = i % 2 == 0 ? 1000 : 3000;
      std::shared_ptr<ConcurrentMap<int>> internal_cache =
          cache_.GetOrCreateInternalCache(token, lifetime_ms);
      std::stringstream ss;
      ss << "item_" << thread_no << "_" << i;
      internal_cache->Insert(ss.str(), i);
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  void TaskClean() {
    for (int i = 0; i < 20; i++) {
      cache_.Clear();
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }

  TwoLevelCacheWithExpiration<int> cache_;
};

TEST_F(TwoLevelCacheWithExpirationTest, RemoveExpiration) {
  std::shared_ptr<ConcurrentMap<int>> lifetime_1s_before_expiration =
      cache_.GetOrCreateInternalCache("lifetime1s", 1000);
  ASSERT_EQ(lifetime_1s_before_expiration->size(), 2);

  // wait for 2s, we expect:
  // lifetime_1s will be expired
  // lifetime_3s will not be expired
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // now clear expired items from the cache
  cache_.RemoveExpiredEntriesFromCache();

  // lifetime_1s (with 2 items) is expired and has been removed from the cache.
  // Now the cache create a new object which has no item.
  std::shared_ptr<ConcurrentMap<int>> lifetime_1s =
      cache_.GetOrCreateInternalCache("lifetime1s", 1000);
  ASSERT_EQ(lifetime_1s->size(), 0);

  // However, lifetime_1s_before_expiration can still access normally and independently
  // from the one in cache
  lifetime_1s_before_expiration->Insert("item3", 3);
  ASSERT_EQ(lifetime_1s_before_expiration->size(), 3);
  ASSERT_EQ(lifetime_1s->size(), 0);

  // lifetime_3s is not expired and still contains 2 items.
  std::shared_ptr<ConcurrentMap<int>> lifetime_3s =
      cache_.GetOrCreateInternalCache("lifetime3s", 3000);
  ASSERT_EQ(lifetime_3s->size(), 2);
}

TEST_F(TwoLevelCacheWithExpirationTest, CleanupPeriodTooBig) {
  // wait for 2s, now:
  // lifetime_1s is expired
  // lifetime_3s isn't expired
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // if cleanup_period is too big (10s), the expired items may not be removed from cache.
  cache_.CheckCacheForExpiredTokens(10000);

  // lifetime_1s (with 2 items) is expired but not removed from the cache, still contains
  // 2 items
  std::shared_ptr<ConcurrentMap<int>> lifetime_1s =
      cache_.GetOrCreateInternalCache("lifetime1s", 1000);
  ASSERT_EQ(lifetime_1s->size(), 2);

  // lifetime_3s is not expired and still contains 2 items.
  std::shared_ptr<ConcurrentMap<int>> lifetime_3s =
      cache_.GetOrCreateInternalCache("lifetime3s", 3000);
  ASSERT_EQ(lifetime_3s->size(), 2);
}

TEST_F(TwoLevelCacheWithExpirationTest, CleanupPeriodOk) {
  // wait for 2s, now:
  // lifetime_1s is expired
  // lifetime_3s isn't expired
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // cleanup_period is 1s, less than or equals lifetime of both items, so the expired
  // items will be removed from cache.
  cache_.CheckCacheForExpiredTokens(1000);

  // lifetime_1s (with 2 items) is expired and has been removed from the cache.
  // Now the cache create a new object which has no item.
  std::shared_ptr<ConcurrentMap<int>> lifetime_1s =
      cache_.GetOrCreateInternalCache("lifetime1s", 1000);
  ASSERT_EQ(lifetime_1s->size(), 0);

  // lifetime_3s is not expired and still contains 2 items.
  std::shared_ptr<ConcurrentMap<int>> lifetime_3s =
      cache_.GetOrCreateInternalCache("lifetime3s", 3000);
  ASSERT_EQ(lifetime_3s->size(), 2);
}

TEST_F(TwoLevelCacheWithExpirationTest, RemoveByToken) {
  cache_.RemoveCacheEntriesForToken("lifetime1s");

  // lifetime_1s (with 2 items) has been removed from the cache.
  // Now the cache create a new object which has no item.
  std::shared_ptr<ConcurrentMap<int>> lifetime_1s =
      cache_.GetOrCreateInternalCache("lifetime1s", 1000);
  ASSERT_EQ(lifetime_1s->size(), 0);

  // lifetime_3s is still contains 2 items.
  std::shared_ptr<ConcurrentMap<int>> lifetime_3s =
      cache_.GetOrCreateInternalCache("lifetime3s", 3000);
  ASSERT_EQ(lifetime_3s->size(), 2);

  cache_.Remove("lifetime3s");
  std::shared_ptr<ConcurrentMap<int>> lifetime_3s_after_removed =
      cache_.GetOrCreateInternalCache("lifetime3s", 3000);
  ASSERT_EQ(lifetime_3s_after_removed->size(), 0);
}

TEST_F(TwoLevelCacheWithExpirationTest, RemoveAllTokens) {
  cache_.RemoveCacheEntriesForAllTokens();

  // All tokens has been removed from the cache.
  // Now the cache create a new object which has no item.
  std::shared_ptr<ConcurrentMap<int>> lifetime_1s =
      cache_.GetOrCreateInternalCache("lifetime1s", 1000);
  ASSERT_EQ(lifetime_1s->size(), 0);

  std::shared_ptr<ConcurrentMap<int>> lifetime_3s =
      cache_.GetOrCreateInternalCache("lifetime3s", 3000);
  ASSERT_EQ(lifetime_3s->size(), 0);
}

TEST_F(TwoLevelCacheWithExpirationTest, Clear) {
  cache_.Clear();

  // All tokens has been removed from the cache.
  // Now the cache create a new object which has no item.
  std::shared_ptr<ConcurrentMap<int>> lifetime_1s =
      cache_.GetOrCreateInternalCache("lifetime1s", 1000);
  ASSERT_EQ(lifetime_1s->size(), 0);

  std::shared_ptr<ConcurrentMap<int>> lifetime_3s =
      cache_.GetOrCreateInternalCache("lifetime3s", 3000);
  ASSERT_EQ(lifetime_3s->size(), 0);
}

TEST_F(TwoLevelCacheWithExpirationTest, MultiThread) {
  std::vector<std::thread> insert_threads;
  for (size_t i = 0; i < 10; i++) {
    insert_threads.push_back(std::thread([this, i]() { this->TaskInsert(i); }));
  }
  std::thread clean_thread([this]() { this->TaskClean(); });

  for (size_t i = 0; i < insert_threads.size(); i++) {
    insert_threads[i].join();
  }
  clean_thread.join();
}

}  // namespace test
}  // namespace encryption
}  // namespace parquet
