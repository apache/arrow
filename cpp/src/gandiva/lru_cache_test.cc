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

#include "gandiva/lru_cache.h"

#include <map>
#include <string>
#include <typeinfo>

#include <gtest/gtest.h>

namespace gandiva {

class TestCacheKey {
 public:
  explicit TestCacheKey(int tmp) : tmp_(tmp) {}
  std::size_t Hash() const { return tmp_; }
  bool operator==(const TestCacheKey& other) const { return tmp_ == other.tmp_; }

 private:
  int tmp_;
};

class TestLruCache : public ::testing::Test {
 public:
  TestLruCache() : cache_(2) {}

 protected:
  LruCache<TestCacheKey, std::string> cache_;
};

TEST_F(TestLruCache, TestEvict) {
  cache_.insert(TestCacheKey(1), "hello");
  cache_.insert(TestCacheKey(2), "hello");
  cache_.insert(TestCacheKey(1), "hello");
  cache_.insert(TestCacheKey(3), "hello");
  // should have evicted key 1
  ASSERT_EQ(2, cache_.size());
  ASSERT_EQ(cache_.get(TestCacheKey(1)), boost::none);
}

TEST_F(TestLruCache, TestLruBehavior) {
  cache_.insert(TestCacheKey(1), "hello");
  cache_.insert(TestCacheKey(2), "hello");
  cache_.get(TestCacheKey(1));
  cache_.insert(TestCacheKey(3), "hello");
  // should have evicted key 2.
  ASSERT_EQ(cache_.get(TestCacheKey(1)).value(), "hello");
}
}  // namespace gandiva
