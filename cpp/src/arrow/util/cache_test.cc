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

#include <atomic>
#include <cstdint>
#include <functional>
#include <ostream>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/cache_internal.h"

namespace arrow {
namespace internal {

template <typename K1, typename V1, typename K2, typename V2>
void AssertPairsEqual(std::pair<K1, V1> left, std::pair<K2, V2> right) {
  ASSERT_EQ(left.first, right.first);
  ASSERT_EQ(left.second, right.second);
}

class IntValue {
 public:
  explicit IntValue(int value = 0) : value_(std::make_shared<int>(value)) {}

  IntValue(const IntValue&) = default;
  IntValue(IntValue&&) = default;
  IntValue& operator=(const IntValue&) = default;
  IntValue& operator=(IntValue&&) = default;

  int value() const { return *value_; }

  bool operator==(const IntValue& other) const { return *value_ == *other.value_; }
  bool operator!=(const IntValue& other) const { return *value_ != *other.value_; }

  friend std::ostream& operator<<(std::ostream& os, IntValue v) {
    os << "IntValue{" << *v.value_ << "}";
    return os;
  }

 private:
  // The shared_ptr makes it easier to detect lifetime bugs
  std::shared_ptr<int> value_;
};

template <typename Value>
Value Identity(Value&& v) {
  return std::forward<Value>(v);
}

class TestLruCache : public ::testing::Test {
 public:
  using K = std::string;
  using V = IntValue;
  using Cache = LruCache<K, V>;

  K MakeKey(int num) { return std::to_string(num); }

  const V* Find(Cache* cache, int num) { return cache->Find(MakeKey(num)); }

  bool Replace(Cache* cache, int num, int value_num) {
    auto pair = cache->Replace(MakeKey(num), V{value_num});
    EXPECT_NE(pair.second, nullptr);
    EXPECT_EQ(*pair.second, V{value_num});
    return pair.first;
  }
};

TEST_F(TestLruCache, Basics) {
  Cache cache(10);

  using namespace std::placeholders;  // NOLINT [build/namespaces]
  auto Replace = std::bind(&TestLruCache::Replace, this, &cache, _1, _2);
  auto Find = std::bind(&TestLruCache::Find, this, &cache, _1);

  ASSERT_EQ(cache.size(), 0);
  ASSERT_EQ(Find(100), nullptr);

  // Insertions
  ASSERT_TRUE(Replace(100, 100));
  ASSERT_TRUE(Replace(101, 101));
  ASSERT_TRUE(Replace(102, 102));
  ASSERT_EQ(cache.size(), 3);
  ASSERT_EQ(*Find(100), V{100});
  ASSERT_EQ(*Find(101), V{101});
  ASSERT_EQ(*Find(102), V{102});

  // Replacements
  ASSERT_FALSE(Replace(100, -100));
  ASSERT_FALSE(Replace(101, -101));
  ASSERT_FALSE(Replace(102, -102));
  ASSERT_EQ(cache.size(), 3);
  ASSERT_EQ(*Find(100), V{-100});
  ASSERT_EQ(*Find(101), V{-101});
  ASSERT_EQ(*Find(102), V{-102});

  ASSERT_EQ(cache.size(), 3);
  cache.Clear();
  ASSERT_EQ(cache.size(), 0);
}

TEST_F(TestLruCache, Eviction) {
  Cache cache(5);

  using namespace std::placeholders;  // NOLINT [build/namespaces]
  auto Replace = std::bind(&TestLruCache::Replace, this, &cache, _1, _2);
  auto Find = std::bind(&TestLruCache::Find, this, &cache, _1);

  for (int i = 100; i < 105; ++i) {
    ASSERT_TRUE(Replace(i, i));
  }
  ASSERT_EQ(cache.size(), 5);

  // Access keys in a specific order
  for (int i : {102, 103, 101, 104, 100}) {
    ASSERT_EQ(*Find(i), V{i});
  }
  // Insert more entries
  ASSERT_TRUE(Replace(105, 105));
  ASSERT_TRUE(Replace(106, 106));
  // The least recently used keys were evicted
  ASSERT_EQ(Find(102), nullptr);
  ASSERT_EQ(Find(103), nullptr);
  for (int i : {100, 101, 104, 105, 106}) {
    ASSERT_EQ(*Find(i), V{i});
  }

  // Alternate insertions and replacements
  // MRU = [106, 105, 104, 101, 100]
  ASSERT_FALSE(Replace(106, -106));
  // MRU = [106, 105, 104, 101, 100]
  ASSERT_FALSE(Replace(100, -100));
  // MRU = [100, 106, 105, 104, 101]
  ASSERT_FALSE(Replace(104, -104));
  // MRU = [104, 100, 106, 105, 101]
  ASSERT_TRUE(Replace(102, -102));
  // MRU = [102, 104, 100, 106, 105]
  ASSERT_TRUE(Replace(101, -101));
  // MRU = [101, 102, 104, 100, 106]
  for (int i : {101, 102, 104, 100, 106}) {
    ASSERT_EQ(*Find(i), V{-i});
  }
  ASSERT_EQ(Find(103), nullptr);
  ASSERT_EQ(Find(105), nullptr);

  // MRU = [106, 100, 104, 102, 101]
  ASSERT_TRUE(Replace(103, -103));
  // MRU = [103, 106, 100, 104, 102]
  ASSERT_TRUE(Replace(105, -105));
  // MRU = [105, 103, 106, 100, 104]
  for (int i : {105, 103, 106, 100, 104}) {
    ASSERT_EQ(*Find(i), V{-i});
  }
  ASSERT_EQ(Find(101), nullptr);
  ASSERT_EQ(Find(102), nullptr);
}

struct Callable {
  std::atomic<int> num_calls{0};

  IntValue operator()(const std::string& s) {
    ++num_calls;
    return IntValue{std::stoi(s)};
  }
};

struct MemoizeLruFactory {
  template <typename Func,
            typename RetType = decltype(MemoizeLru(std::declval<Func>(), 0))>
  RetType operator()(Func&& func, int32_t capacity) {
    return MemoizeLru(std::forward<Func>(func), capacity);
  }
};

struct MemoizeLruThreadUnsafeFactory {
  template <typename Func,
            typename RetType = decltype(MemoizeLruThreadUnsafe(std::declval<Func>(), 0))>
  RetType operator()(Func&& func, int32_t capacity) {
    return MemoizeLruThreadUnsafe(std::forward<Func>(func), capacity);
  }
};

template <typename T>
class TestMemoizeLru : public ::testing::Test {
 public:
  using K = std::string;
  using V = IntValue;
  using MemoizerFactory = T;

  K MakeKey(int num) { return std::to_string(num); }

  void TestBasics() {
    using V = IntValue;
    Callable c;

    auto mem = factory_(c, 5);

    // Cache fills
    for (int i = 0; i < 5; ++i) {
      ASSERT_EQ(mem(MakeKey(i)), V{i});
    }
    ASSERT_EQ(c.num_calls, 5);

    // Cache hits
    for (int i : {1, 3, 4, 0, 2}) {
      ASSERT_EQ(mem(MakeKey(i)), V{i});
    }
    ASSERT_EQ(c.num_calls, 5);

    // Calling with other inputs will cause evictions
    for (int i = 5; i < 8; ++i) {
      ASSERT_EQ(mem(MakeKey(i)), V{i});
    }
    ASSERT_EQ(c.num_calls, 8);
    // Hits
    for (int i : {0, 2, 5, 6, 7}) {
      ASSERT_EQ(mem(MakeKey(i)), V{i});
    }
    ASSERT_EQ(c.num_calls, 8);
    // Misses
    for (int i : {1, 3, 4}) {
      ASSERT_EQ(mem(MakeKey(i)), V{i});
    }
    ASSERT_EQ(c.num_calls, 11);
  }

 protected:
  MemoizerFactory factory_;
};

using MemoizeLruTestTypes =
    ::testing::Types<MemoizeLruFactory, MemoizeLruThreadUnsafeFactory>;

TYPED_TEST_SUITE(TestMemoizeLru, MemoizeLruTestTypes);

TYPED_TEST(TestMemoizeLru, Basics) { this->TestBasics(); }

class TestMemoizeLruThreadSafe : public TestMemoizeLru<MemoizeLruFactory> {};

TEST_F(TestMemoizeLruThreadSafe, Threads) {
  using V = IntValue;
  Callable c;

  auto mem = this->factory_(c, 15);
  const int n_threads = 4;
#ifdef ARROW_VALGRIND
  const int n_iters = 10;
#else
  const int n_iters = 100;
#endif

  auto thread_func = [&]() {
    for (int i = 0; i < n_iters; ++i) {
      const V& orig_value = mem("1");
      // Ensure that some replacements are going on
      // (# distinct keys > cache size)
      for (int j = 0; j < 30; ++j) {
        ASSERT_EQ(mem(std::to_string(j)), V{j});
      }
      ASSERT_EQ(orig_value, V{1});
    }
  };
  std::vector<std::thread> threads;
  for (int i = 0; i < n_threads; ++i) {
    threads.emplace_back(thread_func);
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

}  // namespace internal
}  // namespace arrow
