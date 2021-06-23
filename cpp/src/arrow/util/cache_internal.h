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

#pragma once

#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/util/functional.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

// A LRU (Least recently used) replacement cache
template <typename Key, typename Value>
class LruCache {
 public:
  explicit LruCache(int32_t capacity) : capacity_(capacity) {
    // The map size can temporarily exceed the cache capacity, see Replace()
    map_.reserve(capacity_ + 1);
  }

  ARROW_DISALLOW_COPY_AND_ASSIGN(LruCache);
  ARROW_DEFAULT_MOVE_AND_ASSIGN(LruCache);

  void Clear() {
    items_.clear();
    map_.clear();
    // The C++ spec doesn't tell whether map_.clear() will shrink the map capacity
    map_.reserve(capacity_ + 1);
  }

  int32_t size() const {
    DCHECK_EQ(items_.size(), map_.size());
    return static_cast<int32_t>(items_.size());
  }

  template <typename K>
  Value* Find(K&& key) {
    const auto it = map_.find(key);
    if (it == map_.end()) {
      return nullptr;
    } else {
      // Found => move item at front of the list
      auto list_it = it->second;
      items_.splice(items_.begin(), items_, list_it);
      return &list_it->value;
    }
  }

  template <typename K, typename V>
  std::pair<bool, Value*> Replace(K&& key, V&& value) {
    // Try to insert temporary iterator
    auto pair = map_.emplace(std::forward<K>(key), ListIt{});
    const auto it = pair.first;
    const bool inserted = pair.second;
    if (inserted) {
      // Inserted => push item at front of the list, and update iterator
      items_.push_front(Item{&it->first, std::forward<V>(value)});
      it->second = items_.begin();
      // Did we exceed the cache capacity?  If so, remove least recently used item
      if (static_cast<int32_t>(items_.size()) > capacity_) {
        const bool erased = map_.erase(*items_.back().key);
        DCHECK(erased);
        ARROW_UNUSED(erased);
        items_.pop_back();
      }
      return {true, &it->second->value};
    } else {
      // Already exists => move item at front of the list, and update value
      auto list_it = it->second;
      items_.splice(items_.begin(), items_, list_it);
      list_it->value = std::forward<V>(value);
      return {false, &list_it->value};
    }
  }

 private:
  struct Item {
    // Pointer to the key inside the unordered_map
    const Key* key;
    Value value;
  };
  using List = std::list<Item>;
  using ListIt = typename List::iterator;

  const int32_t capacity_;
  // In most to least recently used order
  std::list<Item> items_;
  std::unordered_map<Key, ListIt> map_;
};

namespace detail {

template <typename Key, typename Value, typename Cache, typename Func>
struct ThreadSafeMemoizer {
  using RetType = Value;

  template <typename F>
  ThreadSafeMemoizer(F&& func, int32_t cache_capacity)
      : func_(std::forward<F>(func)), cache_(cache_capacity) {}

  // The memoizer can't return a pointer to the cached value, because
  // the cache entry may be evicted by another thread.

  Value operator()(const Key& key) {
    std::unique_lock<std::mutex> lock(mutex_);
    const Value* value_ptr;
    value_ptr = cache_.Find(key);
    if (ARROW_PREDICT_TRUE(value_ptr != nullptr)) {
      return *value_ptr;
    }
    lock.unlock();
    Value v = func_(key);
    lock.lock();
    return *cache_.Replace(key, std::move(v)).second;
  }

 private:
  std::mutex mutex_;
  Func func_;
  Cache cache_;
};

template <typename Key, typename Value, typename Cache, typename Func>
struct ThreadUnsafeMemoizer {
  using RetType = const Value&;

  template <typename F>
  ThreadUnsafeMemoizer(F&& func, int32_t cache_capacity)
      : func_(std::forward<F>(func)), cache_(cache_capacity) {}

  const Value& operator()(const Key& key) {
    const Value* value_ptr;
    value_ptr = cache_.Find(key);
    if (ARROW_PREDICT_TRUE(value_ptr != nullptr)) {
      return *value_ptr;
    }
    return *cache_.Replace(key, func_(key)).second;
  }

 private:
  Func func_;
  Cache cache_;
};

template <template <typename...> class Cache, template <typename...> class MemoizerType,
          typename Func,
          typename Key = typename std::decay<call_traits::argument_type<0, Func>>::type,
          typename Value = typename std::decay<call_traits::return_type<Func>>::type,
          typename Memoizer = MemoizerType<Key, Value, Cache<Key, Value>, Func>,
          typename RetType = typename Memoizer::RetType>
static std::function<RetType(const Key&)> Memoize(Func&& func, int32_t cache_capacity) {
  // std::function<> requires copy constructibility
  struct {
    RetType operator()(const Key& key) const { return (*memoized_)(key); }
    std::shared_ptr<Memoizer> memoized_;
  } shared_memoized = {
      std::make_shared<Memoizer>(std::forward<Func>(func), cache_capacity)};

  return shared_memoized;
}

}  // namespace detail

// Apply a LRU memoization cache to a callable.
template <typename Func>
static auto MemoizeLru(Func&& func, int32_t cache_capacity)
    -> decltype(detail::Memoize<LruCache, detail::ThreadSafeMemoizer>(
        std::forward<Func>(func), cache_capacity)) {
  return detail::Memoize<LruCache, detail::ThreadSafeMemoizer>(std::forward<Func>(func),
                                                               cache_capacity);
}

// Like MemoizeLru, but not thread-safe.  This version allows for much faster
// lookups (more than 2x faster), but you'll have to manage thread safety yourself.
// A recommended usage is to declare per-thread caches using `thread_local`
// (see cache_benchmark.cc).
template <typename Func>
static auto MemoizeLruThreadUnsafe(Func&& func, int32_t cache_capacity)
    -> decltype(detail::Memoize<LruCache, detail::ThreadUnsafeMemoizer>(
        std::forward<Func>(func), cache_capacity)) {
  return detail::Memoize<LruCache, detail::ThreadUnsafeMemoizer>(std::forward<Func>(func),
                                                                 cache_capacity);
}

}  // namespace internal
}  // namespace arrow
