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

#include <list>
#include <unordered_map>
#include <utility>

#include "arrow/util/optional.h"
#include "gandiva/base_cache.h"

// modified from boost LRU cache -> the boost cache supported only an
// ordered map.
namespace gandiva {
// a cache which evicts the least recently used item when it is full
template <class Key, class Value>
class LruCache : public BaseCache<Key, Value> {
 public:
  struct hasher {
    template <typename I>
    std::size_t operator()(const I& i) const {
      return i.Hash();
    }
  };
  using map_type =
      std::unordered_map<Key, std::pair<Value, typename std::list<Key>::iterator>,
                         hasher>;

  explicit LruCache(size_t capacity) : BaseCache<Key, Value>(capacity) {}

  LruCache<Key, Value>() : BaseCache<Key, Value>() {}

  size_t size() const override { return map_.size(); }

  size_t capacity() const override { return this->cache_capacity_; }

  bool empty() const override { return map_.empty(); }

  bool contains(const Key& key) override { return map_.find(key) != map_.end(); }

  void insert(const Key& key, const Value& value, const u_long value_to_order) override {
    typename map_type::iterator i = map_.find(key);
    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full
      if (size() >= this->cache_capacity_) {
        // cache is full, evict the least recently used item
        evict();
      }

      // insert the new item
      lru_list_.push_front(key);
      map_[key] = std::make_pair(value, lru_list_.begin());
    }
  }

  arrow::util::optional<Value> get(const Key& key) override {
    // lookup value in the cache
    typename map_type::iterator value_for_key = map_.find(key);
    if (value_for_key == map_.end()) {
      // value not in cache
      return arrow::util::nullopt;
    }

    // return the value, but first update its place in the most
    // recently used list
    typename std::list<Key>::iterator position_in_lru_list = value_for_key->second.second;
    if (position_in_lru_list != lru_list_.begin()) {
      // move item to the front of the most recently used list
      lru_list_.erase(position_in_lru_list);
      lru_list_.push_front(key);

      // update iterator in map
      position_in_lru_list = lru_list_.begin();
      const Value& value = value_for_key->second.first;
      map_[key] = std::make_pair(value, position_in_lru_list);

      // return the value
      return value;
    } else {
      // the item is already at the front of the most recently
      // used list so just return it
      return value_for_key->second.first;
    }
  }

  void clear() override {
    map_.clear();
    lru_list_.clear();
  }

 private:
  void evict() {
    // evict item from the end of most recently used list
    typename std::list<Key>::iterator i = --lru_list_.end();
    map_.erase(*i);
    lru_list_.erase(i);
  }

 private:
  map_type map_;
  std::list<Key> lru_list_;
};
}  // namespace gandiva
