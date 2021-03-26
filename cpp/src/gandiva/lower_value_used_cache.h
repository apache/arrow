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
#include <set>

#include "gandiva/base_cache.h"
#include "arrow/util/optional.h"

// modified cache to support evict policy of lower value used.
namespace gandiva {
// a cache which evicts the lower value used item when it is full
template <class Key, class Value>
class LowerValueUsedCache : public BaseCache<Key, Value>{
 public:
  struct hasher {
    template <typename I>
    std::size_t operator()(const I& i) const {
      return i.Hash();
    }
  };
  using map_type =
  std::unordered_map<Key,
  std::pair<Value, typename std::set<std::pair<u_long, Key>> ::iterator>, hasher>;

  explicit LowerValueUsedCache(size_t capacity) : BaseCache<Key, Value>(capacity) {}

  LowerValueUsedCache<Key, Value>() : BaseCache<Key, Value>() {}

  size_t size() const override { return map_.size(); }

  size_t capacity() const override { return this->cache_capacity_; }

  bool empty() const override { return map_.empty(); }

  bool contains(const Key& key) override { return map_.find(key) != map_.end(); }

  void insert(const Key& key, const Value& value, const u_long value_to_order) override {
    typename map_type::iterator i = map_.find(key);
    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full
      if (size() >= this->cache_capacity_) {
        // check if the value should be inserted on cache, otherwise just return
        if (value_to_order <= lvu_set_.begin()->first) return;

        // cache is full, evict the least recently used item
        evict();
      }

      // insert the new item
      lvu_set_.insert(std::make_pair(value_to_order, key));
      map_[key] = std::make_pair(value, lvu_set_.begin());
    }
  }

  arrow::util::optional<Value> get(const Key& key) override {
    // lookup value in the cache
    typename map_type::iterator value_for_key = map_.find(key);
    if (value_for_key == map_.end()) {
      // value not in cache
      return arrow::util::nullopt;
    }
    return value_for_key->second.first;
  }

  void clear() override {
    map_.clear();
    lvu_set_.clear();
  }

 private:
  void evict() {
    // evict item from the beginning of the set. This set is ordered from the
    // lower value constant to the higher value.
    typename std::set<std::pair<u_long, Key>>::iterator i = lvu_set_.begin();
    map_.erase((*i).second);
    lvu_set_.erase(i);
  }

 private:
  map_type map_;
  std::set<std::pair<u_long, Key>> lvu_set_;
};
}  // namespace gandiva
