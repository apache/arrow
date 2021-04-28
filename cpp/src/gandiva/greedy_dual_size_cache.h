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
#include <queue>
#include <set>
#include <unordered_map>
#include <utility>

#include "arrow/util/optional.h"
#include "gandiva/base_cache.h"

// modified cache to support evict policy using the GreedyDual-Size algorithm.
namespace gandiva {
// a particular LRU based cache which evicts the least recently used item
// considering the different costs for each entry.
template <class Key, class Value>
class GreedyDualSizeCache : public BaseCache<Key, Value> {
 // inner class to define the priority item
 class PriorityItem {
  public:
   PriorityItem(uint64_t actual_priority,
                uint64_t original_priority, Key key) : actual_priority(actual_priority),
                original_priority(original_priority), cache_key(key) {}
   // this ensure that the items with low priority stays in the beginning of the queue,
   // so it can be the one removed by evict operation
   bool operator<(const PriorityItem& other) const {
     return this->actual_priority < other.actual_priority;
   }
   uint64_t actual_priority;
   uint64_t original_priority;
   Key cache_key;
  };
 public:
  struct hasher {
    template <typename I>
    std::size_t operator()(const I& i) const {
      return i.Hash();
    }
  };
  // a map from 'key' to a pair of Value and a pointer to the priority value
  using map_type = std::unordered_map<
      Key, std::pair<Value, typename std::set<PriorityItem>::iterator>, hasher>;

  explicit GreedyDualSizeCache(size_t capacity) : BaseCache<Key, Value>(capacity) {
    this->inflation_cost_ = 0;
  }

  GreedyDualSizeCache<Key, Value>() : BaseCache<Key, Value>() {}

  ~GreedyDualSizeCache() = default;

  size_t size() const override { return map_.size(); }

  size_t capacity() const override { return this->cache_capacity_; }

  bool empty() const override { return map_.empty(); }

  bool contains(const Key& key) override { return map_.find(key) != map_.end(); }

  void insert(const Key& key, const Value& value, const uint64_t priority) override {
    typename map_type::iterator i = map_.find(key);
    // check if element is not in the cache to add it
    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full, to evict an item
      // if it is necessary
      if (size() >= this->cache_capacity_) {
        evict();
      }

      // insert the new item
      auto iter = this->priority_set_.insert(
          PriorityItem(priority + this->inflation_cost_, priority, key));
      // save on map the value and the priority item iterator position
      map_[key] = std::make_pair(value, iter.first);
    }
  }

  arrow::util::optional<Value> get(const Key& key) override {
    // lookup value in the cache
    typename map_type::iterator value_for_key = map_.find(key);
    if (value_for_key == map_.end()) {
      // value not in cache
      return arrow::util::nullopt;
    }
    PriorityItem item = *value_for_key->second.second;
    // if the value was found on the cache, update its cost (original + inflation)
    if (item.actual_priority != item.original_priority + this->inflation_cost_) {
      priority_set_.erase(value_for_key->second.second);
      auto iter = priority_set_.insert(PriorityItem(
          item.original_priority + this->inflation_cost_, item.original_priority,
          item.cache_key));
      value_for_key->second.second = iter.first;
    }
    return value_for_key->second.first;
  }

  void clear() override {
    map_.clear();
    priority_set_.clear();
  }

 private:
  void evict() {
    // evict item from the beginning of the set. This set is ordered from the
    // lower priority value to the higher priority value.
    typename std::set<PriorityItem>::iterator i = priority_set_.begin();
    // update the inflation cost related to the evicted item
    this->inflation_cost_ = (*i).actual_priority;
    map_.erase((*i).cache_key);
    priority_set_.erase(i);
  }

private:
  map_type map_;
  std::set<PriorityItem> priority_set_;
  int64_t inflation_cost_;
};
}  // namespace gandiva
