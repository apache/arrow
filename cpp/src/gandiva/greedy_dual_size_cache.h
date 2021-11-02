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

#include "arrow/util/logging.h"
#include "arrow/util/optional.h"

// modified cache to support evict policy using the GreedyDual-Size algorithm.
namespace gandiva {
// Defines a base value object supported on the cache that may contain properties
template <typename ValueType>
class ValueCacheObject {
 public:
  ValueCacheObject(ValueType module, uint64_t cost, size_t size)
      : module(module), cost(cost), size(size) {}
  ValueType module;
  uint64_t cost;
  size_t size;
  bool operator<(const ValueCacheObject& other) const { return cost < other.cost; }
};

// A particular cache based on the GreedyDual-Size cache which is a generalization of LRU
// which defines costs for each cache values.
// The algorithm associates a cost, C, with each cache value. Initially, when the value
// is brought into cache, C is set to be the cost related to the value (the cost is
// always non-negative). When a replacement needs to be made, the value with the lowest C
// cost is replaced, and then all values reduce their C costs by the minimum value of C
// over all the values already in the cache.
// If a value is accessed, its C value is restored to its initial cost. Thus, the C costs
// of recently accessed values retain a larger portion of the original cost than those of
// values that have not been accessed for a long time. The C costs are reduced as time
// goes and are restored when accessed.

template <class Key, class Value>
class GreedyDualSizeCache {
  // inner class to define the priority item
  class PriorityItem {
   public:
    PriorityItem(uint64_t actual_priority, uint64_t original_priority, Key key)
        : actual_priority(actual_priority),
          original_priority(original_priority),
          cache_key(key) {}
    // this ensure that the items with low priority stays in the beginning of the queue,
    // so it can be the one removed by evict operation
    bool operator<(const PriorityItem& other) const {
      return actual_priority < other.actual_priority;
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
      Key, std::pair<ValueCacheObject<Value>, typename std::set<PriorityItem>::iterator>,
      hasher>;

  explicit GreedyDualSizeCache(size_t capacity) : inflation_(0), capacity_(capacity) {}

  ~GreedyDualSizeCache() = default;

  size_t size() const { return size_; }

  size_t capacity() const { return capacity_; }

  bool empty() {
    size_ = 0;
    return map_.empty();
  }

  bool contains(const Key& key) { return map_.find(key) != map_.end(); }

  void insert(const Key& key, const ValueCacheObject<Value>& value) {
    typename map_type::iterator i = map_.find(key);
    // check if element is not in the cache to add it
    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full, to evict an item
      // if it is necessary
      if (size() >= capacity_) {
        evict();
      }

      // insert the new item
      auto item =
          priority_set_.insert(PriorityItem(value.cost + inflation_, value.cost, key));
      // save on map the value and the priority item iterator position
      map_.emplace(key, std::make_pair(value, item.first));
      size_ += value.size;
    }
  }

  arrow::util::optional<ValueCacheObject<Value>> get(const Key& key) {
    // lookup value in the cache
    typename map_type::iterator value_for_key = map_.find(key);
    if (value_for_key == map_.end()) {
      // value not in cache
      return arrow::util::nullopt;
    }
    PriorityItem item = *value_for_key->second.second;
    // if the value was found on the cache, update its cost (original + inflation)
    if (item.actual_priority != item.original_priority + inflation_) {
      priority_set_.erase(value_for_key->second.second);
      auto iter = priority_set_.insert(PriorityItem(
          item.original_priority + inflation_, item.original_priority, item.cache_key));
      value_for_key->second.second = iter.first;
    }
    return value_for_key->second.first;
  }

  void clear() {
    map_.clear();
    priority_set_.clear();
  }

 private:
  void evict() {
    // TODO: inflation overflow is unlikely to happen but needs to be handled
    //  for correctness.
    // evict item from the beginning of the set. This set is ordered from the
    // lower priority value to the higher priority value.
    typename std::set<PriorityItem>::iterator i = priority_set_.begin();
    // update the inflation cost related to the evicted item
    inflation_ = (*i).actual_priority;
    size_t size_to_decrease = map_.find((*i).cache_key)->second.first.size;
    size_ -= size_to_decrease;
    map_.erase((*i).cache_key);
    priority_set_.erase(i);
  }

  map_type map_;
  std::set<PriorityItem> priority_set_;
  uint64_t inflation_;
  size_t capacity_;
  size_t size_ = 0;
};
}  // namespace gandiva
