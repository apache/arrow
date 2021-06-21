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

#include <llvm/Support/MemoryBuffer.h>

#include <boost/any.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <unordered_map>
#include <utility>

#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "llvm/Support/FileSystem.h"
#include "llvm/Support/Path.h"

// modified from boost LRU cache -> the boost cache supported only an
// ordered map.
namespace gandiva {
// a cache which evicts the least recently used item when it is full
template <class Key, class Value>
class LruCache {
 public:
  using key_type = Key;
  using value_type = Value;
  using list_type = std::list<key_type>;
  struct hasher {
    template <typename I>
    std::size_t operator()(const I& i) const {
      return i.Hash();
    }
  };
  using map_type =
  std::unordered_map<key_type, std::pair<value_type, typename list_type::iterator>,
      hasher>;

  explicit LruCache(size_t capacity) : cache_capacity_(capacity) {}

  ~LruCache() {}

  size_t size() const { return map_.size(); }

  size_t capacity() const { return cache_capacity_; }

  bool empty() const { return map_.empty(); }

  bool contains(const key_type& key) { return map_.find(key) != map_.end(); }

  void insert(const key_type& key, const value_type& value) {
    typename map_type::iterator i = map_.find(key);
    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full
      if (size() >= cache_capacity_) {
        // cache is full, evict the least recently used item
        evict();
      }

      // insert the new item
      lru_list_.push_front(key);
      map_[key] = std::make_pair(value, lru_list_.begin());
      cache_size_ += sizeof(key);
      cache_size_ += sizeof(*value.get());
    }
  }

  void insertObject(key_type& key, const value_type value, size_t object_cache_size) {
    typename map_type::iterator i = map_.find(key);

    if (i == map_.end()) {

      // insert item into the cache, but first check if it is full
      if (getLruCacheSize() >= cache_capacity_) {
        // cache is full, evict the least recently used item
        evitObjectSafely(object_cache_size);
      }

      if (getLruCacheSize() + object_cache_size >= cache_capacity_) {
        // cache will pass the maximum capacity, evict the least recently used items
        evitObjectSafely(object_cache_size);
      }

      // insert the new item
      lru_list_.push_front(key);
      map_[key] = std::make_pair(value, lru_list_.begin());
      size_map_[key] = std::make_pair(object_cache_size, lru_list_.begin());
      cache_size_ += object_cache_size;
    }
  }

  arrow::util::optional<value_type> get(const key_type& key) {
    // lookup value in the cache
    typename map_type::iterator value_for_key = map_.find(key);
    if (value_for_key == map_.end()) {
      // value not in cache
      return arrow::util::nullopt;
    }

    // return the value, but first update its place in the most
    // recently used list
    typename list_type::iterator position_in_lru_list = value_for_key->second.second;
    if (position_in_lru_list != lru_list_.begin()) {
      // move item to the front of the most recently used list
      lru_list_.erase(position_in_lru_list);
      lru_list_.push_front(key);

      // update iterator in map
      position_in_lru_list = lru_list_.begin();
      const value_type& value = value_for_key->second.first;
      map_[key] = std::make_pair(value, position_in_lru_list);

      // return the value
      return value;
    } else {
      // the item is already at the front of the most recently
      // used list so just return it
      return value_for_key->second.first;
    }
  }

  arrow::util::optional<value_type> getObject(const key_type& key) {
    // lookup value in the cache
    typename map_type::iterator value_for_key = map_.find(key);

    std::string obj_file_name = "obj-" + std::to_string(key.Hash()) + ".cache";
    llvm::SmallString<128>obj_cache_file = cache_dir_;
    llvm::sys::path::append(obj_cache_file, obj_file_name);

    if (value_for_key == map_.end()) {
      return arrow::util::nullopt;
    }

    // return the value, but first update its place in the most
    // recently used list
    typename list_type::iterator position_in_lru_list = value_for_key->second.second;
    if (position_in_lru_list != lru_list_.begin()) {
      // move item to the front of the most recently used list
      lru_list_.erase(position_in_lru_list);
      lru_list_.push_front(key);

      // update iterator in map
      position_in_lru_list = lru_list_.begin();
      const value_type& value = value_for_key->second.first;
      map_[key] = std::make_pair(value, position_in_lru_list);

      // return the value
      return value;
    } else {
      // the item is already at the front of the most recently
      // used list so just return it
      return value_for_key->second.first;
    }
  }

  void clear() {
    map_.clear();
    lru_list_.clear();
    cache_size_ = 0;
  }

  std::string toString(){
    auto lru_size = lru_list_.size();
    std::string string = "LRU Cache list size: " + std::to_string(lru_size) + "."
                         + " LRU Cache size: " + std::to_string(cache_size_);
    return string;
  }

  size_t getLruCacheSize(){
    return cache_size_;
  }


 private:
  void evict() {
    // evict item from the end of most recently used list
    typename list_type::iterator i = --lru_list_.end();
    map_.erase(*i);
    lru_list_.erase(i);
  }

  void evictObject() {
    // evict item from the end of most recently used list
    typename list_type::iterator i = --lru_list_.end();
    const size_t size_to_decrease = size_map_.find(*i)->second.first;
    const value_type value = map_.find(*i)->second.first;
    cache_size_ = cache_size_ - size_to_decrease;
    map_.erase(*i);
    size_map_.erase(*i);
    lru_list_.erase(i);
  }

  void evitObjectSafely(size_t object_cache_size) {
    while (cache_size_ + object_cache_size >= cache_capacity_) {
      evictObject();
    }
  }



 private:
  map_type map_;
  list_type lru_list_;
  size_t cache_capacity_;
  size_t cache_size_ = 0;
  std::unordered_map<key_type, std::pair<size_t, typename list_type::iterator>,
      hasher> size_map_;
  llvm::SmallString<128> cache_dir_;
};
}  // namespace gandiva
