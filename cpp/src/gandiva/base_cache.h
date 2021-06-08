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

namespace gandiva {

// Defines a base value object supported on the cache that may contain properties
template <typename ValueType>
class ValueCacheObject {
 public:
  explicit ValueCacheObject(ValueType module, uint64_t cost)
      : module(module), cost(cost) {}
  ValueCacheObject(){};
  ValueType module;
  uint64_t cost;
  bool operator<(const ValueCacheObject& other) const { return this->cost < other.cost; }
};

// A base cache class which defines the main methods that should be implemented
// to expose a different cache with different policies.
template <class Key, class Value>
class BaseCache {
 public:
  explicit BaseCache<Key, Value>(size_t capacity) : cache_capacity_(capacity) {}

  BaseCache<Key, Value>() = default;

  virtual ~BaseCache() = default;

  virtual size_t size() const { return this->cache_capacity_; }

  virtual size_t capacity() const = 0;

  virtual bool empty() const = 0;

  virtual bool contains(const Key& key) = 0;

  virtual void insert(const Key& key, const ValueCacheObject<Value>& value) = 0;

  virtual arrow::util::optional<ValueCacheObject<Value>> get(const Key& key) = 0;

  virtual void clear() = 0;

 private:
  void evict() {}

 protected:
  size_t cache_capacity_{};
};

}  // namespace gandiva
