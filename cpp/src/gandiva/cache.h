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

#include <cstdlib>
#include <memory>
#include <mutex>

#include "gandiva/base_cache.h"
#include "gandiva/greedy_dual_size_cache.h"
#include "gandiva/lru_cache.h"
#include "gandiva/visibility.h"

namespace gandiva {

GANDIVA_EXPORT
int GetCapacity();

GANDIVA_EXPORT
void LogCacheSize(size_t capacity);

GANDIVA_EXPORT
int GetCacheTypeToUse();

template <class KeyType, typename ValueType>
class Cache {
 public:
  explicit Cache(size_t capacity, int cache_type_to_use) {
    if (cache_type_to_use == 0) {
      this->cache_ =
          std::make_unique<GreedyDualSizeCache<KeyType, ValueObject>>(capacity);
    } else {
      this->cache_ = std::make_unique<LruCache<KeyType, ValueObject>>(capacity);
    }
    LogCacheSize(capacity);
  }

  Cache() : Cache(GetCapacity(), GetCacheTypeToUse()) {}

  ValueType GetModule(KeyType cache_key) {
    arrow::util::optional<ValueObject> result;
    mtx_.lock();
    result = (*cache_).get(cache_key);
    mtx_.unlock();
    return result != arrow::util::nullopt ? (*result).module : nullptr;
  }

  void PutModule(KeyType cache_key, ValueType module, uint64_t priority) {
    // Define value_to_order if the cache being used considers it, otherwise define 0
    mtx_.lock();
    ValueObject value = *std::make_unique<ValueObject>(module, priority);
    (*cache_).insert(cache_key, value);
    mtx_.unlock();
  }

 private:
  class ValueObject {
   public:
    explicit ValueObject(ValueType module, uint64_t cost) : module(module), cost(cost) {}
    ValueObject() {};
    ValueType module;
    uint64_t cost;
    bool operator<(const ValueObject& other) const { return this->cost < other.cost; }
  };
  std::unique_ptr<BaseCache<KeyType, ValueObject>> cache_;
  std::mutex mtx_;
};
}  // namespace gandiva
