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
#include <mutex>

#include "gandiva/lru_cache.h"
#include "gandiva/visibility.h"

namespace gandiva {

GANDIVA_EXPORT
int GetCapacity();

GANDIVA_EXPORT
void LogCacheSize(size_t capacity);

template <class KeyType, typename ValueType>
class Cache {
 public:
  explicit Cache(size_t capacity) : cache_(capacity) { LogCacheSize(capacity); }

  Cache() : Cache(GetCapacity()) {}

  ValueType GetObjectCode(KeyType cache_key) {
    arrow::util::optional<ValueType> result;
    mtx_.lock();
    result = cache_.get(cache_key);
    mtx_.unlock();
    return result != arrow::util::nullopt ? *result : nullptr;
  }

  void PutObjectCode(KeyType cache_key, ValueType module) {
    mtx_.lock();
    cache_.insert(cache_key, module);
    mtx_.unlock();
  }

 private:
  LruCache<KeyType, ValueType> cache_;
  std::mutex mtx_;
};
}  // namespace gandiva
