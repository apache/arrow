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
#include <iostream>
#include <mutex>

#include "gandiva/lru_cache.h"

namespace gandiva {

template <class KeyType, typename ValueType>
class Cache {
 public:
  explicit Cache(size_t capacity) : cache_(capacity) {
    std::cout << "Creating gandiva cache with capacity: " << capacity << std::endl;
  }

  Cache() : Cache(GetCapacity()) {}

  ValueType GetModule(KeyType cache_key) {
    arrow::util::optional<ValueType> result;
    mtx_.lock();
    result = cache_.get(cache_key);
    mtx_.unlock();
    return result != arrow::util::nullopt ? *result : nullptr;
  }

  void PutModule(KeyType cache_key, ValueType module) {
    mtx_.lock();
    cache_.insert(cache_key, module);
    mtx_.unlock();
  }

 private:
  static int GetCapacity() {
    int capacity;
    const char* env_cache_size = std::getenv("GANDIVA_CACHE_SIZE");
    if (env_cache_size != nullptr) {
      capacity = std::atoi(env_cache_size);
      if (capacity <= 0) {
        std::cout << "Invalid cache size provided. Using default cache size."
                  << std::endl;
        capacity = DEFAULT_CACHE_SIZE;
      }
    } else {
      capacity = DEFAULT_CACHE_SIZE;
    }
    return capacity;
  }

  LruCache<KeyType, ValueType> cache_;
  static const int DEFAULT_CACHE_SIZE = 500;
  std::mutex mtx_;
};
}  // namespace gandiva
