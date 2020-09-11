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

#include <mutex>
#include <unordered_map>

#include "parquet/key_toolkit_internal.h"

namespace parquet {
namespace encryption {

// Two-level cache with expiration of internal caches according to token lifetime.
// External cache is per token, internal is per string key.
// Wrapper class around:
//    std::unordered_map<std::string,
//    internal::ExpiringCacheEntry<std::unordered_map<std::string, V>>>
// This cache is safe to be shared between threads.
template <typename V>
class TwoLevelCacheWithExpiration {
 public:
  TwoLevelCacheWithExpiration() {
    last_cache_cleanup_timestamp_ = internal::CurrentTimePoint();
  }

  std::unordered_map<std::string, V>& GetOrCreateInternalCache(
      const std::string& access_token, uint64_t cache_entry_lifetime_ms) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto external_cache_entry = cache_.find(access_token);
    if (external_cache_entry == cache_.end() ||
        external_cache_entry->second.IsExpired()) {
      cache_.insert({access_token,
                     internal::ExpiringCacheEntry<std::unordered_map<std::string, V>>(
                         std::unordered_map<std::string, V>(), cache_entry_lifetime_ms)});
    }

    return cache_[access_token].cached_item();
  }

  void RemoveCacheEntriesForToken(const std::string& access_token) {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.erase(access_token);
  }

  void RemoveCacheEntriesForAllTokens() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
  }

  void CheckCacheForExpiredTokens(uint64_t cache_cleanup_period) {
    std::lock_guard<std::mutex> lock(mutex_);

    internal::TimePoint now = internal::CurrentTimePoint();

    if (now > (last_cache_cleanup_timestamp_ +
               std::chrono::milliseconds(cache_cleanup_period))) {
      RemoveExpiredEntriesNoMutex();
      last_cache_cleanup_timestamp_ =
          now + std::chrono::milliseconds(cache_cleanup_period);
    }
  }

  void RemoveExpiredEntriesFromCache() {
    std::lock_guard<std::mutex> lock(mutex_);

    RemoveExpiredEntriesNoMutex();
  }

  void Remove(const std::string& access_token) {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.remove(access_token);
  }

  void Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
  }

 private:
  void RemoveExpiredEntriesNoMutex() {
    for (auto it = cache_.begin(); it != cache_.end();) {
      if (it->second.IsExpired()) {
        it = cache_.erase(it);
      } else {
        ++it;
      }
    }
  }
  std::unordered_map<std::string,
                     internal::ExpiringCacheEntry<std::unordered_map<std::string, V>>>
      cache_;
  internal::TimePoint last_cache_cleanup_timestamp_;
  std::mutex mutex_;
};

}  // namespace encryption
}  // namespace parquet
