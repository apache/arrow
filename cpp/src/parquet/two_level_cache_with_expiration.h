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

#include <chrono>
#include <map>

// #include "arrow/filesystem/filesystem.h"

// in miliseconds
using TimePoint = std::chrono::system_clock::time_point;

static inline TimePoint CurrentTimePoint() { return std::chrono::system_clock::now(); }

template <typename V>
class TwoLevelCacheWithExpiration {
 public:
  TwoLevelCacheWithExpiration() { last_cache_cleanup_timestamp_ = CurrentTimePoint(); }

  std::map<std::string, V> GetOrCreateInternalCache(const std::string& access_token,
                                                    uint64_t cache_entry_life_time) {
    auto external_cache_entry = cache_.find(access_token);
    if (external_cache_entry == cache_.end() || external_cache_entry->isExpired()) {
      cache_.insert(
          {access_token, ExpiringCacheEntry<std::map<std::string, V>>(
                             std::map<std::string, V>(), cache_entry_life_time)});
    }
    return cache_[access_token];
  }

 private:
  template <typename E>
  class ExpiringCacheEntry {
   private:
    TimePoint expiration_timestamp_;
    E cached_item_;

    ExpiringCacheEntry(const E& cached_item, uint64_t expiration_interval_millis)
        : cached_item_(cached_item) {
      expiration_timestamp_ =
          CurrentTimePoint() + std::chrono::milliseconds(expiration_interval_millis);
    }

    bool isExpired() {
      auto now = CurrentTimePoint();
      return (now > expiration_timestamp_);
    }

    const E& cached_item() const { return cached_item_; }
  };

  std::map<std::string, ExpiringCacheEntry<std::map<std::string, V>>> cache_;
  volatile TimePoint last_cache_cleanup_timestamp_;
};
