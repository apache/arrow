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

namespace parquet {
namespace encryption {
namespace internal {

// in miliseconds
using TimePoint = std::chrono::system_clock::time_point;

static inline TimePoint CurrentTimePoint() { return std::chrono::system_clock::now(); }

template <typename E>
class ExpiringCacheEntry {
 public:
  ExpiringCacheEntry() = default;

  ExpiringCacheEntry(const E& cached_item, uint64_t expiration_interval_millis)
      : cached_item_(cached_item) {
    expiration_timestamp_ =
        CurrentTimePoint() + std::chrono::milliseconds(expiration_interval_millis);
  }

  bool IsExpired() {
    auto now = CurrentTimePoint();
    return (now > expiration_timestamp_);
  }

  E& cached_item() { return cached_item_; }

 private:
  TimePoint expiration_timestamp_;
  E cached_item_;
};

}  // namespace internal
}  // namespace encryption
}  // namespace parquet
