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

#include "arrow/util/deduplicate_internal.h"

#include <mutex>
#include <type_traits>
#include <unordered_map>

#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow::util {
namespace {

template <typename T>
struct Deduplicator {
  static_assert(std::is_base_of_v<::arrow::detail::Fingerprintable, T>);

  std::shared_ptr<T> Deduplicate(std::shared_ptr<T> obj) {
    auto lock = std::lock_guard{mutex_};
    if (ARROW_PREDICT_FALSE(++lookups_since_last_pruning_ >= kPruneEvery)) {
      Prune();
      lookups_since_last_pruning_ = 0;
    }

    const std::string& fingerprint = obj->fingerprint();
    if (fingerprint.empty()) {
      // Fingerprinting failure
      return obj;
    }
    auto [it, inserted] = cache_.try_emplace(fingerprint, obj);
    if (inserted) {
      return obj;
    }
    auto cached = it->second.lock();
    if (cached) {
      return cached;
    }
    it->second = obj;
    return obj;
  }

 private:
  void Prune() {
    auto it = cache_.begin();
    while (it != cache_.end()) {
      auto cur = it++;  // cur will be invalidated on erasure, so increment now
      if (cur->second.expired()) {
        cache_.erase(cur);
      }
    }
  }

  static constexpr int kPruneEvery = 100;

  std::mutex mutex_;
  // TODO fingerprints can be large, we should use a fast cryptographic hash instead,
  // such as Blake3
  std::unordered_map<std::string, std::weak_ptr<T>> cache_;
  int lookups_since_last_pruning_ = 0;
};

Deduplicator<Field> g_field_deduplicator;
Deduplicator<Schema> g_schema_deduplicator;

}  // namespace

std::shared_ptr<Field> Deduplicate(std::shared_ptr<Field> field) {
  return g_field_deduplicator.Deduplicate(std::move(field));
}

std::shared_ptr<Schema> Deduplicate(std::shared_ptr<Schema> schema) {
  return g_schema_deduplicator.Deduplicate(std::move(schema));
}

}  // namespace arrow::util
