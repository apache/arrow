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

#include "gandiva/cache.h"

#include "arrow/result.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"

namespace gandiva {

constexpr auto kCacheCapacityEnvVar = "GANDIVA_CACHE_SIZE";
constexpr auto kDefaultCacheSize = 5000;

namespace internal {
int GetCacheCapacityFromEnvVar() {
  auto maybe_env_value = ::arrow::internal::GetEnvVar(kCacheCapacityEnvVar);
  if (!maybe_env_value.ok()) {
    return kDefaultCacheSize;
  }
  const auto env_value = *std::move(maybe_env_value);
  if (env_value.empty()) {
    return kDefaultCacheSize;
  }
  int capacity = 0;
  bool ok = ::arrow::internal::ParseValue<::arrow::Int32Type>(
      env_value.c_str(), env_value.size(), &capacity);
  if (!ok || capacity <= 0) {
    ARROW_LOG(WARNING) << "Invalid cache size provided in " << kCacheCapacityEnvVar
                       << ". Using default cache size: " << kDefaultCacheSize;
    return kDefaultCacheSize;
  }
  return capacity;
}
}  // namespace internal

// Deprecated in 17.0.0. Use GetCacheCapacity instead.
int GetCapacity() { return GetCacheCapacity(); }

int GetCacheCapacity() {
  static const int capacity = internal::GetCacheCapacityFromEnvVar();
  return capacity;
}

void LogCacheSize(size_t capacity) {
  ARROW_LOG(INFO) << "Creating gandiva cache with capacity of " << capacity;
}

}  // namespace gandiva
