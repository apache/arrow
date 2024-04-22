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

namespace gandiva {

static const int DEFAULT_CACHE_SIZE = 5000;

namespace internal {
int GetCapacityFromEnvVar() {
  auto maybe_env_value = ::arrow::internal::GetEnvVar("GANDIVA_CACHE_SIZE");
  if (!maybe_env_value.ok()) {
    return DEFAULT_CACHE_SIZE;
  }
  const auto env_value = *std::move(maybe_env_value);
  if (env_value.empty()) {
    return DEFAULT_CACHE_SIZE;
  }
  int capacity = std::atoi(env_value.c_str());
  if (capacity <= 0) {
    ARROW_LOG(WARNING) << "Invalid cache size provided in GANDIVA_CACHE_SIZE. "
                       << "Using default cache size: " << DEFAULT_CACHE_SIZE;
    return DEFAULT_CACHE_SIZE;
  }
  return capacity;
}
}  // namespace internal

int GetCapacity() {
  static const int capacity = internal::GetCapacityFromEnvVar();
  return capacity;
}

void LogCacheSize(size_t capacity) {
  ARROW_LOG(INFO) << "Creating gandiva cache with capacity of " << capacity;
}

}  // namespace gandiva
