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
#include "arrow/util/logging.h"

namespace gandiva {

#ifdef GANDIVA_ENABLE_OBJECT_CODE_CACHE
static const size_t DEFAULT_CACHE_SIZE = 500000;
#else
static const size_t DEFAULT_CACHE_SIZE = 500;
#endif

int GetCapacity() {
  size_t capacity;
  const char* env_cache_size = std::getenv("GANDIVA_CACHE_SIZE");
  if (env_cache_size != nullptr) {
    capacity = std::atol(env_cache_size);
    if (capacity <= 0) {
      ARROW_LOG(WARNING) << "Invalid cache size provided. Using default cache size: "
                         << DEFAULT_CACHE_SIZE;
      capacity = DEFAULT_CACHE_SIZE;
    }
  } else {
    capacity = DEFAULT_CACHE_SIZE;
  }
  return static_cast<int>(capacity);
}

void LogCacheSize(size_t capacity) {
  ARROW_LOG(INFO) << "Creating gandiva cache with capacity of " << capacity;
}

}  // namespace gandiva
