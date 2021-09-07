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

// static const int DEFAULT_CACHE_SIZE = 500; //old cache capacity of 500 itens list
static const size_t DEFAULT_CACHE_SIZE = 256 * 1024 * 1024;  // bytes or 256 MiB
static const size_t DEFAULT_DISK_CACHE_SIZE = 1ULL * 1024 * 1024 * 1024;  // bytes or 1
                                                                          // GiB
static const size_t DEFAULT_RESERVED_SIZE = 10ULL * 1024 * 1024 * 1024;   // bytes or 10
                                                                          // GiB

size_t GetCapacity() {
  size_t capacity;
  const char* env_cache_size = std::getenv("GANDIVA_CACHE_SIZE");
  if (env_cache_size != nullptr) {
    capacity = std::stoul(env_cache_size);

    if (capacity <= 0) {
      ARROW_LOG(WARNING) << "Invalid cache size provided. Using default cache size: "
                         << DEFAULT_CACHE_SIZE;
      capacity = DEFAULT_CACHE_SIZE;
    }
  } else {
    capacity = DEFAULT_CACHE_SIZE;
  }

  return capacity;
}

size_t GetDiskCapacity() {
  size_t capacity;
  const char* env_disk_cache_size = std::getenv("GANDIVA_DISK_CAPACITY_SIZE");
  if (env_disk_cache_size != nullptr) {
    capacity = std::stoul(env_disk_cache_size);

    if (capacity <= 0) {
      ARROW_LOG(WARNING) << "Invalid cache size provided. Using default cache size: "
                         << DEFAULT_DISK_CACHE_SIZE;
      capacity = DEFAULT_DISK_CACHE_SIZE;
    }

  } else {
    capacity = DEFAULT_DISK_CACHE_SIZE;
  }

  return capacity;
}

size_t GetReserved() {
  size_t reserved;
  const char* env_reserved_size = std::getenv("GANDIVA_DISK_RESERVED_SIZE");
  if (env_reserved_size != nullptr) {
    reserved = std::stoul(env_reserved_size);

    if (reserved <= 0) {
      ARROW_LOG(WARNING) << "Invalid cache size provided. Using default cache size: "
                         << DEFAULT_RESERVED_SIZE;
      reserved = DEFAULT_RESERVED_SIZE;
    }
  } else {
    reserved = DEFAULT_RESERVED_SIZE;
  }

  return reserved;
}

/*void LogCacheSize(size_t capacity) {
  ARROW_LOG(DEBUG) << "Creating gandiva cache with capacity of " << capacity << " bytes";
}*/

void LogCacheSizeSafely(size_t capacity, size_t disk_capacity, size_t reserved) {
  ARROW_LOG(DEBUG) << "Creating gandiva cache with memory capacity of " << capacity
                   << " bytes";
  ARROW_LOG(DEBUG) << "Creating gandiva cache with disk space of " << disk_capacity
                   << " bytes";
  ARROW_LOG(DEBUG) << "Creating gandiva cache with reserved disk space of " << reserved
                   << " bytes";
}

}  // namespace gandiva
