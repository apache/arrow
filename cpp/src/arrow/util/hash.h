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

#ifndef ARROW_UTIL_HASH_H
#define ARROW_UTIL_HASH_H

#include <cstdint>
#include <limits>
#include <memory>

namespace arrow {

class Buffer;
class MemoryPool;
class Status;

typedef int32_t hash_slot_t;
static constexpr hash_slot_t kHashSlotEmpty = std::numeric_limits<int32_t>::max();

// Initially 1024 elements
static constexpr int kInitialHashTableSize = 1 << 10;

// The maximum load factor for the hash table before resizing.
static constexpr double kMaxHashTableLoad = 0.5;

namespace internal {

#define DOUBLE_TABLE_SIZE(SETUP_CODE, COMPUTE_HASH)                              \
  do {                                                                           \
    int64_t new_size = hash_table_size_ * 2;                                     \
                                                                                 \
    std::shared_ptr<Buffer> new_hash_table;                                      \
    RETURN_NOT_OK(internal::NewHashTable(new_size, pool_, &new_hash_table));     \
    int32_t* new_hash_slots =                                                    \
        reinterpret_cast<hash_slot_t*>(new_hash_table->mutable_data());          \
    int64_t new_mod_bitmask = new_size - 1;                                      \
                                                                                 \
    SETUP_CODE;                                                                  \
                                                                                 \
    for (int i = 0; i < hash_table_size_; ++i) {                                 \
      hash_slot_t index = hash_slots_[i];                                        \
                                                                                 \
      if (index == kHashSlotEmpty) {                                             \
        continue;                                                                \
      }                                                                          \
                                                                                 \
      COMPUTE_HASH;                                                              \
      while (kHashSlotEmpty != new_hash_slots[j]) {                              \
        ++j;                                                                     \
        if (ARROW_PREDICT_FALSE(j == new_size)) {                                \
          j = 0;                                                                 \
        }                                                                        \
      }                                                                          \
                                                                                 \
      new_hash_slots[j] = index;                                                 \
    }                                                                            \
                                                                                 \
    hash_table_ = new_hash_table;                                                \
    hash_slots_ = reinterpret_cast<hash_slot_t*>(hash_table_->mutable_data());   \
    hash_table_size_ = new_size;                                                 \
    hash_table_load_threshold_ =                                                 \
        static_cast<int64_t>(static_cast<double>(new_size) * kMaxHashTableLoad); \
    mod_bitmask_ = new_size - 1;                                                 \
  } while (false)

Status NewHashTable(int64_t size, MemoryPool* pool, std::shared_ptr<Buffer>* out);

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_HASH_H
