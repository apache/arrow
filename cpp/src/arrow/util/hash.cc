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

#include "arrow/util/hash.h"

#include "arrow/buffer.h"
#include "arrow/status.h"

namespace arrow {
namespace internal {

Status NewHashTable(int64_t size, MemoryPool* pool, std::shared_ptr<Buffer>* out) {
  auto hash_table = std::make_shared<PoolBuffer>(pool);

  RETURN_NOT_OK(hash_table->Resize(sizeof(hash_slot_t) * size));
  int32_t* slots = reinterpret_cast<hash_slot_t*>(hash_table->mutable_data());
  std::fill(slots, slots + size, kHashSlotEmpty);

  *out = hash_table;
  return Status::OK();
}

}  // namespace internal
}  // namespace arrow
