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

#include "arrow/compute/exec/partition_util.h"
#include <mutex>

namespace arrow {
namespace compute {

PartitionLocks::PartitionLocks()
    : num_prtns_(0),
      locks_(nullptr),
      rand_seed_{0, 0, 0, 0, 0, 0, 0, 0},
      rand_engine_(rand_seed_) {}

PartitionLocks::~PartitionLocks() { CleanUp(); }

void PartitionLocks::Init(int num_prtns) {
  num_prtns_ = num_prtns;
  locks_.reset(new PartitionLock[num_prtns]);
  for (int i = 0; i < num_prtns; ++i) {
    locks_[i].lock.store(false);
  }
}

void PartitionLocks::CleanUp() {
  locks_.reset();
  num_prtns_ = 0;
}

std::atomic<bool>* PartitionLocks::lock_ptr(int prtn_id) {
  ARROW_DCHECK(locks_);
  ARROW_DCHECK(prtn_id >= 0 && prtn_id < num_prtns_);
  return &(locks_[prtn_id].lock);
}

int PartitionLocks::random_int(int num_values) {
  return rand_distribution_(rand_engine_) % num_values;
}

bool PartitionLocks::AcquirePartitionLock(int num_prtns_to_try,
                                          const int* prtn_ids_to_try, bool limit_retries,
                                          int max_retries, int* locked_prtn_id,
                                          int* locked_prtn_id_pos) {
  int trial = 0;
  while (!limit_retries || trial <= max_retries) {
    int prtn_id_pos = random_int(num_prtns_to_try);
    int prtn_id = prtn_ids_to_try[prtn_id_pos];

    std::atomic<bool>* lock = lock_ptr(prtn_id);

    bool expected = false;
    if (lock->compare_exchange_weak(expected, true)) {
      *locked_prtn_id = prtn_id;
      *locked_prtn_id_pos = prtn_id_pos;
      return true;
    }

    ++trial;
  }

  *locked_prtn_id = -1;
  *locked_prtn_id_pos = -1;
  return false;
}

void PartitionLocks::ReleasePartitionLock(int prtn_id) {
  ARROW_DCHECK(prtn_id >= 0 && prtn_id < num_prtns_);
  std::atomic<bool>* lock = lock_ptr(prtn_id);
  lock->store(false);
}

}  // namespace compute
}  // namespace arrow
