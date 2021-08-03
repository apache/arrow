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

#include "arrow/compute/exec/exec_utils.h"

#include <arrow/util/logging.h>

namespace arrow {
namespace compute {

size_t ThreadIndexer::operator()() {
  auto id = std::this_thread::get_id();

  std::unique_lock<std::mutex> lock(mutex_);
  const auto& id_index = *id_to_index_.emplace(id, id_to_index_.size()).first;

  return Check(id_index.second);
}

size_t ThreadIndexer::Capacity() {
  static size_t max_size = arrow::internal::ThreadPool::DefaultCapacity();
  return max_size;
}

size_t ThreadIndexer::Check(size_t thread_index) {
  DCHECK_LT(thread_index, Capacity())
      << "thread index " << thread_index << " is out of range [0, " << Capacity() << ")";

  return thread_index;
}

int AtomicCounter::count() const { return count_.load(); }

util::optional<int> AtomicCounter::total() const {
  int total = total_.load();
  if (total == -1) return {};
  return total;
}

bool AtomicCounter::Increment() {
  DCHECK_NE(count_.load(), total_.load());
  int count = count_.fetch_add(1) + 1;
  if (count != total_.load()) return false;
  return DoneOnce();
}

// return true if the counter is complete
bool AtomicCounter::SetTotal(int total) {
  total_.store(total);
  if (count_.load() != total) return false;
  return DoneOnce();
}

// return true if the counter has not already been completed
bool AtomicCounter::Cancel() { return DoneOnce(); }

// ensure there is only one true return from Increment(), SetTotal(), or Cancel()
bool AtomicCounter::DoneOnce() {
  bool expected = false;
  return complete_.compare_exchange_strong(expected, true);
}

}  // namespace compute
}  // namespace arrow
