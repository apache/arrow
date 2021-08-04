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

#include <mutex>
#include <thread>
#include <unordered_map>

#include "arrow/util/thread_pool.h"

namespace arrow {
namespace compute {

class ThreadIndexer {
 public:
  size_t operator()();

  static size_t Capacity();

 private:
  static size_t Check(size_t thread_index);

  std::mutex mutex_;
  std::unordered_map<std::thread::id, size_t> id_to_index_;
};

class AtomicCounter {
 public:
  AtomicCounter() = default;

  int count() const;

  util::optional<int> total() const;

  // return true if the counter is complete
  bool Increment();

  // return true if the counter is complete
  bool SetTotal(int total);

  // return true if the counter has not already been completed
  bool Cancel();

 private:
  // ensure there is only one true return from Increment(), SetTotal(), or Cancel()
  bool DoneOnce();

  std::atomic<int> count_{0}, total_{-1};
  std::atomic<bool> complete_{false};
};

}  // namespace compute
}  // namespace arrow
