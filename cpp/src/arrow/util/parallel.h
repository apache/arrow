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

#pragma once

#include <utility>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace internal {

// A parallelizer that takes a `Status(int)` function and calls it with
// arguments between 0 and `num_tasks - 1`, on an arbitrary number of threads.

template <class FUNCTION>
Status ParallelFor(int num_tasks, FUNCTION&& func) {
  auto pool = internal::GetCpuThreadPool();
  std::vector<Future<Status>> futures(num_tasks);

  for (int i = 0; i < num_tasks; ++i) {
    ARROW_ASSIGN_OR_RAISE(futures[i], pool->Submit(func, i));
  }
  auto st = Status::OK();
  for (auto& fut : futures) {
    st &= fut.status();
  }
  return st;
}

// A parallelizer that takes a `Status(int)` function and calls it with
// arguments between 0 and `num_tasks - 1`, in sequence or in parallel,
// depending on the input boolean.

template <class FUNCTION>
Status OptionalParallelFor(bool use_threads, int num_tasks, FUNCTION&& func) {
  if (use_threads) {
    return ParallelFor(num_tasks, std::forward<FUNCTION>(func));
  } else {
    for (int i = 0; i < num_tasks; ++i) {
      RETURN_NOT_OK(func(i));
    }
    return Status::OK();
  }
}

}  // namespace internal
}  // namespace arrow
