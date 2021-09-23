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

#ifndef _WIN32
#include <unistd.h>
#endif

#include <atomic>
#include <cstdint>
#include <memory>
#include <queue>
#include <type_traits>
#include <utility>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/cancel.h"
#include "arrow/util/functional.h"
#include "arrow/util/future.h"
#include "arrow/util/macros.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/visibility.h"

#if defined(_MSC_VER)
// Disable harmless warning for decorated name length limit
#pragma warning(disable : 4503)
#endif

namespace arrow {
namespace internal {

class ARROW_EXPORT TaskScheduler : public Executor {
 public:
  static Result<std::shared_ptr<TaskScheduler>> Make(int threads);
  static Result<std::shared_ptr<TaskScheduler>> MakeEternal(int threads);

  ~TaskScheduler() override;

  int GetCapacity() override;

  bool OwnsThisThread() override;

  std::shared_ptr<ThreadPool> pool() { return thread_pool_; }

 protected:
  friend ARROW_EXPORT TaskScheduler* GetCpuTaskScheduler();

  TaskScheduler();

  Status SpawnReal(TaskHints hints, FnOnce<void()> task, StopToken,
                   StopCallback&&) override;

  static std::shared_ptr<TaskScheduler> MakeCpuTaskScheduler();

 private:
  std::shared_ptr<ThreadPool> thread_pool_;
  // std::queue<std::unique_ptr<Task>> task_queue_;
  // std::atomic<int> active_tasks_counter_;
  //
};

ARROW_EXPORT TaskScheduler* GetCpuTaskScheduler();

}  // namespace internal
}  // namespace arrow